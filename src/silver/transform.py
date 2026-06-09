"""Silver Layer Transformation Module

This module transforms Bronze data into clean, validated Silver tables.
Features:
- Data quality validation (timestamps, distances, fares, passenger counts)
- Date range filtering using PIPELINE_START_DATE/END_DATE from config
- Duplicate removal based on business keys
- Derived metrics (trip_duration_minutes, pickup_date)
- Liquid clustering by pickup_date for optimal query performance
- Type-specific validations (yellow/green vs FHV/FHVHV)
"""

from pyspark.sql import SparkSession, functions as F
from datetime import datetime
from typing import Dict

# Import date range configuration
try:
    from config import PIPELINE_START_DATE, PIPELINE_END_DATE
except ImportError:
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config import PIPELINE_START_DATE, PIPELINE_END_DATE



def transform_to_silver(spark: SparkSession, catalog: str, bronze_schema: str, 
                       silver_schema: str, taxi_type: str) -> Dict:
    """Transform Bronze to Silver with validation and cleansing.
    
    Applies quality rules and liquid clustering for optimal performance.
    
    Args:
        spark: Active SparkSession
        catalog: Unity Catalog name
        bronze_schema: Bronze schema name
        silver_schema: Silver schema name
        taxi_type: Type of taxi (yellow, green, fhv, fhvhv)
    
    Returns:
        dict with status, records_out, duration_seconds
    """
    start_time = datetime.now()
    
    
    bronze_table = f"{catalog}.{bronze_schema}.{taxi_type}_trips"
    silver_table = f"{catalog}.{silver_schema}.{taxi_type}_trips_clean"
    
    print(f"\n{'='*60}")
    print(f"Transforming: {taxi_type.upper()} (Bronze to Silver)")
    print(f"{'='*60}")
    
    try:
        # Read Bronze data
        df = spark.table(bronze_table)
        
        # Determine timestamp column names
        if taxi_type == "yellow":
            pickup_col = "tpep_pickup_datetime"
            dropoff_col = "tpep_dropoff_datetime"
        elif taxi_type == "green":
            pickup_col = "lpep_pickup_datetime"
            dropoff_col = "lpep_dropoff_datetime"
        else:  # fhv, fhvhv
            pickup_col = "pickup_datetime"
            dropoff_col = "dropoff_datetime"
        
        # 1. Apply filters (timestamp validation + date range filter)
        df_clean = df \
            .filter(F.col(pickup_col).isNotNull()) \
            .filter(F.col(dropoff_col).isNotNull()) \
            .filter(F.col(dropoff_col) > F.col(pickup_col)) \
            .filter(F.col(pickup_col) >= F.lit(PIPELINE_START_DATE)) \
            .filter(F.col(pickup_col) <= F.lit(PIPELINE_END_DATE)) \
        
        # 2. Yellow/green specific validations
        if taxi_type in ["yellow", "green"]:
            df_clean = df_clean \
                .filter(F.col("trip_distance") > 0) \
                .filter(F.col("trip_distance") <= 500) \
                .filter(F.col("passenger_count").isNotNull()) \
                .filter((F.col("passenger_count") > 0) & (F.col("passenger_count") <= 6)) \
                .filter(F.col("fare_amount") >= 0) \
                .filter(F.col("total_amount") >= 0) \
                .withColumn("payment_type", F.col("payment_type").cast("long"))
        
        # 3. Add enrichments
        df_silver = df_clean \
            .withColumn("trip_duration_minutes",
                       (F.unix_timestamp(dropoff_col) - F.unix_timestamp(pickup_col)) / 60) \
            .withColumn("pickup_date", F.to_date(pickup_col)) \
            .withColumn("_processed_timestamp", F.lit(start_time))
        
        # 4. Remove duplicates
        if taxi_type in ["yellow", "green"]:
            dedup_cols = [pickup_col, dropoff_col, "pulocationid", "dolocationid", "total_amount"]
        else:
            dedup_cols = [pickup_col, dropoff_col, "pulocationid", "dolocationid"]
        
        df_silver = df_silver.dropDuplicates(dedup_cols)
        
        print("\n  Writing Silver table with Liquid Clustering...")
        
        # 5. Write with LIQUID CLUSTERING
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .clusterBy("pickup_date") \
            .saveAsTable(silver_table)
        
        # 6. Get record counts from Delta history
        history = spark.sql(f"DESCRIBE HISTORY {silver_table} LIMIT 1").collect()
        operation_metrics = history[0]['operationMetrics']
        final_count = int(operation_metrics.get('numOutputRows', 0))
        
        print(f"  Silver records: {final_count:,}")
        
        # 7. Add table documentation
        rules_desc = f"Rules: valid timestamps ({PIPELINE_START_DATE} to {PIPELINE_END_DATE})"
        if taxi_type in ["yellow", "green"]:
            rules_desc += ", distance/fare/passenger validated"
        
        spark.sql(f"""
            COMMENT ON TABLE {silver_table} IS 
            '{taxi_type.title()} trips - clean data. {rules_desc}. Liquid clustered by pickup_date.'
        """)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result = {
            "status": "success",
            "table": silver_table,
            "records_out": final_count,
            "duration_seconds": round(duration, 2)
        }
        
        print(f"\n✓ Success: {final_count:,} records in {duration:.1f}s")
        print(f"  Table: {silver_table}")
        print(f"  Clustering: pickup_date (Liquid)")
        print(f"  Throughput: {int(final_count / duration):,} records/sec")
        
        return result
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"status": "error", "taxi_type": taxi_type, "error": str(e)}


def transform_all_taxi_types(spark: SparkSession, catalog: str, bronze_schema: str,
                            silver_schema: str, taxi_types: list) -> list:
    """Transform all taxi types from Bronze to Silver.
    
    Args:
        spark: Active SparkSession
        catalog: Unity Catalog name
        bronze_schema: Bronze schema name
        silver_schema: Silver schema name
        taxi_types: List of taxi types to process
        valid_years: List of valid years to filter (default: YEARS from config.py)
    
    Returns:
        List of result dictionaries
    """
    results = []
    
    for taxi_type in taxi_types:
        result = transform_to_silver(spark, catalog, bronze_schema, silver_schema, 
                                    taxi_type)
        results.append(result)
    
    # Summary
    print(f"\n\n{'='*60}")
    print("SILVER TRANSFORMATION SUMMARY")
    print(f"{'='*60}")
    
    total_out = 0
    total_duration = 0
    
    for r in results:
        if r["status"] == "success":
            table_name = r['table'].split('.')[-1]
            rate = int(r['records_out'] / r['duration_seconds']) if r['duration_seconds'] > 0 else 0
            print(f"{table_name:30s}: {r['records_out']:12,} in {r['duration_seconds']:6.1f}s ({rate:,} rec/s)")
            total_out += r['records_out']
            total_duration += r['duration_seconds']
        else:
            print(f"{r['taxi_type']:30s}: ERROR - {r.get('error', 'Unknown')[:50]}")
    
    if total_out > 0:
        overall_rate = int(total_out / total_duration) if total_duration > 0 else 0
        print(f"\n{'='*60}")
        print(f"Total: {total_out:,} records in {total_duration:.1f}s ({overall_rate:,} rec/s)")
        print(f"✓ Liquid clustering enabled for optimal query performance")
    
    return results


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from config import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, TAXI_TYPES
    
    spark = SparkSession.builder.appName("nyc-taxi-silver-transform").getOrCreate()
    results = transform_all_taxi_types(spark, CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, TAXI_TYPES)
