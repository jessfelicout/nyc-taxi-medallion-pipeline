"""Gold Layer Table Creation Module

This module creates consumption-ready Gold tables optimized for analytics.
Features:
- Column standardization (unified naming across taxi types)
- Temporal dimension enrichment (hour, day of week, is_weekend)
- Business metrics (avg_speed_mph)
- Comprehensive table and column documentation

Note: Only supports yellow and green taxis (FHV/FHVHV lack required columns)
"""

from pyspark.sql import SparkSession, functions as F
from datetime import datetime
from typing import Dict


def create_gold_table(spark: SparkSession, catalog: str, silver_schema: str,
                     gold_schema: str, taxi_type: str) -> Dict:
    """Create Gold consumption table with liquid clustering.
    
    Selects essential columns, standardizes naming,
    adds temporal dimensions, and optimizes for analytics.
    
    Args:
        spark: Active SparkSession
        catalog: Unity Catalog name
        silver_schema: Silver schema name
        gold_schema: Gold schema name
        taxi_type: yellow or green (FHV/FHVHV lack required columns)
    
    Returns:
        dict with status, taxi_type, table, records, duration_seconds
    """
    start_time = datetime.now()
    
    # Validate taxi type
    if taxi_type not in ["yellow", "green"]:
        return {
            "status": "skipped",
            "taxi_type": taxi_type,
            "message": "Gold table only supports yellow/green (FHV/FHVHV lack required columns)"
        }
    
    silver_table = f"{catalog}.{silver_schema}.{taxi_type}_trips_clean"
    gold_table = f"{catalog}.{gold_schema}.{taxi_type}_trips_gold"
    
    print(f"\n{'='*60}")
    print(f"Creating Gold Table: {taxi_type.upper()}")
    print(f"{'='*60}")
    
    try:
        # Read Silver data
        df = spark.table(silver_table)
        
        # Determine source column names
        if taxi_type == "yellow":
            pickup_col = "tpep_pickup_datetime"
            dropoff_col = "tpep_dropoff_datetime"
        else:  # green
            pickup_col = "lpep_pickup_datetime"
            dropoff_col = "lpep_dropoff_datetime"
        
        # Select and standardize columns
        df_gold = df.select(
            F.col("vendorid").alias("vendor_id"),
            F.col("passenger_count"),
            F.col("total_amount"),
            F.col(pickup_col).alias("pickup_datetime"),
            F.col(dropoff_col).alias("dropoff_datetime"),
            F.col("trip_duration_minutes"),
            F.col("trip_distance"),
            F.col("pickup_date"),
            F.col("pulocationid").alias("pickup_location_id"),
            F.col("dolocationid").alias("dropoff_location_id"),
            F.col("_processed_timestamp").alias("silver_processed_at")
        )
        
        # Add temporal dimensions and metrics
        df_gold = df_gold \
            .withColumn("year", F.year("pickup_datetime")) \
            .withColumn("month", F.month("pickup_datetime")) \
            .withColumn("pickup_hour", F.hour("pickup_datetime")) \
            .withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime")) \
            .withColumn("is_weekend", 
                       F.when(F.col("pickup_day_of_week").isin([1, 7]), True)
                       .otherwise(False)) \
            .withColumn("avg_speed_mph",
                       F.when(F.col("trip_duration_minutes") > 0,
                              F.col("trip_distance") / (F.col("trip_duration_minutes") / 60))
                       .otherwise(None)) \
            .withColumn("gold_created_at", F.lit(start_time))
        
        print("\n  Writing Gold table with Liquid Clustering...")
        
        # Write Gold table with LIQUID CLUSTERING (consistent with Silver layer)
        df_gold.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .clusterBy("pickup_date") \
            .saveAsTable(gold_table)
        
        # Get record counts from Delta history
        history = spark.sql(f"DESCRIBE HISTORY {gold_table} LIMIT 1").collect()
        operation_metrics = history[0]['operationMetrics']
        record_count = int(operation_metrics.get('numOutputRows', 0))
        
        print(f"  Gold records: {record_count:,}")
        
        # Add table documentation
        spark.sql(f"""
            COMMENT ON TABLE {gold_table} IS 
            'Gold consumption table for {taxi_type} taxi trips. 
            Essential business columns with standardized naming. 
            Optimized for analytics with temporal dimensions and performance metrics. 
            Liquid clustered by pickup_date (consistent with Silver layer).'
        """)
        
        # Add column comments
        spark.sql(f"ALTER TABLE {gold_table} ALTER COLUMN vendor_id COMMENT 'Taxi vendor identifier (standardized)'")
        spark.sql(f"ALTER TABLE {gold_table} ALTER COLUMN pickup_datetime COMMENT 'Trip start time (standardized from tpep_/lpep_)'")
        spark.sql(f"ALTER TABLE {gold_table} ALTER COLUMN dropoff_datetime COMMENT 'Trip end time (standardized from tpep_/lpep_)'")
        spark.sql(f"ALTER TABLE {gold_table} ALTER COLUMN avg_speed_mph COMMENT 'Average trip speed in miles per hour (Gold enrichment)'")
        spark.sql(f"ALTER TABLE {gold_table} ALTER COLUMN is_weekend COMMENT 'Whether trip occurred on weekend (Saturday/Sunday)'")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result = {
            "status": "success",
            "taxi_type": taxi_type,
            "table": gold_table,
            "records": record_count,
            "duration_seconds": round(duration, 2)
        }
        
        print(f"\n✓ Success: {record_count:,} records in {duration:.1f}s")
        print(f"  Table: {gold_table}")
        print(f"  Clustering: pickup_date (Liquid)")
        
        return result
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"status": "error", "taxi_type": taxi_type, "error": str(e)}


def create_all_gold_tables(spark: SparkSession, catalog: str, silver_schema: str,
                          gold_schema: str, taxi_types: list = ["yellow", "green"]) -> list:
    """Create Gold tables for yellow and green taxis.
    
    Args:
        spark: Active SparkSession
        catalog: Unity Catalog name
        silver_schema: Silver schema name
        gold_schema: Gold schema name
        taxi_types: List of taxi types (default: yellow and green only)
    
    Returns:
        List of result dictionaries
    """
    results = []
    
    for taxi_type in taxi_types:
        result = create_gold_table(spark, catalog, silver_schema, gold_schema, taxi_type)
        results.append(result)
    
    # Summary
    print(f"\n\n{'='*60}")
    print("GOLD LAYER CREATION SUMMARY")
    print(f"{'='*60}")
    
    total_records = 0
    for r in results:
        if r["status"] == "success":
            print(f"  {r['taxi_type'].upper():8s}: {r['records']:,} records ({r['duration_seconds']}s)")
            print(f"   Table: {r['table']}")
            total_records += r['records']
        elif r["status"] == "skipped":
            print(f"  {r['taxi_type'].upper():8s}: SKIPPED - {r['message']}")
        else:
            print(f"  {r['taxi_type'].upper():8s}: ERROR - {r.get('error', 'Unknown')}")
    
    print(f"\nTotal Gold records: {total_records:,}")
    print(f"✓ Liquid clustering enabled (pickup_date) - consistent across layers")
    
    return results


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from config import CATALOG, SILVER_SCHEMA, GOLD_SCHEMA
    
    spark = SparkSession.builder.appName("nyc-taxi-gold-create").getOrCreate()
    results = create_all_gold_tables(spark, CATALOG, SILVER_SCHEMA, GOLD_SCHEMA)
