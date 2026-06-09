"""Bronze Layer Ingestion Module

This module handles ingestion of raw Parquet files into Bronze Delta tables.
Features:
- Schema normalization (lowercase column names)
- Audit metadata injection (_source_file, _ingestion_timestamp)
- Schema evolution support (mergeSchema)
- Automatic table optimization
- Union of multiple files with missing column handling
- Flexible period filtering (all files, specific years, or specific year-months)
"""

from pyspark.sql import SparkSession, functions as F
from datetime import datetime
from typing import Dict, Optional, List


def ingest_to_bronze(spark: SparkSession, catalog: str, raw_path: str, 
                     bronze_schema: str, taxi_type: str, 
                     years: Optional[List[int]] = None, 
                     months: Optional[List[int]] = None,
                     overwrite: bool = False) -> Dict:
    """Ingest raw Parquet files to Bronze Delta tables with audit metadata.
    
    Filtering logic:
    - years=None, months=None → Process ALL files in raw directory
    - years=[2023], months=None → Process all months of 2023
    - years=[2023], months=[1,2,3] → Process only Jan-Mar 2023
    
    Args:
        spark: Active SparkSession
        catalog: Unity Catalog name
        raw_path: Path to raw data volume
        bronze_schema: Bronze schema name
        taxi_type: Type of taxi (yellow, green, fhv, fhvhv)
        years: List of years to include (e.g., [2023]). None = all years
        months: List of months to include (e.g., [1, 2, 3]). None = all months
        overwrite: Overwrite existing data if True
        
    Returns:
        dict with status, table, records, duration_seconds
    """
    start_time = datetime.now()
    
    # Define paths
    raw_dir = f"{raw_path}/{taxi_type}"
    table_name = f"{catalog}.{bronze_schema}.{taxi_type}_trips"
    
    print(f"\n{'='*60}")
    print(f"Processing: {taxi_type.upper()}")
    print(f"{'='*60}")
    
    try:
        # List all parquet files using os module (compatible with imports)
        import os
        
        if not os.path.exists(raw_dir):
            raise FileNotFoundError(f"Directory does not exist: {raw_dir}")
        
        # List ALL parquet files in directory
        all_files = [f for f in os.listdir(raw_dir) if f.endswith('.parquet')]
        
        if not all_files:
            raise FileNotFoundError(f"No parquet files found in {raw_dir}")
        
        # Filter files based on configured years and months (if specified)
        # Expected filename pattern: {taxi_type}_tripdata_{year}-{month:02d}.parquet
        filenames = []
        
        # If both years and months are None, process ALL files
        if years is None and months is None:
            filenames = all_files
            print(f"Processing ALL files (no period filter)")
        else:
            # Filter based on years and/or months
            for filename in all_files:
                # Extract year-month from filename (e.g., "2023-01" from "yellow_tripdata_2023-01.parquet")
                parts = filename.replace('.parquet', '').split('_')
                if len(parts) >= 3:
                    year_month = parts[-1]  # e.g., "2023-01"
                    try:
                        year, month = year_month.split('-')
                        year = int(year)
                        month = int(month)
                        
                        # Apply filters
                        year_match = (years is None) or (year in years)
                        month_match = (months is None) or (month in months)
                        
                        if year_match and month_match:
                            filenames.append(filename)
                    except ValueError:
                        # Skip files with unexpected naming pattern
                        print(f"  Warning: Skipping file with unexpected pattern: {filename}")
                        continue
        
        if not filenames:
            raise FileNotFoundError(
                f"No parquet files found in {raw_dir} for configured period "
                f"(years={years}, months={months})"
            )
        
        print(f"Found {len(filenames)} parquet file(s) matching configured period")
        if years is not None or months is not None:
            print(f"  Years: {years if years else 'ALL'}, Months: {months if months else 'ALL'}")
        if len(all_files) > len(filenames):
            print(f"  (Skipped {len(all_files) - len(filenames)} files outside configured period)")
        
        # Read and normalize each file
        dfs = []
        for filename in filenames:
            file_path = f"{raw_dir}/{filename}"
            df = spark.read.parquet(file_path)
            
            # Normalize column names to lowercase (compute schema once)
            column_mapping = {c: c.lower() for c in df.columns}
            df = df.select([F.col(c).alias(c.lower()) for c in df.columns])
            
            # Add audit metadata using withColumns (more efficient than chained withColumn)
            df = df.withColumns({
                "_source_file": F.lit(filename),
                "_ingestion_timestamp": F.lit(start_time)
            })
            
            dfs.append(df)
        
        # Union all dataframes (handle schema differences)
        df_bronze = dfs[0]
        for df in dfs[1:]:
            df_bronze = df_bronze.unionByName(df, allowMissingColumns=True)
        
        record_count = df_bronze.count()
        
        # Write as Delta Table
        mode = "overwrite" if overwrite else "append"
        
        df_bronze.write \
            .format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true" if overwrite else "false") \
            .saveAsTable(table_name)
        
        # Optimize table
        spark.sql(f"OPTIMIZE {table_name}")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result = {
            "status": "success",
            "taxi_type": taxi_type,
            "table": table_name,
            "records": record_count,
            "duration_seconds": round(duration, 2)
        }
        
        print(f"Success: {record_count:,} records in {duration:.1f}s")
        print(f"Table: {table_name}")
        
        return result
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"status": "error", "taxi_type": taxi_type, "error": str(e)}


def ingest_all_taxi_types(spark: SparkSession, catalog: str, raw_path: str, 
                          bronze_schema: str, taxi_types: list, 
                          years: Optional[List[int]] = None, 
                          months: Optional[List[int]] = None,
                          overwrite: bool = False) -> list:
    """Ingest all taxi types to Bronze layer.
    
    Args:
        spark: Active SparkSession
        catalog: Unity Catalog name
        raw_path: Path to raw data volume
        bronze_schema: Bronze schema name
        taxi_types: List of taxi types to process
        years: List of years to include. None = all years
        months: List of months to include. None = all months
        overwrite: Overwrite existing data if True
    
    Returns:
        List of result dictionaries
    """
    results = []
    
    for taxi_type in taxi_types:
        result = ingest_to_bronze(spark, catalog, raw_path, bronze_schema, taxi_type, years, months, overwrite)
        results.append(result)
    
    # Final summary
    print(f"\n\n{'='*60}")
    print("BRONZE INGESTION SUMMARY")
    print(f"{'='*60}")
    
    total_records = 0
    for r in results:
        if r["status"] == "success":
            print(f"  {r['taxi_type'].upper():8s}: {r['records']:,} records ({r['duration_seconds']}s)")
            total_records += r['records']
        else:
            print(f"  {r['taxi_type'].upper():8s}: {r['status']}")
    
    print(f"\nTotal: {total_records:,} records")
    
    return results


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from config import CATALOG, RAW_PATH, BRONZE_SCHEMA, TAXI_TYPES, YEARS, MONTHS
    
    spark = SparkSession.builder.appName("nyc-taxi-bronze-ingest").getOrCreate()
    results = ingest_all_taxi_types(spark, CATALOG, RAW_PATH, BRONZE_SCHEMA, TAXI_TYPES, YEARS, MONTHS, overwrite=True)
