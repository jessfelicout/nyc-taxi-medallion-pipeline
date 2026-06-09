# Databricks notebook source
# DBTITLE 1,🛠️ Configurações do Pipeline
# NYC Taxi Pipeline Configuration
# This notebook processes RAW data through Bronze and Silver layers

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

CATALOG        = "nyc_taxi"
RAW_SCHEMA     = "raw"
BRONZE_SCHEMA  = "bronze"
SILVER_SCHEMA  = "silver"
GOLD_SCHEMA    = "gold"
VOLUME         = "files"

YEARS               = [2023]
PIPELINE_START_DATE = "2023-01-01"
MONTHS     = list(range(1, 6))
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

PATHS = {
    "raw":    f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}",
    "bronze": f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME}",
}

print("Configuration loaded")
print(f"  Catalog: {CATALOG}")
print(f"  Raw path: {PATHS['raw']}")

# COMMAND ----------

# DBTITLE 1,Setup  - Catálogo e Schemas
# ============================================================
# COMPLETE SETUP - Unity Catalog and Schemas
# ============================================================

print("\n  Initializing medallion architecture setup...\n")

# 1. Create Catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
print(f"✓ Catalog created: {CATALOG}")

# 2. Create RAW Schema + Volume
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.{VOLUME}")
spark.sql(f"""
    ALTER SCHEMA {CATALOG}.{RAW_SCHEMA}
    SET DBPROPERTIES (
        'layer' = 'raw',
        'description' = 'Raw data layer - original Parquet files'
    )
""")
print(f"Schema RAW: {CATALOG}.{RAW_SCHEMA}")
print(f"Volume: /Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}/")

# 3. Create BRONZE Schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"""
    ALTER SCHEMA {CATALOG}.{BRONZE_SCHEMA}
    SET DBPROPERTIES (
        'layer' = 'bronze',
        'description' = 'Ingestion layer - raw data in Delta format with audit metadata'
    )
""")
print(f"✓ Schema BRONZE: {CATALOG}.{BRONZE_SCHEMA}")

# 4. Create SILVER Schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
spark.sql(f"""
    ALTER SCHEMA {CATALOG}.{SILVER_SCHEMA}
    SET DBPROPERTIES (
        'layer' = 'silver',
        'description' = 'Cleansing layer - validated and standardized data'
    )
""")
print(f"✓ Schema SILVER: {CATALOG}.{SILVER_SCHEMA}")

# 5. Create GOLD Schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
spark.sql(f"""
    ALTER SCHEMA {CATALOG}.{GOLD_SCHEMA}
    SET DBPROPERTIES (
        'layer' = 'gold',
        'description' = 'Aggregation layer - consumption-ready datasets'
    )
""")
print(f"✓ Schema GOLD: {CATALOG}.{GOLD_SCHEMA}")

# 6. Summary of Bronze tables to be created
print(f"\n Bronze tables to be created:")
for taxi_type in TAXI_TYPES:
    print(f"  • {CATALOG}.{BRONZE_SCHEMA}.{taxi_type}_trips")


# COMMAND ----------

# DBTITLE 1,Download Input Data from NYC
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List
import time

DOWNLOAD_TIMEOUT = 300  # 5 minutes per file
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
MAX_WORKERS = 4  # Parallel downloads

def download_parquet(taxi_type: str, year: int, month: int) -> Dict:
    """
    Download raw Parquet file from NYC Open Data with retry and timeout.
    
    Returns:
        dict with status, taxi_type, filename, path, size_mb, attempts
    """
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    dest_dir = f"{PATHS['raw']}/{taxi_type}"
    dest_path = f"{dest_dir}/{filename}"
    
    os.makedirs(dest_dir, exist_ok=True)
    
    # 1. Check if file already exists and is valid (idempotency)
    if os.path.exists(dest_path):
        size_mb = os.path.getsize(dest_path) / 1e6
        if size_mb > 0.1:  # Valid file (> 100KB)
            return {
                "status": "skipped",
                "taxi_type": taxi_type,
                "filename": filename,
                "path": dest_path,
                "size_mb": round(size_mb, 1),
                "attempts": 0
            }
    
    # 2. Download with retry
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=DOWNLOAD_TIMEOUT, stream=True)
            response.raise_for_status()
            
            # Streaming download (for large files)
            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Validate size
            size_mb = os.path.getsize(dest_path) / 1e6
            if size_mb < 0.01:  # < 10KB = corrupted
                raise ValueError(f"The data is too small: {size_mb:.2f} MB")
            
            return {
                "status": "success",
                "taxi_type": taxi_type,
                "filename": filename,
                "path": dest_path,
                "size_mb": round(size_mb, 1),
                "attempts": attempt
            }
            
        except Exception as e:
            if attempt < MAX_RETRIES:
                print(f"  Retry {attempt}/{MAX_RETRIES}: {filename} ({str(e)[:50]})")
                time.sleep(RETRY_DELAY * attempt)  # Exponential backoff
            else:
                return {
                    "status": "error",
                    "taxi_type": taxi_type,
                    "filename": filename,
                    "error": str(e),
                    "attempts": attempt
                }


def download_all_parallel() -> Dict:
    """
    Parallel download of all files with monitoring.
    
    Returns:
        dict with download statistics
    """
    start_time = time.time()
    
    # Create task list
    tasks = [(t, y, m) for t in TAXI_TYPES for y in YEARS for m in MONTHS]
    
    print(f"Starting parallel download: {len(tasks)} files, {MAX_WORKERS} workers")
    
    # Execute downloads in parallel
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_parquet, t, y, m): (t, y, m) for t, y, m in tasks}
        for future in as_completed(futures):
            results.append(future.result())
    
    # Calculate statistics
    duration = time.time() - start_time
    success = sum(1 for r in results if r["status"] == "success")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed = sum(1 for r in results if r["status"] == "error")
    total_mb = sum(r.get("size_mb", 0) for r in results if r["status"] in ["success", "skipped"])
    
    stats = {
        "total_files": len(tasks),
        "downloaded": success,
        "skipped": skipped,
        "failed": failed,
        "total_mb": round(total_mb, 1),
        "duration_seconds": round(duration, 1),
        "results": results
    }
    
    # Summary
    print(f"\nDownload complete: {success} new, {skipped} skipped, {failed} failed")
    print(f"Total: {total_mb:.1f} MB in {duration:.1f}s")
    
    if failed > 0:
        print(f"\nWARNING: {failed} file(s) failed:")
        for r in [r for r in results if r["status"] == "error"]:
            print(f"  {r['filename']}: {r['error'][:60]}")
    
    return stats


if __name__ == "__main__":
    stats = download_all_parallel()

# COMMAND ----------

# DBTITLE 1,📊 BRONZE LAYER - Setup
# MAGIC %md
# MAGIC ## Bronze Layer - Ingestion
# MAGIC
# MAGIC The Bronze layer receives raw data and stores it as **Delta Tables** without significant transformations.
# MAGIC
# MAGIC **Objectives:**
# MAGIC * Full ingestion of raw data
# MAGIC * Add audit metadata (timestamp, source file)
# MAGIC * Preserve original data for traceability
# MAGIC * Enable schema evolution for future changes

# COMMAND ----------

# DBTITLE 1,Função de Ingestão Bronze - Padronizada
from pyspark.sql import functions as F
from datetime import datetime

def ingest_to_bronze(taxi_type: str, overwrite: bool = False) -> dict:
    """
    Ingest raw Parquet files to Bronze Delta tables with audit metadata.
    
    Args:
        taxi_type: yellow, green, fhv, or fhvhv
        overwrite: Overwrite existing data if True
        
    Returns:
        dict with status, table, records, duration_seconds
    """
    start_time = datetime.now()
    
    # Define paths
    raw_path = f"{PATHS['raw']}/{taxi_type}/*.parquet"
    table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{taxi_type}_trips"
    
    print(f"\n{'='*60}")
    print(f"Processing: {taxi_type.upper()}")
    print(f"{'='*60}")
    
    try:
        raw_dir = f"{PATHS['raw']}/{taxi_type}"
        files = [f.path for f in dbutils.fs.ls(raw_dir) if f.name.endswith('.parquet')]
        
        dfs = []
        for file_path in files:
            df = spark.read.parquet(file_path)
            # Normalize to lowercase in batch
            df = df.select([F.col(c).alias(c.lower()) for c in df.columns])
            # Add metadata
            df = df.withColumn("_source_file", F.lit(file_path.split("/")[-1])) \
                   .withColumn("_ingestion_timestamp", F.lit(start_time))
            dfs.append(df)
        
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

print("Function ingest_to_bronze() defined")

# COMMAND ----------

# DBTITLE 1,TESTE: Ingestão Bronze - Todos os Tipos
# Execute ingestion for all taxi types
results = []

for taxi_type in TAXI_TYPES:
    result = ingest_to_bronze(taxi_type, overwrite=True)
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

# COMMAND ----------

taxi_type = "green"
bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{taxi_type}_trips"
df = spark.table(bronze_table)
display(df.limit(10))

# COMMAND ----------

# DBTITLE 1,📊 SILVER LAYER - Setup
# MAGIC %md
# MAGIC ## Silver Layer - Cleansing and Validation
# MAGIC
# MAGIC The Silver layer processes data from Bronze applying:
# MAGIC
# MAGIC **Transformations:**
# MAGIC * Data cleansing (nulls, duplicates, outliers)
# MAGIC * Data type standardization
# MAGIC * Quality validation
# MAGIC * Reference data enrichment
# MAGIC * Timestamp conversion to correct timezone
# MAGIC
# MAGIC **Data Quality:**
# MAGIC * Removal of invalid records
# MAGIC * Business rules enforcement
# MAGIC * Transformation documentation

# COMMAND ----------

# DBTITLE 1,Função de Transformação Silver - Yellow Taxi
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

def transform_to_silver(taxi_type: str) -> dict:
    """
    Transform Bronze to Silver with validation and cleansing.
    
    Applies quality rules: timestamp validation, deduplication,
    distance/fare/passenger validation (yellow/green only).
    
    Args:
        taxi_type: yellow, green, fhv, or fhvhv
    
    Returns:
        dict with status, records_in, records_out, removal_pct, duration_seconds
    """
    start_time = datetime.now()
    
    bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.{taxi_type}_trips"
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.{taxi_type}_trips_clean"
    
    print(f"\n{'='*60}")
    print(f"Transforming: {taxi_type.upper()} (Bronze → Silver)")
    print(f"{'='*60}")
    
    try:
        # Read Bronze data
        df = spark.table(bronze_table)
        initial_count = df.count()
        print(f"Bronze records: {initial_count:,}")
        
        # 1. Data cleansing 
        df_clean = df
        
        # Determine timestamp column names based on taxi type
        if taxi_type == "yellow":
            pickup_col = "tpep_pickup_datetime"
            dropoff_col = "tpep_dropoff_datetime"
        elif taxi_type == "green":
            pickup_col = "lpep_pickup_datetime"
            dropoff_col = "lpep_dropoff_datetime"
        else:  # fhv, fhvhv
            pickup_col = "pickup_datetime"
            dropoff_col = "dropoff_datetime"
        
        # Apply yellow/green specific validations
        if taxi_type in ["yellow", "green"]:
            df_clean = df_clean \
                .filter(F.col("trip_distance") > 0) \
                .filter(F.col("trip_distance") <= 500) \
                .filter((F.col("passenger_count").isNull()) | 
                        ((F.col("passenger_count") >= 0) & (F.col("passenger_count") <= 8))) \
                .filter(F.col("fare_amount") >= 0) \
                .filter(F.col("total_amount") >= 0) \
                .withColumn("payment_type", F.col("payment_type").cast("long"))
        
        # Timestamp validation
        df_clean = df_clean \
            .filter(F.col(pickup_col).isNotNull()) \
            .filter(F.col(dropoff_col).isNotNull()) \
            .filter(F.col(dropoff_col) > F.col(pickup_col))
        
        
        # 2. Add technical enrichments
        df_silver = df_clean \
            .withColumn("trip_duration_minutes",
                       (F.unix_timestamp(dropoff_col) - 
                        F.unix_timestamp(pickup_col)) / 60) \
            .withColumn("pickup_date", F.to_date(pickup_col)) \
            .withColumn("_processed_timestamp", F.lit(start_time))
        
        # 3. Remove duplicates
        if taxi_type in ["yellow", "green"]:
            dedup_cols = [pickup_col, dropoff_col, "pulocationid", "dolocationid", "total_amount"]
        else:
            dedup_cols = [pickup_col, dropoff_col, "pulocationid", "dolocationid"]
        
        df_silver = df_silver.dropDuplicates(dedup_cols)
        
        final_count = df_silver.count()
        removed = initial_count - final_count
        removal_pct = (removed / initial_count * 100) if initial_count > 0 else 0
        
        print(f"\nQuality Statistics:")
        print(f"  Records removed: {removed:,} ({removal_pct:.2f}%)")
        print(f"  Silver records: {final_count:,}")
        
        # 4. Write Silver table
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(silver_table)
        
        # 5. Optimize and add constraints
        spark.sql(f"OPTIMIZE {silver_table}")
        
        # Add comments for documentation
        rules_desc = "Rules applied: valid timestamps, trip duration > 0"
        if taxi_type in ["yellow", "green"]:
            rules_desc += ", distance > 0 and <= 500 miles, fare >= 0, passenger count validated"
        
        spark.sql(f"""
            COMMENT ON TABLE {silver_table} IS 
            '{taxi_type.title()} Taxi trips - clean and validated data. {rules_desc}.'
        """)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result = {
            "status": "success",
            "table": silver_table,
            "records_in": initial_count,
            "records_out": final_count,
            "records_removed": removed,
            "removal_pct": round(removal_pct, 2),
            "duration_seconds": round(duration, 2)
        }
        
        print(f"\nSuccess: {final_count:,} records in {duration:.1f}s")
        print(f"  Table: {silver_table}")
        
        return result
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"status": "error", "taxi_type": taxi_type, "error": str(e)}

print("Function transform_to_silver() defined")

# COMMAND ----------

# DBTITLE 1,TEST: Single Taxi Type Silver Transformation
# Test transformation for a single taxi type first
test_taxi_type = "yellow"

print(f"Testing Silver transformation for {test_taxi_type} taxi...\n")
result = transform_to_silver(test_taxi_type)

if result["status"] == "success":
    print(f"\nTest successful!")
    print(f"Bronze records: {result['records_in']:,}")
    print(f"Silver records: {result['records_out']:,}")
    print(f"Records removed: {result['records_removed']:,} ({result['removal_pct']}%)")
    print(f"Duration: {result['duration_seconds']}s")
    print(f"\nTable created: {result['table']}")
else:
    print(f"\nTest failed: {result.get('error', 'Unknown error')}")

# COMMAND ----------

display(spark.table("nyc_taxi.silver.yellow_trips_clean"))

# COMMAND ----------

# DBTITLE 1,Executar Transformação Silver - Yellow
# Execute Silver transformation for all taxi types
results = []

for taxi_type in TAXI_TYPES:
    result = transform_to_silver(taxi_type)
    results.append(result)

# Final summary
print(f"\n\n{'='*60}")
print("SILVER TRANSFORMATION SUMMARY")
print(f"{'='*60}")

total_in = 0
total_out = 0

for r in results:
    if r["status"] == "success":
        print(f"{r['table'].split('.')[-1]:25s}: {r['records_in']:10,} → {r['records_out']:10,} ({r['removal_pct']:5.2f}% removed)")
        total_in += r['records_in']
        total_out += r['records_out']
    else:
        print(f"{r['taxi_type']:25s}: ERROR - {r.get('error', 'Unknown')[:50]}")

if total_in > 0:
    overall_retention = (total_out / total_in * 100)
    print(f"\n{'='*60}")
    print(f"Total: {total_in:,} → {total_out:,} ({overall_retention:.2f}% retention)")

# COMMAND ----------

# DBTITLE 1,📊 GOLD LAYER - Consumption Strategy
# MAGIC %md
# MAGIC ## Gold Layer - Consumption-Ready Data
# MAGIC
# MAGIC The Gold layer provides **business-ready datasets** optimized for analytics, dashboards, and ML.
# MAGIC
# MAGIC ### Design Principles:
# MAGIC
# MAGIC #### 1. **Minimal Column Set**
# MAGIC * Only business-essential columns (avoid unnecessary data sprawl)
# MAGIC * Focus on **your** consumption requirements
# MAGIC * Reduces query complexity and improves performance
# MAGIC
# MAGIC #### 2. **Standardized Naming**
# MAGIC * `pickup_datetime` / `dropoff_datetime` (not `tpep_*` or `lpep_*`)
# MAGIC * Consistent across taxi types for unified queries
# MAGIC * Self-documenting column names
# MAGIC
# MAGIC #### 3. **Business Metrics**
# MAGIC Derived columns moved from Silver to Gold:
# MAGIC * `pickup_hour`, `pickup_day_of_week`, `is_weekend` - Temporal analysis
# MAGIC * `avg_speed_mph` - Performance metrics
# MAGIC * Location IDs standardized for geographic analysis
# MAGIC
# MAGIC #### 4. **Query Optimization**
# MAGIC * **Partitioned by** `year_month` - Monthly partitions for efficient temporal queries
# MAGIC * **Z-Ordered by** location IDs - Geographic queries optimized
# MAGIC * Delta Lake optimizations applied automatically
# MAGIC
# MAGIC #### 5. **Governance**
# MAGIC * Table and column comments for documentation
# MAGIC * Audit trail from Silver (`silver_processed_at`, `gold_created_at`)
# MAGIC * Clear data lineage: Bronze → Silver → Gold
# MAGIC
# MAGIC ### Required Consumption Columns:
# MAGIC
# MAGIC ✅ `vendor_id` - Taxi vendor identifier  
# MAGIC ✅ `passenger_count` - Number of passengers  
# MAGIC ✅ `total_amount` - Total trip cost  
# MAGIC ✅ `pickup_datetime` - Trip start (standardized)  
# MAGIC ✅ `dropoff_datetime` - Trip end (standardized)  
# MAGIC
# MAGIC ### Gold Enrichments:
# MAGIC
# MAGIC ➕ `pickup_hour`, `pickup_day_of_week`, `is_weekend` - Temporal dimensions  
# MAGIC ➕ `avg_speed_mph` - Speed metric (yellow/green)  
# MAGIC ➕ `trip_duration_minutes` - From Silver validation  
# MAGIC ➕ `pickup_location_id`, `dropoff_location_id` - Geographic analysis  
# MAGIC
# MAGIC ### Scope:
# MAGIC
# MAGIC * **Yellow & Green taxis**: Full support (all required columns present)
# MAGIC * **FHV/FHVHV**: Not included (missing `vendor_id`, `passenger_count`, `total_amount`)

# COMMAND ----------

# DBTITLE 1,Gold Layer - Consumption-Ready Data
from pyspark.sql import functions as F
from datetime import datetime

def create_gold_table(taxi_type: str) -> dict:
    """
    Create Gold consumption table with standardized columns and business metrics.
    
    Selects essential columns, standardizes naming,
    adds temporal dimensions, and optimizes for analytics (partitioned, z-ordered).
    
    Args:
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
            "message": "Gold consumption table only supports yellow/green (FHV/FHVHV lack required columns)"
        }
    
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.{taxi_type}_trips_clean"
    gold_table = f"{CATALOG}.{GOLD_SCHEMA}.{taxi_type}_trips_gold"
    
    print(f"\n{'='*60}")
    print(f"Creating Gold Consumption Table: {taxi_type.upper()}")
    print(f"{'='*60}")
    
    try:
        # Read Silver data
        df = spark.table(silver_table)
        
        # Determine source column names based on taxi type
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
            .withColumn("year_month", F.date_format("pickup_datetime", "yyyy-MM")) \
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
        
        record_count = df_gold.count()
        
        # Write Gold table with optimization
        df_gold.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("year_month") \
            .saveAsTable(gold_table)
        
        # Optimize for query performance
        print("\nOptimizing table for analytics...")
        spark.sql(f"OPTIMIZE {gold_table} ZORDER BY (pickup_location_id, dropoff_location_id)")
        
        # Add table documentation
        spark.sql(f"""
            COMMENT ON TABLE {gold_table} IS 
            'Gold consumption table for {taxi_type} taxi trips. 
            Contains only essential business columns with standardized naming. 
            Optimized for analytics with temporal dimensions and performance metrics. 
            Partitioned by year_month (monthly partitions), z-ordered by location IDs.'
        """)
        
        # Add column comments for documentation
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
        
        print(f"\nSuccess: {record_count:,} records in {duration:.1f}s")
        print(f"  Table: {gold_table}")
        print(f"  Partitioned by: year_month (monthly)")
        print(f"  Z-Ordered by: pickup_location_id, dropoff_location_id")
        
        return result
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"status": "error", "taxi_type": taxi_type, "error": str(e)}

print("Function create_gold_table() defined")

# COMMAND ----------

# DBTITLE 1,Create Gold Tables
# Create Gold tables for Yellow and Green taxis

results = []

for taxi_type in ["yellow", "green"]:
    result = create_gold_table(taxi_type)
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
print(f"\nGold layer ready for consumption!")

# COMMAND ----------

# DBTITLE 1,Gold Layer - Data Validation
# Validate Gold layer data quality and schema

taxi_type_to_validate = "yellow"  # Change to "green" to validate green taxi
gold_table = f"{CATALOG}.{GOLD_SCHEMA}.{taxi_type_to_validate}_trips_gold"

print(f"\nGOLD LAYER VALIDATION - {taxi_type_to_validate.upper()}")
print("="*60)

try:
    df_gold = spark.table(gold_table)
    
    # 1. Schema validation - Check required columns
    print("\n1. Schema Validation:")
    required_cols = [
        "vendor_id", "passenger_count", "total_amount",
        "pickup_datetime", "dropoff_datetime"
    ]
    
    missing_cols = [col for col in required_cols if col not in df_gold.columns]
    
    if missing_cols:
        print(f"   Missing required columns: {missing_cols}")
    else:
        print(f"   All required consumption columns present")
    
    print(f"   Total columns: {len(df_gold.columns)}")
    print(f"   Column list: {', '.join(df_gold.columns[:10])}...")
    
    # 2. Data quality checks
    print("\n2. Data Quality:")
    total_records = df_gold.count()
    print(f"   Total records: {total_records:,}")
    
    # Check for nulls in critical columns
    null_checks = df_gold.select([
        (F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / total_records * 100).alias(f"{c}_null_pct")
        for c in required_cols
    ])
    
    print("\n   Null % in required columns:")
    null_checks.display()
    
    # 3. Business metrics summary
    print("\n3. Business Metrics:")
    df_gold.select(
        F.count("*").alias("total_trips"),
        F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph")
    ).display()
    
    # 4. Temporal distribution
    print("\n4. Temporal Distribution:")
    df_gold.groupBy("pickup_date") \
        .agg(F.count("*").alias("trips")) \
        .orderBy("pickup_date") \
        .display()
    
    # 5. Weekend vs Weekday
    print("\n5. Weekend vs Weekday Analysis:")
    df_gold.groupBy("is_weekend") \
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("total_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration")
        ) \
        .display()
    
    # 6. Sample data preview
    print("\n6. Sample Data (first 10 rows):")
    df_gold.select(
        "vendor_id", "passenger_count", "total_amount",
        "pickup_datetime", "pickup_hour", "is_weekend",
        "trip_duration_minutes", "avg_speed_mph"
    ).limit(10).display()
    
    print("\nGold layer validation completed successfully!")
    
except Exception as e:
    print(f"\nValidation failed: {str(e)}")
    print("\nMake sure to execute the Gold creation cell first.")

# COMMAND ----------

# DBTITLE 1,Validação de Qualidade - Análise Silver
# Quality analysis of Silver data (configurable taxi type)
taxi_type_to_analyze = "yellow"  # Change this to analyze different taxi types
silver_table = f"{CATALOG}.{SILVER_SCHEMA}.{taxi_type_to_analyze}_trips_clean"

print(f"\nQUALITY ANALYSIS - SILVER LAYER ({taxi_type_to_analyze.upper()})")
print("="*60)

# Estatísticas descritivas
df_silver = spark.table(silver_table)

print("\n1. Trip Statistics:")

# Generic stats for all taxi types
stats_cols = [
    F.count("*").alias("total_trips"),
    F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min")
]

# Add fare stats only for yellow/green (they have fare columns)
if taxi_type_to_analyze in ["yellow", "green"]:
    stats_cols.extend([
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount")
    ])

df_silver.select(stats_cols).display()

print("\n2. Distribution by Period:")
df_silver.groupBy("pickup_date") \
    .agg(F.count("*").alias("trips")) \
    .orderBy("pickup_date") \
    .display()

print("\n3. Data Completeness (% non-null):")
total = df_silver.count()

# Select columns that exist in the dataframe
available_cols = df_silver.columns
check_cols = []

# Common columns to check across all taxi types
# Note: pickup_hour, pickup_day_of_week, is_weekend moved to Gold layer
for col_name in ["trip_duration_minutes", "pickup_date"]:
    if col_name in available_cols:
        check_cols.append(col_name)

# Yellow/Green specific columns
if taxi_type_to_analyze in ["yellow", "green"]:
    for col_name in ["passenger_count", "ratecodeid", "payment_type", "fare_amount"]:
        if col_name in available_cols:
            check_cols.append(col_name)

if check_cols:
    completeness = df_silver.select([
        (F.count(F.when(F.col(c).isNotNull(), c)) / total * 100).alias(c)
        for c in check_cols
    ])
    completeness.display()
else:
    print("No applicable columns found for completeness check")

print("\nQuality analysis completed")

# COMMAND ----------

# DBTITLE 1,🛡️ Governança e Boas Práticas
# MAGIC %md
# MAGIC ## Data Governance and Best Practices
# MAGIC
# MAGIC ### 1. **Medallion Architecture**
# MAGIC * **Raw (Volume)**: Original data in Parquet, immutable
# MAGIC * **Bronze (Delta)**: Ingestion with audit metadata (`_source_file`, `_ingestion_timestamp`)
# MAGIC * **Silver (Delta)**: Clean, validated and enriched data with derived columns
# MAGIC * **Gold (future)**: Aggregated datasets for consumption (dashboards, ML)
# MAGIC
# MAGIC ### 2. **Data Quality**
# MAGIC * Business rule validation (valid distances, positive fares)
# MAGIC * Duplicate removal based on business keys
# MAGIC * Tracked quality metrics (retention rate)
# MAGIC
# MAGIC ### 3. **Traceability**
# MAGIC * Processing timestamps in each layer
# MAGIC * Source file tracking in Bronze
# MAGIC * Schema evolution enabled
# MAGIC
# MAGIC ### 4. **Performance**
# MAGIC * Delta Lake usage for ACID transactions
# MAGIC * Automatic table optimization (`OPTIMIZE`)
# MAGIC * Schema caching to avoid unnecessary RPCs
# MAGIC
# MAGIC ### 5. **Documentation**
# MAGIC * Comments on schemas and tables
# MAGIC * DBPROPERTIES for governance metadata
# MAGIC * Functions documented with docstrings

# COMMAND ----------

# DBTITLE 1,Comandos de Governança - Catalogo e Metadados
# Useful commands for governance and monitoring

print("NYC Taxi Catalog Summary\n")

# 1. List all schemas
print("1. Available schemas:")
spark.sql(f"SHOW SCHEMAS IN {CATALOG}").display()

# 2. List tables by schema
print("\n2. Bronze tables:")
spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}").display()

print("\n3. Silver tables:")
spark.sql(f"SHOW TABLES IN {CATALOG}.{SILVER_SCHEMA}").display()

# 3. Schema properties
print("\n4. Bronze Schema properties:")
spark.sql(f"DESCRIBE SCHEMA EXTENDED {CATALOG}.{BRONZE_SCHEMA}").display()

# COMMAND ----------

# DBTITLE 1,Estatísticas e Histórico das Tabelas
# View history and statistics of Delta tables

print("Version History - Bronze Yellow\n")

# Version history (Delta Time Travel)
bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.yellow_trips"

try:
    spark.sql(f"DESCRIBE HISTORY {bronze_table}").select(
        "version", "timestamp", "operation", "operationMetrics"
    ).display()
except:
    print("Table does not exist yet. Execute ingestion cells first.")

print("\nSilver Yellow Table Statistics\n")

silver_table = f"{CATALOG}.{SILVER_SCHEMA}.yellow_trips_clean"

try:
    # Table details
    spark.sql(f"DESCRIBE DETAIL {silver_table}").display()
except:
    print("Table does not exist yet. Execute Silver transformations first.")

# COMMAND ----------

# DBTITLE 1,Próximos Passos e Recomendações
# MAGIC %md
# MAGIC ## Next Steps and Recommendations
# MAGIC
# MAGIC ### Completed
# MAGIC * Raw data download with parallelization and retry logic
# MAGIC * Bronze layer: Full ingestion with audit metadata for all taxi types
# MAGIC * Silver layer: Data validation and cleansing for all taxi types
# MAGIC * Gold layer: Consumption-ready tables for Yellow and Green taxis
# MAGIC
# MAGIC ### 1. Pipeline Automation
# MAGIC * Schedule daily/monthly jobs for incremental ingestion
# MAGIC * Implement CDC (Change Data Capture) for real-time updates
# MAGIC * Add data quality alerts and monitoring dashboards
# MAGIC
# MAGIC ### 2. Gold Layer Extensions
# MAGIC * Create aggregated views (daily/hourly summaries)
# MAGIC * Build location-based analytics tables
# MAGIC * Add revenue and performance metric tables
# MAGIC * Consider Gold tables for FHV/FHVHV with their specific schemas
# MAGIC
# MAGIC ### 3. Advanced Optimizations
# MAGIC * Enable Liquid Clustering for adaptive optimization
# MAGIC * Schedule VACUUM operations for storage cleanup
# MAGIC * Implement incremental processing (process only new data)
# MAGIC * Add data quality rules and expectations
# MAGIC
# MAGIC ### 4. Monitoring and Observability
# MAGIC * Track pipeline latency (RAW to Bronze to Silver to Gold)
# MAGIC * Monitor data quality metrics (null rates, duplicates, outliers)
# MAGIC * Set up failure alerts and SLA tracking
# MAGIC
# MAGIC ### 5. Integrations
# MAGIC * Connect to BI tools (Tableau, Power BI, Looker)
# MAGIC * Expose Gold tables via SQL Warehouse for analysts
# MAGIC * Create REST APIs for application consumption
# MAGIC * Build ML models using Gold layer data