# Databricks notebook source
# DBTITLE 1,Setup - Imports e Configuração
# =============================================================================
# NYC Taxi Medallion Architecture Pipeline - Test Notebook
# =============================================================================
# This notebook orchestrates and tests the complete end-to-end pipeline:
# RAW (Volume) → BRONZE (Delta) → SILVER (Delta) → GOLD (Delta)
#
# Project Structure:
#   src/config.py          - Pipeline configuration
#   src/setup.py           - Unity Catalog setup
#   src/bronze/download.py - Raw data download
#   src/bronze/ingest.py   - Bronze ingestion
#   src/silver/transform.py - Silver transformation
#   src/gold/create_tables.py - Gold table creation
# =============================================================================

# Enable autoreload - automatically reload modules when they change
%load_ext autoreload
%autoreload 2

import sys
sys.path.append("/Workspace/Users/jj4evers2s2@gmail.com/nyc-taxi-medallion-pipeline/src")

# Import pipeline modules
from config import (
    BASE_URL, CATALOG, RAW_SCHEMA, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA,
    VOLUME, YEARS, MONTHS, TAXI_TYPES, RAW_PATH, print_config
)
from setup import setup_catalog
from bronze.download import download_all_parallel
from bronze.ingest import ingest_to_bronze, ingest_all_taxi_types
from silver.transform import transform_to_silver, transform_all_taxi_types
from gold.create_tables import create_gold_table, create_all_gold_tables

print("Pipeline modules imported successfully\n")
print_config()

# COMMAND ----------

# DBTITLE 1,Unity Catalog Setup
# =============================================================================
# STEP 1: Unity Catalog Setup
# =============================================================================
# Creates the complete medallion architecture in Unity Catalog:
# - Catalog: nyc_taxi
# - Schemas: raw, bronze, silver, gold (with governance metadata)
# - Volume: raw/files (for Parquet storage)
# =============================================================================

setup_catalog(spark)

print("\n✓ Unity Catalog setup completed successfully!")

# COMMAND ----------

# DBTITLE 1,Download Raw Data
# =============================================================================
# STEP 2: Download Raw Data from NYC Open Data
# =============================================================================
# Downloads Parquet files from NYC TLC for all configured taxi types,
# years, and months. Features:
# - Parallel downloads (4 workers)
# - Retry logic with exponential backoff (3 retries)
# - Idempotency (skips existing valid files)
# - Comprehensive error handling and reporting
# =============================================================================

stats = download_all_parallel(BASE_URL, RAW_PATH, TAXI_TYPES, YEARS, MONTHS)

print(f"\n✓ Download completed: {stats['downloaded']} new, {stats['skipped']} skipped, {stats['failed']} failed")
print(f"  Total data: {stats['total_mb']:.1f} MB")

# COMMAND ----------

# DBTITLE 1,Bronze Layer - Ingest Raw Data
# =============================================================================
# STEP 3: Bronze Layer Ingestion
# =============================================================================
# Ingests raw Parquet files into Bronze Delta tables for all taxi types.
# Features:
# - Schema normalization (lowercase columns)
# - Audit metadata (_source_file, _ingestion_timestamp)
# - Schema evolution support (mergeSchema)
# - Automatic optimization (OPTIMIZE)
# - Union of files with missing column handling
# =============================================================================

bronze_results = ingest_all_taxi_types(
    spark=spark,
    catalog=CATALOG,
    raw_path=RAW_PATH,
    bronze_schema=BRONZE_SCHEMA,
    taxi_types=TAXI_TYPES,
    years=YEARS,      # Only process configured years
    months=MONTHS,    # Only process configured months (respects config.py)
    overwrite=True    # Set False for incremental ingestion
)

print("\n✓ Bronze ingestion completed for all taxi types!")

# COMMAND ----------

# DBTITLE 1,Silver Layer - Transform and Cleanse
# =============================================================================
# STEP 4: Silver Layer Transformation
# =============================================================================
# Transforms Bronze data into clean, validated Silver tables for all taxi types.
# Quality rules applied:
# - Timestamp validation (non-null, pickup before dropoff)
# - Temporal outlier removal (volumetric filter for corrupted years)
# - Distance validation (0-500 miles for yellow/green)
# - Fare validation (non-negative for yellow/green)
# - Passenger count validation (1-6 for yellow/green)
# - Duplicate removal based on business keys
# - Derived metrics (trip_duration_minutes, pickup_date)
# =============================================================================

silver_results = transform_all_taxi_types(
    spark=spark,
    catalog=CATALOG,
    bronze_schema=BRONZE_SCHEMA,
    silver_schema=SILVER_SCHEMA,
    taxi_types=TAXI_TYPES
)

print("\nSilver transformation completed for all taxi types!")

# COMMAND ----------

# DBTITLE 1,Gold Layer - Consumption Tables
# =============================================================================
# STEP 5: Gold Layer - Consumption-Ready Tables
# =============================================================================
# Creates Gold consumption tables optimized for analytics.
# Features:
# - Column standardization (unified naming: pickup_datetime, dropoff_datetime)
# - Temporal dimensions (hour, day_of_week, is_weekend, year_month)
# - Business metrics (avg_speed_mph)
# - Query optimization (partitioned by year_month, z-ordered by location IDs)
# - Comprehensive table and column documentation
#
# Note: Only supports yellow and green taxis (FHV/FHVHV lack required columns)
# =============================================================================

gold_results = create_all_gold_tables(
    spark=spark,
    catalog=CATALOG,
    silver_schema=SILVER_SCHEMA,
    gold_schema=GOLD_SCHEMA,
    taxi_types=["yellow", "green"]  # Only yellow/green have required consumption columns
)

print("\n✓ Gold layer created successfully!")

# COMMAND ----------

# DBTITLE 1,📊 Validation - Gold Layer Quality
# =============================================================================
# DATA QUALITY VALIDATION - GOLD LAYER
# =============================================================================
# Comprehensive validation of Gold layer:
# - Schema validation (required columns)
# - Data quality checks (nulls, completeness)
# - Business metrics summary
# - Temporal distribution analysis
# - Sample data preview
# =============================================================================

from pyspark.sql import functions as F

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

# DBTITLE 1,Validation - Silver Layer Quality
# =============================================================================
# DATA QUALITY VALIDATION - SILVER LAYER
# =============================================================================
# Quality analysis of Silver layer:
# - Trip statistics (averages, totals)
# - Distribution by period
# - Data completeness metrics
# =============================================================================

from pyspark.sql import functions as F

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

# DBTITLE 1,🔍 Governance - Catalog Metadata
# =============================================================================
# GOVERNANCE AND METADATA INSPECTION
# =============================================================================
# Explore Unity Catalog structure:
# - List schemas and tables
# - View schema properties and governance metadata
# =============================================================================

print("NYC Taxi Catalog Summary\n")

# 1. List all schemas
print("1. Available schemas:")
spark.sql(f"SHOW SCHEMAS IN {CATALOG}").display()

# 2. List tables by schema
print("\n2. Bronze tables:")
spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}").display()

print("\n3. Silver tables:")
spark.sql(f"SHOW TABLES IN {CATALOG}.{SILVER_SCHEMA}").display()

print("\n4. Gold tables:")
spark.sql(f"SHOW TABLES IN {CATALOG}.{GOLD_SCHEMA}").display()

# 3. Schema properties
print("\n5. Bronze Schema properties:")
spark.sql(f"DESCRIBE SCHEMA EXTENDED {CATALOG}.{BRONZE_SCHEMA}").display()

# COMMAND ----------

# DBTITLE 1,📊 Delta Lake - History & Statistics
# =============================================================================
# DELTA LAKE HISTORY AND STATISTICS
# =============================================================================
# View Delta Lake table metadata:
# - Version history (time travel)
# - Table details and statistics
# - Operation metrics
# =============================================================================

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