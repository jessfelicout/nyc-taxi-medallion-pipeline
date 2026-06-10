# NYC Taxi Medallion Architecture Pipeline

A  data pipeline implementing the **Medallion Architecture** (Bronze, Silver, Gold) for processing NYC Taxi trip data using **Databricks**, **Delta Lake**, **Unity Catalog**, and **Liquid Clustering**.

## Overview

This pipeline ingests raw NYC Taxi trip data from [NYC Open Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and transforms it through a modern medallion architecture:

| Layer | Format | Description | Key Features |
|-------|---------|-------------|--------------|
| **RAW** | Volume | Original Parquet files | Immutable source, Unity Catalog Volumes |
| **BRONZE** | Delta | Raw data + audit metadata | `_source_file`, `_ingestion_timestamp`, schema evolution |
| **SILVER** | Delta | Clean & validated | Date range filtering, quality rules, deduplication, **Liquid Clustering** |
| **GOLD** | Delta | Consumption-ready | Standardized columns, temporal dimensions, business metrics, **Liquid Clustering** |

## Architecture

```
RAW (Volume)           BRONZE (Delta)         SILVER (Delta)         GOLD (Delta)
  Parquet files    ->  + Audit metadata  ->  + Date filtering   ->  + Business metrics
  Immutable             Schema evolution      + Data validation      Liquid Clustering
                        Column normalization   Deduplication          Standardized columns
                                              Liquid Clustering       Temporal dimensions
```

### Data Flow

1. **Download**: Parallel download of Parquet files from NYC Open Data (4 workers, retry logic)
2. **Bronze Ingestion**: Load raw files into Delta tables respecting configured periods (YEARS/MONTHS)
3. **Silver Transformation**: Apply quality rules, date range filtering (`PIPELINE_START_DATE` to `PIPELINE_END_DATE`), and Liquid Clustering
4. **Gold Creation**: Build consumption-ready tables with standardized columns, temporal dimensions, and business metrics

## Project Structure

```
nyc-taxi-medallion-pipeline/
├── src/                               # Modular Python code organized by layer
│   ├── config.py                      # Pipeline configuration
│   │                                  #   - YEARS, MONTHS, TAXI_TYPES
│   │                                  #   - PIPELINE_START_DATE, PIPELINE_END_DATE
│   │                                  #   - Catalog and schema names
│   ├── setup.py                       # Unity Catalog setup (catalog, schemas, volume)
│   ├── bronze/
│   │   ├── download.py                # Parallel download (4 workers, retry logic)
│   │   └── ingest.py                  # Bronze ingestion (respects YEARS/MONTHS)
│   ├── silver/
│   │   └── transform.py               # Quality rules + date range filter + Liquid Clustering
│   └── gold/
│       └── create_tables.py           # Business metrics + Liquid Clustering
├── analysis/                          # Notebooks for orchestration and analysis
│   ├── pipeline_steps                 # Main orchestration notebook (run entire pipeline)
│   ├── NYC TLC Raw Data Analysis      # Data exploration and quality analysis
│   ├── NYC Taxi Pipeline - ...        # Architecture documentation
│   └── Ifood Case - Analysis          # Business case study
├── README.md                          # This file
└── requirements.txt                   # Python dependencies
```

## Key Features

### Data Processing

* **4 taxi types**: Yellow, Green, FHV (For-Hire Vehicles), FHVHV (High Volume For-Hire)
* **Parallel downloads**: 4-worker pool for efficient file retrieval
* **Retry logic**: Exponential backoff with 3 retries for failed downloads
* **Idempotency**: Skip existing valid files to avoid re-downloading
* **Period filtering**: Respects `YEARS` and `MONTHS` configuration throughout the pipeline
* **Date range filtering**: `PIPELINE_START_DATE` to `PIPELINE_END_DATE` (derived from YEARS/MONTHS)
* **Schema evolution**: Automatic schema merging for varying file structures
* **Liquid Clustering**: Delta Lake's adaptive optimization for query performance
* **Comprehensive data quality**: Validation rules for each taxi type

### Data Quality Rules (Silver Layer)

**All Taxi Types:**
* **Date range filtering**: Only process data between `PIPELINE_START_DATE` and `PIPELINE_END_DATE` (removes temporal outliers)
* **Valid timestamps**: Non-null, pickup before dropoff
* **Positive trip duration**: Calculated from pickup/dropoff times
* **Duplicate removal**: Based on business keys (pickup/dropoff times + locations)

**Yellow/Green Only:**
* **Trip distance**: 0-500 miles (removes GPS errors and extreme outliers)
* **Passenger count**: 1-6 passengers (realistic capacity)
* **Non-negative fares**: Fare amount and total amount >= 0

### Gold Layer Features

* **Column Standardization**: Unified naming across taxi types
  - `pickup_datetime`, `dropoff_datetime` (standardized from `tpep_*` / `lpep_*`)
  - `vendor_id`, `pickup_location_id`, `dropoff_location_id`
* **Temporal Dimensions**: 
  - `year`, `month`, `pickup_hour`, `pickup_day_of_week`, `is_weekend`
* **Business Metrics**: 
  - `avg_speed_mph` (calculated from distance and duration)
* **Query Optimization**: 
  - **Liquid Clustering** by `pickup_date` (consistent with Silver layer)
  - Adaptive optimization for changing query patterns
  - Automatic Delta Lake optimizations

**Note**: Gold layer only supports Yellow and Green taxis (FHV/FHVHV lack required columns like `vendor_id`, `passenger_count`, `total_amount`)

## Prerequisites

* **Databricks Workspace** with Unity Catalog enabled
* **Compute** with Databricks Runtime 13.0+ (or MLR 13.0+)
* **Permissions**: `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME`

## Quick Start

### Option 1: Run the Orchestration Notebook (Recommended)

Open and run the `analysis/pipeline_steps` notebook - it orchestrates the entire pipeline end-to-end.

### Option 2: Run Step-by-Step

#### 1. Configure Parameters

Edit `src/config.py`:

```python
CATALOG = "nyc_taxi"              # Unity Catalog name
YEARS = [2023]                     # Years to process
MONTHS = list(range(1, 6))         # Months to process (1-5 = Jan-May)
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

# Date range is automatically derived:
# PIPELINE_START_DATE = "2023-01-01"
# PIPELINE_END_DATE = "2023-05-31"
```

#### 2. Setup Unity Catalog

```python
from pyspark.sql import SparkSession
from src.setup import setup_catalog

spark = SparkSession.builder.getOrCreate()
setup_catalog(spark)
```

**Creates**:
* Catalog: `nyc_taxi`
* Schemas: `raw`, `bronze`, `silver`, `gold` (with governance metadata)
* Volume: `/Volumes/nyc_taxi/raw/files/`

#### 3. Download Raw Data

```python
from src.config import BASE_URL, RAW_PATH, TAXI_TYPES, YEARS, MONTHS
from src.bronze.download import download_all_parallel

stats = download_all_parallel(BASE_URL, RAW_PATH, TAXI_TYPES, YEARS, MONTHS)
```

#### 4. Ingest to Bronze

```python
from src.config import CATALOG, RAW_PATH, BRONZE_SCHEMA, TAXI_TYPES, YEARS, MONTHS
from src.bronze.ingest import ingest_all_taxi_types

bronze_results = ingest_all_taxi_types(
    spark, CATALOG, RAW_PATH, BRONZE_SCHEMA, TAXI_TYPES,
    years=YEARS, months=MONTHS, overwrite=True
)
```

**Tables Created**:
* `nyc_taxi.bronze.yellow_trips`
* `nyc_taxi.bronze.green_trips`
* `nyc_taxi.bronze.fhv_trips`
* `nyc_taxi.bronze.fhvhv_trips`

#### 5. Transform to Silver

```python
from src.config import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, TAXI_TYPES
from src.silver.transform import transform_all_taxi_types

silver_results = transform_all_taxi_types(
    spark, CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, TAXI_TYPES
)
```

**Tables Created** (with Liquid Clustering by `pickup_date`):
* `nyc_taxi.silver.yellow_trips_clean`
* `nyc_taxi.silver.green_trips_clean`
* `nyc_taxi.silver.fhv_trips_clean`
* `nyc_taxi.silver.fhvhv_trips_clean`

#### 6. Create Gold Tables

```python
from src.config import CATALOG, SILVER_SCHEMA, GOLD_SCHEMA
from src.gold.create_tables import create_all_gold_tables

gold_results = create_all_gold_tables(
    spark, CATALOG, SILVER_SCHEMA, GOLD_SCHEMA,
    taxi_types=["yellow", "green"]
)
```

**Tables Created** (with Liquid Clustering by `pickup_date`):
* `nyc_taxi.gold.yellow_trips_gold`
* `nyc_taxi.gold.green_trips_gold`

## Data Governance

### Metadata Tracking

| Layer | Audit Columns | Purpose |
|-------|---------------|---------|
| **Bronze** | `_source_file`, `_ingestion_timestamp` | Track origin and ingestion time |
| **Silver** | `_processed_timestamp`, `trip_duration_minutes`, `pickup_date` | Track transformation and enable time-based queries |
| **Gold** | `silver_processed_at`, `gold_created_at`, temporal dimensions | Track lineage and enable analytics |

### Schema Properties

Each schema includes governance metadata accessible via:

```sql
DESCRIBE SCHEMA EXTENDED nyc_taxi.bronze;
-- Properties: layer=bronze, description=Ingestion layer...
```

### Table Documentation

All tables include:
* **Table-level comments**: Purpose, transformations, and clustering strategy
* **Column-level comments**: Key business fields (e.g., `avg_speed_mph`, `is_weekend`)
* **Date range documentation**: Silver/Gold tables document the valid date range in their comments

## Performance & Optimization

### Bronze Layer
* Schema evolution with `mergeSchema=True`
* `OPTIMIZE` command after ingestion
* Union with `allowMissingColumns=True` for schema flexibility

### Silver Layer
* **Date range filtering** with predicate pushdown (`PIPELINE_START_DATE` to `PIPELINE_END_DATE`)
* **Liquid Clustering** by `pickup_date` for adaptive query optimization
* Duplicate removal on business keys
* `OPTIMIZE` after transformation

### Gold Layer
* **Liquid Clustering** by `pickup_date` (consistent with Silver)
* Adaptive optimization for changing query patterns
* Optimized for temporal and geographic queries


## Next Steps

### 1. Automation
* Schedule the `pipeline_steps` notebook as a Databricks Job
* Set up incremental ingestion (process only new data)
* Add data quality alerts and monitoring dashboards

### 2. Advanced Analytics
* Location-based analysis using `pickup_location_id` and `dropoff_location_id`
* Time-series forecasting with temporal dimensions
* Revenue and demand pattern analysis by day of week / hour

### 3. Optimizations
* Schedule **VACUUM** operations for storage cleanup
* Implement CDC (Change Data Capture) for real-time updates
* Add data quality expectations with Delta Live Tables

### 4. MLOps & BI Integration
* Build ML models for demand forecasting using Gold tables
* Connect Tableau, Power BI, or Looker to Gold tables
* Deploy models with MLflow and Model Registry

## Resources

* **Data Source**: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (Public Domain)
* **Medallion Architecture**: [Databricks Docs](https://www.databricks.com/glossary/medallion-architecture)
* **Delta Lake**: [Delta Lake Guide](https://docs.delta.io/)
* **Liquid Clustering**: [Databricks Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)
* **Unity Catalog**: [Unity Catalog Best Practices](https://docs.databricks.com/en/data-governance/unity-catalog/best-practices.html)

---

**License**: Public Domain (source data from NYC Open Data)  
**Last Updated**: 2026-06-09
