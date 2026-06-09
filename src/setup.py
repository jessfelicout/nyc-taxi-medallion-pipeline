"""Unity Catalog Setup Module

This module creates the complete medallion architecture structure in Unity Catalog:
- Catalog
- Schemas (raw, bronze, silver, gold)
- Volumes for raw data storage
- Schema properties for governance and documentation
"""

from pyspark.sql import SparkSession
from config import CATALOG, RAW_SCHEMA, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, VOLUME, TAXI_TYPES


def setup_catalog(spark: SparkSession) -> None:
    """Create Unity Catalog and all schemas with governance metadata.
    
    Args:
        spark: Active SparkSession
    """
    print("\n  Initializing medallion architecture setup...\n")

    # 1. Create Catalog
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    print(f"Catalog created: {CATALOG}")

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
    print(f"Schema BRONZE: {CATALOG}.{BRONZE_SCHEMA}")

    # 4. Create SILVER Schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
    spark.sql(f"""
        ALTER SCHEMA {CATALOG}.{SILVER_SCHEMA}
        SET DBPROPERTIES (
            'layer' = 'silver',
            'description' = 'Cleansing layer - validated and standardized data'
        )
    """)
    print(f"Schema SILVER: {CATALOG}.{SILVER_SCHEMA}")

    # 5. Create GOLD Schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
    spark.sql(f"""
        ALTER SCHEMA {CATALOG}.{GOLD_SCHEMA}
        SET DBPROPERTIES (
            'layer' = 'gold',
            'description' = 'Aggregation layer - consumption-ready datasets'
        )
    """)
    print(f"Schema GOLD: {CATALOG}.{GOLD_SCHEMA}")

    # 6. Summary of Bronze tables to be created
    print(f"\n Bronze tables to be created:")
    for taxi_type in TAXI_TYPES:
        print(f"  - {CATALOG}.{BRONZE_SCHEMA}.{taxi_type}_trips")

    print("\nSetup completed successfully!")


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("nyc-taxi-setup").getOrCreate()
    setup_catalog(spark)
