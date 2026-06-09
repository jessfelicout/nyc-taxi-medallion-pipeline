"""NYC Taxi Pipeline Configuration

This module contains all configuration parameters for the medallion architecture pipeline.
It defines catalog structure, data sources, and processing parameters.
"""

# Data source configuration
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Unity Catalog configuration
CATALOG = "nyc_taxi"
RAW_SCHEMA = "raw"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
VOLUME = "files"

# Data processing configuration
YEARS = [2023]
MONTHS = list(range(1, 6))  # January to May
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

# Date range for quality filtering (derived from YEARS and MONTHS)
# Filters out corrupted dates outside this range in Silver layer
PIPELINE_START_DATE = f"{YEARS[0]}-{MONTHS[0]:02d}-01"  # 2023-01-01
PIPELINE_END_DATE = f"{YEARS[-1]}-{MONTHS[-1]:02d}-31"    # 2023-05-31

# Volume path for raw data
RAW_PATH = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME}"


def print_config():
    """Print current configuration settings."""
    print("NYC Taxi Pipeline Configuration")
    print(f"  Catalog: {CATALOG}")
    print(f"  Raw path: {RAW_PATH}")
    print(f"  Processing: {TAXI_TYPES}")
    print(f"  Period: {YEARS}, months {MONTHS}")


if __name__ == "__main__":
    print_config()
