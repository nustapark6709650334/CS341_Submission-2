"""
Configuration & Schema Mapping for NYC Taxi H1 2024 Data Pipeline (Submission 2).
"""

from pathlib import Path

# Base Project Directories
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
REFERENCE_DIR = DATA_DIR / "reference"
CURATED_DIR = DATA_DIR / "curated"
CURATED_PARTITIONS_DIR = CURATED_DIR / "partitions"
CURATED_BOROUGH_PARTS_DIR = CURATED_PARTITIONS_DIR / "borough_by_file"
CURATED_SAMPLE_PARTS_DIR = CURATED_PARTITIONS_DIR / "sample_by_file"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
FIGURES_DIR = OUTPUTS_DIR / "figures"

# Optional Google Colab Drive path (from Submission 1) for hybrid discovery
COLAB_DRIVE_PATH = Path("/content/drive/MyDrive/Colab Notebooks/CS341 MissionB")

# Official NYC TLC Public Dataset Base URLs
TLC_TRIP_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TLC_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Target Scope: First Half of 2024 (Jan - Jun 2024)
TARGET_YEAR = 2024
TARGET_MONTHS = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

# Pipeline Version for State / Manifest Tracking
PIPELINE_RULE_VERSION = "2.0.0"

# Data Quality Rules Configuration
DQ_MIN_DATETIME = "2024-01-01 00:00:00"
DQ_MAX_DATETIME = "2024-07-01 00:00:00"  # Exclusive upper bound for H1 2024
DQ_MAX_DURATION_MINUTES = 1440.0         # 24 hours max reasonable single taxi trip

# Schema Harmonization Mapping per Taxi Type
SCHEMA_MAPPING = {
    "yellow": {
        "pickup_col": "tpep_pickup_datetime",
        "dropoff_col": "tpep_dropoff_datetime",
        "pu_loc_col": "PULocationID",
        "do_loc_col": "DOLocationID",
        "distance_col": "trip_distance",
        "fare_col": "fare_amount",
        "has_distance_fare": True,
        "description": "Yellow Medallion Taxicabs (Street-hail across NYC, concentrated in Manhattan & Airports)",
    },
    "green": {
        "pickup_col": "lpep_pickup_datetime",
        "dropoff_col": "lpep_dropoff_datetime",
        "pu_loc_col": "PULocationID",
        "do_loc_col": "DOLocationID",
        "distance_col": "trip_distance",
        "fare_col": "fare_amount",
        "has_distance_fare": True,
        "description": "Green Boro Taxis (Street-hail above 96th/110th St in Manhattan and in outer boroughs)",
    },
    "fhv": {
        "pickup_col": "pickup_datetime",
        "dropoff_col": "dropOff_datetime",
        "pu_loc_col": "PUlocationID",
        "do_loc_col": "DOlocationID",
        "distance_col": None,
        "fare_col": None,
        "has_distance_fare": False,
        "description": "Traditional For-Hire Vehicles (Community livery, black cars, luxury limousines)",
    },
    "fhvhv": {
        "pickup_col": "pickup_datetime",
        "dropoff_col": "dropoff_datetime",
        "pu_loc_col": "PULocationID",
        "do_loc_col": "DOLocationID",
        "distance_col": "trip_miles",
        "fare_col": "base_passenger_fare",
        "has_distance_fare": True,
        "description": "High-Volume For-Hire Vehicles (App-based rideshare: Uber, Lyft >10,000 trips/day)",
    },
}

# File Paths for Pipeline Outputs & State
ZONE_LOOKUP_PATH = REFERENCE_DIR / "taxi_zone_lookup.csv"
CURATED_MONTHLY_PARQUET = CURATED_DIR / "curated_monthly_summary.parquet"
CURATED_MONTHLY_CSV = CURATED_DIR / "curated_monthly_summary.csv"
CURATED_BOROUGH_PARQUET = CURATED_DIR / "curated_borough_summary.parquet"
CURATED_BOROUGH_CSV = CURATED_DIR / "curated_borough_summary.csv"
SAMPLE_CURATED_TRIPS_PARQUET = CURATED_DIR / "sample_curated_trips.parquet"
SAMPLE_CURATED_TRIPS_CSV = CURATED_DIR / "sample_curated_trips.csv"

STATE_MANIFEST_PATH = OUTPUTS_DIR / "pipeline_state.json"
PIPELINE_LOG_PATH = OUTPUTS_DIR / "pipeline_run.log"
DQ_REPORT_CSV_PATH = OUTPUTS_DIR / "data_quality_report.csv"
DQ_SUMMARY_JSON_PATH = OUTPUTS_DIR / "data_quality_summary.json"
FINAL_ANALYSIS_CSV_PATH = OUTPUTS_DIR / "analysis_result_h1_2024.csv"
