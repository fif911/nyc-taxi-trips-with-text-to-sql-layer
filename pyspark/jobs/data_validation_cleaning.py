"""
PySpark Job: Data Validation and Cleaning

Reads raw NYC Yellow Taxi data from S3, performs validation and cleaning,
and writes cleaned data to S3 processed folder.
"""
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql.functions import col

from utils.common_functions import (
    get_spark_session,
    validate_required_columns,
    remove_null_values,
    standardize_column_names,
    filter_invalid_trips,
    validate_trip_times,
    calculate_trip_duration,
    validate_trip_duration,
    calculate_trip_speed,
    validate_trip_speed,
    remove_duplicates,
    add_temporal_features,
    add_derived_features,
    validate_passenger_count,
    add_quality_flags,
    write_to_s3
)
from utils.config_loader import load_and_set_config, get_s3_bucket


def main():
    """Main function to run data validation and cleaning job."""
    spark = get_spark_session("NYC-Taxi-Data-Validation-Cleaning")
    
    # Load configuration from SSM Parameter Store
    try:
        load_and_set_config()
        s3_bucket = get_s3_bucket()
        if not s3_bucket:
            print("Error: Could not get S3 bucket name from Parameter Store", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Error: Could not load config from SSM Parameter Store: {e}", file=sys.stderr)
        sys.exit(1)
    
    input_path = f"s3://{s3_bucket}/raw/"
    output_path = f"s3://{s3_bucket}/processed/trips_cleaned/"
    
    print(f"Reading data from: {input_path}")
    
    # Read raw data from S3
    df = spark.read.parquet(input_path)
    
    print(f"Initial row count: {df.count()}")
    
    # Critical columns that must not be null
    critical_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount"
    ]
    
    # Validate required columns exist
    try:
        validate_required_columns(df, critical_columns)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Standardize column names
    df = standardize_column_names(df)
    
    # Remove null values in critical columns
    df = remove_null_values(df, [col.lower() for col in critical_columns])
    
    print(f"Row count after removing nulls: {df.count()}")
    
    # Validate trip times (dropoff > pickup, no future dates)
    df = validate_trip_times(df)
    print(f"Row count after validating trip times: {df.count()}")
    
    # Calculate trip duration
    df = calculate_trip_duration(df)
    
    # Validate trip duration (1 minute to 24 hours)
    df = validate_trip_duration(df, min_minutes=1.0, max_minutes=1440.0)
    print(f"Row count after validating trip duration: {df.count()}")
    
    # Calculate trip speed
    df = calculate_trip_speed(df)
    
    # Validate trip speed (1-100 mph)
    df = validate_trip_speed(df, min_mph=1.0, max_mph=100.0)
    print(f"Row count after validating trip speed: {df.count()}")
    
    # Filter invalid trips (negative fares, zero distance)
    df = filter_invalid_trips(df, fare_column="fare_amount", distance_column="trip_distance")
    
    print(f"Row count after filtering invalid trips: {df.count()}")
    
    # Validate passenger count (0-6)
    df = validate_passenger_count(df, max_passengers=6)
    print(f"Row count after validating passenger count: {df.count()}")
    
    # Remove duplicates
    initial_count = df.count()
    df = remove_duplicates(df)
    duplicates_removed = initial_count - df.count()
    if duplicates_removed > 0:
        print(f"Removed {duplicates_removed} duplicate records")
    
    # Add temporal features
    df = add_temporal_features(df)
    
    # Add derived features (tip percentage, trip length category, airport flags)
    df = add_derived_features(df)
    
    # Add data quality flags
    df = add_quality_flags(df)
    
    print(f"Writing cleaned data to: {output_path}")
    
    # Write to S3 with partitioning by year and month
    write_to_s3(
        df,
        output_path,
        format="parquet",
        mode="overwrite",
        partition_by=["pickup_year", "pickup_month"] if "pickup_year" in df.columns and "pickup_month" in df.columns else None
    )
    
    print("✓ Data validation and cleaning completed successfully")
    
    spark.stop()


if __name__ == "__main__":
    main()

