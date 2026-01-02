"""
Common utility functions for NYC Taxi data processing jobs.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, regexp_replace, trim, to_date, to_timestamp, datediff, unix_timestamp, hour, dayofweek, month, year, date_format, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from typing import List, Optional


def get_spark_session(app_name: str = "NYC-Taxi-Analytics") -> SparkSession:
    """
    Create and return a Spark session.
    
    Args:
        app_name: Name for the Spark application
        
    Returns:
        SparkSession instance
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def validate_required_columns(df: DataFrame, required_columns: List[str]) -> DataFrame:
    """
    Validate that required columns exist in the DataFrame.
    
    Args:
        df: Input DataFrame
        required_columns: List of column names that must exist
        
    Returns:
        DataFrame with validated columns
        
    Raises:
        ValueError: If any required column is missing
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    return df


def remove_null_values(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Remove rows with null values in specified columns.
    
    Args:
        df: Input DataFrame
        columns: List of column names to check for nulls
        
    Returns:
        DataFrame with null rows removed
    """
    for column in columns:
        df = df.filter(col(column).isNotNull())
    return df


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardize column names: lowercase, replace spaces with underscores.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with standardized column names
    """
    for old_col in df.columns:
        new_col = old_col.lower().replace(" ", "_")
        if old_col != new_col:
            df = df.withColumnRenamed(old_col, new_col)
    return df


def parse_date_column(df: DataFrame, column: str, format: str = "yyyy-MM-dd HH:mm:ss") -> DataFrame:
    """
    Parse a date/timestamp column.
    
    Args:
        df: Input DataFrame
        column: Name of the column to parse
        format: Expected date format
        
    Returns:
        DataFrame with parsed date column
    """
    from pyspark.sql.functions import to_timestamp
    return df.withColumn(column, to_timestamp(col(column), format))


def filter_invalid_trips(df: DataFrame, 
                         fare_column: str = "fare_amount",
                         distance_column: str = "trip_distance") -> DataFrame:
    """
    Filter out invalid trips (negative fares, zero or negative distance).
    
    Args:
        df: Input DataFrame
        fare_column: Name of the fare column
        distance_column: Name of the distance column
        
    Returns:
        DataFrame with invalid trips filtered out
    """
    df = df.filter(col(fare_column) >= 0)
    df = df.filter(col(distance_column) > 0)
    return df


def validate_trip_times(df: DataFrame) -> DataFrame:
    """
    Validate trip times: ensure dropoff is after pickup, no future dates.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with invalid time trips filtered out
    """
    # Parse datetime columns
    df = df.withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime")))
    df = df.withColumn("dropoff_ts", to_timestamp(col("tpep_dropoff_datetime")))
    
    # Filter: dropoff must be after pickup
    df = df.filter(col("dropoff_ts") > col("pickup_ts"))
    
    # Filter: no future dates (assuming current year is 2025)
    current_timestamp = unix_timestamp(lit("2025-12-31 23:59:59"), "yyyy-MM-dd HH:mm:ss")
    df = df.filter(unix_timestamp(col("pickup_ts")) <= current_timestamp)
    df = df.filter(unix_timestamp(col("dropoff_ts")) <= current_timestamp)
    
    return df


def calculate_trip_duration(df: DataFrame) -> DataFrame:
    """
    Calculate trip duration in minutes.
    
    Args:
        df: Input DataFrame with pickup_ts and dropoff_ts columns
        
    Returns:
        DataFrame with trip_duration_minutes column added
    """
    df = df.withColumn(
        "trip_duration_minutes",
        (unix_timestamp(col("dropoff_ts")) - unix_timestamp(col("pickup_ts"))) / 60.0
    )
    return df


def validate_trip_duration(df: DataFrame, min_minutes: float = 1.0, max_minutes: float = 1440.0) -> DataFrame:
    """
    Filter trips with unrealistic durations.
    
    Args:
        df: Input DataFrame with trip_duration_minutes column
        min_minutes: Minimum valid duration in minutes (default: 1 minute)
        max_minutes: Maximum valid duration in minutes (default: 24 hours = 1440 minutes)
        
    Returns:
        DataFrame with invalid duration trips filtered out
    """
    df = df.filter(
        (col("trip_duration_minutes") >= min_minutes) & 
        (col("trip_duration_minutes") <= max_minutes)
    )
    return df


def calculate_trip_speed(df: DataFrame) -> DataFrame:
    """
    Calculate trip speed in miles per hour.
    
    Args:
        df: Input DataFrame with trip_distance and trip_duration_minutes columns
        
    Returns:
        DataFrame with trip_speed_mph column added
    """
    df = df.withColumn(
        "trip_speed_mph",
        when(col("trip_duration_minutes") > 0,
             col("trip_distance") / (col("trip_duration_minutes") / 60.0))
        .otherwise(lit(None))
    )
    return df


def validate_trip_speed(df: DataFrame, min_mph: float = 1.0, max_mph: float = 100.0) -> DataFrame:
    """
    Filter trips with unrealistic speeds.
    
    Args:
        df: Input DataFrame with trip_speed_mph column
        min_mph: Minimum valid speed in mph (default: 1 mph)
        max_mph: Maximum valid speed in mph (default: 100 mph)
        
    Returns:
        DataFrame with invalid speed trips filtered out
    """
    df = df.filter(
        (col("trip_speed_mph").isNull()) |  # Allow null speeds (stationary trips)
        ((col("trip_speed_mph") >= min_mph) & (col("trip_speed_mph") <= max_mph))
    )
    return df


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Remove duplicate records based on key columns.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with duplicates removed
    """
    # Remove exact duplicates based on key trip identifiers
    key_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pulocationid",
        "dolocationid",
        "trip_distance",
        "fare_amount"
    ]
    
    # Only use columns that exist
    existing_key_columns = [col for col in key_columns if col in df.columns]
    
    if existing_key_columns:
        df = df.dropDuplicates(existing_key_columns)
    
    return df


def add_temporal_features(df: DataFrame) -> DataFrame:
    """
    Add temporal features from pickup datetime.
    
    Args:
        df: Input DataFrame with pickup_ts column
        
    Returns:
        DataFrame with temporal features added
    """
    df = df.withColumn("pickup_year", year(col("pickup_ts")))
    df = df.withColumn("pickup_month", month(col("pickup_ts")))
    df = df.withColumn("pickup_day", date_format(col("pickup_ts"), "d"))
    df = df.withColumn("pickup_hour", hour(col("pickup_ts")))
    df = df.withColumn("pickup_day_of_week", dayofweek(col("pickup_ts")))
    df = df.withColumn("pickup_date", date_format(col("pickup_ts"), "yyyy-MM-dd"))
    
    return df


def add_derived_features(df: DataFrame) -> DataFrame:
    """
    Add derived features like tip percentage, trip length category.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with derived features added
    """
    # Tip percentage
    if "tip_amount" in df.columns and "total_amount" in df.columns:
        df = df.withColumn(
            "tip_percentage",
            when(col("total_amount") > 0,
                 (col("tip_amount") / col("total_amount")) * 100.0)
            .otherwise(lit(0.0))
        )
    
    # Trip length category
    if "trip_distance" in df.columns:
        df = df.withColumn(
            "trip_length_category",
            when(col("trip_distance") < 2.0, "short")
            .when(col("trip_distance") < 5.0, "medium")
            .otherwise("long")
        )
    
    # Airport trip flag (zones 1-3 are typically airports)
    if "pulocationid" in df.columns:
        df = df.withColumn(
            "is_airport_pickup",
            col("pulocationid").isin([1, 2, 3])
        )
    if "dolocationid" in df.columns:
        df = df.withColumn(
            "is_airport_dropoff",
            col("dolocationid").isin([1, 2, 3])
        )
    if "is_airport_pickup" in df.columns and "is_airport_dropoff" in df.columns:
        df = df.withColumn(
            "is_airport_trip",
            col("is_airport_pickup") | col("is_airport_dropoff")
        )
    
    return df


def validate_passenger_count(df: DataFrame, max_passengers: int = 6) -> DataFrame:
    """
    Filter trips with invalid passenger counts.
    
    Args:
        df: Input DataFrame
        max_passengers: Maximum valid passenger count (default: 6)
        
    Returns:
        DataFrame with invalid passenger count trips filtered out
    """
    df = df.filter(
        (col("passenger_count") >= 0) & (col("passenger_count") <= max_passengers)
    )
    return df


def add_quality_flags(df: DataFrame) -> DataFrame:
    """
    Add data quality flags to the DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with quality flags added
    """
    # Add flag for trips with missing pickup or dropoff location
    df = df.withColumn(
        "has_valid_locations",
        when(col("pulocationid").isNotNull() & col("dolocationid").isNotNull(), True)
        .otherwise(False)
    )
    
    # Add flag for trips with valid payment type
    df = df.withColumn(
        "has_valid_payment",
        when(col("payment_type").isNotNull(), True)
        .otherwise(False)
    )
    
    return df


def write_to_s3(df: DataFrame, s3_path: str, format: str = "parquet", mode: str = "overwrite", partition_by: Optional[List[str]] = None):
    """
    Write DataFrame to S3.
    
    Args:
        df: DataFrame to write
        s3_path: S3 path to write to
        format: File format (parquet, csv, etc.)
        mode: Write mode (overwrite, append, etc.)
        partition_by: List of columns to partition by
    """
    writer = df.write.format(format).mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    writer.save(s3_path)

