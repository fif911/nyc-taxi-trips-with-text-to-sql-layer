"""
PySpark Job: Trip Metrics Aggregation

Reads cleaned trip data from S3, aggregates metrics by time periods and zones,
and writes insights to S3.
"""
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql.functions import (
    col, hour, dayofweek, date_format, count, avg, sum,
    window, when
)
from utils.common_functions import get_spark_session, write_to_s3
from utils.config_loader import load_and_set_config, get_s3_bucket


def main():
    """Main function to run trip metrics aggregation job."""
    spark = get_spark_session("NYC-Taxi-Trip-Metrics-Aggregation")
    
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
    
    input_path = f"s3://{s3_bucket}/processed/trips_cleaned/"
    output_base = f"s3://{s3_bucket}/insights/"
    
    print(f"Reading cleaned data from: {input_path}")
    
    # Read cleaned data from S3
    df = spark.read.parquet(input_path)
    
    print(f"Total trips: {df.count()}")
    
    # Use temporal features if already calculated, otherwise parse datetime
    if "pickup_hour" not in df.columns and "tpep_pickup_datetime" in df.columns:
        from pyspark.sql.functions import to_timestamp
        df = df.withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime")))
        df = df.withColumn("pickup_hour", hour("pickup_ts"))
        df = df.withColumn("pickup_day_of_week", dayofweek("pickup_ts"))
        df = df.withColumn("pickup_date", date_format("pickup_ts", "yyyy-MM-dd"))
    elif "pickup_hour" in df.columns:
        # Use existing temporal features
        pass
    
    # 1. Trip volume by hour
    print("Aggregating trip volume by hour...")
    trip_volume_by_hour = df.groupBy("pickup_hour") \
        .agg(
            count("*").alias("trip_count"),
            avg("trip_distance").alias("avg_distance"),
            avg("fare_amount").alias("avg_fare"),
            avg("total_amount").alias("avg_total")
        ) \
        .orderBy("pickup_hour")
    
    write_to_s3(trip_volume_by_hour, f"{output_base}trip_volume_by_hour/", mode="overwrite")
    print(f"✓ Wrote trip_volume_by_hour to {output_base}trip_volume_by_hour/")
    
    # 2. Trip volume by day of week
    print("Aggregating trip volume by day of week...")
    trip_volume_by_day = df.groupBy("pickup_day_of_week") \
        .agg(
            count("*").alias("trip_count"),
            avg("trip_distance").alias("avg_distance"),
            avg("fare_amount").alias("avg_fare"),
            avg("total_amount").alias("avg_total")
        ) \
        .orderBy("pickup_day_of_week")
    
    write_to_s3(trip_volume_by_day, f"{output_base}trip_volume_by_day/", mode="overwrite")
    print(f"✓ Wrote trip_volume_by_day to {output_base}trip_volume_by_day/")
    
    # 3. Trip volume by zone (pickup location)
    if "pulocationid" in df.columns:
        print("Aggregating trip volume by pickup zone...")
        trip_volume_by_zone = df.groupBy("pulocationid") \
            .agg(
                count("*").alias("trip_count"),
                avg("trip_distance").alias("avg_distance"),
                avg("fare_amount").alias("avg_fare"),
                avg("total_amount").alias("avg_total")
            ) \
            .orderBy(col("trip_count").desc())
        
        write_to_s3(trip_volume_by_zone, f"{output_base}trip_volume_by_zone/", mode="overwrite")
        print(f"✓ Wrote trip_volume_by_zone to {output_base}trip_volume_by_zone/")
    
    # 4. Trip duration analysis
    if "trip_duration_minutes" in df.columns:
        print("Analyzing trip duration by time of day...")
        duration_by_hour = df.groupBy("pickup_hour") \
            .agg(
                count("*").alias("trip_count"),
                avg("trip_duration_minutes").alias("avg_duration_minutes"),
                avg("trip_distance").alias("avg_distance")
            ) \
            .orderBy("pickup_hour")
        
        write_to_s3(duration_by_hour, f"{output_base}trip_duration_by_hour/", mode="overwrite")
        print(f"✓ Wrote trip_duration_by_hour to {output_base}trip_duration_by_hour/")
        
        # Duration by zone
        if "pulocationid" in df.columns:
            print("Analyzing trip duration by pickup zone...")
            duration_by_zone = df.groupBy("pulocationid") \
                .agg(
                    count("*").alias("trip_count"),
                    avg("trip_duration_minutes").alias("avg_duration_minutes"),
                    avg("trip_distance").alias("avg_distance")
                ) \
                .orderBy(col("trip_count").desc())
            
            write_to_s3(duration_by_zone, f"{output_base}trip_duration_by_zone/", mode="overwrite")
            print(f"✓ Wrote trip_duration_by_zone to {output_base}trip_duration_by_zone/")
    
    # 5. Speed analysis
    if "trip_speed_mph" in df.columns:
        print("Analyzing trip speed by time of day...")
        speed_by_hour = df.filter(col("trip_speed_mph").isNotNull()) \
            .groupBy("pickup_hour") \
            .agg(
                count("*").alias("trip_count"),
                avg("trip_speed_mph").alias("avg_speed_mph"),
                avg("trip_distance").alias("avg_distance")
            ) \
            .orderBy("pickup_hour")
        
        write_to_s3(speed_by_hour, f"{output_base}trip_speed_by_hour/", mode="overwrite")
        print(f"✓ Wrote trip_speed_by_hour to {output_base}trip_speed_by_hour/")
    
    # 6. Monthly trends
    if "pickup_month" in df.columns:
        print("Analyzing monthly trends...")
        monthly_trends = df.groupBy("pickup_month") \
            .agg(
                count("*").alias("trip_count"),
                avg("trip_distance").alias("avg_distance"),
                avg("fare_amount").alias("avg_fare"),
                avg("total_amount").alias("avg_total"),
                sum("total_amount").alias("total_revenue")
            ) \
            .orderBy("pickup_month")
        
        write_to_s3(monthly_trends, f"{output_base}monthly_trends/", mode="overwrite")
        print(f"✓ Wrote monthly_trends to {output_base}monthly_trends/")
    
    # 7. Trip length category analysis
    if "trip_length_category" in df.columns:
        print("Analyzing trips by length category...")
        trips_by_category = df.groupBy("trip_length_category") \
            .agg(
                count("*").alias("trip_count"),
                avg("trip_distance").alias("avg_distance"),
                avg("fare_amount").alias("avg_fare"),
                avg("total_amount").alias("avg_total"),
                avg("trip_duration_minutes").alias("avg_duration_minutes") if "trip_duration_minutes" in df.columns else count("*").alias("avg_duration_minutes")
            ) \
            .orderBy("trip_length_category")
        
        write_to_s3(trips_by_category, f"{output_base}trips_by_length_category/", mode="overwrite")
        print(f"✓ Wrote trips_by_length_category to {output_base}trips_by_length_category/")
    
    print("✓ Trip metrics aggregation completed successfully")
    
    spark.stop()


if __name__ == "__main__":
    main()

