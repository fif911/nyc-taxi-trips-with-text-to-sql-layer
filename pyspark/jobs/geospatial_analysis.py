"""
PySpark Job: Geospatial Analysis

Analyzes pickup and dropoff zones, identifies popular zones and zone pairs.
"""
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql.functions import col, count, avg, sum, desc
from utils.common_functions import get_spark_session, write_to_s3
from utils.config_loader import load_and_set_config, get_s3_bucket


def main():
    """Main function to run geospatial analysis job."""
    spark = get_spark_session("NYC-Taxi-Geospatial-Analysis")
    
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
    
    # 1. Popular pickup zones
    if "pulocationid" in df.columns:
        print("Analyzing popular pickup zones...")
        popular_pickup_zones = df.groupBy("pulocationid") \
            .agg(
                count("*").alias("pickup_count"),
                avg("trip_distance").alias("avg_distance"),
                avg("fare_amount").alias("avg_fare"),
                avg("total_amount").alias("avg_total")
            ) \
            .withColumnRenamed("pulocationid", "location_id") \
            .orderBy(desc("pickup_count"))
        
        write_to_s3(popular_pickup_zones, f"{output_base}popular_pickup_zones/", mode="overwrite")
        print(f"✓ Wrote popular_pickup_zones to {output_base}popular_pickup_zones/")
    
    # 2. Popular dropoff zones
    if "dolocationid" in df.columns:
        print("Analyzing popular dropoff zones...")
        popular_dropoff_zones = df.groupBy("dolocationid") \
            .agg(
                count("*").alias("dropoff_count"),
                avg("trip_distance").alias("avg_distance"),
                avg("fare_amount").alias("avg_fare"),
                avg("total_amount").alias("avg_total")
            ) \
            .withColumnRenamed("dolocationid", "location_id") \
            .orderBy(desc("dropoff_count"))
        
        write_to_s3(popular_dropoff_zones, f"{output_base}popular_dropoff_zones/", mode="overwrite")
        print(f"✓ Wrote popular_dropoff_zones to {output_base}popular_dropoff_zones/")
    
    # 3. Zone pair analysis (pickup-dropoff pairs)
    if "pulocationid" in df.columns and "dolocationid" in df.columns:
        print("Analyzing pickup-dropoff zone pairs...")
        zone_pair_analysis = df.groupBy("pulocationid", "dolocationid") \
            .agg(
                count("*").alias("trip_count"),
                avg("trip_distance").alias("avg_distance"),
                avg("fare_amount").alias("avg_fare"),
                avg("total_amount").alias("avg_total")
            ) \
            .withColumnRenamed("pulocationid", "pickup_location_id") \
            .withColumnRenamed("dolocationid", "dropoff_location_id") \
            .orderBy(desc("trip_count"))
        
        write_to_s3(zone_pair_analysis, f"{output_base}zone_pair_analysis/", mode="overwrite")
        print(f"✓ Wrote zone_pair_analysis to {output_base}zone_pair_analysis/")
    
    # 4. Airport trip analysis
    if "is_airport_trip" in df.columns:
        print("Analyzing airport trips...")
        airport_trips = df.filter(col("is_airport_trip") == True)
        
        if airport_trips.count() > 0:
            airport_analysis = airport_trips.groupBy("is_airport_pickup", "is_airport_dropoff") \
                .agg(
                    count("*").alias("trip_count"),
                    avg("trip_distance").alias("avg_distance"),
                    avg("fare_amount").alias("avg_fare"),
                    avg("total_amount").alias("avg_total"),
                    sum("total_amount").alias("total_revenue")
                )
            
            write_to_s3(airport_analysis, f"{output_base}airport_trip_analysis/", mode="overwrite")
            print(f"✓ Wrote airport_trip_analysis to {output_base}airport_trip_analysis/")
            
            # Airport zones analysis
            if "pulocationid" in df.columns:
                airport_pickup_zones = airport_trips.filter(col("is_airport_pickup") == True) \
                    .groupBy("pulocationid") \
                    .agg(
                        count("*").alias("pickup_count"),
                        avg("trip_distance").alias("avg_distance"),
                        avg("fare_amount").alias("avg_fare"),
                        avg("total_amount").alias("avg_total")
                    ) \
                    .orderBy(desc("pickup_count"))
                
                write_to_s3(airport_pickup_zones, f"{output_base}airport_pickup_zones/", mode="overwrite")
                print(f"✓ Wrote airport_pickup_zones to {output_base}airport_pickup_zones/")
            
            if "dolocationid" in df.columns:
                airport_dropoff_zones = airport_trips.filter(col("is_airport_dropoff") == True) \
                    .groupBy("dolocationid") \
                    .agg(
                        count("*").alias("dropoff_count"),
                        avg("trip_distance").alias("avg_distance"),
                        avg("fare_amount").alias("avg_fare"),
                        avg("total_amount").alias("avg_total")
                    ) \
                    .orderBy(desc("dropoff_count"))
                
                write_to_s3(airport_dropoff_zones, f"{output_base}airport_dropoff_zones/", mode="overwrite")
                print(f"✓ Wrote airport_dropoff_zones to {output_base}airport_dropoff_zones/")
        else:
            print("⚠ No airport trips found in dataset")
    
    print("✓ Geospatial analysis completed successfully")
    
    spark.stop()


if __name__ == "__main__":
    main()

