"""
PySpark Job: Revenue Insights

Analyzes revenue patterns by payment type, time of day, vendor, and congestion fees.
"""
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql.functions import col, count, avg, sum, hour, desc, when
from utils.common_functions import get_spark_session, write_to_s3
from utils.config_loader import load_and_set_config, get_s3_bucket


def main():
    """Main function to run revenue insights job."""
    spark = get_spark_session("NYC-Taxi-Revenue-Insights")
    
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
    
    # Parse pickup datetime for time-based analysis
    if "tpep_pickup_datetime" in df.columns:
        from pyspark.sql.functions import to_timestamp
        df = df.withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime")))
        df = df.withColumn("pickup_hour", hour("pickup_ts"))
    
    # 1. Revenue by payment type
    if "payment_type" in df.columns:
        print("Analyzing revenue by payment type...")
        revenue_by_payment_type = df.groupBy("payment_type") \
            .agg(
                count("*").alias("trip_count"),
                sum("fare_amount").alias("total_fare"),
                avg("fare_amount").alias("avg_fare"),
                sum("tip_amount").alias("total_tip"),
                avg("tip_amount").alias("avg_tip"),
                sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_total")
            ) \
            .orderBy(desc("total_revenue"))
        
        write_to_s3(revenue_by_payment_type, f"{output_base}revenue_by_payment_type/", mode="overwrite")
        print(f"✓ Wrote revenue_by_payment_type to {output_base}revenue_by_payment_type/")
    
    # 2. Revenue by time of day
    if "pickup_hour" in df.columns:
        print("Analyzing revenue by time of day...")
        revenue_by_time = df.groupBy("pickup_hour") \
            .agg(
                count("*").alias("trip_count"),
                sum("fare_amount").alias("total_fare"),
                avg("fare_amount").alias("avg_fare"),
                sum("tip_amount").alias("total_tip"),
                avg("tip_amount").alias("avg_tip"),
                sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_total")
            ) \
            .orderBy("pickup_hour")
        
        write_to_s3(revenue_by_time, f"{output_base}revenue_by_time/", mode="overwrite")
        print(f"✓ Wrote revenue_by_time to {output_base}revenue_by_time/")
    
    # 3. Congestion fee analysis
    if "congestion_surcharge" in df.columns or "extra" in df.columns:
        print("Analyzing congestion fee impact...")
        congestion_col = "congestion_surcharge" if "congestion_surcharge" in df.columns else "extra"
        
        congestion_fee_analysis = df.groupBy(congestion_col) \
            .agg(
                count("*").alias("trip_count"),
                sum("fare_amount").alias("total_fare"),
                avg("fare_amount").alias("avg_fare"),
                sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_total"),
                sum(congestion_col).alias("total_congestion_fee")
            ) \
            .orderBy(desc("total_congestion_fee"))
        
        write_to_s3(congestion_fee_analysis, f"{output_base}congestion_fee_analysis/", mode="overwrite")
        print(f"✓ Wrote congestion_fee_analysis to {output_base}congestion_fee_analysis/")
    else:
        print("⚠ Warning: Congestion fee column not found, skipping congestion fee analysis")
    
    # 4. Revenue by vendor (if vendor column exists)
    if "vendorid" in df.columns:
        print("Analyzing revenue by vendor...")
        revenue_by_vendor = df.groupBy("vendorid") \
            .agg(
                count("*").alias("trip_count"),
                sum("fare_amount").alias("total_fare"),
                avg("fare_amount").alias("avg_fare"),
                sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_total")
            ) \
            .orderBy(desc("total_revenue"))
        
        write_to_s3(revenue_by_vendor, f"{output_base}revenue_by_vendor/", mode="overwrite")
        print(f"✓ Wrote revenue_by_vendor to {output_base}revenue_by_vendor/")
    
    # 5. Tip analysis
    if "tip_amount" in df.columns and "total_amount" in df.columns:
        print("Analyzing tip patterns...")
        tip_analysis = df.groupBy("payment_type") \
            .agg(
                count("*").alias("trip_count"),
                sum("tip_amount").alias("total_tips"),
                avg("tip_amount").alias("avg_tip"),
                avg("total_amount").alias("avg_total"),
                avg(when(col("total_amount") > 0, (col("tip_amount") / col("total_amount")) * 100.0).otherwise(0.0)).alias("avg_tip_percentage")
            ) \
            .orderBy(desc("total_tips"))
        
        write_to_s3(tip_analysis, f"{output_base}tip_analysis/", mode="overwrite")
        print(f"✓ Wrote tip_analysis to {output_base}tip_analysis/")
    
    # 6. Tip patterns by time of day
    if "tip_amount" in df.columns and "pickup_hour" in df.columns:
        print("Analyzing tip patterns by time of day...")
        tip_by_time = df.groupBy("pickup_hour") \
            .agg(
                count("*").alias("trip_count"),
                sum("tip_amount").alias("total_tips"),
                avg("tip_amount").alias("avg_tip"),
                avg(when(col("total_amount") > 0, (col("tip_amount") / col("total_amount")) * 100.0).otherwise(0.0)).alias("avg_tip_percentage")
            ) \
            .orderBy("pickup_hour")
        
        write_to_s3(tip_by_time, f"{output_base}tip_by_time/", mode="overwrite")
        print(f"✓ Wrote tip_by_time to {output_base}tip_by_time/")
    
    print("✓ Revenue insights analysis completed successfully")
    
    spark.stop()


if __name__ == "__main__":
    main()

