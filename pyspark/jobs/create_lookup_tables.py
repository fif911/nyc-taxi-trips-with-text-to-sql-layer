"""
PySpark Job: Create Lookup Tables

Reads CSV lookup files and writes them as Parquet tables to S3 for use in queries.
This creates reference tables for payment_type, vendor, and taxi zones.
"""
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.common_functions import get_spark_session, write_to_s3
from utils.config_loader import load_and_set_config, get_s3_bucket


def main():
    """Main function to create lookup tables."""
    spark = get_spark_session("NYC-Taxi-Create-Lookup-Tables")
    
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
    
    # Paths
    data_s3_prefix = f"s3://{s3_bucket}/code/pyspark/data/"
    output_base = f"s3://{s3_bucket}/insights/lookups/"
    
    print("Creating lookup tables from CSV files...")
    print(f"Reading from: {data_s3_prefix}")
    print(f"Writing to: {output_base}")
    print()
    
    # 1. Payment Type Lookup
    print("Creating payment_type_lookup table...")
    try:
        payment_type_df = spark.read.option("header", "true").option("inferSchema", "true").csv(
            f"{data_s3_prefix}payment_type_lookup.csv"
        )
        print(f"  Read {payment_type_df.count()} rows")
        
        write_to_s3(payment_type_df, f"{output_base}payment_type_lookup/", mode="overwrite")
        print(f"✓ Wrote payment_type_lookup to {output_base}payment_type_lookup/")
        print()
    except Exception as e:
        print(f"✗ Failed to create payment_type_lookup: {e}", file=sys.stderr)
        sys.exit(1)
    
    # 2. Vendor Lookup
    print("Creating vendor_lookup table...")
    try:
        vendor_df = spark.read.option("header", "true").option("inferSchema", "true").csv(
            f"{data_s3_prefix}vendor_lookup.csv"
        )
        print(f"  Read {vendor_df.count()} rows")
        
        write_to_s3(vendor_df, f"{output_base}vendor_lookup/", mode="overwrite")
        print(f"✓ Wrote vendor_lookup to {output_base}vendor_lookup/")
        print()
    except Exception as e:
        print(f"✗ Failed to create vendor_lookup: {e}", file=sys.stderr)
        sys.exit(1)
    
    # 3. Taxi Zone Lookup
    print("Creating taxi_zone_lookup table...")
    try:
        taxi_zone_df = spark.read.option("header", "true").option("inferSchema", "true").csv(
            f"{data_s3_prefix}taxi_zone_lookup.csv"
        )
        print(f"  Read {taxi_zone_df.count()} rows")
        
        write_to_s3(taxi_zone_df, f"{output_base}taxi_zone_lookup/", mode="overwrite")
        print(f"✓ Wrote taxi_zone_lookup to {output_base}taxi_zone_lookup/")
        print()
    except Exception as e:
        print(f"✗ Failed to create taxi_zone_lookup: {e}", file=sys.stderr)
        sys.exit(1)
    
    print("=" * 80)
    print("✓ All lookup tables created successfully")
    print("=" * 80)
    print()
    print("Lookup tables are available at:")
    print(f"  {output_base}payment_type_lookup/")
    print(f"  {output_base}vendor_lookup/")
    print(f"  {output_base}taxi_zone_lookup/")
    print()
    print("Next step: Run the insights crawler to register these tables in Glue Data Catalog")
    
    spark.stop()


if __name__ == "__main__":
    main()

