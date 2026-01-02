#!/usr/bin/env python3
"""
Validate uploaded NYC Yellow Taxi Parquet files in S3.

Usage:
    python3 3_validate_upload.py [OPTIONS]
    python3 scripts/step4/3_validate_upload.py [OPTIONS]

Options:
    -y, --year YEAR      Year to validate (default: 2025)
    -b, --bucket BUCKET  S3 bucket name (overrides SSM Parameter Store)
    -h, --help           Show this help message

Examples:
    # From scripts/step4/ directory:
    python3 3_validate_upload.py                  # Validate year 2025 (default)
    python3 3_validate_upload.py --year 2024      # Validate year 2024
    python3 3_validate_upload.py --bucket my-bucket  # Validate with custom bucket
    
    # From project root:
    python3 scripts/step4/3_validate_upload.py
    python3 scripts/step4/3_validate_upload.py --year 2024 --bucket my-bucket
"""
import sys
import os
import argparse
from pathlib import Path

# Add project root to path to import config_loader
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import boto3
from pyspark.utils.config_loader import load_and_set_config, get_s3_bucket


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def main():
    parser = argparse.ArgumentParser(
        description="Validate uploaded NYC Yellow Taxi Parquet files in S3."
    )
    parser.add_argument("-y", "--year", default="2025", help="Year to validate (default: 2025)")
    parser.add_argument("-b", "--bucket", help="S3 bucket name (overrides SSM Parameter Store)")
    
    args = parser.parse_args()
    
    # Get bucket name from SSM Parameter Store or argument
    bucket_name = args.bucket
    if not bucket_name:
        try:
            load_and_set_config()  # Load config and set environment variables
            bucket_name = get_s3_bucket()
        except Exception as e:
            print(f"Error: Could not load config from SSM Parameter Store: {e}", file=sys.stderr)
            print("Please provide bucket name with -b option or ensure SSM parameter is configured.", file=sys.stderr)
            sys.exit(1)
    
    if not bucket_name:
        print("Error: Could not get bucket name. Please provide it with -b option.", file=sys.stderr)
        sys.exit(1)
    
    year = args.year
    s3_prefix = f"raw/year={year}/"
    
    print(f"Validating uploads for year {year}")
    print(f"Bucket: {bucket_name}")
    print()
    
    # Initialize S3 client
    s3_client = boto3.client("s3")
    
    # List all objects
    print("1. Verifying file count...")
    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
        if "Contents" in page:
            objects.extend(page["Contents"])
    
    file_count = len(objects)
    print(f"   Found {file_count} files")
    print()
    
    # Check file sizes
    print("2. Checking file sizes...")
    total_size = 0
    for obj in objects:
        size = obj["Size"]
        total_size += size
        print(f"   {obj['Key']}: {format_size(size)}")
    print()
    
    # Verify partitioning structure
    print("3. Verifying partitioning structure...")
    print(f"   Path structure: s3://{bucket_name}/{s3_prefix}")
    # Group by month
    months = set()
    for obj in objects:
        parts = obj["Key"].split("/")
        if len(parts) >= 3 and parts[1].startswith("year=") and parts[2].startswith("month="):
            month = parts[2].split("=")[1]
            months.add(month)
    
    print(f"   Found data for months: {sorted(months)}")
    print()
    
    # Summary
    if file_count > 0:
        print(f"✓ Validation complete. Found {file_count} files in s3://{bucket_name}/{s3_prefix}")
        print(f"  Total size: {format_size(total_size)}")
    else:
        print(f"✗ Warning: No files found in s3://{bucket_name}/{s3_prefix}")
        print("  Make sure files have been uploaded using 2_upload_to_s3.py")
        sys.exit(1)


if __name__ == "__main__":
    main()
