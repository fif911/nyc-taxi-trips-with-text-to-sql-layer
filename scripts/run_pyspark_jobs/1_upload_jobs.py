#!/usr/bin/env python3
"""
Upload PySpark job files to S3 for EMR Serverless execution.

Usage:
    python3 1_upload_jobs.py [OPTIONS]
    python3 scripts/run_pyspark_jobs/1_upload_jobs.py [OPTIONS]

Options:
    -b, --bucket BUCKET  S3 bucket name (overrides SSM Parameter Store)
    -h, --help           Show this help message

Examples:
    # From scripts/run_pyspark_jobs/ directory:
    python3 1_upload_jobs.py                  # Upload jobs using SSM Parameter Store config
    python3 1_upload_jobs.py --bucket my-bucket  # Upload jobs with custom bucket
    
    # From project root:
    python3 scripts/run_pyspark_jobs/1_upload_jobs.py
    python3 scripts/run_pyspark_jobs/1_upload_jobs.py --bucket my-custom-bucket
"""
import sys
import os
import argparse
import zipfile
from pathlib import Path

# Add project root to path to import config_loader
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import boto3
from pyspark.utils.config_loader import load_and_set_config, get_s3_bucket


def main():
    parser = argparse.ArgumentParser(
        description="Upload PySpark job files to S3 for EMR Serverless execution."
    )
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
    
    print("Uploading PySpark jobs to S3...")
    print(f"Bucket: {bucket_name}")
    print()
    
    # Initialize S3 client
    s3_client = boto3.client("s3")
    
    # Upload jobs directory
    jobs_dir = project_root / "pyspark" / "jobs"
    jobs_s3_prefix = "code/pyspark/jobs/"
    
    if not jobs_dir.exists():
        print(f"✗ Jobs directory not found: {jobs_dir}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Uploading jobs to: s3://{bucket_name}/{jobs_s3_prefix}")
    
    try:
        for job_file in jobs_dir.glob("*.py"):
            s3_key = f"{jobs_s3_prefix}{job_file.name}"
            s3_client.upload_file(str(job_file), bucket_name, s3_key)
            print(f"  ✓ Uploaded {job_file.name}")
        
        print("✓ Jobs uploaded successfully")
        print()
    except Exception as e:
        print(f"✗ Failed to upload jobs: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Upload data directory (CSV lookup files)
    data_dir = project_root / "pyspark" / "data"
    data_s3_prefix = "code/pyspark/data/"
    
    if data_dir.exists():
        print(f"Uploading data files to: s3://{bucket_name}/{data_s3_prefix}")
        
        try:
            for data_file in data_dir.glob("*.csv"):
                s3_key = f"{data_s3_prefix}{data_file.name}"
                s3_client.upload_file(str(data_file), bucket_name, s3_key)
                print(f"  ✓ Uploaded {data_file.name}")
            
            print("✓ Data files uploaded successfully")
            print()
        except Exception as e:
            print(f"✗ Failed to upload data files: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("⚠ Data directory not found, skipping data file upload")
        print()
    
    # Upload lookup CSV files from data/ directory
    data_dir = project_root / "data"
    lookup_files = [
        "taxi_zone_lookup.csv",
        "payment_type_lookup.csv",
        "vendor_lookup.csv"
    ]
    
    if data_dir.exists():
        for lookup_file in lookup_files:
            lookup_path = data_dir / lookup_file
            if lookup_path.exists():
                print(f"Uploading {lookup_file} to: s3://{bucket_name}/{data_s3_prefix}")
                try:
                    s3_key = f"{data_s3_prefix}{lookup_file}"
                    s3_client.upload_file(str(lookup_path), bucket_name, s3_key)
                    print(f"  ✓ Uploaded {lookup_file}")
                except Exception as e:
                    print(f"✗ Failed to upload {lookup_file}: {e}", file=sys.stderr)
                    sys.exit(1)
            else:
                print(f"⚠ {lookup_file} not found in data/ directory, skipping")
    else:
        print("⚠ data/ directory not found, skipping lookup file upload")
    
    print()
    
    # Create utils.zip
    print("Creating utils.zip...")
    utils_dir = project_root / "pyspark" / "utils"
    
    if not utils_dir.exists():
        print(f"✗ Utils directory not found: {utils_dir}", file=sys.stderr)
        sys.exit(1)
    
    utils_zip_path = project_root / "utils.zip"
    
    # Remove old zip if exists
    if utils_zip_path.exists():
        utils_zip_path.unlink()
    
    # Create zip with correct structure (utils/ folder inside)
    try:
        with zipfile.ZipFile(utils_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file_path in utils_dir.rglob("*"):
                if file_path.is_file():
                    # Create archive path with utils/ prefix
                    arcname = f"utils/{file_path.relative_to(utils_dir)}"
                    zipf.write(file_path, arcname)
        
        print("✓ Created utils.zip with correct structure")
    except Exception as e:
        print(f"✗ Failed to create utils.zip: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Upload utils.zip to S3
    utils_zip_s3_key = "code/pyspark/utils.zip"
    print(f"Uploading utils.zip to: s3://{bucket_name}/{utils_zip_s3_key}")
    
    try:
        s3_client.upload_file(str(utils_zip_path), bucket_name, utils_zip_s3_key)
        print("✓ Utils.zip uploaded successfully")
        
        # Clean up local zip file
        utils_zip_path.unlink()
    except Exception as e:
        print(f"✗ Failed to upload utils.zip: {e}", file=sys.stderr)
        if utils_zip_path.exists():
            utils_zip_path.unlink()
        sys.exit(1)
    
    print()
    print("✓ All PySpark files uploaded successfully to S3")
    print()
    print("Files are now available at:")
    print(f"  Jobs: s3://{bucket_name}/{jobs_s3_prefix}")
    print(f"  Utils: s3://{bucket_name}/{utils_zip_s3_key}")


if __name__ == "__main__":
    main()
