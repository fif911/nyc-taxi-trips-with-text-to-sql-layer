#!/usr/bin/env python3
"""Check EMR Serverless job logs and results."""
import sys
import gzip
import boto3
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pyspark.utils.config_loader import load_and_set_config, get_s3_bucket


def read_gzipped_s3_file(s3_client, bucket, key):
    """Read and decompress a gzipped file from S3."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return gzip.decompress(obj['Body'].read()).decode('utf-8')
    except Exception as e:
        return f"Error reading {key}: {e}"


def main():
    # Load config
    config = load_and_set_config()
    bucket_name = get_s3_bucket()
    
    # Get job run ID from command line or use latest
    job_run_id = sys.argv[1] if len(sys.argv) > 1 else None
    app_id = config.get("emr_application_id")
    
    if not job_run_id:
        print("Usage: python check_job_results.py <job_run_id>")
        print(f"Example: python check_job_results.py 00g284djp7h6000b")
        sys.exit(1)
    
    s3_client = boto3.client("s3")
    log_prefix = f"logs/emr-serverless/applications/{app_id}/jobs/{job_run_id}/"
    
    print("=" * 80)
    print("EMR SERVERLESS JOB LOGS AND RESULTS")
    print("=" * 80)
    print(f"Job Run ID: {job_run_id}")
    print(f"Application ID: {app_id}")
    print(f"Bucket: {bucket_name}")
    print()
    
    # Get stdout from driver
    print("=" * 80)
    print("SPARK DRIVER STDOUT")
    print("=" * 80)
    stdout_key = f"{log_prefix}SPARK_DRIVER/stdout.gz"
    stdout_content = read_gzipped_s3_file(s3_client, bucket_name, stdout_key)
    print(stdout_content[-5000:] if len(stdout_content) > 5000 else stdout_content)
    print()
    
    # Get stderr from driver
    print("=" * 80)
    print("SPARK DRIVER STDERR")
    print("=" * 80)
    stderr_key = f"{log_prefix}SPARK_DRIVER/stderr.gz"
    stderr_content = read_gzipped_s3_file(s3_client, bucket_name, stderr_key)
    print(stderr_content[-2000:] if len(stderr_content) > 2000 else stderr_content)
    print()
    
    # Check output files
    print("=" * 80)
    print("OUTPUT FILES IN S3")
    print("=" * 80)
    output_prefix = "processed/trips_cleaned/"
    paginator = s3_client.get_paginator("list_objects_v2")
    total_size = 0
    count = 0
    
    for page in paginator.paginate(Bucket=bucket_name, Prefix=output_prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                count += 1
                total_size += obj["Size"]
                if count <= 20:
                    print(f"  {obj['Key']} ({obj['Size']/1024/1024:.2f} MB)")
    
    print()
    print(f"Total files: {count}")
    print(f"Total size: {total_size/1024/1024:.2f} MB")
    print()
    print(f"Output location: s3://{bucket_name}/{output_prefix}")


if __name__ == "__main__":
    main()
