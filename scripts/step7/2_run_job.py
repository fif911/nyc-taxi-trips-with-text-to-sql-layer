#!/usr/bin/env python3
"""
Submit a PySpark job to EMR Serverless.

Usage:
    python3 2_run_job.py <job_script_name> [OPTIONS]
    python3 scripts/step7/2_run_job.py <job_script_name> [OPTIONS]

Arguments:
    job_script_name       Name of the job script (e.g., data_validation_cleaning.py)

Options:
    -a, --app-id ID       EMR Serverless Application ID (overrides SSM Parameter Store)
    -r, --role-arn ARN    EMR Execution Role ARN (overrides SSM Parameter Store)
    -b, --bucket BUCKET   S3 bucket name (overrides SSM Parameter Store)
    -w, --wait            Wait for job completion
    -h, --help            Show this help message

Examples:
    # From scripts/step7/ directory:
    python3 2_run_job.py data_validation_cleaning.py  # Submit job without waiting
    python3 2_run_job.py data_validation_cleaning.py --wait  # Submit and wait for completion
    python3 2_run_job.py trip_metrics_aggregation.py --wait
    python3 2_run_job.py geospatial_analysis.py --wait
    python3 2_run_job.py revenue_insights.py --wait
    python3 2_run_job.py data_validation_cleaning.py -a app-123 --bucket my-bucket  # With custom app ID and bucket
    
    # From project root:
    python3 scripts/step7/2_run_job.py data_validation_cleaning.py --wait
    python3 scripts/step7/2_run_job.py trip_metrics_aggregation.py --wait
"""
import sys
import argparse
import time
from pathlib import Path

# Add project root to path to import config_loader
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import boto3
from pyspark.utils.config_loader import load_and_set_config, get_s3_bucket, get_emr_application_id


def main():
    parser = argparse.ArgumentParser(
        description="Submit a PySpark job to EMR Serverless."
    )
    parser.add_argument("job_script", help="Name of the job script (e.g., data_validation_cleaning.py)")
    parser.add_argument("-a", "--app-id", help="EMR Serverless Application ID (overrides SSM Parameter Store)")
    parser.add_argument("-r", "--role-arn", help="EMR Execution Role ARN (overrides SSM Parameter Store)")
    parser.add_argument("-b", "--bucket", help="S3 bucket name (overrides SSM Parameter Store)")
    parser.add_argument("-w", "--wait", action="store_true", help="Wait for job completion")
    
    args = parser.parse_args()
    
    # Get config from SSM Parameter Store
    try:
        config = load_and_set_config()  # Load config and set environment variables
    except Exception as e:
        print(f"Error: Could not load config from SSM Parameter Store: {e}", file=sys.stderr)
        sys.exit(1)
    
    app_id = args.app_id or config.get("emr_application_id") or get_emr_application_id()
    role_arn = args.role_arn or config.get("emr_execution_role_arn")
    bucket_name = args.bucket or config.get("s3_bucket_name") or get_s3_bucket()
    
    if not app_id:
        print("Error: EMR Application ID not found. Please provide it with -a option.", file=sys.stderr)
        sys.exit(1)
    
    if not role_arn:
        print("Error: EMR Execution Role ARN not found. Please provide it with -r option.", file=sys.stderr)
        sys.exit(1)
    
    if not bucket_name:
        print("Error: S3 bucket name not found. Please provide it with -b option.", file=sys.stderr)
        sys.exit(1)
    
    print("Submitting job to EMR Serverless...")
    print(f"Job script: {args.job_script}")
    print(f"Application ID: {app_id}")
    print(f"Role ARN: {role_arn}")
    print(f"Bucket: {bucket_name}")
    print()
    
    # Construct paths
    entry_point = f"s3://{bucket_name}/code/pyspark/jobs/{args.job_script}"
    log_uri = f"s3://{bucket_name}/logs/emr-serverless/"
    
    print(f"Entry point: {entry_point}")
    print(f"Log URI: {log_uri}")
    print()
    
    # Initialize EMR Serverless client
    emr_client = boto3.client("emr-serverless")
    
    # Check application state and start if needed
    print("Checking EMR application state...")
    try:
        app_response = emr_client.get_application(applicationId=app_id)
        app_state = app_response["application"]["state"]
        print(f"Application state: {app_state}")
        print()
        
        if app_state != "STARTED":
            print("Application needs to be started. Starting now...")
            emr_client.start_application(applicationId=app_id)
            
            print("Waiting for application to start (this may take 1-2 minutes)...")
            wait_count = 0
            max_wait = 20
            
            while wait_count < max_wait:
                time.sleep(10)
                app_response = emr_client.get_application(applicationId=app_id)
                app_state = app_response["application"]["state"]
                wait_count += 1
                print(f"  Attempt {wait_count}: Application state is {app_state}")
                
                if app_state == "STARTED":
                    print("✓ Application started successfully")
                    break
                
                if wait_count >= max_wait:
                    print("✗ Timeout waiting for application to start", file=sys.stderr)
                    sys.exit(1)
            print()
        else:
            print("Application is already started. Proceeding with job submission...")
            print()
    except Exception as e:
        print(f"✗ Error checking application state: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Submit job
    print("Submitting job to EMR Serverless...")
    print()
    
    # Extract job name from script filename (remove .py extension)
    job_name = args.job_script.replace(".py", "")
    
    # Create job driver JSON
    spark_submit_params = (
        f"--py-files s3://{bucket_name}/code/pyspark/utils.zip "
        f"--conf spark.executor.cores=4 "
        f"--conf spark.executor.memory=8g "
        f"--conf spark.driver.cores=2 "
        f"--conf spark.driver.memory=4g"
    )
    
    job_driver = {
        "sparkSubmit": {
            "entryPoint": entry_point,
            "sparkSubmitParameters": spark_submit_params
        }
    }
    
    configuration_overrides = {
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": log_uri
            }
        }
    }
    
    try:
        response = emr_client.start_job_run(
            applicationId=app_id,
            executionRoleArn=role_arn,
            name=job_name,
            jobDriver=job_driver,
            configurationOverrides=configuration_overrides
        )
        job_run_id = response["jobRunId"]
    except Exception as e:
        print(f"✗ Failed to submit job: {e}", file=sys.stderr)
        sys.exit(1)
    
    print("✓ Job submitted successfully")
    print(f"Job Run ID: {job_run_id}")
    print()
    print(f"Monitor logs at: {log_uri}applications/{app_id}/jobs/{job_run_id}/")
    print()
    
    # Wait for completion if requested
    if args.wait:
        print("Waiting for job to complete...")
        print("Press Ctrl+C to stop waiting (job will continue running)")
        print()
        
        while True:
            try:
                response = emr_client.get_job_run(applicationId=app_id, jobRunId=job_run_id)
                state = response["jobRun"]["state"]
                
                print(f"Job state: {state}")
                
                if state == "SUCCESS":
                    print("✓ Job completed successfully")
                    sys.exit(0)
                elif state in ["FAILED", "CANCELLED"]:
                    print(f"✗ Job {state.lower()}")
                    sys.exit(1)
                elif state in ["PENDING", "SCHEDULED", "RUNNING"]:
                    time.sleep(10)
                else:
                    print(f"Unknown state: {state}")
                    time.sleep(10)
            except KeyboardInterrupt:
                print("\nStopped waiting. Job will continue running.")
                print(f"Check status with: aws emr-serverless get-job-run --application-id {app_id} --job-run-id {job_run_id}")
                sys.exit(0)
            except Exception as e:
                print(f"✗ Failed to get job state: {e}", file=sys.stderr)
                sys.exit(1)
    else:
        print("To check job status, run:")
        print(f"  aws emr-serverless get-job-run --application-id {app_id} --job-run-id {job_run_id}")


if __name__ == "__main__":
    main()
