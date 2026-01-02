# Scripts

Python scripts for NYC Taxi Analytics data pipeline operations.

## Setup

1. Create and activate virtual environment:
```bash
cd scripts
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials (via AWS CLI, environment variables, or IAM role):
```bash
aws configure
```

## Scripts

### Ingest Raw Data

Download and upload NYC Yellow Taxi data from public source to S3.

- `ingest_raw_data/1_download_data.py` - Download NYC Yellow Taxi data from public source
- `ingest_raw_data/2_upload_to_s3.py` - Upload downloaded data to S3
- `ingest_raw_data/3_validate_upload.py` - Validate uploaded files in S3

### Verify Glue Catalog

Verify Glue tables exist and test Athena queries.

- `verify_glue_catalog/1_verify_tables.py` - Verify Glue tables exist in the Data Catalog
- `verify_glue_catalog/2_test_athena_query.py` - Test Athena queries on Glue tables

### Run PySpark Jobs

Upload and execute PySpark jobs on EMR Serverless.

- `run_pyspark_jobs/1_upload_jobs.py` - Upload PySpark jobs and data files to S3
- `run_pyspark_jobs/2_run_all_jobs.py` - Run all jobs sequentially (includes create_lookup_tables.py)
- `run_pyspark_jobs/3_check_job_results.py` - Check EMR Serverless job logs and results
- `run_pyspark_jobs/run_job.py` - Helper script to run a single PySpark job (used by 2_run_all_jobs.py)

### Run Glue Crawlers

Execute Glue crawlers to catalog processed data.

- `run_glue_crawlers/1_execute_crawlers.py` - Execute Glue crawlers (raw, processed, insights, or all)
- `run_glue_crawlers/2_verify_tables.py` - Verify processed and insights tables were created
- `run_glue_crawlers/3_check_crawlers.py` - Check crawler status and results

### Test Queries

Run sample Athena queries to verify the data pipeline.

- `test_queries/1_run_sample_queries.py` - Run sample Athena queries on Glue tables

## Configuration

All scripts read configuration from AWS Systems Manager Parameter Store at:
```
/nyc-taxi-analytics/{environment}/config
```

Configuration is loaded automatically using `pyspark.utils.config_loader`.

You can override any configuration value using command-line arguments (e.g., `-b` for bucket name).
