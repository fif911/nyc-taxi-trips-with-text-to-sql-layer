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

### Step 4: Data Ingestion

- `step4/1_download_data.py` - Download NYC Yellow Taxi data from public source
- `step4/2_upload_to_s3.py` - Upload downloaded data to S3
- `step4/3_validate_upload.py` - Validate uploaded files in S3

### Step 5: Glue Catalog

- `step5/1_verify_table.py` - Verify Glue tables exist in the Data Catalog
- `step5/2_test_athena_query.py` - Test Athena queries on Glue tables

### Step 7: EMR Jobs

- `step7/1_upload_jobs.py` - Upload PySpark jobs and data files to S3
- `step7/2_run_job.py` - Submit a single job to EMR Serverless
- `step7/3_run_all_jobs.py` - Run all jobs sequentially (includes create_lookup_tables.py)

### Step 8: Catalog Processed Data

- `step8/1_execute_processed_crawlers.py` - Execute Glue crawlers (raw, processed, insights, or all)
- `step8/2_verify_processed_tables.py` - Verify processed and insights tables were created

### Step 9: Athena Query Layer

- `step9/1_run_sample_queries.py` - Run sample Athena queries on Glue tables
- `step9/2_create_views.py` - Create Athena views for common queries (optional)

## Configuration

All scripts read configuration from AWS Systems Manager Parameter Store at:
```
/nyc-taxi-analytics/{environment}/config
```

Configuration is loaded automatically using `pyspark.utils.config_loader`.

You can override any configuration value using command-line arguments (e.g., `-b` for bucket name).
