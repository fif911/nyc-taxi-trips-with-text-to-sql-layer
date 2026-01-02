# NYC Taxi Analytics Data Assets

A fully automated, standalone AWS data analytics pipeline for NYC Yellow Taxi data using EMR Serverless, Glue, Athena, and a Text-to-SQL interface.

## Overview

This project provides a complete infrastructure-as-code solution for:
- **Data Lake**: S3-based data lake with raw, processed, and insights layers
- **Data Processing**: EMR Serverless for PySpark-based data transformation
- **Data Catalog**: AWS Glue for automatic schema discovery
- **Query Engine**: Athena for SQL-based analytics
- **Text-to-SQL Interface**: Vanna AI web app with natural language query capabilities

## Architecture

```
┌─────────────────┐
│   S3 Data Lake  │
│  (Raw/Processed) │
└────────┬────────┘
         │
    ┌────▼────┐
    │  Glue   │
    │ Catalog │
    └────┬────┘
         │
    ┌────▼────┐      ┌──────────────┐
    │ Athena  │◄─────│   Vanna AI   │
    │         │      │  Text-to-SQL │
    └─────────┘      └──────────────┘
         │
    ┌────▼────┐
    │   EMR   │
    │Serverless│
    └─────────┘
```

## Prerequisites

1. **AWS Account** with appropriate permissions
2. **Terraform** >= 1.0 installed
3. **AWS CLI** configured with credentials
4. **LLM API Key** (OpenAI or Anthropic) for Text-to-SQL functionality

## Quick Start

### 1. Configure Variables

Copy the example file and set your values:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your configuration:

```hcl
region       = "us-east-1"
project_name = "nyc-taxi-analytics"
environment  = "dev"

llm_api_key = "sk-xxxxx"  # Your OpenAI or Anthropic API key
```

**Note**: `terraform.tfvars` is gitignored and will not be committed.

### 2. Initialize Terraform

```bash
cd terraform
terraform init
```

### 3. Review Deployment Plan

```bash
terraform plan
```

This will show all resources that will be created:
- VPC and networking (auto-created)
- S3 buckets for data lake and Athena results
- IAM roles and policies
- Glue database and crawlers
- EMR Serverless application
- Athena workgroup
- EC2 instance for Vanna AI app
- SSM Parameter Store for API key (SecureString)
- EC2 Key Pair (auto-created)

### 4. Deploy Infrastructure

```bash
terraform apply
```

Type `yes` when prompted to confirm.

## Key Pair & SSH Access

**Key pairs are automatically created by Terraform** - no manual setup required!

After deployment:
1. **Private key location**: Check the `private_key_path` output or find it at:
   ```
   terraform/key-pairs/{project_name}-{environment}.pem
   ```

2. **SSH to EC2 instance**:
   ```bash
   ssh -i terraform/key-pairs/nyc-taxi-analytics-dev.pem ec2-user@<public-ip>
   ```
   
   Get the public IP from Terraform outputs:
   ```bash
   terraform output vanna_ai_public_ip
   ```

3. **Security**: 
   - Private key files are gitignored (`.pem` files and `terraform/key-pairs/` directory)
   - Key files have restricted permissions (0400) automatically
   - Keep your private key secure and never commit it

## Accessing Resources

### Streamlit Text-to-SQL App

After deployment, access the web interface:

```bash
# Get the URL
terraform output streamlit_url

# Or get the IP and port
terraform output streamlit_public_ip
# Then visit: http://<public-ip>:8000
```

### AWS Resources

All resources are tagged with:
- `Project`: `{project_name}`
- `Environment`: `{environment}`
- `ManagedBy`: `Terraform`

Find resources in AWS Console using these tags.

### SSM Parameter Store

Configuration and secrets are stored in SSM:
- **LLM API Key**: `/nyc-taxi-analytics/{environment}/llm-api-key` (SecureString)
- **Config JSON**: `/nyc-taxi-analytics/{environment}/config` (String)

## Standalone Deployment

This project is designed for **fully standalone deployment**:

✅ **No external dependencies** - Terraform creates everything:
- VPC and networking infrastructure
- EC2 Key Pair (auto-generated)
- All AWS resources

✅ **Minimal configuration** - Only 4 variables required:
- `region`
- `project_name`
- `environment`
- `llm_api_key`

✅ **Secure by default**:
- Secrets stored in SSM Parameter Store (SecureString)
- Private keys gitignored
- IAM roles follow least-privilege principle

## Project Structure

```
.
├── terraform/              # Infrastructure as Code
│   ├── main.tf            # Main Terraform configuration
│   ├── variables.tf        # Variable definitions
│   ├── outputs.tf          # Output values
│   ├── terraform.tfvars    # Your configuration (gitignored)
│   ├── terraform.tfvars.example  # Example configuration
│   └── modules/            # Reusable modules
│       ├── s3/
│       ├── iam/
│       ├── glue/
│       ├── emr-serverless/
│       ├── athena/
│       └── vanna_ai/
├── pyspark/                # PySpark data processing jobs
│   ├── jobs/              # EMR Serverless jobs
│   └── utils/               # Common utilities
├── scripts/                # Automation scripts
│   ├── step4/              # Data ingestion
│   ├── step5/              # Glue catalog
│   ├── step7/              # EMR jobs
│   ├── step8/              # Processed data catalog
│   └── step9/              # Athena queries
└── text-to-sql/            # Vanna AI Text-to-SQL app
    ├── app.py
    ├── athena_tool.py
    └── glue_training.py
```

## Cost Optimization

The infrastructure is optimized for cost:

- **EC2**: `t3.small` instance (reduced from t3.medium)
- **EMR Serverless**: 
  - Max capacity: 50 vCPU / 100 GB (reduced from 100/200)
  - Auto-stop: 5 minutes idle timeout (reduced from 15)
- **S3 Lifecycle**: Athena results expire after 7 days
- **CloudWatch Logs**: 30-day retention for EMR/Glue logs

Override defaults in `terraform.tfvars` if needed.

## Next Steps

After infrastructure deployment:

1. **Ingest Data**: Run scripts in `scripts/step4/` to download and upload NYC taxi data
2. **Run EMR Jobs**: Execute PySpark jobs via `scripts/step7/` to process data (see Job Execution Order below)
3. **Catalog Data**: Run Glue crawlers via `scripts/step8/` to discover schemas
4. **Query Data**: Use the Vanna AI Text-to-SQL interface or Athena directly

See `scripts/README.md` for detailed script documentation.

### EMR Job Execution Order

**Important**: Jobs must be executed in the correct order due to data dependencies:

#### Phase 1: Independent Jobs
- **`create_lookup_tables.py`** - Creates reference lookup tables (payment types, vendors, taxi zones)
  - **Input**: CSV files from `data/` directory
  - **Output**: `s3://bucket/insights/lookups/`
  - **Dependencies**: None (can run first)

#### Phase 2: Data Cleaning (MUST complete before Phase 3)
- **`data_validation_cleaning.py`** - Validates and cleans raw taxi trip data
  - **Input**: `s3://bucket/raw/` (raw Parquet files)
  - **Output**: `s3://bucket/processed/trips_cleaned/` (partitioned by year/month)
  - **Dependencies**: None, but **must complete** before insight jobs
  - **Note**: This is the most resource-intensive job and should run alone

#### Phase 3: Insight Jobs (Can run in parallel after Phase 2)
These jobs can run in parallel as they all read from the cleaned data:

- **`trip_metrics_aggregation.py`** - Aggregates trip metrics by time and zone
  - **Input**: `s3://bucket/processed/trips_cleaned/`
  - **Output**: `s3://bucket/insights/trip_volume_by_*`, `trip_duration_by_*`, etc.

- **`geospatial_analysis.py`** - Analyzes pickup/dropoff zones and zone pairs
  - **Input**: `s3://bucket/processed/trips_cleaned/`
  - **Output**: `s3://bucket/insights/popular_*_zones`, `zone_pair_analysis`, etc.

- **`revenue_insights.py`** - Analyzes revenue patterns by payment type, time, vendor
  - **Input**: `s3://bucket/processed/trips_cleaned/`
  - **Output**: `s3://bucket/insights/revenue_by_*`, `tip_analysis`, etc.

#### Execution Methods

**Option 1: Automated (Recommended)**
```bash
# Run all jobs with proper dependency handling
python3 scripts/step7/3_run_all_jobs.py --skip-upload --wait
```

**Option 2: Manual Sequential**
```bash
# Phase 1
python3 scripts/step7/2_run_job.py create_lookup_tables.py --wait

# Phase 2 (must complete)
python3 scripts/step7/2_run_job.py data_validation_cleaning.py --wait

# Phase 3 (can run in parallel)
python3 scripts/step7/2_run_job.py trip_metrics_aggregation.py --wait &
python3 scripts/step7/2_run_job.py geospatial_analysis.py --wait &
python3 scripts/step7/2_run_job.py revenue_insights.py --wait &
wait  # Wait for all background jobs
```

**Why this order matters:**
- Insight jobs (Phase 3) depend on cleaned data from `data_validation_cleaning.py`
- Running all jobs in parallel can cause resource contention and OOM errors
- The cleaning job is memory-intensive and benefits from running alone

## Cleanup

To destroy all resources:

```bash
cd terraform
terraform destroy
```

**Warning**: This will delete all resources including data in S3 buckets. Make sure to backup any important data first.

## Troubleshooting

### Key Pair Issues

If you lose the private key:
1. The key pair is managed by Terraform - you can't retrieve the private key from AWS
2. You'll need to either:
   - Restore from backup
   - Destroy and recreate the EC2 instance (data will be lost)
   - Create a new key pair manually and update Terraform

### SSH Access Denied

1. Check security group allows SSH (port 22) from your IP
2. Verify key file permissions: `chmod 400 terraform/key-pairs/*.pem`
3. Use correct user: `ec2-user` for Amazon Linux 2023

### Vanna AI App Not Accessible

1. Check security group allows port 8000 from your IP
2. Verify EC2 instance is running: `terraform output vanna_ai_instance_id`
3. Check application logs: SSH to instance and run `journalctl -u text-to-sql -f`

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
