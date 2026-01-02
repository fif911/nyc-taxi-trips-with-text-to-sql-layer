terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Glue Database
resource "aws_glue_catalog_database" "nyc_taxi_db" {
  name        = var.database_name
  description = "Database for NYC Taxi analytics data"

  parameters = {
    "classification" = "parquet"
  }

  tags = {
    Name        = var.database_name
    Environment = var.environment
    Project     = var.project_name
  }
}

# Glue Crawler for raw data
resource "aws_glue_crawler" "raw" {
  database_name = aws_glue_catalog_database.nyc_taxi_db.name
  name          = "${var.project_name}-${var.environment}-raw-crawler"
  role          = var.glue_crawler_role_arn

  s3_target {
    path = "s3://${var.s3_bucket_name}/raw/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableLevelConfiguration = 2
    }
  })

  tags = {
    Name        = "${var.project_name}-${var.environment}-raw-crawler"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Glue Crawler for processed data
resource "aws_glue_crawler" "processed" {
  database_name = aws_glue_catalog_database.nyc_taxi_db.name
  name          = "${var.project_name}-${var.environment}-processed-crawler"
  role          = var.glue_crawler_role_arn

  s3_target {
    path = "s3://${var.s3_bucket_name}/processed/trips_cleaned"
    # Exclude patterns if needed (e.g., temp files, _SUCCESS files)
    exclusions = [
      "**/_temporary/**",
      "**/_SUCCESS",
      "**/*.tmp"
    ]
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  # No configuration block needed - pointing directly at the table path
  # Glue will automatically:
  #   - Create a table named "trips_cleaned" (from the directory name)
  #   - Detect Hive-style partitions (pickup_year=2024/, pickup_month=12/)

  tags = {
    Name        = "${var.project_name}-${var.environment}-processed-crawler"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Glue Crawler for insights data
resource "aws_glue_crawler" "insights" {
  database_name = aws_glue_catalog_database.nyc_taxi_db.name
  name          = "${var.project_name}-${var.environment}-insights-crawler"
  role          = var.glue_crawler_role_arn

  s3_target {
    path = "s3://${var.s3_bucket_name}/insights/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableLevelConfiguration = 2
    }
  })

  tags = {
    Name        = "${var.project_name}-${var.environment}-insights-crawler"
    Environment = var.environment
    Project     = var.project_name
  }
}

# CloudWatch Log Groups for Glue Crawlers with 30-day retention
# Note: These log groups use standard AWS names that may be auto-created by Glue.
# If they already exist, import them: terraform import module.glue.aws_cloudwatch_log_group.glue_crawlers /aws-glue/crawlers
resource "aws_cloudwatch_log_group" "glue_crawlers" {
  name              = "/aws-glue/crawlers"
  retention_in_days = 30

  tags = {
    Name        = "${var.project_name}-glue-crawlers-logs"
    Environment = var.environment
    Project     = var.project_name
  }
}

# CloudWatch Log Group for Glue Jobs (if any are created later)
# If it already exists, import it: terraform import module.glue.aws_cloudwatch_log_group.glue_jobs /aws-glue/jobs
resource "aws_cloudwatch_log_group" "glue_jobs" {
  name              = "/aws-glue/jobs"
  retention_in_days = 30

  tags = {
    Name        = "${var.project_name}-glue-jobs-logs"
    Environment = var.environment
    Project     = var.project_name
  }
}
