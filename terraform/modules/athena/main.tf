terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Athena Workgroup (Custom)
resource "aws_athena_workgroup" "nyc_taxi_analytics" {
  name = var.workgroup_name

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.query_results_bucket_name}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }

  tags = {
    Name        = var.workgroup_name
    Environment = var.environment
    Project     = var.project_name
  }
}

# Configure Primary Workgroup (used by AWS Console by default)
# This ensures queries work in the AWS Console without manual workgroup selection
resource "aws_athena_workgroup" "primary" {
  name = "primary"

  configuration {
    enforce_workgroup_configuration    = false # Allow override for flexibility
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.query_results_bucket_name}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }

  tags = {
    Name        = "primary"
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
  }
}

