variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 data lake bucket"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
}

# Removed glue_database_name variable - using wildcards in IAM policies instead

