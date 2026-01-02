variable "database_name" {
  description = "Name of the Glue database"
  type        = string
}

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 data lake bucket"
  type        = string
}

variable "glue_crawler_role_arn" {
  description = "ARN of the Glue crawler IAM role"
  type        = string
}

