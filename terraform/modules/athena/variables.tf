variable "workgroup_name" {
  description = "Name of the Athena workgroup"
  type        = string
}

variable "query_results_bucket_name" {
  description = "Name of the S3 bucket for Athena query results"
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

