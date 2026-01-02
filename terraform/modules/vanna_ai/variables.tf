variable "project_name" {
  description = "Project name for resource naming"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where EC2 instance will be deployed"
  type        = string
}

variable "subnet_id" {
  description = "Subnet ID where EC2 instance will be deployed"
  type        = string
}

variable "key_pair_name" {
  description = "EC2 key pair name"
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "t3.small"
}

variable "root_volume_size" {
  description = "Root volume size in GB for EC2 instance"
  type        = number
  default     = 20
}

variable "allowed_ssh_cidr" {
  description = "CIDR blocks allowed to SSH"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "allowed_app_cidr" {
  description = "CIDR blocks allowed to access Vanna AI"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "llm_api_key_ssm_path" {
  description = "SSM Parameter Store path for LLM API key"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, prod, etc.)"
  type        = string
}

variable "llm_provider" {
  description = "LLM provider: 'openai' or 'anthropic'"
  type        = string
  default     = "openai"
}

variable "llm_model" {
  description = "LLM model name (optional, will use provider defaults if not specified)"
  type        = string
  default     = ""
}

variable "athena_database" {
  description = "Athena database name"
  type        = string
}

variable "athena_s3_staging" {
  description = "S3 path for Athena results"
  type        = string
}

variable "athena_workgroup" {
  description = "Athena workgroup name"
  type        = string
  default     = "primary"
}

variable "athena_results_bucket" {
  description = "S3 bucket name for Athena results (without s3://)"
  type        = string
}

variable "data_bucket" {
  description = "S3 bucket name for data source"
  type        = string
}

variable "glue_database" {
  description = "Glue database name"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}

variable "security_group_id" {
  description = "Security group ID for EC2 instance (created in main.tf)"
  type        = string
}

variable "efs_id" {
  description = "EFS file system ID (optional, for reference)"
  type        = string
  default     = ""
}

variable "efs_dns_name" {
  description = "EFS DNS name for mounting (required if using EFS)"
  type        = string
  default     = ""
}

variable "efs_mount_point" {
  description = "EFS mount point path (default: /mnt/efs/vanna/chroma_db)"
  type        = string
  default     = "/mnt/efs/vanna/chroma_db"
}

