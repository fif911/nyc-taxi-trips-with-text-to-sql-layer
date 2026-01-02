# S3 Outputs
output "s3_bucket_name" {
  description = "Name of the S3 data lake bucket"
  value       = module.s3.bucket_name
}

output "s3_bucket_arn" {
  description = "ARN of the S3 data lake bucket"
  value       = module.s3.bucket_arn
}

output "athena_results_bucket_name" {
  description = "Name of the S3 bucket for Athena query results"
  value       = module.s3.athena_results_bucket_name
}

# IAM Outputs
output "emr_execution_role_arn" {
  description = "EMR Serverless Execution Role ARN"
  value       = module.iam.emr_execution_role_arn
}

output "glue_crawler_role_arn" {
  description = "Glue Crawler Role ARN"
  value       = module.iam.glue_crawler_role_arn
}

# Glue Outputs
output "glue_database_name" {
  description = "Glue Database Name"
  value       = module.glue.database_name
}

output "glue_raw_crawler_name" {
  description = "Glue Crawler for raw data"
  value       = module.glue.raw_crawler_name
}

output "glue_processed_crawler_name" {
  description = "Glue Crawler for processed data"
  value       = module.glue.processed_crawler_name
}

output "glue_insights_crawler_name" {
  description = "Glue Crawler for insights data"
  value       = module.glue.insights_crawler_name
}

# EMR Serverless Outputs
output "emr_application_id" {
  description = "EMR Serverless Application ID"
  value       = module.emr_serverless.application_id
}

output "emr_application_arn" {
  description = "EMR Serverless Application ARN"
  value       = module.emr_serverless.application_arn
}

# Athena Outputs
output "athena_workgroup_name" {
  description = "Athena Workgroup Name"
  value       = module.athena.workgroup_name
}

output "athena_query_result_location" {
  description = "S3 location for Athena query results"
  value       = module.athena.query_result_location
}

# SSM Parameter Store Output
output "ssm_config_parameter_name" {
  description = "SSM Parameter Store path for configuration JSON"
  value       = aws_ssm_parameter.config.name
}

# Vanna AI Outputs
output "vanna_ai_url" {
  description = "Vanna AI application URL"
  value       = module.vanna_ai.vanna_ai_url
}

output "vanna_ai_public_ip" {
  description = "Vanna AI EC2 public IP"
  value       = module.vanna_ai.public_ip
}

output "vanna_ai_ssh" {
  description = "SSH command to connect to Vanna AI instance"
  value       = module.vanna_ai.ssh_command
}

output "vanna_ai_instance_id" {
  description = "Vanna AI EC2 instance ID"
  value       = module.vanna_ai.instance_id
}

output "private_key_path" {
  description = "Path to the private key file for SSH access"
  value       = local_file.private_key.filename
  sensitive   = true
}

# EFS Outputs
output "efs_id" {
  description = "EFS file system ID for ChromaDB storage"
  value       = module.efs.efs_id
}

output "efs_dns_name" {
  description = "EFS DNS name for mounting"
  value       = module.efs.efs_dns_name
}

output "efs_mount_point" {
  description = "EFS mount point path"
  value       = module.efs.efs_mount_point
}

