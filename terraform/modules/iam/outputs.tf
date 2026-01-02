output "emr_execution_role_arn" {
  description = "ARN of the EMR Serverless execution role"
  value       = aws_iam_role.emr_serverless_execution.arn
}

output "glue_crawler_role_arn" {
  description = "ARN of the Glue crawler role"
  value       = aws_iam_role.glue_crawler.arn
}

