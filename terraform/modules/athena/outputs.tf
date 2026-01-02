output "workgroup_name" {
  description = "Name of the Athena workgroup"
  value       = aws_athena_workgroup.nyc_taxi_analytics.name
}

output "query_result_location" {
  description = "S3 location for Athena query results"
  value       = "s3://${var.query_results_bucket_name}/"
}

