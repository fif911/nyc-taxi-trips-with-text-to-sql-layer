output "application_id" {
  description = "ID of the EMR Serverless application"
  value       = aws_emrserverless_application.nyc_taxi_analytics.id
}

output "application_arn" {
  description = "ARN of the EMR Serverless application"
  value       = aws_emrserverless_application.nyc_taxi_analytics.arn
}

