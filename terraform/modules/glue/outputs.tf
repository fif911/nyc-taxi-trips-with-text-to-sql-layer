output "database_name" {
  description = "Name of the Glue database"
  value       = aws_glue_catalog_database.nyc_taxi_db.name
}

output "raw_crawler_name" {
  description = "Name of the Glue crawler for raw data"
  value       = aws_glue_crawler.raw.name
}

output "processed_crawler_name" {
  description = "Name of the Glue crawler for processed data"
  value       = aws_glue_crawler.processed.name
}

output "insights_crawler_name" {
  description = "Name of the Glue crawler for insights data"
  value       = aws_glue_crawler.insights.name
}

