output "efs_id" {
  description = "EFS file system ID"
  value       = aws_efs_file_system.vanna_chromadb.id
}

output "efs_dns_name" {
  description = "EFS DNS name for mounting"
  value       = "${aws_efs_file_system.vanna_chromadb.id}.efs.${data.aws_region.current.name}.amazonaws.com"
}

data "aws_region" "current" {}

output "efs_arn" {
  description = "EFS file system ARN"
  value       = aws_efs_file_system.vanna_chromadb.arn
}

output "efs_mount_point" {
  description = "Recommended mount point path"
  value       = "/mnt/efs/vanna/chroma_db"
}
