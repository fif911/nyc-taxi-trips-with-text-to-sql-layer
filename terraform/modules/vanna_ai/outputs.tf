output "instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.vanna_ai_app.id
}

output "public_ip" {
  description = "Public IP address"
  value       = aws_eip.vanna_ai_eip.public_ip
}

output "vanna_ai_url" {
  description = "Vanna AI application URL"
  value       = "http://${aws_eip.vanna_ai_eip.public_ip}:8000"
}

output "ssh_command" {
  description = "SSH command to connect"
  value       = "ssh -i your-key.pem ec2-user@${aws_eip.vanna_ai_eip.public_ip}"
}

output "security_group_id" {
  description = "Security group ID"
  value       = var.security_group_id
}

