variable "project_name" {
  description = "Project name for resource naming"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, prod, etc.)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where EFS will be deployed"
  type        = string
}

variable "subnet_id" {
  description = "Subnet ID where EFS mount target will be created"
  type        = string
}

variable "vanna_ai_security_group_id" {
  description = "Security group ID of Vanna AI EC2 instances (for EFS access)"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
