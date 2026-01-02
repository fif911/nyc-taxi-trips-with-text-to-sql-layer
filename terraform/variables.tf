variable "region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "nyc-taxi-analytics"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "emr_release_label" {
  description = "EMR release label (e.g., emr-6.15.0)"
  type        = string
  default     = "emr-6.15.0"
}

variable "vanna_ai_instance_type" {
  description = "EC2 instance type for Vanna AI"
  type        = string
  default     = "t3.small"
}

variable "vanna_ai_root_volume_size" {
  description = "Root volume size in GB for Vanna AI EC2 instance (minimum 30GB for Amazon Linux 2023 AMI)"
  type        = number
  default     = 30
}

variable "llm_api_key" {
  description = "LLM API key (OpenAI or Anthropic)"
  type        = string
  sensitive   = true
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


variable "allowed_ssh_cidr" {
  description = "CIDR blocks for SSH access"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "allowed_app_cidr" {
  description = "CIDR blocks for Vanna AI access"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "tags" {
  description = "Additional tags to apply to resources"
  type        = map(string)
  default     = {}
}

variable "emr_max_capacity_cpu" {
  description = "Maximum CPU capacity for EMR Serverless (e.g., '150 vCPU')"
  type        = string
  default     = "150 vCPU"
}

variable "emr_max_capacity_memory" {
  description = "Maximum memory capacity for EMR Serverless (e.g., '300 GB')"
  type        = string
  default     = "300 GB"
}

variable "emr_autostop_idle_timeout_minutes" {
  description = "Idle timeout in minutes before EMR Serverless auto-stops"
  type        = number
  default     = 5
}
