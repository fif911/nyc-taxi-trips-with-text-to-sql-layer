variable "application_name" {
  description = "Name of the EMR Serverless application"
  type        = string
  default     = "nyc-taxi-analytics"
}

variable "release_label" {
  description = "EMR release label (e.g., emr-6.15.0)"
  type        = string
  default     = "emr-6.15.0"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "max_capacity_cpu" {
  description = "Maximum CPU capacity for EMR Serverless (e.g., '50 vCPU')"
  type        = string
  default     = "50 vCPU"
}

variable "max_capacity_memory" {
  description = "Maximum memory capacity for EMR Serverless (e.g., '100 GB')"
  type        = string
  default     = "100 GB"
}

variable "autostop_idle_timeout_minutes" {
  description = "Idle timeout in minutes before EMR Serverless auto-stops"
  type        = number
  default     = 5
}

variable "initial_driver_cpu" {
  description = "Initial driver CPU capacity (e.g., '2 vCPU')"
  type        = string
  default     = "2 vCPU"
}

variable "initial_driver_memory" {
  description = "Initial driver memory capacity (e.g., '4 GB')"
  type        = string
  default     = "4 GB"
}

variable "initial_executor_cpu" {
  description = "Initial executor CPU capacity (e.g., '4 vCPU')"
  type        = string
  default     = "4 vCPU"
}

variable "initial_executor_memory" {
  description = "Initial executor memory capacity (e.g., '8 GB')"
  type        = string
  default     = "8 GB"
}

variable "initial_executor_count" {
  description = "Initial number of executor workers"
  type        = number
  default     = 1
}
