terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# EMR Serverless Application
resource "aws_emrserverless_application" "nyc_taxi_analytics" {
  name          = var.application_name
  release_label = var.release_label
  type          = "SPARK"

  initial_capacity {
    initial_capacity_type = "Driver"

    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = var.initial_driver_cpu
        memory = var.initial_driver_memory
      }
    }
  }

  initial_capacity {
    initial_capacity_type = "Executor"

    initial_capacity_config {
      worker_count = var.initial_executor_count
      worker_configuration {
        cpu    = var.initial_executor_cpu
        memory = var.initial_executor_memory
      }
    }
  }

  maximum_capacity {
    cpu    = var.max_capacity_cpu
    memory = var.max_capacity_memory
  }

  auto_start_configuration {
    enabled = false
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.autostop_idle_timeout_minutes
  }

  tags = {
    Name        = var.application_name
    Environment = var.environment
    Project     = var.project_name
  }
}

# CloudWatch Log Group for EMR Serverless with 30-day retention
resource "aws_cloudwatch_log_group" "emr_serverless" {
  name              = "/aws/emr-serverless/${var.application_name}"
  retention_in_days = 30

  tags = {
    Name        = "${var.application_name}-logs"
    Environment = var.environment
    Project     = var.project_name
  }
}
