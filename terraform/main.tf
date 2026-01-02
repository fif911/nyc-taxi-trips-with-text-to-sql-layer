terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
  }

  backend "local" {
    # Using local backend for now
    # Can be changed to S3 backend for team collaboration
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# S3 Module
module "s3" {
  source = "./modules/s3"

  bucket_name  = "${var.project_name}-${var.environment}-data-lake-${data.aws_caller_identity.current.account_id}"
  project_name = var.project_name
  environment  = var.environment
}

# IAM Module (depends on S3)
module "iam" {
  source = "./modules/iam"

  project_name  = var.project_name
  environment   = var.environment
  s3_bucket_arn = module.s3.bucket_arn
  region        = var.region
}

# Glue Module (depends on S3 and IAM)
module "glue" {
  source = "./modules/glue"

  database_name         = "${var.project_name}_${var.environment}_db"
  project_name          = var.project_name
  environment           = var.environment
  s3_bucket_name        = module.s3.bucket_name
  glue_crawler_role_arn = module.iam.glue_crawler_role_arn

  depends_on = [module.iam]
}

# EMR Serverless Module
module "emr_serverless" {
  source = "./modules/emr-serverless"

  application_name              = "${var.project_name}-${var.environment}"
  release_label                 = var.emr_release_label
  project_name                  = var.project_name
  environment                   = var.environment
  max_capacity_cpu              = var.emr_max_capacity_cpu
  max_capacity_memory           = var.emr_max_capacity_memory
  autostop_idle_timeout_minutes = var.emr_autostop_idle_timeout_minutes
}

# Athena Module (depends on S3 for query results bucket)
module "athena" {
  source = "./modules/athena"

  workgroup_name            = "${var.project_name}-${var.environment}-workgroup"
  query_results_bucket_name = module.s3.athena_results_bucket_name
  project_name              = var.project_name
  environment               = var.environment
}

# SSM Parameter for LLM API Key (SecureString)
resource "aws_ssm_parameter" "llm_api_key" {
  name        = "/${var.project_name}/${var.environment}/llm-api-key"
  description = "LLM API key for Text-to-SQL functionality"
  type        = "SecureString"
  value       = var.llm_api_key

  tags = {
    Name        = "${var.project_name}-${var.environment}-llm-api-key"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Vanna AI Security Group (created first so EFS can reference it)
resource "aws_security_group" "vanna_ai_sg" {
  name        = "${var.project_name}-${var.environment}-vanna-ai-sg"
  description = "Security group for Vanna AI Text-to-SQL FastAPI app"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_ssh_cidr
    description = "SSH access"
  }

  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = var.allowed_app_cidr
    description = "FastAPI app access"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }

  tags = merge(var.tags, {
    Name        = "${var.project_name}-${var.environment}-vanna-ai-sg"
    Environment = var.environment
    Project     = var.project_name
  })
}

# EFS Module for ChromaDB persistent storage
module "efs" {
  source = "./modules/efs"

  project_name               = var.project_name
  environment                = var.environment
  vpc_id                     = aws_vpc.main.id
  subnet_id                  = aws_subnet.public.id
  vanna_ai_security_group_id = aws_security_group.vanna_ai_sg.id

  tags = merge(
    {
      Environment = var.environment
    },
    var.tags
  )

  depends_on = [
    aws_vpc.main,
    aws_subnet.public,
    aws_security_group.vanna_ai_sg
  ]
}

# Vanna AI Module (depends on S3, Glue, Athena, and EFS)
module "vanna_ai" {
  source = "./modules/vanna_ai"

  project_name      = var.project_name
  environment       = var.environment
  aws_region        = var.region
  vpc_id            = aws_vpc.main.id
  subnet_id         = aws_subnet.public.id
  key_pair_name     = aws_key_pair.vanna_ai.key_name
  instance_type     = var.vanna_ai_instance_type
  root_volume_size  = var.vanna_ai_root_volume_size
  security_group_id = aws_security_group.vanna_ai_sg.id

  llm_api_key_ssm_path  = aws_ssm_parameter.llm_api_key.name
  llm_provider          = var.llm_provider
  llm_model             = var.llm_model
  athena_database       = module.glue.database_name
  athena_workgroup      = module.athena.workgroup_name
  athena_s3_staging     = module.athena.query_result_location
  athena_results_bucket = module.s3.athena_results_bucket_name
  data_bucket           = module.s3.bucket_name
  glue_database         = module.glue.database_name

  # EFS configuration
  efs_id          = module.efs.efs_id
  efs_dns_name    = module.efs.efs_dns_name
  efs_mount_point = module.efs.efs_mount_point

  allowed_ssh_cidr = var.allowed_ssh_cidr
  allowed_app_cidr = var.allowed_app_cidr

  tags = merge(
    {
      Environment = var.environment
    },
    var.tags
  )

  depends_on = [
    module.s3,
    module.glue,
    module.athena,
    module.efs,
    aws_ssm_parameter.llm_api_key,
    aws_vpc.main,
    aws_subnet.public,
    aws_internet_gateway.main,
    aws_key_pair.vanna_ai,
    aws_security_group.vanna_ai_sg
  ]
}

# Data source to get current AWS account ID
data "aws_caller_identity" "current" {}

# VPC for the project
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name        = "${var.project_name}-${var.environment}-vpc"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name        = "${var.project_name}-${var.environment}-igw"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Public Subnet
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = {
    Name        = "${var.project_name}-${var.environment}-public-subnet"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Route Table for Public Subnet
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name        = "${var.project_name}-${var.environment}-public-rt"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Route Table Association
resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Data source for availability zones
data "aws_availability_zones" "available" {
  state = "available"
}

# EC2 Key Pair for SSH access
resource "aws_key_pair" "vanna_ai" {
  key_name   = "${var.project_name}-${var.environment}-key"
  public_key = tls_private_key.vanna_ai.public_key_openssh

  tags = {
    Name        = "${var.project_name}-${var.environment}-key"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Generate private key
resource "tls_private_key" "vanna_ai" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

# Save private key locally (gitignored)
resource "local_file" "private_key" {
  content         = tls_private_key.vanna_ai.private_key_pem
  filename        = "${path.module}/key-pairs/${var.project_name}-${var.environment}.pem"
  file_permission = "0400"
}

# SSM Parameter Store - Store all configuration as JSON
resource "aws_ssm_parameter" "config" {
  name = "/${var.project_name}/${var.environment}/config"
  type = "String"
  value = jsonencode({
    environment                  = var.environment
    region                       = var.region
    project_name                 = var.project_name
    s3_bucket_name               = module.s3.bucket_name
    s3_bucket_arn                = module.s3.bucket_arn
    athena_results_bucket_name   = module.s3.athena_results_bucket_name
    emr_execution_role_arn       = module.iam.emr_execution_role_arn
    glue_crawler_role_arn        = module.iam.glue_crawler_role_arn
    glue_database_name           = module.glue.database_name
    glue_raw_crawler_name        = module.glue.raw_crawler_name
    glue_processed_crawler_name  = module.glue.processed_crawler_name
    glue_insights_crawler_name   = module.glue.insights_crawler_name
    emr_application_id           = module.emr_serverless.application_id
    emr_application_arn          = module.emr_serverless.application_arn
    athena_workgroup_name        = module.athena.workgroup_name
    athena_query_result_location = module.athena.query_result_location
  })

  description = "Configuration parameters for ${var.project_name} ${var.environment} environment"

  tags = {
    Name        = "${var.project_name}-${var.environment}-config"
    Environment = var.environment
    Project     = var.project_name
  }

  depends_on = [
    module.s3,
    module.iam,
    module.glue,
    module.emr_serverless,
    module.athena
  ]
}

