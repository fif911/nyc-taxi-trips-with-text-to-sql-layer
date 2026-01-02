# EFS File System for ChromaDB persistent storage
resource "aws_efs_file_system" "vanna_chromadb" {
  creation_token                  = "${var.project_name}-${var.environment}-vanna-chromadb"
  performance_mode = "generalPurpose"
  throughput_mode  = "bursting"
  # Removed provisioned_throughput_in_mibps for cost optimization
  # Bursting mode is free and sufficient for development/testing
  encrypted                       = true

  tags = merge(var.tags, {
    Name        = "${var.project_name}-${var.environment}-vanna-chromadb-efs"
    Environment = var.environment
    Project     = var.project_name
    Purpose     = "ChromaDB storage for Vanna AI"
  })

  lifecycle_policy {
    transition_to_ia = "AFTER_30_DAYS"
  }
}

# EFS Security Group - Allow NFS traffic from Vanna AI EC2 instances
resource "aws_security_group" "efs_sg" {
  name        = "${var.project_name}-${var.environment}-efs-sg"
  description = "Security group for EFS file system (NFS access)"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [var.vanna_ai_security_group_id]
    description     = "NFS access from Vanna AI EC2 instances"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }

  tags = merge(var.tags, {
    Name        = "${var.project_name}-${var.environment}-efs-sg"
    Environment = var.environment
    Project     = var.project_name
  })
}

# EFS Mount Target in the subnet
resource "aws_efs_mount_target" "vanna_chromadb" {
  file_system_id  = aws_efs_file_system.vanna_chromadb.id
  subnet_id       = var.subnet_id
  security_groups = [aws_security_group.efs_sg.id]
}
