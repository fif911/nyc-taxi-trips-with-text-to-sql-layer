data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "vanna_ai_app" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.instance_type
  key_name               = var.key_pair_name
  iam_instance_profile   = aws_iam_instance_profile.vanna_ai_profile.name
  vpc_security_group_ids = [var.security_group_id]
  subnet_id              = var.subnet_id

  user_data = templatefile("${path.module}/user_data.sh", {
    llm_api_key_ssm_path = var.llm_api_key_ssm_path
    llm_provider         = var.llm_provider
    llm_model            = var.llm_model
    athena_database      = var.athena_database
    athena_workgroup     = var.athena_workgroup
    athena_s3_staging    = var.athena_s3_staging
    glue_database        = var.glue_database
    aws_region           = var.aws_region
    app_s3_bucket        = var.data_bucket
    app_s3_key           = aws_s3_object.vanna_ai_app.key
    efs_dns_name         = var.efs_dns_name != "" ? var.efs_dns_name : ""
    efs_mount_point      = var.efs_mount_point != "" ? var.efs_mount_point : "/opt/vanna/chroma_db"
  })

  root_block_device {
    volume_size = var.root_volume_size
    volume_type = "gp3"
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-vanna-ai-app"
  })

  depends_on = [
    aws_s3_object.vanna_ai_app,
    aws_s3_object.athena_tool,
    aws_s3_object.glue_training,
    aws_s3_object.index_html,
    aws_s3_object.config_py,
    aws_s3_object.chart_tool
  ]
}

# Upload application files to S3 for EC2 to download
resource "aws_s3_object" "vanna_ai_app" {
  bucket = var.data_bucket
  key    = "vanna_ai/app.py"
  source = "${path.root}/../text-to-sql/app.py"
  etag   = filemd5("${path.root}/../text-to-sql/app.py")

  tags = merge(var.tags, {
    Name = "${var.project_name}-text-to-sql-app.py"
  })
}

resource "aws_s3_object" "athena_tool" {
  bucket = var.data_bucket
  key    = "vanna_ai/athena_tool.py"
  source = "${path.root}/../text-to-sql/athena_tool.py"
  etag   = filemd5("${path.root}/../text-to-sql/athena_tool.py")

  tags = merge(var.tags, {
    Name = "${var.project_name}-athena-tool.py"
  })
}

resource "aws_s3_object" "glue_training" {
  bucket = var.data_bucket
  key    = "vanna_ai/glue_training.py"
  source = "${path.root}/../text-to-sql/glue_training.py"
  etag   = filemd5("${path.root}/../text-to-sql/glue_training.py")

  tags = merge(var.tags, {
    Name = "${var.project_name}-glue-training.py"
  })
}

resource "aws_s3_object" "index_html" {
  bucket = var.data_bucket
  key    = "vanna_ai/index.html"
  source = "${path.root}/../text-to-sql/index.html"
  etag   = filemd5("${path.root}/../text-to-sql/index.html")

  tags = merge(var.tags, {
    Name = "${var.project_name}-index.html"
  })
}

resource "aws_s3_object" "config_py" {
  bucket = var.data_bucket
  key    = "vanna_ai/config.py"
  source = "${path.root}/../text-to-sql/config.py"
  etag   = filemd5("${path.root}/../text-to-sql/config.py")

  tags = merge(var.tags, {
    Name = "${var.project_name}-config.py"
  })
}

resource "aws_s3_object" "chart_tool" {
  bucket = var.data_bucket
  key    = "vanna_ai/chart_tool.py"
  source = "${path.root}/../text-to-sql/chart_tool.py"
  etag   = filemd5("${path.root}/../text-to-sql/chart_tool.py")

  tags = merge(var.tags, {
    Name = "${var.project_name}-chart-tool.py"
  })
}

resource "aws_eip" "vanna_ai_eip" {
  instance = aws_instance.vanna_ai_app.id
  domain   = "vpc"

  tags = merge(var.tags, {
    Name = "${var.project_name}-vanna-ai-eip"
  })
}

