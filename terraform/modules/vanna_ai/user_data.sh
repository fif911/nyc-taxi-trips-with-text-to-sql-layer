#!/bin/bash
set -e

# Log all output
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
echo "Starting user-data script at $(date)"

# Update system
yum update -y

# Install required packages
yum install -y \
    amazon-efs-utils \
    python3.11 \
    python3.11-pip \
    git \
    aws-cli \
    jq

# Install EFS mount helper if not already available
if [ ! -f /sbin/mount.efs ]; then
    yum install -y amazon-efs-utils
fi

# Create EFS mount point directory
EFS_MOUNT_POINT="${efs_mount_point}"
EFS_DNS_NAME="${efs_dns_name}"

if [ -n "$EFS_DNS_NAME" ] && [ -n "$EFS_MOUNT_POINT" ]; then
    echo "Mounting EFS: $EFS_DNS_NAME to $EFS_MOUNT_POINT"
    
    # Create mount point directory
    mkdir -p "$EFS_MOUNT_POINT"
    chmod 755 "$EFS_MOUNT_POINT"
    
    # Mount EFS - try DNS first, then fallback to mount target IP
    # Get mount target IP from AWS metadata or describe-mount-targets
    MOUNT_TARGET_IP=$(aws efs describe-mount-targets --file-system-id $(echo "$EFS_DNS_NAME" | cut -d'.' -f1) --region $(curl -s http://169.254.169.254/latest/meta-data/placement/region) --query 'MountTargets[0].IpAddress' --output text 2>/dev/null)
    
    if [ -n "$MOUNT_TARGET_IP" ]; then
        echo "Using mount target IP: $MOUNT_TARGET_IP"
        mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 "$MOUNT_TARGET_IP:/" "$EFS_MOUNT_POINT" || {
            echo "Failed to mount using IP, trying DNS..."
            mount -t efs -o tls,iam "$EFS_DNS_NAME:/" "$EFS_MOUNT_POINT" || {
                echo "Failed to mount EFS with IAM, trying without IAM..."
                mount -t efs -o tls "$EFS_DNS_NAME:/" "$EFS_MOUNT_POINT"
            }
        }
    else
        # Fallback to DNS-based mount
        mount -t efs -o tls,iam "$EFS_DNS_NAME:/" "$EFS_MOUNT_POINT" || {
            echo "Failed to mount EFS, trying without IAM..."
            mount -t efs -o tls "$EFS_DNS_NAME:/" "$EFS_MOUNT_POINT"
        }
    fi
    
    # Add to fstab for persistence
    echo "$EFS_DNS_NAME:/ $EFS_MOUNT_POINT efs _netdev,tls,iam 0 0" >> /etc/fstab
    
    # Create ChromaDB directory on EFS
    mkdir -p "$EFS_MOUNT_POINT"
    chown ec2-user:ec2-user "$EFS_MOUNT_POINT"
    chmod 755 "$EFS_MOUNT_POINT"
    
    echo "✅ EFS mounted successfully at $EFS_MOUNT_POINT"
else
    echo "⚠️  EFS configuration not provided, using local storage"
    EFS_MOUNT_POINT="/opt/vanna/chroma_db"
    mkdir -p "$EFS_MOUNT_POINT"
    chown ec2-user:ec2-user "$EFS_MOUNT_POINT"
fi

# Set environment variables
export EFS_MOUNT_POINT="$EFS_MOUNT_POINT"
export AWS_REGION="${aws_region}"

# Create application directory
APP_DIR="/opt/vanna-ai"
mkdir -p "$APP_DIR"
cd "$APP_DIR"

# Download application files from S3
echo "Downloading application files from S3..."
aws s3 cp s3://${app_s3_bucket}/${app_s3_key} app.py
aws s3 cp s3://${app_s3_bucket}/vanna_ai/athena_tool.py athena_tool.py
aws s3 cp s3://${app_s3_bucket}/vanna_ai/glue_training.py glue_training.py
aws s3 cp s3://${app_s3_bucket}/vanna_ai/config.py config.py
aws s3 cp s3://${app_s3_bucket}/vanna_ai/index.html index.html

# Download chart_tool.py if it exists
aws s3 cp s3://${app_s3_bucket}/vanna_ai/chart_tool.py chart_tool.py || echo "chart_tool.py not found, skipping"

# Create Python virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install --upgrade pip
pip install \
    vanna[fastapi]>=2.0.0 \
    chromadb>=0.4.0 \
    openai>=1.0.0 \
    fastapi>=0.104.0 \
    uvicorn[standard]>=0.24.0 \
    boto3>=1.28.0 \
    pyathena>=3.0.0 \
    pandas>=2.0.0 \
    python-dotenv>=1.0.0 \
    plotly>=5.18.0

# Set up environment variables from SSM
echo "Retrieving configuration from SSM Parameter Store..."
LLM_API_KEY=$(aws ssm get-parameter --name "${llm_api_key_ssm_path}" --with-decryption --region "${aws_region}" --query 'Parameter.Value' --output text)

# Create .env file
cat > .env <<ENVFILE
AWS_REGION=${aws_region}
ATHENA_DATABASE=${athena_database}
ATHENA_WORKGROUP=${athena_workgroup}
ATHENA_S3_STAGING=${athena_s3_staging}
GLUE_DATABASE=${glue_database}
OPENAI_API_KEY=$LLM_API_KEY
LLM_PROVIDER=${llm_provider}
LLM_MODEL=${llm_model}
EFS_MOUNT_POINT=$EFS_MOUNT_POINT
ENVFILE

chmod 600 .env

# Create systemd service for Vanna AI
cat > /etc/systemd/system/vanna-ai.service <<SERVICEFILE
[Unit]
Description=Vanna AI Text-to-SQL FastAPI Application
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=$APP_DIR
Environment="PATH=$APP_DIR/venv/bin"
EnvironmentFile=$APP_DIR/.env
ExecStart=$APP_DIR/venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
SERVICEFILE

# Enable and start the service
systemctl daemon-reload
systemctl enable vanna-ai
systemctl start vanna-ai

echo "✅ Vanna AI service started"
echo "Application will be available at http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):8000"
echo "User-data script completed at $(date)"
