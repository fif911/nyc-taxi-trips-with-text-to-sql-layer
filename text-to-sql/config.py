"""
Configuration for Athena Text-to-SQL Interface.

This module loads configuration from environment variables and AWS SSM Parameter Store.
Supports both .env file (for local development) and SSM Parameter Store (for production).
"""
import os
from typing import Optional

# Around line 14-19, replace the try block with:
try:
    from dotenv import load_dotenv
    from pathlib import Path
    project_root = Path(__file__).parent.parent
    env_path = project_root / '.env'
    result = load_dotenv(dotenv_path=env_path)
    print(f"DEBUG: load_dotenv result: {result}")
    print(f"DEBUG: env_path: {env_path}")
    print(f"DEBUG: LLM_API_KEY from env: {os.getenv('LLM_API_KEY')[:10] if os.getenv('LLM_API_KEY') else None}")
except ImportError:
    print("DEBUG: dotenv not installed")
    pass
except Exception as e:
    print(f"DEBUG: Exception loading .env: {e}")
    pass

# Try to load from .env file (for local development)
try:
    from dotenv import load_dotenv
    from pathlib import Path
    # Load .env from the project root directory (parent of text-to-sql/)
    project_root = Path(__file__).parent.parent
    env_path = project_root / '.env'
    load_dotenv(dotenv_path=env_path)
except ImportError:
    # python-dotenv not installed, skip .env loading
    pass
except Exception:
    # If .env file doesn't exist, continue without it
    pass

# Try to load from SSM Parameter Store (for production)
try:
    import boto3
    import json
    
    def load_config_from_ssm(parameter_name: Optional[str] = None, region: Optional[str] = None) -> dict:
        """Load configuration from AWS SSM Parameter Store."""
        if parameter_name is None:
            env = os.environ.get("ENVIRONMENT", os.environ.get("ENV", "dev"))
            project_name = os.environ.get("PROJECT_NAME", "nyc-taxi-analytics")
            parameter_name = f"/{project_name}/{env}/config"
        
        if region is None:
            region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        
        try:
            ssm_client = boto3.client("ssm", region_name=region)
            response = ssm_client.get_parameter(Name=parameter_name)
            config_str = response["Parameter"]["Value"]
            return json.loads(config_str)
        except Exception:
            # SSM not available, return empty dict
            return {}
    
    # Try to load from SSM
    ssm_config = load_config_from_ssm()
except Exception:
    ssm_config = {}


class Config:
    """Configuration for Athena Text-to-SQL"""
    
    # AWS Credentials (can be from environment, IAM role, or SSM)
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID') or ssm_config.get('aws_access_key_id')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY') or ssm_config.get('aws_secret_access_key')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1') or ssm_config.get('region', 'us-east-1')
    
    # Athena Configuration (from SSM or environment)
    # Database name follows pattern: {project_name}_{environment}_db
    # Default removed - must be set via ATHENA_DATABASE env var or SSM
    ATHENA_DATABASE = os.getenv('ATHENA_DATABASE') or ssm_config.get('glue_database_name') or os.getenv('GLUE_DATABASE')
    ATHENA_WORKGROUP = os.getenv('ATHENA_WORKGROUP') or ssm_config.get('athena_workgroup_name', 'primary')
    ATHENA_S3_STAGING = os.getenv('ATHENA_S3_STAGING') or ssm_config.get('athena_query_result_location')
    
    # Glue Data Catalog Configuration
    GLUE_DATABASE = ATHENA_DATABASE  # Same as Athena database
    GLUE_CATALOG_ID = os.getenv('GLUE_CATALOG_ID') or ssm_config.get('glue_catalog_id', None)  # Optional, defaults to account ID
    
    # LLM Configuration (OpenAI only - no Anthropic support)
    # Support multiple variable naming patterns for flexibility
    LLM_API_KEY = os.getenv('OPENAI_API_KEY') or os.getenv('LLM_API_KEY')
    
    # Always use OpenAI
    LLM_PROVIDER = 'openai'
    
    # Model defaults to GPT-5 Mini
    LLM_MODEL = os.getenv('OPENAI_MODEL') or os.getenv('LLM_MODEL') or 'gpt-5-mini'
    
    # Application Settings
    AUTO_TRAIN = os.getenv('AUTO_TRAIN', 'true').lower() == 'true'
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        required = [
            'AWS_REGION',
            'ATHENA_DATABASE',
            'ATHENA_S3_STAGING',
            'LLM_API_KEY'
        ]
        
        missing = []
        for key in required:
            value = getattr(cls, key)
            if not value:
                missing.append(key)
        
        if missing:
            raise ValueError(f"Missing required config: {', '.join(missing)}")
        
        return True

