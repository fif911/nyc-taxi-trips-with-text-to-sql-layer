"""
Configuration loader from AWS Systems Manager Parameter Store.

This module provides functions to load configuration from SSM Parameter Store
and set them as environment variables for use in PySpark jobs.
"""
import os
import json
import boto3
from typing import Dict, Optional


def load_config_from_ssm(parameter_name: Optional[str] = None, region: Optional[str] = None) -> Dict[str, str]:
    """
    Load configuration from AWS SSM Parameter Store.
    
    Args:
        parameter_name: SSM parameter name (defaults to /nyc-taxi-analytics/{ENV}/config)
        region: AWS region (defaults to us-east-1 or AWS_DEFAULT_REGION env var)
        
    Returns:
        Dictionary of configuration values
        
    Raises:
        ClientError: If parameter is not found or access is denied
    """
    # Determine parameter name
    if parameter_name is None:
        env = os.environ.get("ENVIRONMENT", os.environ.get("ENV", "dev"))
        project_name = os.environ.get("PROJECT_NAME", "nyc-taxi-analytics")
        parameter_name = f"/{project_name}/{env}/config"
    
    # Determine region
    if region is None:
        region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    
    # Create SSM client
    ssm_client = boto3.client("ssm", region_name=region)
    
    # Get parameter
    response = ssm_client.get_parameter(Name=parameter_name)
    
    # Parse JSON value
    config_str = response["Parameter"]["Value"]
    config = json.loads(config_str)
    
    return config


def set_env_from_config(config: Dict[str, str]) -> None:
    """
    Set environment variables from configuration dictionary.
    
    Args:
        config: Configuration dictionary
    """
    for key, value in config.items():
        # Convert keys to uppercase with underscores for environment variable naming
        env_key = key.upper()
        os.environ[env_key] = str(value)


def load_and_set_config(parameter_name: Optional[str] = None, region: Optional[str] = None) -> Dict[str, str]:
    """
    Load configuration from SSM Parameter Store and set as environment variables.
    
    This is a convenience function that combines load_config_from_ssm and set_env_from_config.
    
    Args:
        parameter_name: SSM parameter name (defaults to /nyc-taxi-analytics/{ENV}/config)
        region: AWS region (defaults to us-east-1 or AWS_DEFAULT_REGION env var)
        
    Returns:
        Dictionary of configuration values
    """
    config = load_config_from_ssm(parameter_name, region)
    set_env_from_config(config)
    return config


# Common configuration keys that jobs typically need
def get_s3_bucket() -> str:
    """Get S3 bucket name from environment."""
    return os.environ.get("S3_BUCKET_NAME", "")


def get_region() -> str:
    """Get AWS region from environment."""
    return os.environ.get("REGION", os.environ.get("AWS_REGION", "us-east-1"))


def get_environment() -> str:
    """Get environment name from environment."""
    return os.environ.get("ENVIRONMENT", os.environ.get("ENV", "dev"))


def get_glue_database_name() -> str:
    """Get Glue database name from environment."""
    return os.environ.get("GLUE_DATABASE_NAME", "")


def get_emr_application_id() -> str:
    """Get EMR Serverless application ID from environment."""
    return os.environ.get("EMR_APPLICATION_ID", "")
