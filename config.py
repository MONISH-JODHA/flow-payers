import os
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_ENVIRONMENT = "uat"
STAGING_BUCKET = "ck-data-pipeline-stage-bucket-airflow"
DEFAULT_APP = "aws_az_analytics_application_refresh"
DEFAULT_MODULE = "analytics"
DEFAULT_PROCESSING_MODE = "production"
MAX_COPY_WORKERS = 100

# S3 Configuration
S3_CONFIG = {
    'region_name': 'us-east-2',
    'addressing_style': 'path',
    'use_accelerate_endpoint': False,
    'use_dualstack_endpoint': False,
    'signature_version': 's3v4',
    'max_attempts': 3
}

# RabbitMQ Configuration
RABBITMQ_CONFIG = {
    'host': os.environ.get('RABBITMQ_HOST', 'b-77fd0f83-6a7a-467d-a0a8-0f3c39997ac7.mq.us-east-1.amazonaws.com'),
    'username': os.environ.get('RABBITMQ_USERNAME', 'cloudomonic-prod-user'),
    'password': os.environ.get('RABBITMQ_PASSWORD', '82HCh6cL9a31'),
    'exchange_name': "payer_refresh_exchange",
    'routing_key_base': "refresh.notification",
    'port': 5671
}

# Virtual Host Mapping
VHOST_MAPPING = {
    'dev': 'dev2',
    'dev2': 'dev2',
    'uat': 'cloudonomic_uat',
    'qa1': 'qa1',
    'prod': 'cloudonomic_prod' 
}

# Payer Configurations
PAYER_CONFIGS = {
    "671238551718": {
                "name": "1mg",
                "bucket": "aws-1mg-edp-parquet-cur-nonprod",
                "path": "1mg-edp/cur-hourly-athena-data-export-1mg-edp/data",
                "access_type": "SAME_ACCOUNT"
            },
            "519933445287": {  # AISPL-2
                "name": "aispl-2",
                "bucket": "aws-aispl-2-parquet-cur-nonprod",
                "path": "aispl-2/cur-hourly-athena-data-export-aispl-2/data",
                "access_type": "SAME_ACCOUNT"
            },
            "113288186989": {  # Testbook
                "name": "testbook",
                "bucket": "aws-testbook-edp-parquet-cur-nonprod",
                "path": "testbook-edp/cur-hourly-athena-data-export-testbook-edp/data",
                "access_type": "SAME_ACCOUNT"
            },
            "741843927392": {  # Lenskart
                "name": "lenskart",
                "bucket": "aws-lenskart-edp-parquet-cur-nonprod",
                "path": "lenskart-edp/cur-hourly-athena-data-export-lenskart-edp/data",
                "access_type": "SAME_ACCOUNT"
            },
            "455843933884": {  # Anarock (uses lenskart bucket)
                "name": "anarock",
                "bucket": "aws-lenskart-edp-parquet-cur-nonprod",
                "path": "lenskart-edp/cur-hourly-athena-data-export-lenskart-edp/data",
                "access_type": "SAME_ACCOUNT"
            },
            "460003782465": {  # US1
                "name": "us-1",
                "bucket": "aws-us-1-parquet-cur-nonprod",
                "path": "us-1/cur-hourly-athena-data-export-us-1/data",
                "access_type": "SAME_ACCOUNT"
            },
            "807725649461": {  # US2
                "name": "us-2",
                "bucket": "aws-us-2-parquet-cur-nonprod",
                "path": "us-2/cur-hourly-athena-data-export-us-2/data",
                "access_type": "SAME_ACCOUNT"
            }
}




# CloudWatch Configuration
CLOUDWATCH_CONFIG = {
    'namespace': 'FargateDataCopy',
    'region': 'us-east-2'
}

def get_environment_config(environment: str) -> Dict[str, Any]:
    """Get environment-specific configuration"""
    config = {
        'environment': environment,
        'vhost': VHOST_MAPPING.get(environment.lower(), 'dev2'),
        'staging_bucket': STAGING_BUCKET
    }
    
    # Environment-specific overrides
    if environment.lower() == 'prod':
        config.update({
            'staging_bucket': "ck-data-pipeline-new-master-staging",
            's3_region': 'us-east-1'
        })
    
    return config
