import os
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Core Application Constants ---
DEFAULT_ENVIRONMENT = "uat"
DEFAULT_APP = "aws_az_analytics_application_refresh"
DEFAULT_MODULE = "analytics"
DEFAULT_PROCESSING_MODE = "production"
MAX_COPY_WORKERS = 100

# --- Environment-Specific Base Configurations ---
NON_PROD_STAGING_BUCKET = "ck-data-pipeline-stage-bucket-airflow"
PROD_STAGING_BUCKET = "ck-data-pipeline-new-master-staging"

# S3 Configuration
S3_CONFIG = {
    'addressing_style': 'path',
    'use_accelerate_endpoint': False,
    'use_dualstack_endpoint': False,
    'signature_version': 's3v4',
    'max_attempts': 3
}

# --- RabbitMQ Configuration ---
# BEST PRACTICE: These credentials should be moved to a secrets manager (e.g., AWS Secrets Manager)
# and retrieved at runtime, not stored in source code.
RABBITMQ_CONFIG = {
    'prod': {
        'host': 'b-77fd0f83-6a7a-467d-a0a8-0f3c39997ac7.mq.us-east-1.amazonaws.com',
        'username': 'cloudomonic-prod-user',
        'password': '82HCh6cL9a31', 'port': 5671
    },
    'non_prod': {
        'host': 'b-8dbfd2e0-0cca-43b7-bc79-b6ffc3d878eb.mq.us-east-1.amazonaws.com',
        'username': 'cloudonomic_app', 'password': '61DZZT631Uq4', 'port': 5671
    },
    'exchange_name': "payer_refresh_exchange",
    'routing_key_base': "*"
}

VHOST_MAPPING = {
    'dev': 'dev2', 'dev2': 'dev2', 'uat': 'cloudonomic_uat',
    'qa1': 'qa1', 'prod': 'cloudonomic_prod'
}

# --- Snowflake Configuration ---
# BEST PRACTICE: These credentials should also be in a secrets manager.
SNOWFLAKE_CONFIG = {
    'user': 'payer_uat_jenkins', 'password': 'R8xVD7w!8A',
    'account': 'tmb05570.us-east-1', 'warehouse': 'PAYER_UAT_SUMMARY_ETL',
    'database': 'payer_uat', 'schema': 'payer_analytics_summary',
    'role': 'ROLE_UAT_PAYER_SUMMARY_ETL',
    'config_table': 'payers_buckets_path'
}

# --- FALLBACK Payer Configurations ---
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
                "access_type": "SAME_ACCOUNT"}
}

# --- CloudWatch Configuration ---
CLOUDWATCH_CONFIG = {
    'namespace': 'FargateDataCopy',
    'region': os.environ.get('AWS_REGION', 'us-east-2') # Region is dynamically set by the Fargate task
}

# --- START OF CHANGE ---
# --- Slack Notification Configuration ---
# BEST PRACTICE: These webhook URLs are secrets and should NOT be stored in source code.
# They should be stored in AWS Secrets Manager and loaded into the Fargate task's
# environment variables at runtime. The code reads from environment variables first.
SLACK_CONFIG = {
    'channel_prod_success': 'cln-data-notification-refresh-analytics-prod',
    'webhook_url_prod_success': os.environ.get('SLACK_WEBHOOK_URL_PROD_SUCCESS', 'https://hooks.slack.com/services/YOUR/PLACEHOLDER/URL_PROD_SUCCESS'),
    
    'channel_prod_failure': 'cln-data-notification-failures-prod',
    'webhook_url_prod_failure': os.environ.get('SLACK_WEBHOOK_URL_PROD_FAILURE', 'https://hooks.slack.com/services/YOUR/PLACEHOLDER/URL_PROD_FAILURE'),

    'channel_non_prod': 'sec',
    'webhook_url_non_prod': os.environ.get('SLACK_WEBHOOK_URL_NON_PROD', 'https://hooks.slack.com/services/YOUR/PLACEHOLDER/URL_NON_PROD')
}
# --- END OF CHANGE ---

def get_environment_config(environment: str) -> Dict[str, Any]:
    """
    Get environment-specific configuration for S3 and other services.
    This function is central to making the application environment-aware.
    """
    env_lower = environment.lower()
    
    # Start with non-prod defaults
    config = {
        'environment': env_lower,
        'vhost': VHOST_MAPPING.get(env_lower, 'dev2'), # Default to dev2 vhost if env not in mapping
        'staging_bucket': NON_PROD_STAGING_BUCKET,
        's3_region': 'us-east-2',
        'rabbitmq_creds': RABBITMQ_CONFIG['non_prod']
    }

    # Environment-specific overrides for Production
    if env_lower == 'prod':
        config.update({
            'staging_bucket': PROD_STAGING_BUCKET,
            's3_region': 'us-east-1',
            'rabbitmq_creds': RABBITMQ_CONFIG['prod']
        })
        
    logger.info(f"Loaded configuration for environment: '{env_lower}'")
    logger.info(f"  > S3 Region: {config['s3_region']}")
    logger.info(f"  > Staging Bucket: {config['staging_bucket']}")
    logger.info(f"  > RabbitMQ Host: {config['rabbitmq_creds']['host']}")
    logger.info(f"  > RabbitMQ VHost: {config['vhost']}")

    return config