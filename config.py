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
    "113288186989": {"name": "testbook", "bucket": "aws-testbook-edp-parquet-data-export-cur", "path": "testbook-edp/cur-hourly-athena-data-export-testbook-edp/data", "access_type": "SAME_ACCOUNT"},
    "195938419053": {"name": "girnar", "bucket": "aws-girnar-edp-parquet-data-export-cur", "path": "girnar-edp/cur-hourly-athena-data-export-girnar-edp/data", "access_type": "SAME_ACCOUNT"},
    "671238551718": {"name": "1mg", "bucket": "aws-1mg-edp-parquet-cur-nonprod", "path": "1mg-edp/cur-hourly-athena-data-export-1mg-edp/data", "access_type": "SAME_ACCOUNT"},
    "439282795115": {"name": "aispl-1", "bucket": "aws-aispl-1-parquet-data-export-cur", "path": "aispl-1/cur-hourly-athena-data-export-aispl-1/data", "access_type": "SAME_ACCOUNT"},
    "519933445287": {"name": "aispl-2", "bucket": "aws-aispl-2-parquet-data-export-cur", "path": "aispl-2/cur-hourly-athena-data-export-aispl-2/data", "access_type": "SAME_ACCOUNT"},
    "011645438205": {"name": "knowlarity", "bucket": "aws-knowlarity-edp-parquet-data-export-cur", "path": "knowlariity-edp/cur-hourly-athena-data-export-knowlariity-edp/data", "access_type": "SAME_ACCOUNT"},
    "741843927392": {"name": "lenskart", "bucket": "aws-lenskart-edp-parquet-data-export-cur", "path": "lenskart-edp/cur-hourly-athena-data-export-lenskart-edp/data", "access_type": "SAME_ACCOUNT"},
    "632046312110": {"name": "spinny", "bucket": "aws-spinny-edp-parquet-data-export-cur", "path": "spinny/cur-hourly-athena-data-export-spinny/data", "access_type": "SAME_ACCOUNT"},
    "494269106312": {"name": "tatasky", "bucket": "aws-tatasky-edp-parquet-data-export-cur", "path": "tatasky-edp/cur-hourly-athena-data-export-tatasky-edp/data", "access_type": "SAME_ACCOUNT"},
    "033692241534": {"name": "upgrad-aispl", "bucket": "aws-upgrad-aispl-edp-parquet-data-export-cur", "path": "upgrad-aispl/cur-hourly-athena-data-export-upgrad-aispl/data", "access_type": "SAME_ACCOUNT"},
    "674600239845": {"name": "upgrad-us", "bucket": "aws-upgrad-us-edp-parquet-data-export-cur", "path": "upgrad-us-edp/cur-hourly-athena-data-export-upgrad-us-edp/data", "access_type": "SAME_ACCOUNT"},
    "460003782465": {"name": "us-1", "bucket": "aws-us-1-parquet-data-export-cur", "path": "us-1/cur-hourly-athena-data-export-us-1/data", "access_type": "SAME_ACCOUNT"},
    "807725649461": {"name": "us-2", "bucket": "aws-us-2-parquet-data-export-cur", "path": "us-2/cur-hourly-athena-data-export-us-2/data", "access_type": "SAME_ACCOUNT"},
    "504206109722": {"name": "mobikwik", "bucket": "aws-mobikwik-aispl-edp-parquet-data-export-cur", "path": "mobikwik-edp/cur-hourly-athena-data-export-mobikwik-edp/data", "access_type": "SAME_ACCOUNT"},
    "260392007941": {"name": "dealshare", "bucket": "aws-dealshare-edp-parquet-data-export-cur", "path": "dealshare-edp/cur-hourly-athena-data-export-dealshare-edp/data", "access_type": "SAME_ACCOUNT"},
    "292075176918": {"name": "zepto-edp", "bucket": "aws-zepto-edp-parquet-data-export-cur", "path": "zepto-edp/cur-hourly-athena-data-export-zepto-edp/data", "access_type": "SAME_ACCOUNT"},
    "589249755172": {"name": "supersix-edp", "bucket": "aws-supersix-edp-parquet-data-export-cur", "path": "supersix-edp/cur-hourly-athena-data-export-supersix-edp/data", "access_type": "SAME_ACCOUNT"},
    "619872399533": {"name": "kreditbee-edp", "bucket": "aws-kreditbee-edp-parquet-data-export-cur", "path": "kreditbee-ples/cur-hourly-athena-data-export-kreditbee-ples/data", "access_type": "SAME_ACCOUNT"},
    "283556814272": {"name": "rently", "bucket": "aws-rently-parquet-data-export-cur", "path": "rently/cur-hourly-athena-data-export-rently/data", "access_type": "SAME_ACCOUNT"},
    "152230102898": {"name": "ttn-poc", "bucket": "aws-ttn-poc-parquet-data-export-cur", "path": "ttn-pocs/cur-hourly-athena-data-export-ttn-pocs/data", "access_type": "SAME_ACCOUNT"},
    "380972828295": {"name": "us-ples", "bucket": "aws-us-ples-parquet-data-export-cur", "path": "us-ples/cur-hourly-athena-data-export-us-ples/data", "access_type": "SAME_ACCOUNT"},
    "349725317764": {"name": "us-3", "bucket": "aws-us-3-parquet-data-export-cur", "path": "us-3/cur-hourly-athena-data-export-us-3/data", "access_type": "SAME_ACCOUNT"},
    "822269656649": {"name": "inc-ris", "bucket": "aws-inc-ris-parquet-data-export-cur", "path": "aws-inc-ris/cur-hourly-parquet-ttn-data-export-aws-inc-ris/data", "access_type": "SAME_ACCOUNT"},
    "661913431778": {"name": "aispl-ples", "bucket": "aws-aispl-ples-parquet-data-export-cur", "path": "aispl-ples/cur-hourly-athena-data-export-aispl-ples/data", "access_type": "SAME_ACCOUNT"},
    "382454855708": {"name": "exotel-edp", "bucket": "aws-exotel-edp-parquet-data-export-cur", "path": "exotel-edp/cur-hourly-athena-data-export-exotel-edp/data", "access_type": "SAME_ACCOUNT"},
    "500480550139": {"name": "seclore-edp", "bucket": "aws-seclore-edp-parquet-data-export-cur", "path": "seclore-edp/cur-hourly-athena-data-export-seclore-edp/data", "access_type": "SAME_ACCOUNT"},
    "103507656984": {"name": "myhealthcare-edp", "bucket": "aws-myhealthcare-edp-parquet-data-export-cur", "path": "myhealthcare-edp/cur-hourly-athena-data-export-myhealthcare-edp/data", "access_type": "SAME_ACCOUNT"},
    "249240095470": {"name": "headout-edp", "bucket": "aws-headout-edp-parquet-data-export-cur", "path": "headout-edp/cur-hourly-athena-data-export-headout-edp/data", "access_type": "SAME_ACCOUNT"},
    "898504190233": {"name": "hackerearth-edp", "bucket": "aws-hackerearth-edp-parquet-data-export-cur", "path": "hackerearth-edp/cur-hourly-athena-data-export-hackerearth-edp/data", "access_type": "SAME_ACCOUNT"},
    "455843933884": {"name": "Anarock-EDP", "bucket": "aws-anarock-edp-parquet-data-export-cur", "path": "anarock-edp/cur-hourly-athena-data-export-anarock-edp/data", "access_type": "SAME_ACCOUNT"},
    "203521699266": {"name": "Locus-EDP", "bucket": "aws-locus-edp-parquet-data-export-cur", "path": "locus-edp/cur-hourly-athena-data-export-locus-edp/data", "access_type": "SAME_ACCOUNT"},
    "834948706325": {"name": "Pratilipi-EDP", "bucket": "aws-pratilipi-edp-parquet-data-export-cur", "path": "pratilipi-edp/cur-hourly-athena-data-export-pratilipi-edp/data", "access_type": "SAME_ACCOUNT"},
    "330192110303": {"name": "FranConnect-EDP", "bucket": "aws-franconnect-edp-parquet-data-export-cur", "path": "franconnect-edp/cur-hourly-athena-data-export-franconnect-edp/data", "access_type": "SAME_ACCOUNT"},
    "023193590244": {"name": "Infinx-EDP", "bucket": "aws-infinx-edp-parquet-data-export-cur", "path": "infinx-edp/cur-hourly-athena-data-export-infinx-edp/data", "access_type": "SAME_ACCOUNT"},
    "100334561081": {"name": "Changejar-EDP", "bucket": "aws-changejar-edp-parquet-data-export-cur", "path": "changejar-edp/cur-hourly-athena-data-export-changejar-edp/data", "access_type": "SAME_ACCOUNT"},
    "692432739891": {"name": "Protium-EDP", "bucket": "aws-protium-edp-parquet-data-export-cur", "path": "protium-edp/cur-hourly-athena-data-export-protium-edp/data", "access_type": "SAME_ACCOUNT"},
    "799607440812": {"name": "FreightTiger-EDP", "bucket": "aws-freighttiger-edp-parquet-data-export-cur", "path": "freighttiger-edp/cur-hourly-athena-data-export-freighttiger-edp/data", "access_type": "SAME_ACCOUNT"},
    "767236169660": {"name": "MyGate-EDP", "bucket": "aws-mygate-edp-parquet-data-export-cur", "path": "mygate-edp/cur-hourly-athena-data-export-mygate-edp/data", "access_type": "SAME_ACCOUNT"},
    "119074166528": {"name": "TTN-OTT-BU", "bucket": "aws-us-internal-parquet-data-export-cur", "path": "mediaready/cur-hourly-athena-data-export-mediaready/data", "access_type": "SAME_ACCOUNT"},
    "533267033979": {"name": "OneAssist-EDP", "bucket": "aws-oneassist-edp-parquet-data-export-cur", "path": "oneassist-edp/cur-hourly-athena-data-export-oneassist-edp/data", "access_type": "SAME_ACCOUNT"},
    "654654346581": {"name": "UrbanPiper-EDP", "bucket": "aws-urbanpiper-edp-parquet-data-export-cur", "path": "urbanpiper-edp/cur-hourly-athena-data-export-urbanpiper-edp/data", "access_type": "SAME_ACCOUNT"},
    "058264412165": {"name": "Spyne-EDP", "bucket": "aws-spyne-edp-parquet-data-export-cur", "path": "spyne-edp/cur-hourly-athena-data-export-spyne-edp/data", "access_type": "SAME_ACCOUNT"},
    "851725549084": {"name": "CountryDelight-EDP", "bucket": "aws-countrydelight-edp-parquet-data-export-cur", "path": "countrydelight-edp/cur-hourly-athena-data-export-countrydelight-edp/data", "access_type": "SAME_ACCOUNT"},
    "533267336559": {"name": "Onclusive-EDP", "bucket": "aws-onclusive-edp-parquet-data-export-cur", "path": "onclusive-edp/cur-hourly-athena-data-export-onclusive-edp/data", "access_type": "SAME_ACCOUNT"},
    "590184145849": {"name": "Gromo-EDP", "bucket": "aws-gromo-edp-parquet-data-export-cur", "path": "gromo-edp/cur-hourly-athena-data-export-gromo-edp/data", "access_type": "SAME_ACCOUNT"},
    "339712964480": {"name": "Mason-EDP", "bucket": "aws-mason-edp-parquet-data-export-cur", "path": "mason-edp/cur-hourly-athena-data-export-mason-edp/data", "access_type": "SAME_ACCOUNT"},
    "017820705900": {"name": "Icertis-EDP", "bucket": "aws-icertis-edp-parquet-data-export-cur", "path": "icertis-edp/cur-hourly-athena-data-export-icertis-edp/data", "access_type": "SAME_ACCOUNT"},
    "767397940187": {"name": "Innthink-EDP", "bucket": "aws-innthink-edp-parquet-data-export-cur", "path": "innthink-edp/cur-hourly-athena-data-export-innthink-edp/data", "access_type": "SAME_ACCOUNT"},
    "339713081940": {"name": "Netcore-EDP", "bucket": "aws-netcore-edp-parquet-data-export-cur", "path": "netcore-edp/cur-hourly-athena-data-export-netcore-edp/data", "access_type": "SAME_ACCOUNT"},
    "867344475028": {"name": "RecruitCRM-EDP", "bucket": "aws-recruitcrm-edp-parquet-data-export-cur", "path": "recruitcrm-edp/cur-replication/data", "access_type": "SAME_ACCOUNT"},
    "495599776555": {"name": "Bidgely-EDP", "bucket": "aws-bidgely-edp-parquet-data-export-cur", "path": "bidgely-edp/cur-hourly-athena-data-export-bidgely-edp/data", "access_type": "SAME_ACCOUNT"},
    "039612881052": {"name": "VisitHealth-EDP", "bucket": "aws-visithealth-edp-parquet-data-export-cur", "path": "visithealth-edp/cur-hourly-athena-data-export-visithealth-edp/data", "access_type": "SAME_ACCOUNT"},
    "869935079364": {"name": "Zigram-EDP", "bucket": "aws-zigram-edp-parquet-data-export-cur", "path": "zigram-edp/cur-hourly-athena-data-export-zigram-edp/data", "access_type": "SAME_ACCOUNT"},
    "761018862995": {"name": "HealthKart-EDP", "bucket": "aws-healthkart-edp-parquet-data-export-cur-second", "path": "healthkart-edp/cur-hourly-athena-data-export-healthkart-edp-second/data", "access_type": "SAME_ACCOUNT"},
    "108782083343": {"name": "Scrut-EDP", "bucket": "aws-scrut-edp-parquet-data-export-cur", "path": "scrut-edp/cur-hourly-athena-data-export-scrut-edp/data", "access_type": "SAME_ACCOUNT"},
    "127214182398": {"name": "Mitsogo-EDP", "bucket": "aws-mitsogo-edp-parquet-data-export-cur", "path": "mitsogo-edp/cur-hourly-athena-data-export-mitsogo-edp/data", "access_type": "SAME_ACCOUNT"},
    "221082169201": {"name": "Lemnisk-EDP", "bucket": "aws-lemnisk-edp-parquet-data-export-cur", "path": "lemnisk-edp/cur-hourly-athena-data-export-lemnisk-edp/data", "access_type": "SAME_ACCOUNT"},
    "277707103693": {"name": "Attain-EDP", "bucket": "aws-attain-edp-parquet-data-export-cur", "path": "attain-edp/cur-hourly-athena-data-export-attain-edp/data", "access_type": "SAME_ACCOUNT"},
    "047719631542": {"name": "mFine-EDP", "bucket": "aws-mfine-edp-parquet-data-export-cur", "path": "mfine-edp/cur-hourly-athena-data-export-mfine-edp/data", "access_type": "SAME_ACCOUNT"},
    "940482451702": {"name": "Crexi-EDP", "bucket": "aws-crexi-edp-parquet-data-export-cur", "path": "crexi-edp/cur-hourly-athena-data-export-crexi-edp/data", "access_type": "SAME_ACCOUNT"},
    "571600865295": {"name": "CashKaro-EDP", "bucket": "aws-cashkaro-edp-parquet-data-export-cur", "path": "cashkaro-edp/cur-hourly-athena-data-export-cashkaro-edp/data", "access_type": "SAME_ACCOUNT"},
    "961341557716": {"name": "SysCloud-EDP", "bucket": "aws-syscloud-edp-parquet-data-export-cur", "path": "syscloud-edp/cur-hourly-athena-data-export-syscloud-edp/data", "access_type": "SAME_ACCOUNT"},
    "626635405348": {"name": "Healthians-EDP", "bucket": "aws-healthians-edp-parquet-data-export-cur", "path": "healthians-edp/cur-hourly-athena-data-export-healthians-edp/data", "access_type": "SAME_ACCOUNT"},
    "571600853246": {"name": "Convr-EDP", "bucket": "aws-convr-edp-parquet-data-export-cur", "path": "convr-edp/cur-hourly-athena-data-export-convr-edp/data", "access_type": "SAME_ACCOUNT"},
    "762233751595": {"name": "Needl-EDP", "bucket": "aws-needl-edp-parquet-data-export-cur", "path": "needl-edp/cur-hourly-athena-data-export-needl-edp/data", "access_type": "SAME_ACCOUNT"},
    "376129887374": {"name": "CK-Internal-POCs", "bucket": "aws-ck-internal-poc-parquet-data-export-cur", "path": "ck-internal-poc/cur-hourly-athena-data-export-ck-internal-poc/data", "access_type": "SAME_ACCOUNT"},
    "014498662306": {"name": "Jiffy-EDP", "bucket": "aws-jiffy-edp-parquet-data-export-cur", "path": "jiffy-edp/cur-hourly-athena-data-export-jiffy-edp/data", "access_type": "SAME_ACCOUNT"},
    "014498662767": {"name": "MPS-Admin", "bucket": "aws-mps-edp-parquet-data-export-cur", "path": "mps-edp/cur-hourly-athena-data-export-mps-edp/data", "access_type": "SAME_ACCOUNT"},
    "055922553378": {"name": "MM-EDP", "bucket": "aws-mm-edp-parquet-data-export-cur", "path": "mm-edp/cur-hourly-athena-data-export-mm-edp/data", "access_type": "SAME_ACCOUNT"}
}

# --- CloudWatch Configuration ---
CLOUDWATCH_CONFIG = {
    'namespace': 'FargateDataCopy',
    'region': os.environ.get('AWS_REGION', 'us-east-2') # Region is dynamically set by the Fargate task
}

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