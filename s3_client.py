import os
import boto3
import logging
from typing import List, Dict, Any
import boto3.session
from botocore.exceptions import ClientError
from config import S3_CONFIG, PAYER_CONFIGS, MAX_COPY_WORKERS
from snowflake_external_table import SnowflakeConfigFetcher # Import new class

logger = logging.getLogger(__name__)

class S3Client:
    """Enhanced S3 client for cross-account and cross-region operations"""
    def __init__(self, region_name=None):
        self.s3_client = None
        self.region = region_name or S3_CONFIG['region_name']
        self._init_s3_client()

    def _init_s3_client(self):
        """Initialize S3 client with VPC endpoint bypass and an appropriately sized connection pool."""
        try:
            session = boto3.session.Session()
            client_config = boto3.session.Config(
                s3={
                    'addressing_style': S3_CONFIG['addressing_style'],
                    'use_accelerate_endpoint': S3_CONFIG['use_accelerate_endpoint'],
                    'use_dualstack_endpoint': S3_CONFIG['use_dualstack_endpoint']
                },
                signature_version=S3_CONFIG['signature_version'],
                retries={'max_attempts': S3_CONFIG['max_attempts']},
                max_pool_connections=MAX_COPY_WORKERS
            )
            self.s3_client = session.client(
                's3',
                region_name=self.region,
                config=client_config
            )
            logger.info(f"S3 client initialized ({self.region}) with connection pool size: {MAX_COPY_WORKERS}")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {str(e)}")
            raise

    def validate_bucket_access(self, bucket_name: str) -> bool:
        """Validate bucket access"""
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Validated access to bucket: {bucket_name}")
            return True
        except Exception as e:
            logger.error(f"No access to bucket {bucket_name}: {str(e)}")
            return False

    def list_objects_with_metadata(self, bucket: str, prefix: str) -> Dict[str, Dict[str, Any]]:
        """
        Lists all objects under a prefix, returning a map of
        filename -> {'ETag': str, 'Size': int}.
        Efficiently handles pagination.
        """
        objects_map = {}
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            for page in pages:
                for obj in page.get('Contents', []):
                    filename = os.path.basename(obj['Key'])
                    if filename:
                        objects_map[filename] = {
                            'ETag': obj['ETag'].strip('"'),
                            'Size': obj['Size']
                        }
            logger.debug(f"Found {len(objects_map)} objects with metadata in s3://{bucket}/{prefix}")
        except ClientError as e:
            logger.error(f"Failed to list objects in s3://{bucket}/{prefix}: {e}")
            raise
        return objects_map

    def copy_single_file(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> bool:
        """Copy a single file with enhanced error handling"""
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key
            )
            logger.debug(f"Successfully copied: {os.path.basename(source_key)}")
            return True
        except ClientError as e:
            logger.error(f"Failed to copy s3://{source_bucket}/{source_key} to s3://{dest_bucket}/{dest_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred during copy of {source_key}: {e}")
            return False

class PayerConfigManager:
    """
    Manage payer configurations.
    Primary source: Snowflake table.
    Fallback source: Local config.py file.
    """
    def __init__(self, environment: str):
        self.environment = environment
        self.configs = {}
        self._load_configs()
        self._update_config_for_environment()

    def _load_configs(self):
        """Load payer configs from Snowflake, with a fallback to local file."""
        logger.info("Attempting to load payer configurations from Snowflake (primary source)...")
        try:
            fetcher = SnowflakeConfigFetcher(self.environment)
            snowflake_configs = fetcher.get_payer_configs()
            if snowflake_configs:
                self.configs = snowflake_configs
                logger.info(f"Successfully loaded {len(self.configs)} payer configurations from Snowflake.")
                return
            else:
                logger.warning("Snowflake returned no payer configurations. Will attempt to use local fallback.")
        except Exception as e:
            logger.error(f"Failed to load payer configurations from Snowflake: {e}. Using local fallback.")

        # Fallback to local config if Snowflake fails or returns no data
        logger.warning("Using fallback payer configurations from local config.py file.")
        self.configs = PAYER_CONFIGS.copy()
        if not self.configs:
             logger.error("CRITICAL: Fallback local configuration is also empty. No payer configs available.")


    def get_payer_config(self, payer_id: str) -> Dict[str, Any]:
        """Get a specific payer's configuration."""
        config = self.configs.get(payer_id)
        if not config:
            logger.error(f"Configuration for payer_id '{payer_id}' not found in Snowflake or local config.")
        return config

    def _update_config_for_environment(self):
        """Update configurations based on environment (prod vs non-prod)."""
        if self.environment.lower() == 'prod':
            logger.info("Updating configurations for PRODUCTION environment.")
            for payer_id, config in self.configs.items():
                if 'bucket' in config and '-nonprod' in config['bucket']:
                    config['bucket'] = config['bucket'].replace('-nonprod', '')
                    logger.debug(f"Updated bucket for {payer_id} to '{config['bucket']}' for production.")
        logger.info(f"Configurations finalized for environment: {self.environment}")