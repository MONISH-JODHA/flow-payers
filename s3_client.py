
import os
import boto3
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import boto3.session
from botocore.exceptions import ClientError
from config import S3_CONFIG, PAYER_CONFIGS, MAX_COPY_WORKERS
from snowflake_external_table import SnowflakeConfigFetcher

logger = logging.getLogger(__name__)

class S3Client:
    """Enhanced S3 client for cross-account and cross-region operations"""
    def __init__(self, region_name=None):
        self.region = region_name or S3_CONFIG.get('region_name', 'us-east-2')
        self.s3_client = None
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
            self.s3_client = session.client('s3', region_name=self.region, config=client_config)
            logger.info(f"S3 client initialized for region '{self.region}' with connection pool size: {MAX_COPY_WORKERS}")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {str(e)}")
            raise

    def can_access_bucket(self, bucket_name: str) -> bool:
        """Checks if the role has s3:ListBucket permission on a bucket."""
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.debug(f"Access to bucket '{bucket_name}' confirmed.")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] in ('403', 'AccessDenied'):
                logger.warning(f"Access DENIED for bucket '{bucket_name}'.")
            else:
                logger.error(f"Error checking access for bucket '{bucket_name}': {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking bucket access for '{bucket_name}': {e}")
            return False

    def list_objects_with_metadata(self, bucket: str, prefix: str, since: Optional[datetime] = None) -> Dict[str, Dict[str, Any]]:
        """
        Lists all objects under a prefix, returning a map of full_key -> {'ETag', 'Size'}.
        Optionally, only returns objects modified *since* a given datetime.
        """
        objects_map = {}
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            for page in pages:
                for obj in page.get('Contents', []):
                    if since and obj['LastModified'] <= since:
                        continue 
                    
                    full_key = obj['Key']
                    if full_key and not full_key.endswith('/'): 
                        objects_map[full_key] = {
                            'ETag': obj['ETag'].strip('"'),
                            'Size': obj['Size'],
                            'LastModified': obj['LastModified']
                        }
            if since:
                logger.debug(f"Found {len(objects_map)} objects modified since {since} in s3://{bucket}/{prefix}")
            else:
                logger.debug(f"Found {len(objects_map)} objects with metadata in s3://{bucket}/{prefix}")
        except ClientError as e:
            logger.error(f"Failed to list objects in s3://{bucket}/{prefix}: {e}")
            raise
        return objects_map

    def copy_single_file(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> bool:
        """Copy a single file with enhanced error handling."""
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.s3_client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
            logger.debug(f"Successfully copied: {os.path.basename(source_key)}")
            return True
        except ClientError as e:
            logger.error(f"Failed to copy s3://{source_bucket}/{source_key} to s3://{dest_bucket}/{dest_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred during copy of {source_key}: {e}")
            return False


    def delete_objects_by_prefix(self, bucket: str, prefix: str) -> bool:
        """
        Deletes all objects under a given prefix in an S3 bucket.

        Args:
            bucket (str): The name of the S3 bucket.
            prefix (str): The prefix to delete objects from.

        Returns:
            bool: True if deletion was successful or no objects were found, False otherwise.
        """
        logger.warning(f"Preparing to delete all objects under prefix: s3://{bucket}/{prefix}")
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            
            objects_to_delete = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects_to_delete.append({'Key': obj['Key']})

            if not objects_to_delete:
                logger.info(f"No objects found to delete under prefix: s3://{bucket}/{prefix}")
                return True

            logger.info(f"Found {len(objects_to_delete)} objects to delete.")
            
            for i in range(0, len(objects_to_delete), 1000):
                chunk = objects_to_delete[i:i + 1000]
                response = self.s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': chunk, 'Quiet': True}
                )
                if 'Errors' in response:
                    logger.error(f"Errors occurred during S3 object deletion: {response['Errors']}")
                    return False

            logger.info(f"Successfully deleted all objects under prefix: s3://{bucket}/{prefix}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete objects from s3://{bucket}/{prefix}: {e}", exc_info=True)
            return False
        
class PayerConfigManager:
    """
    Manages payer configurations.
    Primary source: Snowflake table.
    Fallback source: Local config.py file.
    """
    def __init__(self, environment: str):
        self.environment = environment
        self.snowflake_configs = {}
        self.fallback_configs = PAYER_CONFIGS.copy()
        self._load_snowflake_configs()

    def _load_snowflake_configs(self):
        """Load payer configs from Snowflake, with a fallback to local file."""
        logger.info("Attempting to load payer configurations from Snowflake (primary source)...")
        try:
            fetcher = SnowflakeConfigFetcher(self.environment)
            self.snowflake_configs = fetcher.get_payer_configs()
            logger.info(f"SUCCESS: Loaded {len(self.snowflake_configs)} payer configurations from Snowflake.")
        except Exception as e:
            logger.error(f"Failed to load configs from Snowflake: {e}. Will rely solely on local fallback.", exc_info=True)
            self.snowflake_configs = {}

    def get_payer_config(self, payer_id: str) -> Dict[str, Any]:
        """
        Gets a specific payer's configuration from Snowflake first.
        If not found, it tries the local fallback config.
        """
        # Try primary source first
        config = self.snowflake_configs.get(payer_id)
        if config:
            logger.debug(f"Using Snowflake configuration for payer '{payer_id}'.")
            return self._finalize_config(config)
        
        # Try fallback source
        config = self.fallback_configs.get(payer_id)
        if config:
            logger.warning(f"Payer '{payer_id}' not in Snowflake configs. Using local fallback configuration.")
            return self._finalize_config(config)
            
        logger.error(f"CRITICAL: Configuration for payer_id '{payer_id}' not found in Snowflake OR local config.")
        return None

    def get_fallback_config(self, payer_id: str) -> Dict[str, Any]:
        """Explicitly gets the fallback configuration for a payer."""
        config = self.fallback_configs.get(payer_id)
        if config:
            logger.warning(f"Explicitly using local FALLBACK configuration for payer '{payer_id}'.")
            return self._finalize_config(config)
        logger.error(f"CRITICAL: Fallback configuration for payer_id '{payer_id}' not found.")
        return None

    def _finalize_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Applies environment-specific transformations to a config dictionary."""
        if self.environment.lower() == 'prod':
            if 'bucket' in config and '-nonprod' in config['bucket']:
                original_bucket = config['bucket']
                config['bucket'] = original_bucket.replace('-nonprod', '')
                logger.debug(f"Updated bucket for prod: '{original_bucket}' -> '{config['bucket']}'")
        return config