import os
import boto3
import logging
from typing import List, Dict, Any
import boto3.session
from botocore.exceptions import ClientError
# Import the MAX_COPY_WORKERS constant to use it for the connection pool
from config import S3_CONFIG, PAYER_CONFIGS, MAX_COPY_WORKERS
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
            # Create a Boto3 configuration object to pass to the client
            client_config = boto3.session.Config(
                s3={
                    'addressing_style': S3_CONFIG['addressing_style'],
                    'use_accelerate_endpoint': S3_CONFIG['use_accelerate_endpoint'],
                    'use_dualstack_endpoint': S3_CONFIG['use_dualstack_endpoint']
                },
                signature_version=S3_CONFIG['signature_version'],
                retries={'max_attempts': S3_CONFIG['max_attempts']},
                # --- THIS IS THE FIX ---
                # Match the connection pool size to the number of worker threads to avoid exhaustion.
                max_pool_connections=MAX_COPY_WORKERS
            )
            # Initialize the client with the custom configuration
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
            logger.info(f" Validated access to bucket: {bucket_name}")
            return True
        except Exception as e:
            logger.error(f" No access to bucket {bucket_name}: {str(e)}")
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
                    # Use os.path.basename to reliably get the final file part of the key
                    filename = os.path.basename(obj['Key'])
                    if filename: # Ensure it's a file and not the folder itself
                        objects_map[filename] = {
                            'ETag': obj['ETag'].strip('"'), # ETag can be quoted
                            'Size': obj['Size']
                        }
            logger.debug(f"Found {len(objects_map)} objects with metadata in s3://{bucket}/{prefix}")
        except ClientError as e:
            logger.error(f"Failed to list objects in s3://{bucket}/{prefix}: {e}")
            raise
        return objects_map
    def get_files_from_s3_folder(self, bucket_name: str, folder_prefix: str) -> List[str]:
        """Get all files from S3 folder"""
        try:
            logger.info(f"Scanning folder: s3://{bucket_name}/{folder_prefix}")
            files = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        file_key = obj['Key']
                        if not file_key.endswith('/') and file_key != folder_prefix:
                            files.append(file_key)
                            logger.debug(f"   Found: {file_key}")
            logger.info(f"Total files found: {len(files)}")
            return files
        except Exception as e:
            logger.error(f"Error scanning folder: {str(e)}")
            raise
    def copy_single_file(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> bool:
        """Copy a single file with enhanced error handling and cross-region support"""
        try:
            # Verify source file exists
            try:
                source_obj = self.s3_client.head_object(Bucket=source_bucket, Key=source_key)
            except self.s3_client.exceptions.NoSuchKey:
                logger.error(f"Source file does not exist: s3://{source_bucket}/{source_key}")
                return False
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            # Try the copy operation with enhanced error handling
            try:
                self.s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=dest_bucket,
                    Key=dest_key
                )
            except ClientError as copy_error:
                # If VPC endpoint error, try with a different approach
                if "VPC endpoints do not support cross-region requests" in str(copy_error):
                    logger.warning(f"VPC endpoint cross-region issue for {source_key}, trying alternative method...")
                    try:
                        # Create a new S3 client without specific VPC-related configs for this one-off operation
                        alt_s3_client = boto3.client(
                            's3', config=boto3.session.Config(signature_version='s3v4')
                        )
                        alt_s3_client.copy_object(
                            CopySource=copy_source, Bucket=dest_bucket, Key=dest_key
                        )
                        logger.info(f"Cross-region copy successful for {source_key} using alternative method.")
                    except Exception as alt_error:
                        logger.error(f"Alternative copy method also failed for {source_key}: {alt_error}")
                        raise copy_error # Re-raise the original error
                else:
                    raise copy_error # Re-raise other copy errors
            # Verify the copy
            try:
                dest_obj = self.s3_client.head_object(Bucket=dest_bucket, Key=dest_key)
                if dest_obj['ContentLength'] == source_obj['ContentLength']:
                    logger.debug(f"Successfully copied: {os.path.basename(source_key)} ({source_obj['ContentLength']} bytes)")
                    return True
                else:
                    logger.error(f"File size mismatch after copy: {source_key} (Source: {source_obj['ContentLength']}, Dest: {dest_obj['ContentLength']})")
                    return False
            except Exception as verify_error:
                logger.error(f"Copy verification failed for {dest_key}: {str(verify_error)}")
                return False
        except Exception as e:
            logger.error(f"Failed to copy {source_key}: {str(e)}")
            return False
class PayerConfigManager:
    """Manage payer configurations and auto-detection"""
    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client
        self.configs = PAYER_CONFIGS.copy()
    def get_payer_config(self, payer_id: str) -> Dict[str, Any]:
        """Get payer configuration based on actual bucket list"""
        if payer_id not in self.configs:
            logger.warning(f"Payer {payer_id} not in static config. Attempting auto-detection.")
            detected_config = self._auto_detect_config(payer_id)
            if detected_config:
                self.configs[payer_id] = detected_config
            else:
                logger.error(f"Auto-detection failed for payer {payer_id}. Cannot proceed.")
                return None # Return None to indicate failure
        return self.configs.get(payer_id)
    def _auto_detect_config(self, payer_id: str) -> Dict[str, Any]:
        """Auto-detect payer configuration by testing bucket existence"""
        possible_configs = [
            {
                "name": f"auto-edp-{payer_id}",
                "bucket": f"aws-{payer_id}-edp-parquet-data-export-cur",
                "path": f"{payer_id}-edp/cur-hourly-athena-data-export-{payer_id}-edp/data",
                "access_type": "SAME_ACCOUNT"
            },
            {
                "name": f"auto-payer-{payer_id}",
                "bucket": f"aws-{payer_id}-parquet-data-export-cur",
                "path": f"{payer_id}/cur-hourly-athena-data-export-{payer_id}/data",
                "access_type": "SAME_ACCOUNT"
            }
        ]
        for config in possible_configs:
            try:
                if self.s3_client.validate_bucket_access(config['bucket']):
                    logger.info(f"Auto-detected bucket for payer {payer_id}: {config['bucket']}")
                    return config
            except:
                continue
        return None
    def update_config_for_environment(self, environment: str):
        """Update configurations based on environment (prod vs non-prod)"""
        if environment.lower() == 'prod':
            logger.info("Updating configurations for PRODUCTION environment.")
            for payer_id, config in self.configs.items():
                if '-nonprod' in config['bucket']:
                    config['bucket'] = config['bucket'].replace('-nonprod', '')
                    logger.debug(f"Updated bucket for {payer_id} to '{config['bucket']}' for production.")
        logger.info(f"Configurations updated for environment: {environment}")
