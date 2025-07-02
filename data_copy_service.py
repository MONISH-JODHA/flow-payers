import os
import logging
import traceback
from typing import List, Dict, Any
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

# Use your detailed S3 client implementation from the previous correct answer
from s3_client import S3Client, PayerConfigManager
from rabbitmq_client import RabbitMQNotifier
from config import get_environment_config, MAX_COPY_WORKERS 

# Import Snowflake module
try:
    from snowflake_external_table import create_external_table_and_process
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    logging.warning("Snowflake module not available, skipping related steps.")

logger = logging.getLogger(__name__)

class FargateDataCopyService:
    """
    Enhanced ECS/Fargate service for data copy operations with RabbitMQ notifications and Snowflake integration.
    This version keeps the original methods and integrates performance enhancements.
    """
    
    def __init__(self, environment='dev'):
        self.environment = environment
        self.env_config = get_environment_config(environment)
        
        # Initialize your robust S3 client
        s3_region = self.env_config.get('s3_region', 'us-east-2')
        self.s3_client = S3Client(region_name=s3_region)
        
        # Initialize your PayerConfigManager
        self.payer_config_manager = PayerConfigManager(self.s3_client)
        self.payer_config_manager.update_config_for_environment(environment) 
        
        # Initialize RabbitMQ notifier
        try:
            self.rabbitmq_notifier = RabbitMQNotifier(environment)
            logger.info("RabbitMQNotifier initialized successfully.")
        except Exception as e:
            self.rabbitmq_notifier = None
            logger.error(f"Failed to initialize RabbitMQNotifier: {e}. RabbitMQ notifications will be disabled.", exc_info=True)
        
        logger.info(f"FargateDataCopyService initialized for environment: {environment} with {MAX_COPY_WORKERS} workers.")
    
    # This is a new helper method required for the thread pool
    def _copy_file_worker(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> bool:
        """A simple, robust worker function for the thread pool to execute."""
        try:
            return self.s3_client.copy_single_file(source_bucket, source_key, dest_bucket, dest_key)
        except Exception as e:
            logger.error(f"Thread worker failed for {source_key}: {e}", exc_info=True)
            return False

    def get_payer_config(self, payer_id: str) -> Dict[str, Any]:
        """Get payer configuration"""
        return self.payer_config_manager.get_payer_config(payer_id)
    
    def validate_bucket_access(self, bucket_name: str) -> bool:
        """Validate bucket access"""
        return self.s3_client.validate_bucket_access(bucket_name)
    
    def get_files_from_s3_folder(self, bucket_name: str, folder_prefix: str) -> List[str]:
        """Get all files from S3 folder"""
        return self.s3_client.get_files_from_s3_folder(bucket_name, folder_prefix)
    
    def copy_single_file(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> bool:
        """Copy a single file"""
        return self.s3_client.copy_single_file(source_bucket, source_key, dest_bucket, dest_key)
    
    def copy_payer_data(self, args: Dict[str, Any]) -> bool:
        """
        Copy payer data files with enhanced RabbitMQ notification and multithreading.
        This method is now driven by a thread pool for performance.
        """
        all_copy_tasks = []
        processed_payer_ids = []
        
        try:
            logger.info("Starting multithreaded payer data copy process...")
            
            # Extract parameters from args
            data_files_metadata = args.get('data_files_version_metadata', [])
            staging_bucket = args.get('staging_bucket')
            env = args.get('env')
            year = args.get('year')
            month = args.get('month')
            module = args.get('module', 'analytics')
            app = args.get('app', 'aws_az_analytics_application_refresh')
            partner_id = args.get('partner_id', 1)
            
            if not all([data_files_metadata, staging_bucket, env, year, month]):
                raise ValueError("Missing required parameters for copy_payer_data")
            
            if not self.validate_bucket_access(staging_bucket):
                raise Exception(f"Cannot access staging bucket: {staging_bucket}")
            
            # Prepare a flat list of all copy tasks from all payers
            for payer_data in data_files_metadata:
                if not payer_data[1]:  # Skip if not ready to copy (e.g., no new files)
                    logger.info(f"Skipping payer {payer_data[0]} (marked as not ready for copy)")
                    continue
                
                payer_id = payer_data[0]
                processed_payer_ids.append(payer_id)
                files_to_copy = payer_data[3] # This is now a list of specific files to copy
                source_bucket = payer_data[4]
                
                dest_prefix = f"{app}/{module}/{env}/year={year}/month={month}/payer-{payer_id}/"
                
                for source_key in files_to_copy:
                    filename = os.path.basename(source_key)
                    dest_key = f"{dest_prefix}{filename}"
                    all_copy_tasks.append({
                        "source_bucket": source_bucket, "source_key": source_key,
                        "dest_bucket": staging_bucket, "dest_key": dest_key
                    })

            # Execute tasks in a thread pool
            total_tasks = len(all_copy_tasks)
            if total_tasks == 0:
                logger.warning("No files were queued for copying.")
                # Send success notification as no work was needed.
                if self.rabbitmq_notifier:
                    try:
                        self.rabbitmq_notifier.send_notification(
                            month=int(month), year=int(year), module=module,
                            payer_ids=processed_payer_ids, status="SUCCESS", partner_id=int(partner_id)
                        )
                    except Exception as notification_error:
                        logger.error(f"Failed to send RabbitMQ notification for no-copy success: {notification_error}", exc_info=True)
                return True # Success, as no work was needed.

            success_count = 0
            failed_count = 0
            with ThreadPoolExecutor(max_workers=MAX_COPY_WORKERS) as executor:
                future_to_task = {executor.submit(self._copy_file_worker, **task): task for task in all_copy_tasks}
                for i, future in enumerate(as_completed(future_to_task), 1):
                    if future.result():
                        success_count += 1
                    else:
                        failed_count += 1
                    if i % 250 == 0 or i == total_tasks:
                        logger.info(f"Copy progress: {i}/{total_tasks} processed. Success: {success_count}, Failed: {failed_count}")

            # Overall summary and status determination
            logger.info(f"\n--- Overall Copy Summary ---")
            logger.info(f"    Total files copied: {success_count}")
            logger.info(f"    Total files failed: {failed_count}")
            logger.info(f"    Destination: s3://{staging_bucket}/{app}/{module}/{env}/year={year}/month={month}/")
            
            overall_success = (failed_count == 0)
            
            # If data copy was successful, proceed with Snowflake
            if overall_success and SNOWFLAKE_AVAILABLE:
                try:
                    logger.info("Starting Snowflake external table creation...")
                    create_external_table_and_process(
                        env=env, module=module, year=int(year), month=int(month),
                        staging_bucket=staging_bucket, payer_ids=processed_payer_ids, app=app
                    )
                    logger.info("Snowflake external table created successfully!")
                except Exception as snowflake_error:
                    logger.error(f"Snowflake external table creation failed: {snowflake_error}", exc_info=True)
                    overall_success = False # Downgrade success status
            elif overall_success and not SNOWFLAKE_AVAILABLE:
                logger.warning("Snowflake module not available, skipping external table creation.")
            else:
                logger.warning("Skipping Snowflake processing due to data copy failures.")
            
            # Send final notification
            if processed_payer_ids and self.rabbitmq_notifier:
                final_status = "SUCCESS" if overall_success else "FAILED"
                logger.info(f"Publishing final {final_status} notification after complete processing...")
                try:
                    self.rabbitmq_notifier.send_notification(
                        month=int(month), year=int(year), module=module,
                        payer_ids=processed_payer_ids, status=final_status, partner_id=int(partner_id)
                    )
                except Exception as notification_error:
                    logger.error(f"Failed to send final RabbitMQ notification: {notification_error}", exc_info=True)
            elif not processed_payer_ids:
                logger.info("No payers processed, skipping RabbitMQ notification.")
            else: # self.rabbitmq_notifier is None
                logger.warning("RabbitMQNotifier not initialized, skipping final notification.")

            return overall_success
                
        except Exception as e:
            logger.error(f"Error in copy_payer_data: {str(e)}", exc_info=True)
            return False
    
    def process_multiple_payers(self, payer_ids: List[str], year: int, month: int, 
                               staging_bucket: str, app: str, module: str, 
                               partner_id: int) -> bool:
        """
        Process multiple payers and prepare metadata for batch processing.
        **This method now performs the intelligent sync check.**
        """
        all_payer_metadata = []
        total_files_across_payers = 0
        total_files_to_copy = 0
        total_files_skipped = 0
        processed_payer_ids = payer_ids[:] # Create a copy to track processed payers for notifications
        
        for payer_id in payer_ids:
            logger.info(f"\n--- Analyzing Payer: {payer_id} ---")
            
            config = self.get_payer_config(payer_id) 
            if config is None:
                logger.error(f"Payer configuration not found for ID: {payer_id}. Skipping.")
                # Send individual failed notification for this payer if RabbitMQ is available
                if self.rabbitmq_notifier:
                    try:
                        self.rabbitmq_notifier.send_notification(
                            month=int(month), year=int(year), module=module,
                            payer_ids=[payer_id], status="FAILED", partner_id=int(partner_id),
                            message=f"Configuration not found for payer {payer_id}"
                        )
                    except Exception as notification_error:
                        logger.error(f"Failed to send RabbitMQ notification for config error: {notification_error}", exc_info=True)
                continue
            
            source_bucket = config.get('bucket')
            folder_path_template = config.get('path')
            payer_name = config.get('name')
            
            if not all([source_bucket, folder_path_template, payer_name]):
                logger.error(f"Incomplete configuration for Payer ID: {payer_id}. Missing bucket, path, or name. Skipping.")
                # Send individual failed notification for this payer if RabbitMQ is available
                if self.rabbitmq_notifier:
                    try:
                        self.rabbitmq_notifier.send_notification(
                            month=int(month), year=int(year), module=module,
                            payer_ids=[payer_id], status="FAILED", partner_id=int(partner_id),
                            message=f"Incomplete configuration for payer {payer_id}"
                        )
                    except Exception as notification_error:
                        logger.error(f"Failed to send RabbitMQ notification for incomplete config: {notification_error}", exc_info=True)
                continue

            source_prefix = f"{folder_path_template}/BILLING_PERIOD={str(year)}-{str(month).zfill(2)}/"
            dest_prefix = f"{app}/{module}/{self.environment}/year={year}/month={month}/payer-{payer_id}/"

            logger.info(f"   Payer Name: {payer_name}")
            logger.info(f"   Source Path: s3://{source_bucket}/{source_prefix}")
            
            try:
                # Use the metadata method for intelligent sync
                source_files_map = self.s3_client.list_objects_with_metadata(source_bucket, source_prefix)
                total_files_across_payers += len(source_files_map)

                if not source_files_map:
                    logger.warning(f"No files found for payer {payer_id} in {source_prefix}")
                    all_payer_metadata.append([payer_id, False, "current", [], source_bucket])
                    # Send individual notification if no files found
                    if self.rabbitmq_notifier:
                        try:
                            self.rabbitmq_notifier.send_notification(
                                month=int(month), year=int(year), module=module,
                                payer_ids=[payer_id], status="NO_FILES_FOUND", partner_id=int(partner_id),
                                message=f"No files found for payer {payer_id} in S3 path."
                            )
                        except Exception as notification_error:
                            logger.error(f"Failed to send RabbitMQ notification for no files found: {notification_error}", exc_info=True)
                    continue

                dest_files_map = self.s3_client.list_objects_with_metadata(staging_bucket, dest_prefix)
                
                files_that_need_copying = []
                for filename, source_meta in source_files_map.items():
                    dest_meta = dest_files_map.get(filename)
                    if dest_meta and dest_meta['ETag'] == source_meta['ETag'] and dest_meta['Size'] == source_meta['Size']:
                        total_files_skipped += 1
                    else:
                        files_that_need_copying.append(f"{source_prefix}{filename}")

                if files_that_need_copying:
                    logger.info(f"   Found {len(files_that_need_copying)} new/modified files for payer {payer_id}.")
                    total_files_to_copy += len(files_that_need_copying)
                    all_payer_metadata.append([
                        payer_id, True, "current", files_that_need_copying, source_bucket
                    ])
                else:
                    logger.info(f"   All {len(source_files_map)} files for payer {payer_id} are already up-to-date.")
                    all_payer_metadata.append([
                        payer_id, False, "current", [], source_bucket
                    ])
                    # Send individual notification if files are up-to-date
                    if self.rabbitmq_notifier:
                        try:
                            self.rabbitmq_notifier.send_notification(
                                month=int(month), year=int(year), module=module,
                                payer_ids=[payer_id], status="UP_TO_DATE", partner_id=int(partner_id),
                                message=f"All files for payer {payer_id} are already up-to-date."
                            )
                        except Exception as notification_error:
                            logger.error(f"Failed to send RabbitMQ notification for up-to-date status: {notification_error}", exc_info=True)
                    
            except Exception as e:
                logger.error(f"Failed to get/process files for payer {payer_id}: {e}", exc_info=True)
                all_payer_metadata.append([payer_id, False, "current", [], source_bucket])
                # Send individual failed notification for this payer due to file processing error
                if self.rabbitmq_notifier:
                    try:
                        self.rabbitmq_notifier.send_notification(
                            month=int(month), year=int(year), module=module,
                            payer_ids=[payer_id], status="FAILED", partner_id=int(partner_id),
                            message=f"Failed to process files for payer {payer_id}: {e}"
                        )
                    except Exception as notification_error:
                        logger.error(f"Failed to send RabbitMQ notification for file processing error: {notification_error}", exc_info=True)
        
        # Check if we have any work to do
        if total_files_to_copy == 0:
            logger.info("\nSUCCESS: All data for all specified payers is already synchronized or no files were found.")
            # Final success notification for overall sync status if all were skipped/empty
            if self.rabbitmq_notifier and processed_payer_ids:
                try:
                    self.rabbitmq_notifier.send_notification(
                        month=month, year=year, module=module, payer_ids=processed_payer_ids,
                        status="OVERALL_UP_TO_DATE", partner_id=partner_id,
                        message="All specified payers were already up-to-date or had no files to copy."
                    )
                except Exception as notification_error:
                    logger.error(f"Failed to send overall UP_TO_DATE RabbitMQ notification: {notification_error}", exc_info=True)
            elif not processed_payer_ids:
                logger.warning("No payers were processed at all, no overall notification sent.")
            return True
        
        # Prepare arguments for the batch processing method `copy_payer_data`
        args = {
            "app": app,
            "module": module,
            "env": self.environment,
            "year": year,
            "month": month,
            "partner_id": partner_id,
            "staging_bucket": staging_bucket,
            "data_files_version_metadata": all_payer_metadata
        }
        
        logger.info(f"\n--- Batch Processing Prepared ---")
        logger.info(f"   Total source files analyzed: {total_files_across_payers}")
        logger.info(f"   Total files to be copied: {total_files_to_copy}")
        logger.info(f"   Total files skipped (up-to-date): {total_files_skipped}")
        
        # Execute the batch copy for all payers
        logger.info("\nStarting batch copy operation...")
        return self.copy_payer_data(args)
