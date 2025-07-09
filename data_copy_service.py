
import os
import logging
from typing import List, Dict, Any, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from datetime import datetime, timezone

from s3_client import S3Client, PayerConfigManager
from config import get_environment_config, MAX_COPY_WORKERS

try:
    from snowflake_external_table import create_external_table_and_process, SnowflakeExternalTableManager
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    logging.warning("Snowflake module not available, skipping related steps.")

logger = logging.getLogger(__name__)


class FargateDataCopyService:
    """
    Orchestrates the data copy operation from source payer accounts to a central staging bucket.
    This service analyzes payers, cleans the destination, copies new data files in parallel, 
    and triggers downstream processing in Snowflake.
    """

    def __init__(self, environment: str = 'uat'):
        self.environment = environment
        self.env_config = get_environment_config(environment)
        s3_region = self.env_config.get('s3_region')

        self.s3_client = S3Client(region_name=s3_region)
        self.payer_config_manager = PayerConfigManager(self.environment)

        if SNOWFLAKE_AVAILABLE:
            self.snowflake_manager = SnowflakeExternalTableManager(self.environment, 'analytics')
        else:
            self.snowflake_manager = None

        logger.info(f"FargateDataCopyService initialized for env: '{environment}' with {MAX_COPY_WORKERS} workers.")

    def process_multiple_payers(self, payer_ids: List[str], year: int, month: int,
                               staging_bucket: str, app: str, module: str) -> Dict[str, Any]:
        """
        Main orchestration method. Analyzes multiple payers, determines files to sync,
        and triggers the copy and processing logic.
        """
        logger.info(f"Starting analysis for {len(payer_ids)} payers for {year}-{month:02}.")

        payers_with_new_files = []
        payers_failed_analysis = []

        if self.snowflake_manager:
            try:
                self.snowflake_manager.connect()
            except Exception as e:
                logger.error(f"Cannot connect to Snowflake for timestamps; will process all files. Error: {e}")
                self.snowflake_manager = None

        for payer_id in payer_ids:
            status, result = self._analyze_single_payer(payer_id, year, month)
            if status == 'HAS_NEW_FILES':
                payers_with_new_files.append(result)
            elif status == 'UP_TO_DATE':
                pass
            else:  # FAILED
                payers_failed_analysis.append(payer_id)

        if self.snowflake_manager:
            self.snowflake_manager.close_connection()

        if not payers_with_new_files:
            logger.info("No payers had new files to copy. Task is considered successful.")
            return {"status": "UP_TO_DATE", "failed_payers": payers_failed_analysis}

        copy_summary = self._execute_copy_and_snowflake_process(
            payers_with_new_files, staging_bucket, app, module, year, month
        )

  
        if copy_summary["failed"] == 0:
            return {
                "status": "SUCCESS",
                "copy_summary": copy_summary,
                "failed_payers": payers_failed_analysis  
            }
        else:
            return {
                "status": "FAILED",
                "copy_summary": copy_summary,
                "failed_payers": payers_failed_analysis
            }
        # --- END OF LOGIC CHANGE ---


    def _analyze_single_payer(self, payer_id: str, year: int, month: int) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        Performs analysis for a single payer to find new files.
        """
        logger.info(f"\n--- Analyzing Payer: {payer_id} ---")
        try:
            config = self.payer_config_manager.get_payer_config(payer_id)
            if not config:
                logger.error(f"Skipping payer {payer_id}: no configuration found.")
                return 'FAILED', None

            source_bucket = config.get('bucket')
            if not self.s3_client.can_access_bucket(source_bucket):
                logger.warning(f"Access denied for bucket '{source_bucket}' from primary config for payer {payer_id}.")
                config = self.payer_config_manager.get_fallback_config(payer_id)
                if not config:
                    logger.error(f"Skipping payer {payer_id}: access denied and no fallback available.")
                    return 'FAILED', None
                source_bucket = config.get('bucket')

            source_path_base = config.get('path')
            
            last_processed_ts = None
            if self.snowflake_manager:
                last_processed_ts = self.snowflake_manager.get_last_processed_timestamp(payer_id)
                if last_processed_ts:
                    last_processed_ts = last_processed_ts.replace(tzinfo=timezone.utc)

            current_month_prefix = f"{source_path_base.rstrip('/')}/data/BILLING_PERIOD={year}-{month:02}/"
            prefixes_to_scan = [current_month_prefix]
            
            logger.info(f"Scanning S3 prefixes: {prefixes_to_scan}")

            files_to_copy_list = []
            for prefix in prefixes_to_scan:
                found_files = self.s3_client.list_objects_with_metadata(source_bucket, prefix, since=last_processed_ts)
                files_to_copy_list.extend(found_files.keys())
            
            if files_to_copy_list:
                logger.info(f"   Found {len(files_to_copy_list)} new files to process.")
                metadata = {
                    "payer_id": payer_id,
                    "files_to_copy": files_to_copy_list,
                    "source_bucket": source_bucket
                }
                return 'HAS_NEW_FILES', metadata
            else:
                logger.info(f"   All files for payer {payer_id} are already up-to-date.")
                return 'UP_TO_DATE', None

        except Exception as e:
            logger.error(f"An unexpected error occurred analyzing files for payer {payer_id}: {e}", exc_info=True)
            return 'FAILED', None
            
    def _execute_copy_and_snowflake_process(self, payers_with_new_files: List[Dict], staging_bucket: str,
                                            app: str, module: str, year: int, month: int) -> Dict[str, int]:
        """
        Manages the cleanup, parallel file copy, and subsequent Snowflake processing.
        """
        summary = {"success": 0, "failed": 0, "total": 0}
        all_copy_tasks = []
        processed_payer_ids = [p['payer_id'] for p in payers_with_new_files]

        logger.info(f"Preparing to copy files for {len(processed_payer_ids)} payers who have new data.")

        for payer_data in payers_with_new_files:
            payer_id = payer_data['payer_id']
            source_bucket = payer_data['source_bucket']
            dest_prefix = f"{app}/{module}/{self.environment}/year={year}/month={month}/payer-{payer_id}/"

            logger.info(f"Cleaning destination for payer {payer_id} before copy...")
            if not self.s3_client.delete_objects_by_prefix(staging_bucket, dest_prefix):
                logger.error(f"Halting process for payer {payer_id} due to failure in cleaning destination. This payer's files will not be copied.")
                continue

            for source_key in payer_data['files_to_copy']:
                filename = os.path.basename(source_key)
                dest_key = f"{dest_prefix}{filename}"
                all_copy_tasks.append({
                    "source_bucket": source_bucket, "source_key": source_key,
                    "dest_bucket": staging_bucket, "dest_key": dest_key
                })

        total_tasks = len(all_copy_tasks)
        summary["total"] = total_tasks
        if total_tasks == 0:
            logger.warning("No files were queued for copying after cleanup phase. This may be due to cleanup failures.")
            return summary

        logger.info(f"Starting multithreaded copy of {total_tasks} files...")
        with ThreadPoolExecutor(max_workers=MAX_COPY_WORKERS) as executor:
            future_to_task = {executor.submit(self.s3_client.copy_single_file, **task): task for task in all_copy_tasks}
            for i, future in enumerate(as_completed(future_to_task), 1):
                if future.result():
                    summary["success"] += 1
                else:
                    summary["failed"] += 1
                if i % 250 == 0 or i == total_tasks:
                    logger.info(f"Copy progress: {i}/{total_tasks} | Success: {summary['success']}, Failed: {summary['failed']}")

        logger.info(f"--- S3 Copy Summary ---")
        logger.info(f"  Total files copied successfully: {summary['success']}")
        logger.info(f"  Total files failed to copy: {summary['failed']}")

        if summary["failed"] > 0:
            logger.warning("Skipping Snowflake processing due to data copy failures.")
            return summary

        if SNOWFLAKE_AVAILABLE and processed_payer_ids:
            try:
                logger.info("Starting Snowflake external table creation...")
                create_external_table_and_process(
                    env=self.environment, module=module, year=year, month=month,
                    staging_bucket=staging_bucket, payer_ids=processed_payer_ids, app=app
                )
                logger.info("Snowflake external table process completed successfully!")
            except Exception as snowflake_error:
                logger.error(f"Snowflake external table creation failed: {snowflake_error}", exc_info=True)
                summary["failed"] = total_tasks
        else:
            logger.warning("Snowflake module not available or no payers had data to process, skipping external table creation.")

        return summary