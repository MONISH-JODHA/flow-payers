#
# data_copy_service.py (Corrected and Simplified)
#
import os
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from datetime import datetime, timezone

# We no longer need relativedelta, so it's removed.

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
    """Orchestrates the data copy operation from source to staging."""
    def __init__(self, environment='uat'):
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

    def _copy_file_worker(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> bool:
        """A simple, robust worker function for the thread pool to execute."""
        try:
            return self.s3_client.copy_single_file(source_bucket, source_key, dest_bucket, dest_key)
        except Exception as e:
            logger.error(f"Thread worker failed for {source_key}: {e}", exc_info=True)
            return False
            
    def process_multiple_payers(self, payer_ids: List[str], year: int, month: int,
                               staging_bucket: str, app: str, module: str) -> Dict[str, Any]:
        """
        Analyzes multiple payers, determines files to sync, and triggers the copy.
        Only processes files newer than the last processed timestamp in Snowflake.
        """
        all_payer_metadata = []
        files_to_copy_count = 0
        failed_payers = []
        
        # --- This connection is now only used for getting timestamps ---
        if self.snowflake_manager:
            try:
                self.snowflake_manager.connect()
            except Exception as e:
                logger.error(f"Cannot connect to Snowflake to get timestamps; will process all files. Error: {e}")
                self.snowflake_manager = None
        # ---

        for payer_id in payer_ids:
            logger.info(f"\n--- Analyzing Payer: {payer_id} ---")
            
            config = self.payer_config_manager.get_payer_config(payer_id)
            if not config:
                logger.error(f"Skipping payer {payer_id}: no configuration found.")
                failed_payers.append(payer_id)
                continue

            source_bucket = config.get('bucket')
            if not self.s3_client.can_access_bucket(source_bucket):
                logger.warning(f"Access denied for bucket '{source_bucket}' from primary config for payer {payer_id}.")
                config = self.payer_config_manager.get_fallback_config(payer_id)
                if not config:
                    logger.error(f"Skipping payer {payer_id}: access denied and no fallback available.")
                    failed_payers.append(payer_id)
                    continue
                source_bucket = config.get('bucket')

            source_path = config.get('path')
            
            last_processed_ts = None
            if self.snowflake_manager:
                last_processed_ts = self.snowflake_manager.get_last_processed_timestamp(payer_id)
                if last_processed_ts:
                    last_processed_ts = last_processed_ts.replace(tzinfo=timezone.utc)

            # We must scan both the current and previous month's folders
            # to catch late-arriving data.
            # --- START OF FIX ---
            # The redundant `/data` is removed from the prefix construction.
            # The path from config.py already contains the correct base path.
            current_month_prefix = f"{source_path.rstrip('/')}/BILLING_PERIOD={year}-{month:02}/"
            prev_month = month - 1 if month > 1 else 12
            prev_year = year if month > 1 else year - 1
            prev_month_prefix = f"{source_path.rstrip('/')}/BILLING_PERIOD={prev_year}-{prev_month:02}/"
            # --- END OF FIX ---
            
            prefixes_to_scan = [current_month_prefix, prev_month_prefix]
            logger.info(f"Scanning S3 prefixes: {prefixes_to_scan}")

            try:
                files_to_copy_list = []
                for prefix in prefixes_to_scan:
                    # Note: We are using a new "list_objects_with_metadata" that returns full keys
                    found_files = self.s3_client.list_objects_with_metadata(source_bucket, prefix, since=last_processed_ts)
                    # We need the full key, not just the filename
                    files_to_copy_list.extend(found_files.keys()) 
                
                if files_to_copy_list:
                    logger.info(f"   Found {len(files_to_copy_list)} new files to process.")
                    files_to_copy_count += len(files_to_copy_list)
                    all_payer_metadata.append({
                        "payer_id": payer_id, "ready_to_copy": True,
                        "files_to_copy": files_to_copy_list, "source_bucket": source_bucket
                    })
                else:
                    logger.info(f"   All files for payer {payer_id} are already up-to-date.")

            except Exception as e:
                logger.error(f"An unexpected error occurred analyzing files for payer {payer_id}: {e}", exc_info=True)
                failed_payers.append(payer_id)

        # --- Close the timestamp-checking connection ---
        if self.snowflake_manager:
            self.snowflake_manager.close_connection()
        # ---

        if files_to_copy_count == 0 and not failed_payers:
            logger.info("\nSUCCESS: All data for all specified payers is already synchronized.")
            return {"status": "UP_TO_DATE", "failed_payers": []}

        args = {
            "app": app, "module": module, "env": self.environment, "year": year, "month": month,
            "staging_bucket": staging_bucket, "data_files_version_metadata": all_payer_metadata
        }
        
        copy_summary = self.copy_payer_data(args)

        overall_success = (copy_summary["failed"] == 0 and not failed_payers)
        return {
            "status": "SUCCESS" if overall_success else "FAILED",
            "copy_summary": copy_summary,
            "failed_payers": failed_payers
        }

    def copy_payer_data(self, args: Dict[str, Any]) -> Dict[str, int]:
        """
        Copies data for multiple payers using a thread pool and returns a summary.
        """
        all_copy_tasks = []
        processed_payer_ids = []
        summary = {"success": 0, "failed": 0, "total": 0}

        logger.info("Starting multithreaded payer data copy process...")
        data_files_metadata = args.get('data_files_version_metadata', [])
        staging_bucket = args.get('staging_bucket')
        env = args.get('env')
        year = args.get('year')
        month = args.get('month')
        module = args.get('module')
        app = args.get('app')

        for payer_data in data_files_metadata:
            if not payer_data['ready_to_copy']:
                continue
            payer_id = payer_data['payer_id']
            processed_payer_ids.append(payer_id)
            source_bucket = payer_data['source_bucket']
            # Staging destination path should always be based on the *current* processing month
            dest_prefix = f"{app}/{module}/{env}/year={year}/month={month}/payer-{payer_id}/"

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
            logger.warning("No new or modified files were queued for copying.")
            return summary

        with ThreadPoolExecutor(max_workers=MAX_COPY_WORKERS) as executor:
            future_to_task = {executor.submit(self._copy_file_worker, **task): task for task in all_copy_tasks}
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

        copy_successful = (summary["failed"] == 0)
        if copy_successful and SNOWFLAKE_AVAILABLE and processed_payer_ids:
            try:
                logger.info("Starting Snowflake external table creation...")
                create_external_table_and_process(
                    env=env, module=module, year=int(year), month=int(month),
                    staging_bucket=staging_bucket, payer_ids=processed_payer_ids, app=app
                )
                logger.info("Snowflake external table process completed successfully!")
            except Exception as snowflake_error:
                logger.error(f"Snowflake external table creation failed: {snowflake_error}", exc_info=True)
                # Set copy summary to failed state if snowflake step fails
                summary["success"] = 0
                summary["failed"] = total_tasks
        elif not processed_payer_ids:
             logger.info("Skipping Snowflake processing as no payers were successfully analyzed.")
        elif copy_successful and not SNOWFLAKE_AVAILABLE:
            logger.warning("Snowflake module not available, skipping external table creation.")
        else:
            logger.warning("Skipping Snowflake processing due to data copy failures.")

        return summary