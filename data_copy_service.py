import os
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from s3_client import S3Client, PayerConfigManager
from rabbitmq_client import RabbitMQNotifier
from config import get_environment_config, MAX_COPY_WORKERS

try:
    from snowflake_external_table import create_external_table_and_process
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    logging.warning("Snowflake module not available, skipping related steps.")

logger = logging.getLogger(__name__)

class FargateDataCopyService:
    """Orchestrates the data copy operation from source to staging."""
    def __init__(self, environment='dev'):
        self.environment = environment
        self.env_config = get_environment_config(environment)
        s3_region = self.env_config.get('s3_region', 'us-east-2')
        self.s3_client = S3Client(region_name=s3_region)
        self.payer_config_manager = PayerConfigManager(self.environment)
        logger.info(f"FargateDataCopyService initialized for env: {environment} with {MAX_COPY_WORKERS} workers.")

    def _copy_file_worker(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> bool:
        """A simple, robust worker function for the thread pool to execute."""
        try:
            return self.s3_client.copy_single_file(source_bucket, source_key, dest_bucket, dest_key)
        except Exception as e:
            logger.error(f"Thread worker failed for {source_key}: {e}", exc_info=True)
            return False

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

        # Prepare a flat list of all copy tasks from all payers
        for payer_data in data_files_metadata:
            if not payer_data['ready_to_copy']:
                continue
            payer_id = payer_data['payer_id']
            processed_payer_ids.append(payer_id)
            source_bucket = payer_data['source_bucket']
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
            return summary # Success, as no work was needed.

        # Execute tasks in a thread pool
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

        # If data copy was successful, proceed with Snowflake
        copy_successful = (summary["failed"] == 0)
        if copy_successful and SNOWFLAKE_AVAILABLE:
            try:
                logger.info("Starting Snowflake external table creation...")
                create_external_table_and_process(
                    env=env, module=module, year=int(year), month=int(month),
                    staging_bucket=staging_bucket, payer_ids=processed_payer_ids, app=app
                )
                logger.info("Snowflake external table process completed successfully!")
            except Exception as snowflake_error:
                logger.error(f"Snowflake external table creation failed: {snowflake_error}", exc_info=True)
                summary["failed"] = total_tasks # Mark all as failed if Snowflake step fails
        elif copy_successful and not SNOWFLAKE_AVAILABLE:
            logger.warning("Snowflake module not available, skipping external table creation.")
        else:
            logger.warning("Skipping Snowflake processing due to data copy failures.")

        return summary

    def process_multiple_payers(self, payer_ids: List[str], year: int, month: int,
                               staging_bucket: str, app: str, module: str) -> Dict[str, Any]:
        """
        Analyzes multiple payers, determines which files need syncing, and triggers the copy.
        """
        all_payer_metadata = []
        files_to_copy_count = 0
        failed_payers = []

        for payer_id in payer_ids:
            logger.info(f"\n--- Analyzing Payer: {payer_id} ---")
            config = self.payer_config_manager.get_payer_config(payer_id)
            if not config:
                logger.error(f"Skipping payer {payer_id} due to missing configuration.")
                failed_payers.append(payer_id)
                continue

            source_bucket = config.get('bucket')
            source_path = config.get('path')
            source_prefix = f"{source_path}/BILLING_PERIOD={str(year)}-{str(month).zfill(2)}/"
            dest_prefix = f"{app}/{module}/{self.environment}/year={year}/month={month}/payer-{payer_id}/"

            logger.info(f"   Source Path: s3://{source_bucket}/{source_prefix}")
            try:
                source_files_map = self.s3_client.list_objects_with_metadata(source_bucket, source_prefix)
                if not source_files_map:
                    logger.warning(f"No files found for payer {payer_id} in {source_prefix}")
                    continue

                dest_files_map = self.s3_client.list_objects_with_metadata(staging_bucket, dest_prefix)
                files_that_need_copying = []
                for filename, source_meta in source_files_map.items():
                    dest_meta = dest_files_map.get(filename)
                    if not dest_meta or dest_meta['ETag'] != source_meta['ETag'] or dest_meta['Size'] != source_meta['Size']:
                        files_that_need_copying.append(f"{source_prefix}{filename}")

                if files_that_need_copying:
                    logger.info(f"   Found {len(files_that_need_copying)} new/modified files.")
                    files_to_copy_count += len(files_that_need_copying)
                    all_payer_metadata.append({
                        "payer_id": payer_id,
                        "ready_to_copy": True,
                        "files_to_copy": files_that_need_copying,
                        "source_bucket": source_bucket
                    })
                else:
                    logger.info(f"   All {len(source_files_map)} files for payer {payer_id} are already up-to-date.")

            except Exception as e:
                logger.error(f"Failed to analyze files for payer {payer_id}: {e}", exc_info=True)
                failed_payers.append(payer_id)

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