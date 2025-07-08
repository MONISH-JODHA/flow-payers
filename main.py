# main.py

import sys
import logging
import os

from input_validator import ParameterProcessor
from data_copy_service import FargateDataCopyService
from rabbitmq_client import RabbitMQNotifier
from cloudwatch_utils import send_task_completion, send_error_metric, send_processing_metrics
# --- START OF CHANGE ---
from slack_notifier import send_slack_notification
# --- END OF CHANGE ---
from config import get_environment_config, DEFAULT_ENVIRONMENT

logger = logging.getLogger(__name__)

def log_processing_parameters(params):
    """Log processing parameters"""
    logger.info("--- Processing Parameters ---")
    for key, value in params.items():
        if key == 'payer_ids':
            logger.info(f"  Payer Ids (Unique): {value}")
        else:
            logger.info(f"  {key.replace('_', ' ').title()}: {value}")
    logger.info("-----------------------------")

def main():
    """Main function for the Fargate task."""
    task_status = "Failed"  # Default to Failed
    status_reason = "UnknownError"
    failure_details = "An unknown error occurred."
    params = None
    original_payer_ids_for_notification = []

    environment = os.environ.get('ENV', os.environ.get('ENVIRONMENT', DEFAULT_ENVIRONMENT)).lower()
    logger.info(f"Detected initial environment: '{environment}' for notification purposes.")

    try:
        logger.info("=" * 80)
        logger.info("Starting Fargate Data Copy Task")
        logger.info("=" * 80)

        params = ParameterProcessor.get_parameters()
        original_payer_ids_for_notification = params.get('payer_ids', [])

        # Ensure we only process unique payer IDs
        unique_payer_ids = sorted(list(set(original_payer_ids_for_notification)))
        if len(original_payer_ids_for_notification) != len(unique_payer_ids):
            logger.warning(f"Duplicate payer IDs found in input. Processing unique set: {unique_payer_ids}")
        params['payer_ids'] = unique_payer_ids

        environment = params.get('environment', environment)
        log_processing_parameters(params)

        env_config = get_environment_config(environment)
        params['staging_bucket'] = env_config['staging_bucket']
        logger.info(f"Using staging bucket for '{environment}': {params['staging_bucket']}")

        copy_service = FargateDataCopyService(environment)

        result = copy_service.process_multiple_payers(
            payer_ids=params['payer_ids'],
            year=params['year'],
            month=params['month'],
            staging_bucket=params['staging_bucket'],
            app=params['app'],
            module=params['module']
        )
        
        # --- SIMPLIFIED STATUS LOGIC ---
        if result["status"] == "SUCCESS":
            task_status = "Success"
            status_reason = "ProcessingComplete"
            summary = result['copy_summary']
            failure_details = f"Task finished successfully. Copied {summary.get('success', 0)} files."
            if result.get("failed_payers"):
                 failure_details += f" Payers that failed analysis (config issue): {result['failed_payers']}"
            logger.info(f"SUCCESS: {failure_details}")
            send_processing_metrics(summary.get('success', 0), summary.get('failed', 0), len(params['payer_ids']))
        
        elif result["status"] == "UP_TO_DATE":
            task_status = "Success"
            status_reason = "AlreadySynchronized"
            failure_details = "Task finished successfully, all valid payers were already up-to-date."
            if result.get("failed_payers"):
                 failure_details += f" Payers that failed analysis (config issue): {result['failed_payers']}"
            logger.info(f"SUCCESS: {failure_details}")
            send_processing_metrics(0, 0, len(params['payer_ids']))

        else: # FAILED
            task_status = "Failed"
            status_reason = "ProcessingFailure"
            summary = result.get('copy_summary', {'success': 0, 'failed': 0})
            failed_analysis_payers = result.get("failed_payers", [])
            failure_details = (
                f"Processing failed. Copied {summary.get('success', 0)}/{summary.get('total', 0)} files. "
                f"Payers that failed analysis: {failed_analysis_payers}"
            )
            logger.error(f"FAILED: {failure_details}")
            send_processing_metrics(summary.get('success', 0), summary.get('failed', 0), len(params['payer_ids']))

    except Exception as e:
        logger.error(f"A fatal error occurred in the Fargate task: {e}", exc_info=True)
        status_reason = type(e).__name__
        failure_details = f"Fatal error: {str(e)}"
        send_error_metric('FatalError', str(e))

    finally:
        logger.info(f"--- Task Finalizing ---")
        logger.info(f"  Final Status: {task_status}")
        logger.info(f"  Reason: {status_reason}")

        send_task_completion(task_status, status_reason, Environment=environment)

        # --- START OF CHANGE ---
        # Send Slack notification. This is wrapped in its own try/except
        # to ensure that a failure here doesn't stop other finalization steps.
        try:
            send_slack_notification(
                status=task_status,
                environment=environment,
                details=failure_details,
                params=params
            )
        except Exception as slack_error:
            logger.error(f"CRITICAL: Failed to send final Slack notification: {slack_error}", exc_info=True)
        # --- END OF CHANGE ---

        try:
            notifier = RabbitMQNotifier(environment)
            # Use original list for notification so the caller knows what was requested
            payer_ids_for_mq = original_payer_ids_for_notification if params else []
            
            notifier.send_notification(
                month=params.get('month', 0) if params else 0,
                year=params.get('year', 0) if params else 0,
                module=params.get('module', 'unknown') if params else 'unknown',
                payer_ids=payer_ids_for_mq,
                status=task_status.upper(),
                partner_id=params.get('partner_id', 0) if params else 0,
                message=failure_details
            )

        except Exception as notify_error:
            logger.error(f"CRITICAL: Failed to send final RabbitMQ notification: {notify_error}", exc_info=True)

        logger.info("=" * 80)
        sys.exit(0 if task_status == "Success" else 1)

if __name__ == "__main__":
    main()