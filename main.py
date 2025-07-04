# main.py (Corrected with Robust Failure Notification)

import sys
import logging
import traceback
import os
import json

from input_validator import ParameterProcessor
from data_copy_service import FargateDataCopyService
from rabbitmq_client import RabbitMQNotifier
from cloudwatch_utils import send_task_completion, send_error_metric, send_processing_metrics
from config import get_environment_config, DEFAULT_ENVIRONMENT

# Using the root logger configured in config.py
logger = logging.getLogger(__name__)

def log_processing_parameters(params):
    """Log processing parameters"""
    logger.info("--- Processing Parameters ---")
    for key, value in params.items():
        logger.info(f"  {key.replace('_', ' ').title()}: {value}")
    logger.info("-----------------------------")

def main():
    """Main function for the Fargate task."""
    task_status = "Failed"
    status_reason = "UnknownError"
    params = None

    environment = os.environ.get('ENV', os.environ.get('ENVIRONMENT', DEFAULT_ENVIRONMENT)).lower()
    logger.info(f"Detected initial environment: '{environment}' for notification purposes.")

    try:
        logger.info("=" * 80)
        logger.info("Starting Fargate Data Copy Task")
        logger.info("=" * 80)

        params = ParameterProcessor.get_parameters()
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

        if result["status"] == "UP_TO_DATE":
            logger.info("SUCCESS: All payer data was already synchronized.")
            task_status = "Success"
            status_reason = "AlreadySynchronized"
            send_processing_metrics(0, 0, len(params['payer_ids']))
        elif result["status"] == "SUCCESS":
            logger.info("SUCCESS: All payer data copied and processed successfully!")
            task_status = "Success"
            status_reason = "ProcessingComplete"
            summary = result['copy_summary']
            send_processing_metrics(summary['success'], summary['failed'], len(params['payer_ids']))
        else:  # FAILED
            logger.error("FAILED: Some or all operations failed!")
            status_reason = "ProcessingFailure"
            summary = result.get('copy_summary', {'success': 0, 'failed': 0})
            send_processing_metrics(summary['success'], summary['failed'], len(params['payer_ids']))
            if result.get("failed_payers"):
                logger.error(f"Payers with failures: {result['failed_payers']}")

    except Exception as e:
        logger.error(f"A fatal error occurred in the Fargate task: {e}", exc_info=True)
        status_reason = type(e).__name__ 
        send_error_metric('FatalError', str(e))

    finally:
        logger.info(f"--- Task Finalizing ---")
        logger.info(f"  Final Status: {task_status}")
        logger.info(f"  Reason: {status_reason}")

        send_task_completion(task_status, status_reason, Environment=environment)

        try:
            notifier = RabbitMQNotifier(environment)

            if params:
                logger.info("Sending detailed RabbitMQ notification...")
                notifier.send_notification(
                    month=params['month'],
                    year=params['year'],
                    module=params.get('module', 'unknown'),
                    payer_ids=params.get('payer_ids', []),
                    status=task_status.upper(),
                    partner_id=params.get('partner_id', 0),
                    message=f"Task finished with status: {status_reason}"
                )
            else:
                logger.warning("Parameters were not parsed. Sending a minimal failure notification.")
                notifier.send_notification(
                    month=0,
                    year=0,
                    module="unknown",
                    payer_ids=[],
                    status=task_status.upper(),
                    partner_id=0,
                    message=f"Task failed with a fatal error before parameters could be parsed. Reason: {status_reason}"
                )

        except Exception as notify_error:
            logger.error(f"CRITICAL: Failed to send final RabbitMQ notification: {notify_error}", exc_info=True)

        logger.info("=" * 80)
        sys.exit(0 if task_status == "Success" else 1)


if __name__ == "__main__":
    main()