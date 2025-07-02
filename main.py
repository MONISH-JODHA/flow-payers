import sys
import logging
import traceback
import os

from input_validator import InputReader, ParameterProcessor
from data_copy_service import FargateDataCopyService
from rabbitmq_client import RabbitMQNotifier
from cloudwatch_utils import send_task_completion, send_error_metric
from config import get_environment_config

# Configure logging for ECS/Fargate
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def log_environment_variables():
    """Log environment variables with security masking"""
    
    logger.info(" Debug: All Environment Variables:")
    for key, value in sorted(os.environ.items()):
        # Mask sensitive values
        if any(sensitive in key.upper() for sensitive in ['PASSWORD', 'SECRET', 'KEY', 'TOKEN']):
            masked_value = '*' * len(value) if value else 'None'
            logger.info(f"   {key}={masked_value}")
        else:
            # Show first 100 chars to avoid log spam
            display_value = value[:100] + '...' if value and len(value) > 100 else value
            logger.info(f"   {key}={display_value}")


def parse_input_parameters():
    """Parse and validate input parameters from various sources"""
    
    logger.info("Reading input parameters...")
    
    try:
        # Use the parameter processor to get and validate parameters
        params = ParameterProcessor.get_parameters()
        
        # Additional business rule validation
        params = ParameterProcessor.validate_parameter_ranges(params)
        
        return params
        
    except Exception as e:
        logger.error(f" Parameter parsing failed: {str(e)}")
        send_task_completion('Failed', 'InvalidInput')
        raise


def log_processing_parameters(params):
    """Log processing parameters"""
    logger.info(" Processing Parameters:")
    logger.info(f"   Environment: {params['environment']}")
    logger.info(f"   Year: {params['year']}")
    logger.info(f"   Month: {params['month']}")
    logger.info(f"   Payer IDs: {params['payer_ids']} ({len(params['payer_ids'])} payers)")
    logger.info(f"   Partner ID: {params['partner_id']}")
    logger.info(f"   Module: {params['module']}")
    logger.info(f"   Staging Bucket: {params['staging_bucket']}")
    logger.info(f"   Processing Mode: {params['processing_mode']}")
    logger.info(f"   App: {params['app']}")

def handle_production_mode(copy_service, params):
    """Handle production mode processing"""
    logger.info("PRODUCTION MODE: Processing real data files...")
    
    # Process multiple payers
    success = copy_service.process_multiple_payers(
        payer_ids=params['payer_ids'],
        year=params['year'],
        month=params['month'],
        staging_bucket=params['staging_bucket'],
        app=params['app'],
        module=params['module'],
        partner_id=params['partner_id']
    )
    
    if success:
        logger.info("SUCCESS: All payer data copied and processed successfully!")
        send_task_completion('Success', 
                           PayerCount=str(len(params['payer_ids'])),
                           Environment=params['environment'])
        return True
    else:
        logger.error("FAILED: Some or all operations failed!")
        send_task_completion('Failed', 'ProcessingFailure',
                           PayerCount=str(len(params['payer_ids'])),
                           Environment=params['environment'])
        return False


def send_failure_notification_on_error(raw_json_data=None):
    """Send failure notification when the process fails"""
    try:
        # Try to get raw JSON data first
        if not raw_json_data:
            raw_json_data = InputReader.read_json_input()
            
        if raw_json_data:
            payer_ids = [str(p) for p in raw_json_data.get('payers', [])]
            if payer_ids:
                environment = raw_json_data.get('environment', 'dev')
                notifier = RabbitMQNotifier(environment)
                notifier.send_notification(
                    month=int(raw_json_data.get('month', 1)),
                    year=int(raw_json_data.get('year', 2025)),
                    module=str(raw_json_data.get('module', 'analytics')).lower(),
                    payer_ids=payer_ids,
                    status="FAILED",
                    partner_id=int(raw_json_data.get('partnerId', 1))
                )
                logger.info(" Sent failure notification to RabbitMQ")
    except Exception as notification_error:
        logger.error(f"Failed to send failure notification: {str(notification_error)}")


def validate_environment_setup():
    """Validate that the environment is properly set up"""
    
    logger.info(" Validating environment setup...")
    
    # Check critical environment variables
    critical_vars = ['AWS_DEFAULT_REGION', 'AWS_REGION']
    
    for var in critical_vars:
        if os.environ.get(var):
            logger.info(f" {var}: {os.environ.get(var)}")
            break
    else:
        logger.warning(" No AWS region environment variables found, using defaults")
    
    # Check if running in ECS
    if os.environ.get('ECS_CONTAINER_METADATA_URI_V4'):
        logger.info(" Running in ECS Fargate environment")
    else:
        logger.info(" Running in local/development environment")
    
    # Check available memory and CPU (if in ECS)
    # try:
    #     import psutil
    #     memory_gb = psutil.virtual_memory().total / (1024**3)
    #     cpu_count = psutil.cpu_count()
    #     logger.info(f" Available resources: {memory_gb:.1f}GB RAM, {cpu_count} CPUs")
    # except ImportError:
    #     logger.debug("psutil not available, skipping resource check")


def main():
    """Main function for enhanced Fargate task with modular architecture"""
    
    raw_json_data = None
    
    try:
        logger.info("Starting Enhanced Fargate Data Copy Task with Snowflake Integration")
        logger.info("Version 4.0 - Modular Architecture")
        logger.info("="*80)
        
        # Validate environment setup
        validate_environment_setup()
        
        # Debug: Print environment variables
        log_environment_variables()
        
        # Store raw JSON data early for error handling
        raw_json_data = InputReader.read_json_input()
        
        # Parse and validate input parameters
        params = parse_input_parameters()
        
        # Log processing parameters
        log_processing_parameters(params)
        
        # Get environment configuration
        env_config = get_environment_config(params['environment'])
        logger.info(f" Environment config: {env_config}")
        
        # Update staging bucket from environment config if different
        if env_config.get('staging_bucket') != params['staging_bucket']:
            logger.info(f"Updating staging bucket from {params['staging_bucket']} to {env_config['staging_bucket']}")
            params['staging_bucket'] = env_config['staging_bucket']
        
        # Initialize the data copy service
        logger.info(f"Initializing FargateDataCopyService for environment: {params['environment']}")
        copy_service = FargateDataCopyService(params['environment'])
        
            # Force production mode for any non-mock mode
        if params['processing_mode'] != 'production':
            logger.warning(f"Processing mode '{params['processing_mode']}' detected, forcing to production")
            params['processing_mode'] = 'production'
            
        success = handle_production_mode(copy_service, params)
        
        # Exit with appropriate code
        if success:
            logger.info("Task completed successfully!")
            logger.info("="*80)
            sys.exit(0)
        else:
            logger.error("Task failed!")
            logger.error("="*80)
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Task interrupted by user")
        send_task_completion('Failed', 'UserInterrupt')
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Fatal error in Fargate task: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error("Full traceback:")
        logger.error(traceback.format_exc())
        
        # Send error metrics and notifications
        send_error_metric('FatalError', str(e))
        send_task_completion('Failed', 'FatalError')
        
        # Try to send failure notification
        send_failure_notification_on_error(raw_json_data)
        
        logger.error("="*80)
        sys.exit(1)


if __name__ == "__main__":
    main()
