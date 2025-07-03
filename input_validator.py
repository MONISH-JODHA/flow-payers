import os
import sys
import json
import logging
from typing import Dict, Any, Optional

from config import STAGING_BUCKET, DEFAULT_APP, DEFAULT_PROCESSING_MODE, DEFAULT_ENVIRONMENT

logger = logging.getLogger(__name__)

class InputReader:
    """Read and parse input for the Fargate task."""

    @staticmethod
    def read_json_input() -> Optional[Dict[str, Any]]:
        """
        Read JSON input, prioritizing the 'TASK_INPUT_JSON' environment variable.
        """
        json_string = os.environ.get('TASK_INPUT_JSON')
        if json_string:
            logger.info("Found JSON input in 'TASK_INPUT_JSON' environment variable.")
            try:
                return json.loads(json_string)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON from 'TASK_INPUT_JSON': {e}")
                logger.error(f"Invalid JSON string: {json_string[:200]}...")
                return None
        return None

    @staticmethod
    def construct_from_individual_env_vars() -> Optional[Dict[str, Any]]:
        """
        Fallback method to construct input from individual environment variables.
        """
        logger.info("Attempting to construct input from individual environment variables as a fallback.")
        try:
            payers_str = os.environ.get('PAYERS') or os.environ.get('PAYER_ID')
            if not payers_str:
                return None

            constructed_json = {
                'year': int(os.environ['YEAR']),
                'month': int(os.environ['MONTH']),
                'payers': [p.strip() for p in payers_str.split(',')],
                'partnerId': int(os.environ['PARTNER_ID']),
                'module': os.environ.get('MODULE', DEFAULT_MODULE),
                'env': os.environ.get('ENV', DEFAULT_ENVIRONMENT)
            }
            logger.info(f"Successfully constructed JSON from individual env vars: {constructed_json}")
            return constructed_json
        except (KeyError, ValueError) as e:
            logger.debug(f"Could not construct JSON from individual env vars, missing or invalid value: {e}")
            return None


class InputValidator:
    """Validate and normalize input parameters."""

    @staticmethod
    def validate_and_normalize(json_data: dict) -> Dict[str, Any]:
        """Validate JSON input parameters and normalize them for the application."""
        logger.info(f"Validating JSON input: {json.dumps(json_data, indent=2)}")

        required_fields = ['year', 'month', 'payers', 'partnerId', 'module']
        missing = [field for field in required_fields if field not in json_data]
        if missing:
            raise ValueError(f"Missing required JSON parameters: {', '.join(missing)}")

        try:
            year = int(json_data['year'])
            if not 2020 <= year <= 2030:
                raise ValueError("Year must be between 2020 and 2030")

            month = int(json_data['month'])
            if not 1 <= month <= 12:
                raise ValueError("Month must be between 1 and 12")

            payers = json_data['payers']
            if not isinstance(payers, list) or not payers:
                raise ValueError("Payers must be a non-empty list")
            payer_ids = [str(p) for p in payers]

            partner_id = int(json_data['partnerId'])
            environment = str(json_data.get('env') or json_data.get('environment', DEFAULT_ENVIRONMENT)).lower()
            module = str(json_data['module']).lower()

            return {
                'year': year,
                'month': month,
                'payer_ids': payer_ids,
                'partner_id': partner_id,
                'environment': environment,
                'module': module,
                'staging_bucket': STAGING_BUCKET,
                'app': DEFAULT_APP,
                'processing_mode': DEFAULT_PROCESSING_MODE
            }
        except (ValueError, TypeError) as e:
            logger.error(f"Parameter validation error: {e}")
            raise


class ParameterProcessor:
    """Gets and validates parameters from the best available source."""

    @staticmethod
    def get_parameters() -> Dict[str, Any]:
        """
        Gets parameters from the primary source (JSON env var) or falls back to
        individual env vars.
        """
        # Try primary method: A single JSON object in an environment variable
        json_data = InputReader.read_json_input()
        if json_data:
            logger.info("📋 Using JSON input method (Primary).")
            return InputValidator.validate_and_normalize(json_data)

        # Try fallback method: Individual environment variables
        json_data = InputReader.construct_from_individual_env_vars()
        if json_data:
            logger.info("📋 Using individual environment variables (Fallback).")
            return InputValidator.validate_and_normalize(json_data)

        raise ValueError("No valid input parameters found from any source. Please set TASK_INPUT_JSON.")