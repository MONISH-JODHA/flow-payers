#!/usr/bin/env python3
"""
Input validation and parsing utilities (Final Corrected Version)
"""
import os
import sys
import json
import logging
from typing import Dict, Any, Optional

from config import DEFAULT_APP, DEFAULT_PROCESSING_MODE, DEFAULT_ENVIRONMENT, DEFAULT_MODULE

logger = logging.getLogger(__name__)

class InputReader:
    """Reads input from the environment, supporting multiple possible variable names."""

    @staticmethod
    def read_json_input() -> Optional[Dict[str, Any]]:
        """
        Gets input JSON by checking a list of possible environment variable names.
        This makes the application resilient to how the calling service provides input.
        """
        # List of possible environment variable names to check, in order of priority.
        # Based on debugging, 'event' is the one currently being used by the backend.
        possible_var_names = ['event', 'TASK_INPUT_JSON']
        
        json_string = None
        source_var_name = None

        for var_name in possible_var_names:
            value = os.environ.get(var_name)
            if value:
                json_string = value
                source_var_name = var_name
                logger.info(f"Found input data in environment variable: '{source_var_name}'")
                break # Stop searching once we find one

        if not json_string:
            logger.error("="*50)
            logger.error("FATAL: No input JSON found.")
            logger.error(f"The application checked for the following environment variables and found none: {possible_var_names}")
            logger.error("Please ensure the service triggering this task is setting one of these variables.")
            logger.error("="*50)
            return None

        try:
            data = json.loads(json_string)
            logger.info(f"Successfully parsed JSON from '{source_var_name}'.")
            return data
        except json.JSONDecodeError as e:
            logger.error(f"FATAL: Failed to decode JSON from '{source_var_name}'. Error: {e}")
            logger.error(f"Invalid JSON string received: {json_string[:500]}...")
            return None


class InputValidator:
    """Validate and normalize input parameters"""

    @staticmethod
    def validate_and_normalize(json_data: dict) -> Dict[str, Any]:
        """Validate input parameters from a dictionary and convert to a standard internal format."""
        if not isinstance(json_data, dict):
            raise TypeError("Input must be a dictionary.")

        logger.info(f"Validating received data: {json.dumps(json_data)}")

        # Handle legacy 'payerAccountIds' key for backward compatibility
        if 'payerAccountIds' in json_data and 'payers' not in json_data:
            logger.warning("API compatibility: Translating 'payerAccountIds' to 'payers'.")
            json_data['payers'] = json_data.pop('payerAccountIds')

        # Check for presence of required fields
        required_fields = ['year', 'month', 'payers']
        missing = [field for field in required_fields if json_data.get(field) is None]
        if missing:
            raise ValueError(f"Payload is missing required non-null values for: {', '.join(missing)}")

        try:
            # --- Type casting and validation ---
            year = int(json_data['year'])
            month = int(json_data['month'])
            payers = json_data['payers']
            
            # Safely get optional values
            partner_id = int(json_data.get('partnerId') or 0)
            environment = str(json_data.get('environment') or json_data.get('env') or DEFAULT_ENVIRONMENT).lower()
            module = str(json_data.get('module') or DEFAULT_MODULE).lower()

            # --- Value range and format validation ---
            if not 2020 <= year <= 2035:
                raise ValueError(f"Year '{year}' is out of the valid range (2020-2035)")
            if not 1 <= month <= 12:
                raise ValueError(f"Month '{month}' is out of the valid range (1-12)")
            if not isinstance(payers, list) or not payers:
                raise ValueError("The 'payers' field must be a non-empty list.")
            
            validated_params = {
                'year': year,
                'month': month,
                'payer_ids': [str(p) for p in payers], # Ensure all payer IDs are strings
                'partner_id': partner_id,
                'environment': environment,
                'module': module,
                'app': DEFAULT_APP,
                'processing_mode': DEFAULT_PROCESSING_MODE
            }
            
            logger.info("Parameter validation successful.")
            return validated_params
            
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Parameter validation failed: {e}", exc_info=True)
            raise


class ParameterProcessor:
    """Process and normalize parameters from the environment."""
    
    @staticmethod
    def get_parameters() -> Dict[str, Any]:
        """Get parameters from the environment."""
        logger.info("Attempting to get and validate task parameters...")
        raw_data = InputReader.read_json_input()
        
        if raw_data:
            return InputValidator.validate_and_normalize(raw_data)
        
        raise ValueError("Could not retrieve parameters. Check logs for details.")