import os
import sys
import json
import logging
import select
from typing import Dict, Any, Optional, List

from config import STAGING_BUCKET, DEFAULT_APP, DEFAULT_PROCESSING_MODE, DEFAULT_ENVIRONMENT

logger = logging.getLogger(__name__)

class InputReader:
    """Read input from various sources"""
    
    @staticmethod
    def read_json_input() -> Optional[Dict[str, Any]]:
        """
        Read JSON input from ALL possible sources that developers might use:
        1. Command line arguments
        2. Environment variables (multiple possibilities)
        3. Standard input (stdin)
        4. ECS task overrides
        5. Any environment variable containing JSON
        """
        json_data = None
        
        logger.info("Searching for JSON input from all possible sources...")
        
# ------------------------------------------- Method 1: Try command line arguments ---------------------------------------------------------
        if len(sys.argv) > 1:
            for i, arg in enumerate(sys.argv[1:], 1):
                try:
                    json_data = json.loads(arg)
                    logger.info(f" JSON input found in command line argument {i}")
                    return json_data
                except json.JSONDecodeError:
                    logger.debug(f"Argument {i} is not JSON: {arg[:50]}...")
                    continue
        
# -------------------------------------------- Method 2: Try common environment variable names -----------------------------------------------
        possible_env_vars = [
            'TASK_INPUT_JSON',
            'INPUT_JSON', 
            'JSON_INPUT',
            'TASK_PARAMS',
            'PARAMETERS',
            'CONFIG_JSON',
            'PAYLOAD'
        ]
        
        for env_var in possible_env_vars:
            env_value = os.environ.get(env_var)
            if env_value:
                try:
                    json_data = json.loads(env_value)
                    logger.info(f" JSON input found in environment variable: {env_var}")
                    return json_data
                except json.JSONDecodeError:
                    logger.debug(f"Environment variable {env_var} is not valid JSON")
                    continue
        
# --------------------------------------------- Method 3: Search ALL environment variables for JSON-like content ----------------------------------
        logger.info("Scanning all environment variables for JSON content...")
        for key, value in os.environ.items():
            # Skip system variables and known non-JSON vars
            if key.startswith(('PATH', 'HOME', 'USER', 'SHELL', 'TERM', 'LANG', 'PWD', 'HOSTNAME', 'AWS_', 'PYTHON')):
                continue
            
            # Look for JSON-like strings (starting with { and containing expected fields)
            if value and value.strip().startswith('{') and ('month' in value or 'year' in value or 'payer' in value):
                try:
                    json_data = json.loads(value)
                    # Validate it looks like our expected format
                    if isinstance(json_data, dict) and any(key in json_data for key in ['month', 'year', 'payers', 'partnerId']):
                        logger.info(f" JSON input found in environment variable: {key}")
                        return json_data
                except json.JSONDecodeError:
                    continue
        
# ---------------------------------------------- Method 4: Try reading from stdin ------------------------------------------------------------------
        try:
            if select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
                stdin_input = sys.stdin.read().strip()
                if stdin_input:
                    json_data = json.loads(stdin_input)
                    logger.info(" JSON input received from stdin")
                    return json_data
        except json.JSONDecodeError as e:
            logger.debug(f"Stdin content is not valid JSON: {e}")
        except Exception as e:
            logger.debug(f"No stdin input available: {e}")
        
# ----------------------------------------------- Method 5: Check for file-based input (some orchestrators use files) -------------------------------
        possible_files = ['/tmp/task_input.json', '/app/input.json', './input.json']
        for file_path in possible_files:
            try:
                if os.path.exists(file_path):
                    with open(file_path, 'r') as f:
                        json_data = json.load(f)
                        logger.info(f" JSON input found in file: {file_path}")
                        return json_data
            except Exception as e:
                logger.debug(f"Could not read {file_path}: {e}")
        
# ------------------------------------------------ Method 6: Try to construct from individual environment variables (fallback) -----------------------
        logger.info("Attempting to construct JSON from individual environment variables...")
        constructed_json = {}
        
# ------------------------------------------------ Check for individual parameters --------------------------------------------------
        env_mappings = {
            'month': ['MONTH', 'TASK_MONTH', 'INPUT_MONTH'],
            'year': ['YEAR', 'TASK_YEAR', 'INPUT_YEAR'], 
            'payers': ['PAYERS', 'PAYER_IDS', 'PAYER_ID', 'TASK_PAYERS'],
            'partnerId': ['PARTNER_ID', 'PARTNERID', 'TASK_PARTNER_ID'],
            'module': ['MODULE', 'TASK_MODULE', 'INPUT_MODULE']
        }
        
# ------------------------------------------------ Code to construct own json input if not valid json is found ----------------------------------------        
        for json_key, env_keys in env_mappings.items():
            for env_key in env_keys:
                value = os.environ.get(env_key)
                if value:
                    # Handle payers specially (could be comma-separated)
                    if json_key == 'payers':
                        if ',' in value:
                            constructed_json[json_key] = [p.strip() for p in value.split(',')]
                        elif value.startswith('[') and value.endswith(']'):
                            try:
                                constructed_json[json_key] = json.loads(value)
                            except:
                                constructed_json[json_key] = [value.strip('[]')]
                        else:
                            constructed_json[json_key] = [value]
                    else:
                        # Try to convert to appropriate type
                        try:
                            if json_key in ['month', 'year', 'partnerId']:
                                constructed_json[json_key] = int(value)
                            else:
                                constructed_json[json_key] = value
                        except ValueError:
                            constructed_json[json_key] = value
                    break
        
        if len(constructed_json) >= 3:  # Need at least 3 parameters to be valid
            logger.info(f"JSON constructed from individual environment variables: {constructed_json}")
            return constructed_json
        
        logger.error("No valid JSON input found from any source")
        logger.info(" Checked sources:")
        logger.info("   - Command line arguments")
        logger.info("   - Standard environment variables (TASK_INPUT_JSON, etc.)")
        logger.info("   - All environment variables for JSON content")
        logger.info("   - Standard input (stdin)")
        logger.info("   - Input files")
        logger.info("   - Individual environment variables")
        
        return None


class InputValidator:
    """Validate and normalize input parameters"""
    
    @staticmethod
    def validate_json_parameters(json_data: dict) -> Dict[str, Any]:
        """Validate JSON input parameters and convert to internal format"""
        
        logger.info(" Validating JSON input parameters...")
        logger.info(f" Received JSON: {json.dumps(json_data, indent=2)}")
        
        # Required parameters mapping
        required_fields = {
            'year': 'Year for data processing (e.g., 2025)',
            'month': 'Month for data processing (e.g., 6)', 
            'payers': 'List of payer IDs to process (e.g., ["671238551718"])',
            'partnerId': 'Partner ID (integer, e.g., 1000018)',
            'module': 'Module name (e.g., "Analytics")'
        }
        
        missing_params = []
        validated_params = {}
        
        # Check required parameters
        for field, description in required_fields.items():
            if field not in json_data or json_data[field] is None:
                missing_params.append(f" {field}: {description}")
            else:
                validated_params[field] = json_data[field]
                logger.info(f" {field}: {json_data[field]}")
        
        if missing_params:
            logger.error(" Missing required JSON parameters:")
            for missing in missing_params:
                logger.error(f"   {missing}")
            logger.error("\n Expected JSON format:")
            logger.error('   {"year": 2025, "month": 6, "payers": ["671238551718"], "partnerId": 1000018, "module": "Analytics"}')
            raise ValueError(f"Missing required JSON parameters: {', '.join([p.split(':')[0].strip() for p in missing_params])}")
        
        # Validate data types and values
        try:
            # Validate year
            year = int(validated_params['year'])
            if year < 2020 or year > 2030:
                raise ValueError("Year must be between 2020 and 2030")
            
            # Validate month  
            month = int(validated_params['month'])
            if month < 1 or month > 12:
                raise ValueError("Month must be between 1 and 12")
            
            # Validate payers (must be list)
            payers = validated_params['payers']
            if not isinstance(payers, list) or len(payers) == 0:
                raise ValueError("Payers must be a non-empty list")
            payer_ids = [str(p) for p in payers]
            
            # Validate partnerId
            partner_id = int(validated_params['partnerId'])
            
            # Use environment from JSON if provided, otherwise default to DEFAULT_ENVIRONMENT
            # Check both 'env' and 'environment' keys for robustness
            environment_from_json = json_data.get('env') or json_data.get('environment')
            
            if environment_from_json:
                environment = str(environment_from_json).lower()
                logger.info(f"Environment: {environment} (from JSON input)")
            else:
                environment = DEFAULT_ENVIRONMENT.lower()
                logger.info(f"Environment: {environment} (from config default)")
            
            # Validate module
            module = str(validated_params['module']).lower()
            
            # Return validated parameters
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
            
        except ValueError as e:
            logger.error(f"Parameter validation error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected validation error: {e}")
            raise ValueError(f"Parameter validation failed: {e}")
    
    @staticmethod
    def validate_environment_parameters() -> Dict[str, Any]:
        """Validate environment variables (original method) - kept for backward compatibility"""
        
        logger.info("Validating environment parameters...")
        
        # Required parameters that must be provided
        required_params = {
            'ENV': 'Environment (dev, dev2, dev3, qa1, qa2, uat, prod)',
            'YEAR': 'Year for data processing (e.g., 2025)',
            'MONTH': 'Month for data processing (e.g., 6)',
            'PAYER_ID': 'Payer ID to process',
            'STAGING_BUCKET': 'S3 staging bucket name',
            'PARTNER_ID': 'Partner ID (integer)'
        }
        
        # Optional parameters with defaults
        optional_params = {
            'MODULE': 'analytics',
            'APP': DEFAULT_APP,
            'PROCESSING_MODE': DEFAULT_PROCESSING_MODE,
            'SOURCE_BUCKET': 'Auto-detected from payer configuration',
            'SOURCE_PATH': 'Auto-detected from payer configuration',
            'ENABLE_SNOWFLAKE': 'true'
        }
        
        missing_params = []
        provided_params = {}
        
        # Check required parameters
        for param, description in required_params.items():
            value = os.environ.get(param)
            if not value:
                missing_params.append(f"{param}: {description}")
            else:
                provided_params[param] = value
                logger.info(f"{param}: {value}")
        
        # Check optional parameters
        for param, default in optional_params.items():
            value = os.environ.get(param, default)
            provided_params[param] = value
            logger.info(f"{param}: {value}")
        
        if missing_params:
            logger.error("Missing required environment parameters:")
            for missing in missing_params:
                logger.error(f"   {missing}")
            logger.error("\n Other developers should provide these parameters when triggering the Fargate task")
            raise ValueError(f"Missing required parameters: {', '.join([p.split(':')[0].strip() for p in missing_params])}")
        
        logger.info("All required environment parameters validated successfully")
        return provided_params
    
    @staticmethod
    def convert_env_to_standard_format(env_params: Dict[str, str]) -> Dict[str, Any]:
        """Convert environment parameters to standard format matching JSON validation"""
        
        # Convert environment variables to same format as JSON
        payer_ids_input = env_params.get('PAYER_ID', '')
        payer_ids = [pid.strip() for pid in payer_ids_input.split(',') if pid.strip()]
        
        return {
            'year': int(env_params['YEAR']),
            'month': int(env_params['MONTH']),
            'payer_ids': payer_ids,
            'partner_id': int(env_params['PARTNER_ID']),
            'environment': env_params['ENV'].lower(),
            'module': env_params['MODULE'].lower(),
            'staging_bucket': env_params.get('STAGING_BUCKET', STAGING_BUCKET),
            'app': env_params.get('APP', DEFAULT_APP),
            'processing_mode': env_params.get('PROCESSING_MODE', DEFAULT_PROCESSING_MODE).lower()
        }


class ParameterProcessor:
    """Process and normalize parameters from different sources"""
    
    @staticmethod
    def get_parameters() -> Dict[str, Any]:
        """Get parameters from the best available source"""
        
        # Try JSON input first (preferred method)
        json_data = InputReader.read_json_input()
        
        if json_data:
            logger.info("📋 Using JSON input method")
            return InputValidator.validate_json_parameters(json_data)
        
        # Fallback to environment variables
        logger.info("No JSON input found, trying environment variables...")
        try:
            env_params = InputValidator.validate_environment_parameters()
            validated_params = InputValidator.convert_env_to_standard_format(env_params)
            logger.info("Using environment variables method")
            return validated_params
            
        except Exception as env_error:
            logger.error(f" Environment variables validation failed: {env_error}")
            logger.error(" No valid input found from JSON or environment variables!")
            logger.error(" Please provide input via:")
            logger.error("   1. JSON: Set TASK_INPUT_JSON environment variable")
            logger.error("   2. Environment variables: Set YEAR, MONTH, PAYER_ID, etc.")
            raise ValueError("No valid input parameters found from any source")
    
    @staticmethod 
    def validate_parameter_ranges(params: Dict[str, Any]) -> Dict[str, Any]:
        """Additional validation for parameter ranges and business rules"""
        
        # Validate year range
        if params['year'] < 2020 or params['year'] > 2030:
            raise ValueError(f"Year {params['year']} is outside valid range (2020-2030)")
        
        # Validate month range
        if params['month'] < 1 or params['month'] > 12:
            raise ValueError(f"Month {params['month']} is outside valid range (1-12)")
        
        # Validate payer IDs format
        for payer_id in params['payer_ids']:
            if not payer_id.isdigit() or len(payer_id) != 12:
                logger.warning(f"Payer ID {payer_id} doesn't match expected 12-digit format")
        
        # Validate environment
        valid_environments = ['dev', 'dev2', 'qa1', 'uat', 'prod', 'staging']
        if params['environment'] not in valid_environments:
            logger.warning(f"Environment '{params['environment']}' not in standard list: {valid_environments}")
        
        # Validate processing mode
        valid_modes = ['production', 'mock', 'test']
        if params['processing_mode'] not in valid_modes:
            logger.warning(f"Processing mode '{params['processing_mode']}' not recognized, using 'production'")
            params['processing_mode'] = 'production'
        
        return params
