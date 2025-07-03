#
# snowflake_external_table.py (Corrected with proper DDL construction)
#
import os
import json
import logging
import boto3
import snowflake.connector
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from botocore.exceptions import ClientError

from config import SNOWFLAKE_CONFIG

logger = logging.getLogger(__name__)

def _split_s3_path(s3_path: str) -> Tuple[str, str]:
    """Splits an s3 path like 's3://bucket/path/to/folder' into bucket and path."""
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]
    parts = s3_path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    return bucket.strip(), key.strip()

class SnowflakeConfigFetcher:
    """Fetches and processes payer configuration from a dedicated Snowflake table."""
    def __init__(self, env: str):
        self.env = env.lower()
        self.connection = None
        self.cursor = None
        self.snowflake_params = SNOWFLAKE_CONFIG
        self.config_table = "aws_az_share.metadata.pro_refresh_config"
        logger.info(f"Using Snowflake config table: {self.config_table}")

    def _connect(self):
        try:
            logger.info(f"Connecting to Snowflake to fetch payer configurations for env: {self.env}")
            self.connection = snowflake.connector.connect(**self.snowflake_params)
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to Snowflake for config fetching.")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake for config fetching: {e}")
            raise

    def _close(self):
        if self.cursor: self.cursor.close()
        if self.connection: self.connection.close()

    def get_payer_configs(self) -> Dict[str, Any]:
        """Fetches and maps payer configs from the Snowflake table."""
        configs = {}
        try:
            self._connect()
            query = f"""
            SELECT PAYER_ACCOUNT_ID, PAYER_NAME, PAYER_BUCKET_PATH
            FROM {self.config_table} WHERE ENABLE_REFRESH_PAYER = TRUE
            """
            logger.info(f"Executing query to fetch payer configs: {query.strip()}")
            self.cursor.execute(query)
            results = self.cursor.fetchall()

            for payer_account_id, payer_name, payer_bucket_path in results:
                bucket, path = _split_s3_path(payer_bucket_path)
                configs[str(payer_account_id)] = {
                    "name": payer_name, "bucket": bucket, "path": path,
                    "access_type": "CROSS_ACCOUNT"
                }
            logger.info(f"Fetched and processed {len(configs)} active payer configurations from Snowflake.")
        except Exception as e:
            logger.error(f"Error fetching configs from Snowflake: {e}")
            raise
        finally:
            self._close()
        return configs

class SnowflakeExternalTableManager:
    """Manages Snowflake external table operations for analytics processing."""
    def __init__(self, env: str, module: str):
        self.env = env.lower()
        self.module = module.lower()
        self.connection = None
        self.cursor = None
        logger.info(f"Initializing SnowflakeExternalTableManager for {self.module} in {self.env}")

    def get_last_processed_timestamp(self, payer_id: str) -> Optional[datetime]:
        """Queries the final table to get the latest timestamp for a specific payer."""
        if not self.connection or not self.cursor or self.connection.is_closed():
            self.connect()
        
        query = """
        SELECT MAX(LINEITEM_USAGESTARTDATE) 
        FROM CK_ANALYTICS_APPLICATION_RI_WASTAGE_HOURLY 
        WHERE BILL_PAYERACCOUNTID = %s
        """
        try:
            logger.info(f"Querying for last processed timestamp for payer {payer_id}")
            self.cursor.execute(query, (payer_id,))
            result = self.cursor.fetchone()
            if result and result[0]:
                last_timestamp = result[0]
                logger.info(f"Last processed timestamp for payer {payer_id} is {last_timestamp}")
                return last_timestamp
            else:
                logger.info(f"No previous data found for payer {payer_id}. Will process all data.")
                return None
        except Exception as e:
            logger.error(f"Could not get last processed timestamp for payer {payer_id}: {e}. Will process all data.")
            return None

    def get_secret_value(self, secret_id: str) -> Dict[str, Any]:
        """Get secret from AWS Secrets Manager."""
        try:
            client = boto3.client('secretsmanager', region_name='us-east-1')
            response = client.get_secret_value(SecretId=secret_id)
            return json.loads(response['SecretString'])
        except ClientError as e:
            logger.error(f"Could not retrieve secret {secret_id}: {e}")
            raise

    def create_db_connection_analytics(self) -> snowflake.connector.SnowflakeConnection:
        """Create Snowflake connection for analytics module, aware of environment."""
        if self.env == 'prod':
            logger.info("Connecting to Snowflake PROD environment for analytics")
            secret_id = 'snowflake/jenkins/payer/cln_data_payer_summary_prod'
            secret = self.get_secret_value(secret_id)

            if not secret:
                raise ValueError(f"Could not retrieve PROD secret '{secret_id}'. Cannot connect.")

            override_warehouse = 'PAYER_PROD_SUMMARY_PROFITABILITY_APP'
            logger.warning("--- TEMPORARY WAREHOUSE OVERRIDE IN EFFECT ---")
            logger.warning(f"Original warehouse from secret: {secret.get('warehouse')}")
            logger.warning(f"Forcing use of warehouse: {override_warehouse}")
            secret['warehouse'] = override_warehouse
            
            logger.info(f"Connecting with user '{secret.get('user')}' and overridden warehouse '{secret.get('warehouse')}'")
            return snowflake.connector.connect(**secret)
        else:
            logger.info(f"Connecting to Snowflake UAT/Non-Prod for analytics (env: {self.env})")
            return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

    def connect(self):
        """Establish Snowflake connection based on module and environment."""
        try:
            if self.module == 'analytics':
                self.connection = self.create_db_connection_analytics()
            else:
                raise ValueError(f"Unsupported module: {self.module}")
            self.cursor = self.connection.cursor()
            logger.info(f"Snowflake connection established for module: {self.module}")
        except Exception as e:
            logger.error(f"Failed to establish Snowflake connection: {e}", exc_info=True)
            raise

    def close_connection(self):
        if self.cursor: self.cursor.close()
        if self.connection and not self.connection.is_closed(): self.connection.close()
        logger.info("Snowflake connection closed")
    
    def get_storage_integration(self) -> str:
        return 'AWS_S3_CK_DATAPIPELINE_NON_PROD_INC' if self.env != 'prod' else 'aws_s3_billdesk'

    def table_refresh(self, year: int, month: int, staging_bucket: str, payer_ids: List[str], app: str):
        if not self.connection or not self.cursor:
            raise ValueError("Snowflake connection not established.")
        if self.module == 'analytics':
            self._process_analytics_module(year, month, staging_bucket, payer_ids, app)
        else:
            raise ValueError(f"Unsupported module for table refresh: {self.module}")

    def _process_analytics_module(self, year: int, month: int, staging_bucket: str, payer_ids: List[str], app: str):
        """Process analytics module - create stage, infer schema, create external table, and run queries."""
        stage_name = 'wastage_analytics_stage_application'
        stage_url = f's3://{staging_bucket}/{app}/{self.module}/{self.env}/year={year}/month={month}/'
        storage_integration = self.get_storage_integration()
        
        create_stage_query = f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL = '{stage_url}'
        STORAGE_INTEGRATION = {storage_integration}
        FILE_FORMAT = (TYPE = 'PARQUET', COMPRESSION = 'SNAPPY');
        """

        logger.info(f"stage_query: {create_stage_query}")
        logger.info(f"Creating analytics stage '{stage_name}' with URL: {stage_url}")
        self.cursor.execute(create_stage_query)
        logger.info("Analytics stage created successfully.")

        
        query = f'''SELECT COLUMN_NAME,TYPE,EXPRESSION,COLUMN_NAME || ' ' || TYPE || ' AS ' || '(' || EXPRESSION || ')' FROM TABLE(
                                INFER_SCHEMA(
                                LOCATION=> '@{stage_name}',
                                file_format => 'parquet_working_format'
                                                ));'''


        self.cursor.execute(query)
        cur_schema: list = self.cursor.fetchall()
        cur_columns: list = [x[3].lower() for x in cur_schema]

        logger.info(f"query stage name: {query}")


        logger.info("🔍 Inferring schema from Parquet files...")
        columns_result: str = ", ".join(cur_columns)

        table_name = f"analytics_application_table_{year}_{month}"
        create_external_table = f'''CREATE OR REPLACE EXTERNAL TABLE {table_name}
                                ({columns_result})
                                LOCATION = @{stage_name},
                                FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY');'''
        
        self.cursor.execute(create_external_table)







        logger.info(f"Creating external table: {create_external_table}")
        # self.cursor.execute(create_external_table)
        logger.info(f"External table '{table_name}' created successfully with inferred schema.")

        logger.info(f"Refreshing external table: {table_name}")

        self._run_analytics_queries(year, month, payer_ids)

    def _run_analytics_queries(self, year: int, month: int, payer_ids: List[str]):
        query_file_path = "analytics_wastage_queries.sql"
        if not os.path.exists(query_file_path):
            logger.warning(f"Analytics query file not found: {query_file_path}. Skipping.")
            return

        logger.info(f"Attempting to run analytics queries from: {query_file_path}")
        try:
            with open(query_file_path, "r") as f:
                query_sql = f.read()
            
            payer_ids_sql_str = ",".join([f"'{p}'" for p in payer_ids]) if payer_ids else "''"
            
            query_sql = query_sql.replace('#startyear', str(year))
            query_sql = query_sql.replace('#startmonth', str(month))
            query_sql = query_sql.replace('(#payers_ids)', f"({payer_ids_sql_str})")
            query_sql = query_sql.replace('(#payers_id)', f"({payer_ids_sql_str})")
            
            logger.info("--- EXECUTING FINAL SNOWFLAKE SCRIPT ---")
            logger.info(query_sql[:1000] + "...")
            logger.info("-----------------------------------------")

            for cur in self.connection.execute_string(query_sql):
                logger.info(f"Executed a statement in the script. Rows affected: {cur.rowcount}")
            
            logger.info("All analytics queries/script executed successfully.")
        except Exception as e:
            logger.error(f"A NON-FATAL ERROR occurred while executing analytics queries: {e}")


def create_external_table_and_process(env: str, module: str, year: int, month: int,
                                    staging_bucket: str, payer_ids: List[str], app: str):
    snowflake_manager = None
    try:
        snowflake_manager = SnowflakeExternalTableManager(env, module)
        snowflake_manager.connect()
        snowflake_manager.table_refresh(year, month, staging_bucket, payer_ids, app)
        return True
    except Exception as e:
        logger.error(f"Failed to create external table and process data: {e}", exc_info=True)
        raise
    finally:
        if snowflake_manager:
            snowflake_manager.close_connection()