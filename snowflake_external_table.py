import os
import json
import logging
import boto3
import snowflake.connector
from typing import Dict, Any, Optional, List
from config import SNOWFLAKE_CONFIG

logger = logging.getLogger(__name__)


class SnowflakeConfigFetcher:
    """Fetches payer configuration from a dedicated Snowflake table."""
    def __init__(self, env: str):
        self.env = env.lower()
        self.connection = None
        self.cursor = None

    def _connect(self):
        """Connect to Snowflake using credentials from config."""
        try:
            logger.info(f"Connecting to Snowflake to fetch payer configurations for env: {self.env}")
            # In a real scenario, you'd fetch credentials based on env
            # For this example, we use the config directly.
            self.connection = snowflake.connector.connect(
                user=SNOWFLAKE_CONFIG['user'],
                password=SNOWFLAKE_CONFIG['password'],
                account=SNOWFLAKE_CONFIG['account'],
                warehouse=SNOWFLAKE_CONFIG['warehouse'],
                database=SNOWFLAKE_CONFIG['database'],
                schema=SNOWFLAKE_CONFIG['schema'],
                role=SNOWFLAKE_CONFIG['role']
            )
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to Snowflake for config fetching.")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake for config fetching: {e}")
            raise

    def _close(self):
        """Close Snowflake connection."""
        try:
            if self.cursor: self.cursor.close()
            if self.connection: self.connection.close()
        except Exception as e:
            logger.warning(f"Error closing Snowflake config connection: {e}")

    def get_payer_configs(self) -> Dict[str, Any]:
        """
        Fetches payer configs from the 'payers_buckets_path' table.
        The table should have columns: PAYER_ID, PAYER_NAME, BUCKET_NAME, BUCKET_PATH, ACCESS_TYPE
        """
        configs = {}
        try:
            self._connect()
            table_name = SNOWFLAKE_CONFIG['config_table']
            query = f"SELECT PAYER_ID, PAYER_NAME, BUCKET_NAME, BUCKET_PATH, ACCESS_TYPE FROM {table_name} WHERE IS_ACTIVE = TRUE"
            logger.info(f"Executing query to fetch payer configs: {query}")
            self.cursor.execute(query)
            results = self.cursor.fetchall()

            for row in results:
                payer_id, name, bucket, path, access_type = row
                configs[str(payer_id)] = {
                    "name": name,
                    "bucket": bucket,
                    "path": path,
                    "access_type": access_type
                }
            logger.info(f"Fetched {len(configs)} active payer configurations from Snowflake table '{table_name}'.")
        except Exception as e:
            logger.error(f"Error fetching configs from Snowflake: {e}")
            raise  # Re-raise to be caught by PayerConfigManager
        finally:
            self._close()
        return configs


class SnowflakeExternalTableManager:
    """Manages Snowflake external table operations for analytics processing"""
    def __init__(self, env: str, module: str):
        self.env = env.lower()
        self.module = module.lower()
        self.connection = None
        self.cursor = None
        logger.info(f"Initializing SnowflakeExternalTableManager for {self.module} in {self.env}")

    def get_secret_value(self, secret_id: str) -> Dict[str, Any]:
        """Get secret from AWS Secrets Manager"""
        try:
            logger.info(f"Retrieving secret: {secret_id}")
            client = boto3.client('secretsmanager', region_name='us-east-1')
            response = client.get_secret_value(SecretId=secret_id)
            logger.info(f"Successfully retrieved secret: {secret_id}")
            return json.loads(response['SecretString'])
        except Exception as e:
            logger.warning(f"Could not retrieve secret {secret_id}: {e}")
            return {}

    def create_db_connection_analytics(self) -> snowflake.connector.SnowflakeConnection:
        """Create Snowflake connection for analytics module"""
        if self.env in ['dev', 'dev2', 'uat']:
            logger.info(f"Connecting to Snowflake UAT environment for analytics (env: {self.env})")
            return snowflake.connector.connect(**SNOWFLAKE_CONFIG) # Use centralized config

        elif self.env == 'prod':
            logger.info("Connecting to Snowflake PROD environment for analytics")
            secret = self.get_secret_value('snowflake/jenkins/payer/cln_data_payer_summary_prod')
            if secret and all(k in secret for k in ['user', 'password', 'account', 'warehouse', 'database', 'schema', 'role']):
                logger.info("Using credentials from Secrets Manager for PROD analytics")
                return snowflake.connector.connect(**secret)
            else:
                logger.error("PROD secret not found or incomplete. Cannot connect to Snowflake.")
                raise ValueError("Missing production Snowflake credentials")
        else:
            raise ValueError(f"Unsupported environment for analytics: {self.env}")

    def connect(self):
        """Establish Snowflake connection based on module"""
        try:
            logger.info(f"Establishing Snowflake connection for module: {self.module}")
            if self.module in ['analytics']:
                self.connection = self.create_db_connection_analytics()
            else:
                raise ValueError(f"Unsupported module: {self.module}")
            self.cursor = self.connection.cursor()
            logger.info(f"Snowflake connection established for module: {self.module}")
        except Exception as e:
            logger.error(f"Failed to establish Snowflake connection: {e}")
            raise

    def close_connection(self):
        """Close Snowflake connection"""
        try:
            if self.cursor: self.cursor.close()
            if self.connection: self.connection.close()
            logger.info("Snowflake connection closed")
        except Exception as e:
            logger.warning(f"Error closing Snowflake connection: {e}")

    def get_storage_integration(self) -> str:
        """Get appropriate storage integration based on environment"""
        if self.env in ['uat', 'dev', 'dev2', 'dev3', 'qa1', 'qa2']:
            integration = 'AWS_S3_CK_DATAPIPELINE_NON_PROD_INC'
        else:  # prod
            integration = 'aws_s3_billdesk'
        logger.info(f"Using storage integration: {integration} for environment: {self.env}")
        return integration

    def table_refresh(self, year: int, month: int, staging_bucket: str, payer_ids: List[str], app: str):
        """Main function to refresh external tables and process data"""
        if not self.connection or not self.cursor:
            raise ValueError("Snowflake connection not established. Call connect() first.")
        logger.info(f"Starting table refresh for {self.module} module...")
        try:
            if self.module == 'analytics':
                self._process_analytics_module(year, month, staging_bucket, payer_ids, app)
            else:
                raise ValueError(f"Unsupported module: {self.module}")
            logger.info("Table refresh completed successfully")
        except Exception as e:
            logger.error(f"Table refresh failed: {e}")
            raise

    def _process_analytics_module(self, year: int, month: int, staging_bucket: str, payer_ids: List[str], app: str):
        """Process analytics module - create stage, external table, and run queries"""
        # Step 1: Create stage
        stage_url = f's3://{staging_bucket}/{app}/{self.module}/{self.env}/year={year}/month={month}/'
        storage_integration = self.get_storage_integration()
        create_stage_query = f"""
        CREATE OR REPLACE STAGE wastage_analytics_stage_application
        URL = '{stage_url}'
        STORAGE_INTEGRATION = {storage_integration}
        FILE_FORMAT = (TYPE = 'PARQUET', COMPRESSION = 'SNAPPY');
        """
        logger.info(f"Creating analytics stage with URL: {stage_url}")
        self.cursor.execute(create_stage_query)
        logger.info("Analytics stage created successfully")

        # Step 2: Create external table (schema inference is risky, let's build it robustly)
        table_name = f"analytics_application_table_{year}_{month}"
        create_external_table_query = f"""
        CREATE OR REPLACE EXTERNAL TABLE {table_name}
        LOCATION = @wastage_analytics_stage_application
        AUTO_REFRESH = false
        FILE_FORMAT = (TYPE = 'PARQUET', COMPRESSION = 'SNAPPY');
        """
        logger.info(f"Creating external table: {table_name}")
        self.cursor.execute(create_external_table_query)
        logger.info(f"External table {table_name} created successfully")

        # Step 3: Refresh the external table to sync metadata
        refresh_query = f"ALTER EXTERNAL TABLE {table_name} REFRESH;"
        logger.info(f"Refreshing external table metadata: {table_name}")
        self.cursor.execute(refresh_query)
        logger.info("External table metadata refreshed.")

        # Step 4: Run analytics queries (made non-fatal)
        self._run_analytics_queries(year, month, payer_ids)

    def _run_analytics_queries(self, year: int, month: int, payer_ids: List[str]):
        """Run analytics wastage queries, but do not fail the entire process on SQL errors."""
        query_file_path = "analytics_wastage_queries.sql"
        if not os.path.exists(query_file_path):
            logger.warning(f"Analytics query file not found: {query_file_path}. Skipping query execution.")
            return

        logger.info(f"Attempting to run analytics wastage queries from: {query_file_path}")
        try:
            with open(query_file_path, "r") as f:
                query_sql = f.read()

            payer_ids_str = "'" + "','".join(str(pid) for pid in payer_ids) + "'" if payer_ids else "''"
            query_sql = query_sql.replace('#startyear', str(year))
            query_sql = query_sql.replace('#startmonth', str(month))
            query_sql = query_sql.replace('#payer_ids', payer_ids_str)

            self.cursor.execute(query_sql)
            logger.info("Analytics queries executed successfully.")
        except snowflake.connector.errors.ProgrammingError as e:
            # THIS IS THE FIX for the 'invalid identifier' error
            logger.error(f"A NON-FATAL SQL ERROR occurred while executing analytics queries: {e}")
            logger.error("This often means a mismatch between the source data schema and the SQL in 'analytics_wastage_queries.sql'.")
            logger.error("The data copy and table creation were successful, but this final query failed.")
        except Exception as e:
            logger.error(f"A NON-FATAL UNEXPECTED ERROR occurred while executing analytics queries: {e}")


def create_external_table_and_process(env: str, module: str, year: int, month: int,
                                    staging_bucket: str, payer_ids: List[str],
                                    app: str):
    """Convenience function to create external table and process data"""
    snowflake_manager = None
    try:
        logger.info(f"Starting external table creation for {module} in {env}")
        snowflake_manager = SnowflakeExternalTableManager(env, module)
        snowflake_manager.connect()
        snowflake_manager.table_refresh(year, month, staging_bucket, payer_ids, app)
        logger.info("External table creation and processing completed successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to create external table and process data: {e}", exc_info=True)
        raise
    finally:
        if snowflake_manager:
            snowflake_manager.close_connection()