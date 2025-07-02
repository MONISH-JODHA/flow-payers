import os
import json
import logging
import boto3 
import snowflake.connector
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

class SnowflakeExternalTableManager:
    """Manages Snowflake external table operations for analytics processing"""
    
    def __init__(self, env: str, module: str):
        """
        Initialize Snowflake manager
        
        Args:
            env: Environment (uat, prod, dev, dev2, etc.)
            module: Module name (analytics, auto-analytics)
        """
        self.env = env.lower()  # Normalize to lowercase
        self.module = module.lower()  # Normalize to lowercase
        self.connection = None
        self.cursor = None
        
        logger.info(f"Initializing SnowflakeExternalTableManager for {self.module} in {self.env}")
        
    def get_secret_value(self, secret_id: str) -> Dict[str, Any]:
        """Get secret from AWS Secrets Manager"""
        try:
            logger.info(f"Retrieving secret: {secret_id}")
            # Corrected: Use boto3.client directly, specifying region_name
            client = boto3.client('secretsmanager', region_name='us-east-1') # Explicitly set region for Secrets Manager
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
            conn = snowflake.connector.connect(
                user='payer_uat_jenkins',
                password='R8xVD7w!8A',
                account='tmb05570.us-east-1',
                warehouse='PAYER_UAT_SUMMARY_ETL',
                database='payer_uat',
                schema='payer_analytics_summary',
                role='ROLE_UAT_PAYER_SUMMARY_ETL'
            )
            logger.info("Connected to Snowflake UAT environment for analytics")
            return conn
            
        elif self.env == 'prod':
            logger.info("Connecting to Snowflake PROD environment for analytics")
            
            # Try to get credentials from secrets manager first
            secret = self.get_secret_value('snowflake/jenkins/payer/cln_data_payer_summary_prod')
            
            if secret and all(k in secret for k in ['user', 'password', 'account', 'warehouse', 'database', 'schema', 'role']):
                logger.info("Using credentials from Secrets Manager for PROD analytics")
                conn = snowflake.connector.connect(
                    user=secret.get('user'),
                    password=secret.get('password'),
                    account=secret.get('account'),
                    warehouse=secret.get('warehouse'),
                    database=secret.get('database'),
                    schema=secret.get('schema'),
                    role=secret.get('role')
                )
            else:
                logger.warning("Could not use Secrets Manager (secret not found or incomplete). Using hardcoded production credentials (NOT RECOMMENDED).")
                conn = snowflake.connector.connect(
                    user='payer_jenkins',
                    password='/3qn=(9N)&',
                    account='khb76882.us-east-1',
                    warehouse='PAYER_PROD_2xlarge_adhoc',
                    database='payer_prod',
                    schema='payer_analytics_summary',
                    role='ROLE_PROD_PAYER_SUMMARY_ETL'
                )

            logger.info("Connected to Snowflake PROD environment for analytics")
            return conn
        else:
            raise ValueError(f"Unsupported environment for analytics: {self.env}")
    
    def create_db_connection_auto_analytics(self) -> snowflake.connector.SnowflakeConnection:
        """Create Snowflake connection for auto-analytics module"""
        logger.info(f"Connecting to Snowflake for auto-analytics in {self.env}")
        
        # Get credentials from secrets manager for auto-analytics
        secret = self.get_secret_value(f'snowflake-auto-analytics-{self.env}')
        if not secret:
            raise ValueError(f"Auto-analytics Snowflake credentials not configured for {self.env}")
            
        conn = snowflake.connector.connect(
            user=secret.get('user'),
            password=secret.get('password'),
            account=secret.get('account'),
            warehouse=secret.get('warehouse'),
            database=secret.get('database'),
            schema=secret.get('schema'),
            role=secret.get('role')
        )
        logger.info(f"Connected to Snowflake {self.env.upper()} environment for auto-analytics")
        return conn
    
    def connect(self):
        """Establish Snowflake connection based on module"""
        try:
            logger.info(f"Establishing Snowflake connection for module: {self.module}")
            
            if self.module in ['analytics']:
                self.connection = self.create_db_connection_analytics()
            elif self.module == 'auto-analytics':
                self.connection = self.create_db_connection_auto_analytics()
            else:
                raise ValueError(f" Unsupported module: {self.module}")
            
            self.cursor = self.connection.cursor()
            logger.info(f"Snowflake connection established for module: {self.module}")
            
        except Exception as e:
            logger.error(f"Failed to establish Snowflake connection: {e}")
            raise
    
    def close_connection(self):
        """Close Snowflake connection"""
        try:
            if self.cursor:
                self.cursor.close()
                logger.debug("Snowflake cursor closed")
            if self.connection:
                self.connection.close()
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
    
    def table_refresh(self, year: int, month: int, staging_bucket: str, payer_ids: List[str], app: str = 'aws_az_analytics_application_refresh'):
        """
        Main function to refresh external tables and process data
        
        Args:
            year: Year for data processing
            month: Month for data processing
            staging_bucket: S3 staging bucket name
            payer_ids: List of payer IDs being processed
            app: Application name for S3 path
        """
        if not self.connection or not self.cursor:
            raise ValueError("Snowflake connection not established. Call connect() first.")
        
        logger.info(f"Starting table refresh for {self.module} module...")
        logger.info(f"Processing payer IDs: {payer_ids}")
        logger.info(f"Date: {year}-{month:02d}")
        logger.info(f"Staging bucket: {staging_bucket}")
        
        try:
            if self.module == 'analytics':
                self._process_analytics_module(year, month, staging_bucket, payer_ids, app)
            elif self.module == 'auto-analytics':
                self._process_auto_analytics_module(year, month, staging_bucket, payer_ids, app)
            else:
                raise ValueError(f"Unsupported module: {self.module}")
            
            logger.info("Table refresh completed successfully")
            
        except Exception as e:
            logger.error(f"Table refresh failed: {e}")
            raise
    
    def _process_analytics_module(self, year: int, month: int, staging_bucket: str, payer_ids: List[str], app: str):
        """Process analytics module - create stage, external table, and run queries"""
        
        logger.info("Processing analytics module...")
        
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
        logger.debug(f"SQL: {create_stage_query}")
        
        try:
            self.cursor.execute(create_stage_query)
            logger.info("Analytics stage created successfully")
        except Exception as e:
            logger.error(f"Failed to create analytics stage: {e}")
            raise
        
        # Step 2: Infer schema from parquet files
        infer_schema_query = """
        SELECT COLUMN_NAME, TYPE, EXPRESSION, 
               COLUMN_NAME || ' ' || TYPE || ' AS ' || '(' || EXPRESSION || ')' 
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '@wastage_analytics_stage_application',
                FILE_FORMAT => 'parquet_working_format'
            )
        );
        """
        
        logger.info(" Inferring schema from parquet files...")
        
        try:
            self.cursor.execute(infer_schema_query)
            schema_results = self.cursor.fetchall()
            
            if not schema_results:
                raise ValueError("No schema could be inferred from the parquet files")
            
            # Extract column definitions (convert to lowercase as in original)
            column_definitions = [row[3].lower() for row in schema_results]
            columns_result = ", ".join(column_definitions)
            
            logger.info(f"Schema inferred with {len(column_definitions)} columns")
            
        except Exception as e:
            logger.error(f"Failed to infer schema: {e}")
            raise
        
        # Step 3: Create external table
        table_name = f"analytics_application_table_{year}_{month}"
        create_external_table_query = f"""
        CREATE OR REPLACE EXTERNAL TABLE {table_name}
        ({columns_result})
        LOCATION = @wastage_analytics_stage_application
        FILE_FORMAT = (TYPE = 'PARQUET', COMPRESSION = 'SNAPPY');
        """
        
        logger.info(f"Creating external table: {table_name}")
        logger.debug(f"SQL: {create_external_table_query}")
        
        try:
            self.cursor.execute(create_external_table_query)
            logger.info(f"External table {table_name} created successfully")
        except Exception as e:
            logger.error(f"Failed to create external table: {e}")
            raise
        
        # Step 4: Run analytics queries if file exists
        self._run_analytics_queries(year, month, payer_ids)
        
        logger.info("Analytics module processing completed")
    
    def _process_auto_analytics_module(self, year: int, month: int, staging_bucket: str, payer_ids: List[str], app: str):
        """Process auto-analytics module"""
        logger.info("Processing auto-analytics module...")
        logger.info(f"Processing payer IDs: {payer_ids}")
        
        # Step 1: Create stage for auto-analytics
        stage_url = f's3://{staging_bucket}/{app}/{self.module}/{self.env}/year={year}/month={month}/'
        storage_integration = self.get_storage_integration()
        
        create_stage_query = f"""
        CREATE OR REPLACE STAGE auto_analytics_stage_application 
        URL = '{stage_url}'
        STORAGE_INTEGRATION = {storage_integration}
        FILE_FORMAT = (TYPE = 'PARQUET', COMPRESSION = 'SNAPPY');
        """
        
        logger.info(f"Creating auto-analytics stage with URL: {stage_url}")
        logger.debug(f"SQL: {create_stage_query}")
        
        try:
            self.cursor.execute(create_stage_query)
            logger.info("Auto-analytics stage created successfully")
        except Exception as e:
            logger.error(f"Failed to create auto-analytics stage: {e}")
            raise
        
        # Add auto-analytics specific processing here
        # You can extend this based on your auto-analytics requirements
        
        logger.info("Auto-analytics processing completed")
    
    def _run_analytics_queries(self, year: int, month: int, payer_ids: List[str]):
        """Run analytics wastage queries if the SQL file exists"""
        
        # Check multiple possible locations for the query file
        possible_paths = [
            "analytics_wastage_queries.sql",
            "/app/analytics_wastage_queries.sql",
            "./analytics_wastage_queries.sql",
            os.path.join(os.path.dirname(__file__), "analytics_wastage_queries.sql")
        ]
        
        query_file_path = None
        for path in possible_paths:
            if os.path.exists(path):
                query_file_path = path
                break
        
        if query_file_path:
            logger.info(f"Running analytics wastage queries from: {query_file_path}")
            logger.info(f"Query parameters: year={year}, month={month}, payer_ids={payer_ids}")
            
            try:
                with open(query_file_path, "r") as query_file:
                    query_sql = query_file.read()
                
                # Replace placeholders with actual values
                query_sql = query_sql.replace('#startyear', str(year))
                query_sql = query_sql.replace('#startmonth', str(month))
                
                # Convert payer_ids list to proper format for SQL IN clauses
                # Example: ['671238551718', '519933445287'] -> '671238551718','519933445287'
                if payer_ids:
                    payer_ids_str = "'" + "','".join(str(pid) for pid in payer_ids) + "'"
                else:
                    payer_ids_str = "''"
                
                query_sql = query_sql.replace('#payer_ids', payer_ids_str)
                
                logger.info(f"Formatted payer_ids for SQL: {payer_ids_str}")
                logger.debug(f"SQL Query: {query_sql}")
                
                # Execute the query
                self.cursor.execute(query_sql)
                
                # Get results if it's a SELECT query
                try:
                    results = self.cursor.fetchall()
                    if results:
                        logger.info(f"Query returned {len(results)} rows")
                    else:
                        logger.info("Query executed successfully (no results returned)")
                except Exception:
                    # Not a SELECT query, that's fine
                    logger.info("Query executed successfully")
                
                logger.info("Analytics queries executed successfully")
                
            except Exception as e:
                logger.error(f" Failed to execute analytics queries: {e}")
                raise
        else:
            logger.warning(f"Analytics query file not found in any of these locations:")
            for path in possible_paths:
                logger.warning(f"   - {path}")
            logger.info("Skipping analytics query execution")

# Convenience function for direct usage (updated for modular architecture)
def create_external_table_and_process(env: str, module: str, year: int, month: int, 
                                    staging_bucket: str, payer_ids: List[str], 
                                    app: str = 'aws_az_analytics_application_refresh'):
    """
    Convenience function to create external table and process data
    
    Args:
        env: Environment (uat, prod, dev, etc.)
        module: Module name (analytics, auto-analytics)
        year: Year for data processing
        month: Month for data processing
        staging_bucket: S3 staging bucket name
        payer_ids: List of payer IDs being processed
        app: Application name for S3 path
    """
    snowflake_manager = None
    try:
        logger.info(f"Starting external table creation for {module} in {env}")
        logger.info(f"Parameters: year={year}, month={month}, payer_ids={payer_ids}")
        logger.info(f"Staging bucket: {staging_bucket}")
        logger.info(f"App: {app}")
        
        snowflake_manager = SnowflakeExternalTableManager(env, module)
        snowflake_manager.connect()
        snowflake_manager.table_refresh(year, month, staging_bucket, payer_ids, app)
        
        logger.info(" External table creation and processing completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create external table and process data: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise
    finally:
        if snowflake_manager:
            snowflake_manager.close_connection()
            logger.info("Snowflake connection cleanup completed")