Fargate Data Copy & Snowflake Processing Pipeline

This document provides a detailed overview of the Fargate-based data processing pipeline. The pipeline is designed to be a robust, environment-aware system that copies billing data from various source S3 buckets, stages it, and then processes it using Snowflake.

Table of Contents

Overview & Core Functionality

High-Level Architecture Diagram

Data Flow & Execution Logic

Key Features

Project Structure

Configuration

How to Run the Fargate Task

Production Example

Non-Production Example

Error Handling & Resilience

Dependencies

1. Overview & Core Functionality

The primary goal of this pipeline is to automate the synchronization of AWS Cost and Usage Report (CUR) data from multiple source accounts into a central staging S3 bucket and then trigger a series of complex analytical queries in Snowflake.

The process is containerized using Docker and deployed as an ephemeral task on AWS Fargate. It is designed to be triggered on a schedule or manually for specific months and customer accounts (payers).

Core steps:

Ingest Parameters: Reads input parameters (year, month, payers) from a JSON object passed as an environment variable.

Fetch Configuration: Retrieves payer-specific configurations (source bucket, path) from a central Snowflake table.

Incremental Sync: Intelligently scans the source S3 buckets and only copies new or modified Parquet files to a central staging bucket. It determines what is "new" by checking against the last processed timestamp for that payer in the final Snowflake analytics table.

Create External Table: Once new data is staged, it creates a Snowflake external table pointing to this new data.

Run Analytics: Executes a large, multi-statement SQL script (analytics_wastage_queries.sql) against the new data to update the final analytics tables (e.g., ck_analytics_application_ri_wastage_hourly).

Report Status: Sends completion status and metrics to CloudWatch and (optionally) a final notification to RabbitMQ.

2. High-Level Architecture Diagram
Generated mermaid
graph TD
    subgraph "AWS Account (Pipeline)"
        A[AWS ECS Run Task] -- "TASK_INPUT_JSON" --> B[Fargate Task];
        B -- "Connects as Payer" --> C[Snowflake DB];
        B -- "Reads source config" --> C;
        B -- "Checks last timestamp" --> C;
        B -- "Writes to staging" --> D[Staging S3 Bucket];
        B -- "Creates external table" --> C;
        B -- "Runs analytics.sql" --> C;
        B -- "Sends Metrics" --> E[CloudWatch];
        B -- "Sends Notifications" --> F[RabbitMQ];
    end

    subgraph "Source AWS Accounts"
        G[Source S3 Bucket 1]
        H[Source S3 Bucket 2]
        I[...]
    end

    B -- "Reads source data (cross-account)" --> G;
    B -- "Reads source data (cross-account)" --> H;
    B -- "Reads source data (cross-account)" --> I;

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#bbf,stroke:#333,stroke-width:2px

3. Data Flow & Execution Logic

The application follows a precise, sequential flow orchestrated by main.py.

Initialization (main.py)

The task starts. ParameterProcessor reads and validates the TASK_INPUT_JSON to get parameters like env, year, month, and payers.

The env parameter (prod or dev) is passed to config.get_environment_config(). This function acts as a central switchboard, returning a dictionary of environment-specific settings (S3 region, staging bucket, RabbitMQ credentials, etc.).

The FargateDataCopyService is initialized with the correct environment.

Configuration Loading (s3_client.py: PayerConfigManager)

Inside FargateDataCopyService, the PayerConfigManager is created.

It first attempts to connect to Snowflake using SnowflakeConfigFetcher to load all active payer configurations from the aws_az_share.metadata.pro_refresh_config table.

If the Snowflake connection or query fails, it logs an error and gracefully falls back to using the hardcoded PAYER_CONFIGS dictionary from config.py.

Incremental File Discovery (data_copy_service.py: process_multiple_payers)

The service iterates through the list of payer_ids provided in the input.

For each payer, it connects to Snowflake via SnowflakeExternalTableManager.get_last_processed_timestamp() to find the timestamp of the last record inserted into the final ck_analytics_application_ri_wastage_hourly table.

It then scans the source S3 bucket(s) for the current and previous months (BILLING_PERIOD=...). This is crucial for catching late-arriving data.

The s3_client.list_objects_with_metadata() function is called with the since=last_processed_ts parameter. It only returns files with a LastModified date greater than the last known timestamp.

If no new files are found, it logs "Already Synchronized" and moves to the next payer.

S3 Data Copy (data_copy_service.py: copy_payer_data)

If new files are discovered, a list of copy tasks is created.

A ThreadPoolExecutor is used to copy files from the source bucket to the staging bucket in parallel for high performance.

The destination path in the staging bucket is structured by year, month, and payer ID for clear partitioning (e.g., .../year=2025/month=7/payer-671238551718/).

Snowflake Processing (snowflake_external_table.py: create_external_table_and_process)

This function is only called if the S3 copy was successful.

It establishes a new connection to Snowflake, using AWS Secrets Manager for production credentials and hardcoded values for non-prod.

It creates a stage pointing to the new data in the staging bucket.

It uses INFER_SCHEMA to dynamically determine the column structure of the new Parquet files. This makes the pipeline resilient to schema changes in the source data.

It creates an external table (e.g., analytics_application_table_2025_7) using the inferred schema.

Finally, it calls _run_analytics_queries.

Analytics Execution (snowflake_external_table.py: _run_analytics_queries)

This function reads the entire analytics_wastage_queries.sql file into a string.

It performs simple string replacement to inject the correct year, month, and payer_ids into the SQL script.

It executes the entire multi-statement SQL script using connection.execute_string().

Crucially, this step is wrapped in a try...except block. If the SQL script fails (e.g., due to a syntax error or a column not found), it logs a NON-FATAL ERROR and allows the pipeline to continue and report success, as the data copy part was successful.

4. Key Features

Environment-Aware: A single codebase runs in both production and non-production environments. The behavior is controlled by the env parameter in the task input.

Dynamic Configuration: Payer source locations are centrally managed in a Snowflake table, reducing the need for code changes.

Resilient Fallback: If the primary Snowflake configuration source fails, the application gracefully falls back to a hardcoded configuration in config.py.

Incremental Processing: The pipeline is stateful. It checks the last processed timestamp in the destination table and only syncs new files, making it highly efficient for frequent runs.

Late-Arriving Data Handling: The S3 scan automatically checks both the current and previous months' billing folders to ensure data that is delivered late is not missed.

Dynamic Schema Handling: Uses Snowflake's INFER_SCHEMA to automatically adapt to changes in the source data structure, preventing failures when new columns are added.

Parallelized Data Transfer: Uses a thread pool to copy S3 objects in parallel, significantly speeding up the process for large datasets.

Comprehensive Logging & Monitoring: Provides detailed logs at each step and sends key metrics (files copied, task completion status) to CloudWatch.

5. Project Structure
Generated code
.
├── Dockerfile                  # Defines the container image
├── README.md                   # This file
├── analytics_wastage_queries.sql # The large, multi-statement SQL script
├── config.py                   # Central configuration, environment switching
├── cloudwatch_utils.py         # Utilities for sending CloudWatch metrics
├── data_copy_service.py        # Core orchestration logic for S3 sync
├── input_validator.py          # Parses and validates task input
├── main.py                     # Main entry point for the Fargate task
├── rabbitmq_client.py          # Client for sending RabbitMQ notifications
├── requirements.txt            # Python package dependencies
├── s3_client.py                # S3 client and PayerConfigManager
└── snowflake_external_table.py # Handles all Snowflake interactions
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
IGNORE_WHEN_COPYING_END
6. Configuration

config.py: This is the main configuration hub. It contains default settings and the get_environment_config function which provides environment-specific values for:

S3 Staging Bucket (PROD_STAGING_BUCKET, NON_PROD_STAGING_BUCKET)

S3 Region

RabbitMQ credentials and virtual hosts (VHOST_MAPPING)

Default Snowflake credentials (for non-prod)

analytics_wastage_queries.sql: This file contains the business logic for analytics. It uses placeholders (#startyear, #startmonth, (#payers_ids)) that are replaced by the Python script at runtime.

AWS Secrets Manager: Production Snowflake credentials are not stored in code. They are fetched at runtime from AWS Secrets Manager for security. The secret ID is defined in snowflake_external_table.py.

7. How to Run the Fargate Task

The task is executed using the aws ecs run-task CLI command. The key is to provide the correct parameters within the TASK_INPUT_JSON environment variable.

Production Example

This command runs the task for Payer 671238551718 for July 2025 in the production environment.

Generated bash
aws ecs run-task \
    --cluster aws_az_analytics_operations_cluster_prod1 \
    --task-definition aws_az_analytics_operations_td_PROD:2 \
    --launch-type FARGATE \
    --platform-version 1.4.0 \
    --region us-east-1 \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-06b8915f632f8f383],securityGroups=[sg-026cc505e234ac970],assignPublicIp=ENABLED}" \
    --started-by "manual-run-for-july-data" \
    --overrides '{
        "containerOverrides": [
            {
                "name": "aws_az_analytics_operations_container_prod",
                "environment": [
                    {
                        "name": "TASK_INPUT_JSON",
                        "value": "{\"year\": 2025, \"month\": 7, \"payers\": [\"671238551718\"], \"partnerId\": 1000218, \"module\": \"analytics\", \"env\": \"prod\"}"
                    }
                ]
            }
        ]
    }'
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Bash
IGNORE_WHEN_COPYING_END
Non-Production Example

This command runs the task for two payers for July 2025 in the development environment.

Generated bash
aws ecs run-task \
    --cluster aws_az_analytics_operations_cluster \
    --task-definition aws_az_analytics_operations_td:32 \
    --launch-type FARGATE \
    --platform-version 1.4.0 \
    --region us-east-2 \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-0abfaa8f46ae6edab],securityGroups=[sg-05540ff3d6cf23b1e],assignPublicIp=ENABLED}" \
    --started-by "manual-run-for-july-data-dev" \
    --overrides '{
        "containerOverrides": [
            {
                "name": "aws_az_analytics_operations_container",
                "environment": [
                    {
                        "name": "TASK_INPUT_JSON",
                        "value": "{\"year\": 2025, \"month\": 7, \"payers\": [\"671238551718\", \"1122334445\"], \"partnerId\": 1000218, \"module\": \"analytics\", \"env\": \"dev\"}"
                    }
                ]
            }
        ]
    }'
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Bash
IGNORE_WHEN_COPYING_END
8. Error Handling & Resilience

The pipeline has several layers of error handling:

Fatal Errors: A try...except block in main.py catches any unhandled exceptions. It logs the error, sends a FatalError metric to CloudWatch, and ensures the task exits with a non-zero status code.

Configuration Fallback: If PayerConfigManager cannot connect to Snowflake, it automatically uses the local PAYER_CONFIGS as a backup.

S3 Access Fallback: If FargateDataCopyService determines it cannot access a bucket defined in the Snowflake config (due to cross-account permissions), it will attempt to use the local fallback configuration for that specific payer.

Non-Fatal SQL Errors: The execution of analytics_wastage_queries.sql is designed to be non-fatal. If the script fails, an error is logged, but the overall pipeline task is still considered successful because the critical data copy step was completed. This prevents a bad SQL query from blocking the entire data flow.

Notification Retries: The RabbitMQNotifier has built-in retry logic with exponential backoff to handle transient network issues when sending the final notification.

9. Dependencies

The required Python packages are listed in requirements.txt. Key dependencies include:

boto3: For all AWS interactions (S3, ECS, Secrets Manager, CloudWatch).

snowflake-connector-python: For connecting to and querying Snowflake.

python-dateutil: Used for date calculations, specifically relativedelta.

pika: For communicating with RabbitMQ.
