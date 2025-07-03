import os
import json
import ssl
import time
import logging
from typing import List, Optional
import pika
from pika.exceptions import AMQPConnectionError

try:
    from config import RABBITMQ_CONFIG, VHOST_MAPPING
except ImportError:
    # Fallback configuration if the config module isn't available in a different context
    logging.warning("Could not import from 'config', using fallback RabbitMQ configuration.")
    RABBITMQ_CONFIG = {
        'host': os.environ.get('RABBITMQ_HOST', 'b-77fd0f83-6a7a-467d-a0a8-0f3c39997ac7.mq.us-east-1.amazonaws.com'),
        'username': os.environ.get('RABBITMQ_USERNAME', 'cloudomonic-prod-user'),
        'password': os.environ.get('RABBITMQ_PASSWORD', '82HCh6cL9a31'),
        'exchange_name': "payer_refresh_exchange",
        'routing_key_base': "*", # Correct routing key for a topic exchange
        'port': 5671
    }
    VHOST_MAPPING = {
        'dev': 'dev2',
        'dev2': 'dev2',
        'uat': 'cloudonomic_uat',
        'qa1': 'qa1',
        'prod': 'cloudonomic_prod'
    }

logger = logging.getLogger(__name__)

class RabbitMQNotifier:
    """
    A self-contained RabbitMQ notifier designed to connect, send a single message
    to a topic exchange, and then disconnect gracefully. Ideal for ephemeral tasks.
    """
    def __init__(self, environment: str = 'prod'):
        """
        Initializes the notifier with connection details from the application config.

        Args:
            environment (str): The target environment (e.g., 'dev', 'uat', 'prod'),
                               which determines the virtual host.
        """
        self.host = RABBITMQ_CONFIG['host']
        self.port = RABBITMQ_CONFIG['port']
        self.username = RABBITMQ_CONFIG['username']
        self.password = RABBITMQ_CONFIG['password']
        self.exchange_name = RABBITMQ_CONFIG['exchange_name']
        self.routing_key = RABBITMQ_CONFIG['routing_key_base']
        self.virtual_host = self._get_virtual_host(environment)
        logger.info(f"RabbitMQNotifier configured for env:'{environment}' -> vhost:'{self.virtual_host}'")

    def _get_virtual_host(self, environment: str) -> str:
        """
        Determines the correct RabbitMQ virtual host based on the environment mapping.
        An explicit environment variable `RABBITMQ_VHOST` can override the mapping.
        """
        env_lower = environment.lower()
        explicit_vhost = os.environ.get('RABBITMQ_VHOST')
        if explicit_vhost:
            logger.info(f"Using explicit RABBITMQ_VHOST override: {explicit_vhost}")
            return explicit_vhost

        mapped_vhost = VHOST_MAPPING.get(env_lower)
        if mapped_vhost:
            return mapped_vhost
        else:
            available_envs = ', '.join(VHOST_MAPPING.keys())
            error_msg = f"Unknown environment '{environment}'. Cannot map to a vhost. Available environments: {available_envs}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def _get_connection_params(self) -> pika.ConnectionParameters:
        """
        Constructs the Pika connection parameters object with SSL/TLS options.
        """
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        # Recommended secure cipher suite
        ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')

        credentials = pika.PlainCredentials(self.username, self.password)
        
        return pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.virtual_host,
            credentials=credentials,
            ssl_options=pika.SSLOptions(context=ssl_context),
            heartbeat=600,
            blocked_connection_timeout=300
        )

    def send_notification(self, month: int, year: int, module: str, payer_ids: List[str], status: str, partner_id: int, message: Optional[str] = None) -> bool:
        """
        Establishes a connection, sends a single notification message with retry logic,
        and ensures the connection is closed.

        Args:
            month (int): The month of the data processed.
            year (int): The year of the data processed.
            module (str): The name of the processing module (e.g., 'analytics').
            payer_ids (List[str]): A list of payer IDs included in the process.
            status (str): The final status of the task ('SUCCESS', 'FAILED', etc.).
            partner_id (int): The partner ID associated with the task.
            message (Optional[str]): An optional descriptive message.

        Returns:
            bool: True if the message was successfully published, False otherwise.
        """
        payload = {
            "month": month,
            "year": year,
            "module": module,
            "payerIds": payer_ids,
            "status": status.upper(),
            "partnerId": partner_id
        }
        if message:
            payload["message"] = message

        message_body = json.dumps(payload, indent=2)
        connection = None
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Step 1: Establish a new connection for this attempt
                params = self._get_connection_params()
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                
                # Enable publisher confirms to ensure the message is received by the broker
                channel.confirm_delivery()

                # Step 2: Publish the message
                channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=self.routing_key, # Use the correct routing key
                    body=message_body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Make message persistent
                        content_type='application/json'
                    ),
                    mandatory=True # Return message to publisher if it cannot be routed
                )
                
                logger.info("Successfully published message to RabbitMQ.")
                logger.info(f"  - Exchange: {self.exchange_name}, Routing Key: {self.routing_key}")
                logger.info(f"  - Message: {message_body}")
                return True

            except AMQPConnectionError as e:
                logger.error(f"RabbitMQ connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))  # Exponential backoff
                else:
                    logger.error("Failed to connect to RabbitMQ after all retries.")
            except Exception as e:
                logger.error(f"An unexpected error occurred while sending RabbitMQ notification: {e}", exc_info=True)
                # Break on unexpected errors as retrying may not help
                break
            finally:
                # Step 3: Ensure the connection is closed
                if connection and connection.is_open:
                    connection.close()
                    logger.debug("RabbitMQ connection for this attempt has been closed.")
        
        # If the loop completes without returning True, it means all attempts failed.
        return False