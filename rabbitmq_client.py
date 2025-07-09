# rabbitmq_client.py (Corrected to handle failure messages gracefully)

import os
import json
import ssl
import time
import logging
from typing import List, Optional
import pika
from pika.exceptions import AMQPConnectionError

from config import RABBITMQ_CONFIG, VHOST_MAPPING, get_environment_config

logger = logging.getLogger(__name__)

class RabbitMQNotifier:
    """
    A self-contained RabbitMQ notifier designed to connect, send a single message
    to a topic exchange, and then disconnect gracefully. Ideal for ephemeral tasks.
    """
    def __init__(self, environment: str = 'uat'):
        """
        Initializes the notifier with connection details based on the provided environment.

        Args:
            environment (str): The target environment (e.g., 'dev', 'uat', 'prod').
        """
        self.env_config = get_environment_config(environment)
        creds = self.env_config['rabbitmq_creds']

        self.host = creds['host']
        self.port = creds['port']
        self.username = creds['username']
        self.password = creds['password']
        self.virtual_host = self.env_config['vhost']
        
        self.exchange_name = RABBITMQ_CONFIG['exchange_name']
        self.routing_key = RABBITMQ_CONFIG['routing_key_base']
        
        logger.info(f"RabbitMQNotifier configured for env:'{environment}' -> vhost:'{self.virtual_host}'")

    def _get_connection_params(self) -> pika.ConnectionParameters:
        """Constructs the Pika connection parameters object with SSL/TLS options."""
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
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
        
        This version creates the exact payload format the consumer expects.
        """
        # "analytics" -> "Analytics"
        formatted_module = module.capitalize()

        payload = {
            "month": month, 
            "year": year, 
            "module": formatted_module,
            "payerIds": payer_ids,
            "status": status.upper(), 
            "partnerId": partner_id
        }
        
        log_message = f"Task finished with status: {status}. Reason: {message}" if message else f"Task finished with status: {status}."
        
        message_body = json.dumps(payload, indent=2)
        connection = None
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                params = self._get_connection_params()
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.confirm_delivery()

                channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=self.routing_key,
                    body=message_body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  
                        content_type='application/json'
                    ),
                    mandatory=True
                )
                
                logger.info("Successfully published message to RabbitMQ.")
                logger.info(f"  - Exchange: {self.exchange_name}, Routing Key: {self.routing_key}")
                logger.info(f"  - Payload Sent: {message_body}")
                logger.info(f"  - Internal Status Message: {log_message}") 
                return True

            except AMQPConnectionError as e:
                logger.error(f"RabbitMQ connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                else:
                    logger.error("Failed to connect to RabbitMQ after all retries.")
            except Exception as e:
                logger.error(f"An unexpected error occurred while sending RabbitMQ notification: {e}", exc_info=True)
                break
            finally:
                if connection and connection.is_open:
                    connection.close()
                    logger.debug("RabbitMQ connection for this attempt has been closed.")
        
        return False