import os
import json
import ssl
import time
import logging
import traceback
from typing import List, Dict, Any, Optional
import pika

# Import with error handling in case config module isn't available
try:
    from config import RABBITMQ_CONFIG, VHOST_MAPPING
except ImportError:
    # Fallback configuration if config module isn't available
    RABBITMQ_CONFIG = {
        'host': os.environ.get('RABBITMQ_HOST', 'b-77fd0f83-6a7a-467d-a0a8-0f3c39997ac7.mq.us-east-1.amazonaws.com'),
        'username': os.environ.get('RABBITMQ_USERNAME', 'cloudomonic-prod-user'),
        'password': os.environ.get('RABBITMQ_PASSWORD', '82HCh6cL9a31'),
        'exchange_name': "payer_refresh_exchange",
        'routing_key_base': "refresh.notification",
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

class RabbitmqConfigure:
    def __init__(
            self,
            host: str,
            routing_key: str,
            exchange: str,
            virtual_host: str = "/"
    ):
        """Configure Rabbit Mq Server"""
        self.host = host
        self.routingKey = routing_key
        self.exchange = exchange
        self.virtual_host = virtual_host


class RabbitMq:
    def __init__(self, server_url, username, password):
        self.server = server_url
        self.username = username
        self.password = password
        self._connection = None
        self._channel = None
        self._connect()

    def _connect(self):
        """Establish connection with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._connection = pika.BlockingConnection(self.__get_connection_params())
                self._channel = self._connection.channel()
                self._channel.confirm_delivery()
                logger.info(f" RabbitMQ connection established to {self.server.host}")
                logger.info(f"   Virtual Host: {self.server.virtual_host}")
                logger.info(f"   Exchange: {self.server.exchange}")
                return
            except Exception as e:
                logger.error(f" Connection attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(" Failed to establish connection after all retries")
                    raise
                else:
                    time.sleep(5 * (attempt + 1))  # Exponential backoff

    def __get_connection_params(self):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        url = f"amqps://{self.username}:{self.password}@{self.server.host}:5671"
        params = pika.URLParameters(url)
        params.ssl_options = pika.SSLOptions(context=ssl_context)
        params.virtual_host = self.server.virtual_host
        return params

    def publish(self, message=None):
        try:
            if not self._connection or self._connection.is_closed:
                self._connect()
                
            # Convert message to JSON string if it's a dict
            if isinstance(message, dict):
                message_body = json.dumps(message)
            else:
                message_body = str(message)
            
            self._channel.basic_publish(
                exchange=self.server.exchange,
                routing_key=self.server.routingKey,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )
            
            logger.info(" Message published successfully")
            logger.info(f" Exchange: {self.server.exchange}")
            logger.info(f" Routing Key: {self.server.routingKey}")
            logger.info(f" Virtual Host: {self.server.virtual_host}")
            logger.info(f" Message: {message_body}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish message: {str(e)}")
            return False
        finally:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
                logger.info("RabbitMQ connection closed")


class RabbitMQNotifier:
    """RabbitMQ notifications using custom RabbitMQ pattern"""
    
    def __init__(self, environment='prod'): 
        # RabbitMQ configuration from environment variables or config
        self.rabbitmq_host = RABBITMQ_CONFIG['host']
        self.rabbitmq_username = RABBITMQ_CONFIG['username']
        self.rabbitmq_password = RABBITMQ_CONFIG['password']
        
        # Get the correct virtual host based on environment
        self.rabbitmq_vhost = self._get_virtual_host(environment)
        
        self.exchange_name = RABBITMQ_CONFIG['exchange_name']
        self.routing_key_base = RABBITMQ_CONFIG['routing_key_base']
        
    def _get_virtual_host(self, environment: str) -> str:
        """Get virtual host based on environment"""
        env_lower = environment.lower()
        
        # Check for explicit RABBITMQ_VHOST environment variable override
        explicit_vhost = os.environ.get('RABBITMQ_VHOST')
        if explicit_vhost and explicit_vhost != 'auto':
            logger.info(f" Using explicit RABBITMQ_VHOST override: {explicit_vhost}")
            return explicit_vhost
        
        # Get mapped virtual host
        mapped_vhost = VHOST_MAPPING.get(env_lower)
        if mapped_vhost:
            logger.info(f"Environment '{environment}' mapped to virtual host: {mapped_vhost}")
            return mapped_vhost
        else:
            # Raise error for unknown environment
            available_envs = ', '.join(VHOST_MAPPING.keys())
            error_msg = f"Unknown environment '{environment}'. Available environments: {available_envs}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    def send_notification(self, month: int, year: int, module: str, payer_ids: List[str], status: str, partner_id: int = 1, message: Optional[str] = None):
        """Send task completion notification directly to queue"""
        try:
            # Create notification payload
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
            
            # Configure for direct queue publishing using default exchange
            server = RabbitmqConfigure(
                host=self.rabbitmq_host,
                routing_key="*",  # Queue name as routing key
                exchange=self.exchange_name,
                virtual_host=self.rabbitmq_vhost
            )
            
            # Create RabbitMQ client
            rabbitmq = RabbitMq(
                server_url=server, 
                username=self.rabbitmq_username, 
                password=self.rabbitmq_password
            )
            
            # Use existing publish method with proper configuration
            success = rabbitmq.publish(message=payload)
            
            if success:
                logger.info(f"Final notification sent to payer_refresh_queue")
                logger.info(f"Message status: {status.upper()}")
            else:
                logger.error(f"Failed to send final notification")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to send RabbitMQ notification: {str(e)}")
            logger.error(traceback.format_exc())
            return False
