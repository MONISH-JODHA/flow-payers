#!/usr/bin/env python3
"""
CloudWatch utilities for monitoring and metrics
"""
import boto3
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from config import CLOUDWATCH_CONFIG

logger = logging.getLogger(__name__)

class CloudWatchMetrics:
    """CloudWatch metrics handler"""
    
    def __init__(self, region_name: Optional[str] = None):
        # Region can be set by the Fargate task's region or passed in
        self.region = region_name or CLOUDWATCH_CONFIG['region']
        self.namespace = CLOUDWATCH_CONFIG['namespace']
        self.cloudwatch = None
        self._init_client()
    
    def _init_client(self):
        """Initialize CloudWatch client"""
        try:
            self.cloudwatch = boto3.client('cloudwatch', region_name=self.region)
            logger.debug(f"CloudWatch client initialized for region: {self.region}")
        except Exception as e:
            logger.error(f"Failed to initialize CloudWatch client: {e}")
            self.cloudwatch = None
    
    def send_metric(self, metric_name: str, value: float, dimensions: Optional[Dict[str, str]] = None, unit: str = 'Count'):
        """Send metric to CloudWatch"""
        if not self.cloudwatch:
            logger.warning("CloudWatch client not available, skipping metric")
            return False
        
        try:
            dimensions = dimensions or {}
            
            metric_data = {
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Timestamp': datetime.utcnow()
            }
            
            if dimensions:
                metric_data['Dimensions'] = [
                    {'Name': k, 'Value': v} for k, v in dimensions.items()
                ]
            
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[metric_data]
            )
            
            logger.debug(f"Sent CloudWatch metric: {metric_name} = {value}")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to send CloudWatch metric: {e}")
            return False
    
    def send_task_completion_metric(self, status: str, reason: Optional[str] = None, **extra_dimensions):
        """Send task completion metric with standard dimensions"""
        dimensions = {'Status': status}
        
        if reason:
            dimensions['Reason'] = reason
        
        # Add any extra dimensions
        dimensions.update(extra_dimensions)
        
        return self.send_metric('TaskCompleted', 1, dimensions)
    
    def send_file_processing_metrics(self, files_copied: int, files_failed: int, payer_count: int):
        """Send file processing metrics"""
        # PayerCount is not a great dimension as it can have high cardinality.
        # Sending it as a separate metric is better.
        
        success = True
        success &= self.send_metric('FilesCopied', files_copied)
        success &= self.send_metric('FilesFailed', files_failed)
        success &= self.send_metric('PayersProcessed', payer_count)
        
        return success
    
    def send_error_metric(self, error_type: str, error_message: str = None):
        """Send error metric"""
        dimensions = {'ErrorType': error_type}
        
        if error_message:
            # Truncate error message to avoid dimension limits
            dimensions['ErrorMessage'] = error_message[:255]
        
        return self.send_metric('ProcessingError', 1, dimensions)

# Global instance for easy access, region is determined by environment
cloudwatch_metrics = CloudWatchMetrics()

# Convenience functions
def send_cloudwatch_metric(metric_name: str, value: float, dimensions: Dict[str, str] = None):
    """Global function for sending CloudWatch metrics (backward compatibility)"""
    return cloudwatch_metrics.send_metric(metric_name, value, dimensions)

def send_task_completion(status: str, reason: str = None, **extra_dimensions):
    """Send task completion metric"""
    return cloudwatch_metrics.send_task_completion_metric(status, reason, **extra_dimensions)

def send_processing_metrics(files_copied: int, files_failed: int, payer_count: int):
    """Send file processing metrics"""
    return cloudwatch_metrics.send_file_processing_metrics(files_copied, files_failed, payer_count)

def send_error_metric(error_type: str, error_message: str = None):
    """Send error metric"""
    return cloudwatch_metrics.send_error_metric(error_type, error_message)