# slack_notifier.py

import os
import json
import logging
import requests
from typing import Dict, Any, Optional, Tuple

from config import SLACK_CONFIG

logger = logging.getLogger(__name__)

def _get_slack_details(environment: str, status: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Determines the correct Slack webhook URL and channel based on environment and status.
    
    Returns:
        A tuple of (webhook_url, channel_name)
    """
    env = environment.lower()
    stat = status.lower()

    if env == 'prod':
        if stat == 'success':
            webhook_url = os.environ.get('SLACK_WEBHOOK_URL_PROD_SUCCESS', SLACK_CONFIG.get('webhook_url_prod_success'))
            channel = SLACK_CONFIG.get('channel_prod_success')
            return webhook_url, channel
        else: # Failure
            webhook_url = os.environ.get('SLACK_WEBHOOK_URL_PROD_FAILURE', SLACK_CONFIG.get('webhook_url_prod_failure'))
            channel = SLACK_CONFIG.get('channel_prod_failure')
            return webhook_url, channel
    else: # Non-prod environments
        webhook_url = os.environ.get('SLACK_WEBHOOK_URL_NON_PROD', SLACK_CONFIG.get('webhook_url_non_prod'))
        channel = SLACK_CONFIG.get('channel_non_prod')
        return webhook_url, channel

def _format_slack_message(status: str, environment: str, details: str, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Creates a well-formatted Slack message using blocks."""
    
    status_icon = ":white_check_mark:" if status.lower() == 'success' else ":x:"
    color = "#36a64f" if status.lower() == 'success' else "#d50200"

    title_text = f"{status_icon} Fargate Task: *{status.upper()}* in *{environment.upper()}*"
    
    # Safely extract params for the message body
    year = params.get('year', 'N/A') if params else 'N/A'
    month = params.get('month', 'N/A') if params else 'N/A'
    module = params.get('module', 'N/A') if params else 'N/A'
    payer_count = len(params.get('payer_ids', [])) if params else 0
    
    message_body = (
        f"*Environment:* `{environment.upper()}`\n"
        f"*Module:* `{module}`\n"
        f"*Period:* `{year}-{month:02d}`\n"
        f"*Payer Count:* `{payer_count}`\n\n"
        f"*Details:*\n```{details}```"
    )

    payload = {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": title_text,
                            "emoji": True
                        }
                    },
                    {
                        "type": "divider"
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": message_body
                        }
                    }
                ]
            }
        ]
    }
    return payload


def send_slack_notification(status: str, environment: str, details: str, params: Optional[Dict[str, Any]]):
    """
    Sends a notification to the appropriate Slack channel.

    Args:
        status (str): The final status of the task ('Success' or 'Failed').
        environment (str): The environment the task ran in (e.g., 'uat', 'prod').
        details (str): A message describing the outcome.
        params (Optional[Dict[str, Any]]): The processed input parameters for context.
    """
    logger.info("Preparing Slack notification...")
    
    webhook_url, channel = _get_slack_details(environment, status)

    if not webhook_url or 'YOUR/PLACEHOLDER' in webhook_url:
        logger.warning(f"Slack notification for '{channel}' is not configured. Skipping.")
        return

    payload = _format_slack_message(status, environment, details, params)
    
    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        if response.status_code == 200:
            logger.info(f"Successfully sent Slack notification to channel: {channel}")
        else:
            logger.error(f"Failed to send Slack notification to '{channel}'. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logger.error(f"An exception occurred while sending Slack notification to '{channel}': {e}", exc_info=True)