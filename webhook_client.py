"""
N8N Webhook Client for MCP Database Report Generator
Handles sending report data to N8N webhook endpoints
"""

import os
import requests
from typing import Dict
from utils import log


def process_resort_name_for_webhook(resort_name: str) -> str:
    return resort_name.strip().lower().replace(" ", "-")


def send_to_n8n_webhooks(resort_name: str, report_json: Dict) -> Dict[str, bool]:
    processed_name = process_resort_name_for_webhook(resort_name)

    # Base URL from environment or default
    base_url = os.getenv('N8N_WEBHOOK_BASE', 'https://n8n-v2.mcp.hyperplane.dev')

    webhook_urls = [
        f"{base_url}/webhook-test/{processed_name}-dmr-collector",
        f"{base_url}/webhook/{processed_name}-dmr-collector"
    ]

    results = {}
    success_count = 0

    for url in webhook_urls:
        try:
            response = requests.post(
                url,
                json=report_json,
                timeout=30,
                headers={'Content-Type': 'application/json'}
            )

            if response.status_code in [200, 201, 202]:
                results[url] = True
                success_count += 1
            else:
                results[url] = False
                log(f"Webhook failed for {resort_name}: {url} (HTTP {response.status_code})", "WARNING")

        except requests.exceptions.Timeout:
            results[url] = False
            log(f"Webhook timeout for {resort_name}: {url}", "WARNING")
        except requests.exceptions.RequestException as e:
            results[url] = False
            log(f"Webhook error for {resort_name}: {url} - {str(e)}", "WARNING")

    if success_count > 0:
        log(f"Report data sent to {success_count}/{len(webhook_urls)} webhooks for {resort_name}", "SUCCESS")

    return results
