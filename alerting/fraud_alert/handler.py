"""
Lambda entry point. Receives an EventBridge event and sends a fraud alert email via SES.

Event flow:
  Spark gold job  →  EventBridge (FraudflowBus)  →  this Lambda  →  SES  →  inbox
  CLI put-events  →  (same path from EventBridge onward)
"""

import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

from fraud_alert.email_formatter import format_fraud_email

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context) -> dict:
    # Log the full raw event — essential for debugging in CloudWatch and for
    # verifying the EventBridge rule is matching and routing correctly.
    logger.info("Received event: %s", json.dumps(event))

    try:
        detail = event["detail"]
    except KeyError:
        # A malformed event is not retriable — log and return without raising.
        # Raising would put the invocation into an error state and EventBridge
        # may retry it, which wastes invocations on a permanently bad payload.
        logger.error("Event missing 'detail' key — malformed EventBridge event")
        return {"statusCode": 400, "body": "Missing detail key"}

    # Bracket access (not .get()) for required env vars: if they're absent it's a
    # deployment error, not a runtime error, and a loud KeyError is appropriate.
    sender_email = os.environ["SENDER_EMAIL"]
    recipient_email = os.environ["RECIPIENT_EMAIL"]
    aws_region = os.environ.get("AWS_REGION_OVERRIDE", "us-east-1")

    try:
        subject, html_body, plain_text_body = format_fraud_email(detail)
    except Exception:
        logger.exception("Failed to format fraud email from detail: %s", detail)
        return {"statusCode": 500, "body": "Email formatting error"}

    ses_client = boto3.client("ses", region_name=aws_region)

    try:
        response = ses_client.send_email(
            Source=sender_email,
            Destination={"ToAddresses": [recipient_email]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Html": {"Data": html_body, "Charset": "UTF-8"},
                    "Text": {"Data": plain_text_body, "Charset": "UTF-8"},
                },
            },
        )
        # Log the SES MessageId so you can correlate a Lambda invocation with a
        # specific message in the SES sending activity dashboard.
        logger.info(
            "Alert sent — SES MessageId: %s | card: %s | fraud_type: %s",
            response["MessageId"],
            detail.get("card_id", "?"),
            detail.get("fraud_type", "?"),
        )
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        msg = exc.response["Error"]["Message"]
        # Common codes: MessageRejected (unverified address in sandbox),
        # Throttling (rate limit), MailFromDomainNotVerified.
        logger.error("SES ClientError [%s]: %s", code, msg)
        return {"statusCode": 500, "body": f"SES error: {code}"}
    except Exception:
        logger.exception("Unexpected error sending email")
        return {"statusCode": 500, "body": "Unexpected error"}

    return {"statusCode": 200, "body": "Alert sent successfully"}
