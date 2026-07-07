"""
Builds the subject line and HTML/plain-text email body for a fraud alert.
No AWS dependencies — testable with a plain Python dict.
"""

from datetime import datetime, timezone


_FRAUD_TYPE_COLORS = {
    "amount_spike": "#e67e22",
    "velocity_burst": "#e74c3c",
    "impossible_travel": "#8e44ad",
}


def _badge(fraud_type: str) -> str:
    color = _FRAUD_TYPE_COLORS.get(fraud_type, "#7f8c8d")
    label = fraud_type.replace("_", " ").title()
    return (
        f'<span style="background:{color};color:#fff;padding:2px 8px;'
        f'border-radius:4px;font-size:13px;">{label}</span>'
    )


def format_fraud_email(detail: dict) -> tuple[str, str, str]:
    """Return (subject, html_body, plain_text_body)."""
    card_id = detail.get("card_id", "N/A")
    fraud_type = detail.get("fraud_type", "N/A")
    txn_id = detail.get("transaction_id", "N/A")
    merchant_category = detail.get("merchant_category", "N/A")
    country = detail.get("country", "N/A")
    timestamp = detail.get("timestamp", "N/A")

    try:
        amount_str = f"${float(detail.get('amount', 0)):.2f}"
    except (TypeError, ValueError):
        amount_str = str(detail.get("amount", "N/A"))

    try:
        confidence_pct = f"{float(detail.get('confidence_score', 0)):.0%}"
    except (TypeError, ValueError):
        confidence_pct = str(detail.get("confidence_score", "N/A"))

    alert_generated = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    subject = (
        f"[FRAUD ALERT] {fraud_type} on card {card_id} — confidence {confidence_pct}"
    )

    rows = [
        ("Transaction ID", txn_id),
        ("Card ID", card_id),
        ("Fraud Type", _badge(fraud_type)),
        ("Amount", amount_str),
        ("Merchant Category", merchant_category),
        ("Country", country),
        ("Detection Timestamp", timestamp),
        ("Confidence Score", confidence_pct),
        ("Alert Generated At", alert_generated),
    ]

    table_rows = "\n".join(
        f"""        <tr>
          <td style="padding:8px 12px;border:1px solid #ddd;font-weight:bold;background:#f9f9f9;">{label}</td>
          <td style="padding:8px 12px;border:1px solid #ddd;">{value}</td>
        </tr>"""
        for label, value in rows
    )

    html_body = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#333;">
  <h2 style="color:#c0392b;border-bottom:2px solid #c0392b;padding-bottom:8px;">
    Fraud Alert — Action Required
  </h2>
  <p>
    A fraud signal has been detected on your card. Review the details below
    and contact your issuer if this activity is unexpected.
  </p>
  <table style="border-collapse:collapse;width:100%;margin-top:16px;">
{table_rows}
  </table>
  <p style="margin-top:24px;font-size:12px;color:#999;">
    This alert was generated automatically by FraudFlow. Do not reply to this email.
  </p>
</body>
</html>"""

    plain_text_rows = "\n".join(
        f"{label}: {value}" for label, value in rows
    )
    plain_text_body = f"FRAUD ALERT — {fraud_type} on card {card_id}\n\n{plain_text_rows}"

    return subject, html_body, plain_text_body
