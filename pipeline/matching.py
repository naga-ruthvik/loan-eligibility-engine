"""Send one completion notification to n8n."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID

from sqlalchemy import select

from database.models import WebhookDelivery
from database.session import session_scope

from .config import get_pipeline_settings


LOGGER = logging.getLogger(__name__)


def handle_matching(
    event: dict[str, Any], context: object
) -> dict[str, list[dict[str, str]]]:
    """Process SQS messages and let SQS retry failed webhook calls."""

    del context
    failures: list[dict[str, str]] = []

    for record in event.get("Records", []):
        message_id = record.get("messageId") or record.get("receiptHandle")
        try:
            body = json.loads(record.get("body", "{}"))
            process_delivery(UUID(str(body["delivery_id"])))
        except Exception:
            LOGGER.exception("Webhook delivery failed")
            if message_id:
                failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}


def process_delivery(delivery_id: UUID) -> None:
    """Load a saved payload, call n8n, and update the delivery status."""

    settings = get_pipeline_settings()
    now = datetime.now(timezone.utc)
    lock_until = now + timedelta(seconds=settings.webhook_lock_seconds)

    # Claim the delivery before making the HTTP request. If Lambda receives a
    # duplicate SQS message while another attempt is active, it exits safely.
    with session_scope() as session:
        delivery = session.scalar(
            select(WebhookDelivery)
            .where(WebhookDelivery.id == delivery_id)
            .with_for_update()
        )
        if delivery is None:
            LOGGER.warning("Webhook delivery does not exist: %s", delivery_id)
            return
        if delivery.status == "sent":
            return
        if (
            delivery.status == "sending"
            and delivery.locked_until
            and delivery.locked_until > now
        ):
            return

        delivery.status = "sending"
        delivery.attempt_count += 1
        delivery.locked_until = lock_until
        payload = dict(delivery.payload)

    if not settings.n8n_webhook_url:
        _fail_delivery(delivery_id, "N8N_WEBHOOK_URL is not configured", None)
        raise RuntimeError("N8N_WEBHOOK_URL is not configured")

    # Workflow B must be configured to respond after its last node finishes.
    # Only then do we start Workflow C for email notifications.
    status_code = _post_webhook(
        delivery_id,
        settings.n8n_webhook_url,
        payload,
        settings.webhook_timeout_seconds,
    )

    if not settings.workflow_c_webhook_url:
        _fail_delivery(
            delivery_id,
            "N8N_WORKFLOW_C_WEBHOOK_URL is not configured",
            None,
        )
        raise RuntimeError("N8N_WORKFLOW_C_WEBHOOK_URL is not configured")

    # Workflow C receives the same small payload, only after Workflow B has
    # returned a successful response.
    status_code = _post_webhook(
        delivery_id,
        settings.workflow_c_webhook_url,
        payload,
        settings.webhook_timeout_seconds,
    )

    # Only mark the delivery sent after n8n has returned a successful status.
    with session_scope() as session:
        delivery = session.scalar(
            select(WebhookDelivery)
            .where(WebhookDelivery.id == delivery_id)
            .with_for_update()
        )
        if delivery is not None:
            delivery.status = "sent"
            delivery.delivered_at = datetime.now(timezone.utc)
            delivery.locked_until = None
            delivery.last_error = None
            delivery.last_http_status = status_code


def _post_webhook(
    delivery_id: UUID, url: str, payload: dict[str, Any], timeout: int
) -> int:
    """POST the small completion payload and return n8n's HTTP status."""

    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            status_code = int(response.status)
            if not 200 <= status_code < 300:
                _fail_delivery(
                    delivery_id, f"n8n returned HTTP {status_code}", status_code
                )
                raise RuntimeError(f"n8n returned HTTP {status_code}")
            return status_code
    except HTTPError as exc:
        _fail_delivery(delivery_id, f"n8n returned HTTP {exc.code}", exc.code)
        raise RuntimeError(f"n8n returned HTTP {exc.code}") from exc
    except (URLError, TimeoutError, OSError) as exc:
        _fail_delivery(delivery_id, "n8n request failed", None)
        raise RuntimeError("n8n request failed") from exc


def _fail_delivery(
    delivery_id: UUID, message: str, status_code: int | None
) -> None:
    """Save the last error, then let SQS retry the message."""

    with session_scope() as session:
        delivery = session.scalar(
            select(WebhookDelivery)
            .where(WebhookDelivery.id == delivery_id)
            .with_for_update()
        )
        if delivery is not None:
            delivery.status = "failed"
            delivery.last_error = message[:4000]
            delivery.last_http_status = status_code
            delivery.locked_until = None
            delivery.updated_at = datetime.now(timezone.utc)
