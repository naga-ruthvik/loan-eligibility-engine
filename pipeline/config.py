"""Environment-backed settings for the ingestion pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def _required(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"{name} environment variable is required")
    return value.strip()


def _positive_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if parsed <= 0:
        raise RuntimeError(f"{name} must be greater than zero")
    return parsed


def _optional(name: str) -> str | None:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else None


@dataclass(frozen=True, slots=True)
class PipelineSettings:
    """Runtime configuration shared by Lambda handlers."""

    upload_bucket: str
    ingestion_queue_url: str
    matching_queue_url: str
    n8n_webhook_url: str | None
    workflow_c_webhook_url: str | None
    presigned_url_expires_seconds: int = 900
    ingestion_batch_rows: int = 1000
    webhook_timeout_seconds: int = 10
    webhook_lock_seconds: int = 600


@lru_cache(maxsize=1)
def get_pipeline_settings() -> PipelineSettings:
    return PipelineSettings(
        upload_bucket=_required("UPLOAD_BUCKET"),
        ingestion_queue_url=_required("INGESTION_QUEUE_URL"),
        matching_queue_url=_required("MATCHING_QUEUE_URL"),
        n8n_webhook_url=_optional("N8N_WEBHOOK_URL"),
        workflow_c_webhook_url=_optional("N8N_WORKFLOW_C_WEBHOOK_URL"),
        presigned_url_expires_seconds=_positive_int(
            "PRESIGNED_URL_EXPIRES_SECONDS", 900
        ),
        ingestion_batch_rows=_positive_int("INGESTION_BATCH_ROWS", 1000),
        webhook_timeout_seconds=_positive_int("N8N_WEBHOOK_TIMEOUT_SECONDS", 10),
        webhook_lock_seconds=_positive_int("N8N_WEBHOOK_LOCK_SECONDS", 600),
    )
