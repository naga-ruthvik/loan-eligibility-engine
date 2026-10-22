"""HTTP handler that creates an ingestion job and presigned S3 upload URL."""

from __future__ import annotations

import json
import os
from uuid import uuid4

import boto3
from botocore.config import Config

from database.models import IngestionJob
from database.session import session_scope

from .config import get_pipeline_settings


# Generate a region-specific SigV4 URL. Without this, S3 can return a 307
# redirect from the global endpoint to ap-southeast-2. Browsers often do not
# complete redirected cross-origin PUT requests, so the upload appears stuck.
s3 = boto3.client(
    "s3",
    region_name=os.getenv("AWS_REGION", "ap-southeast-2"),
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "virtual"},
    ),
)


def create_upload_url(event: dict, context: object) -> dict:
    """Create a job and return a presigned URL for a direct CSV upload."""

    del context
    settings = get_pipeline_settings()
    job_id = uuid4()
    object_key = f"incoming/{job_id}/users.csv"

    upload_url = s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": settings.upload_bucket,
            "Key": object_key,
            "ContentType": "text/csv",
        },
        ExpiresIn=settings.presigned_url_expires_seconds,
    )

    with session_scope() as session:
        session.add(
            IngestionJob(
                id=job_id,
                s3_bucket=settings.upload_bucket,
                s3_key=object_key,
            )
        )

    return _json_response(
        201,
        {
            "job_id": str(job_id),
            "bucket": settings.upload_bucket,
            "key": object_key,
            "upload_url": upload_url,
            "required_headers": {"Content-Type": "text/csv"},
        },
    )


def _json_response(status_code: int, body: dict[str, object]) -> dict[str, object]:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
