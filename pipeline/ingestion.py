"""Resumable, idempotent S3 CSV ingestion worker."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import unquote_plus
from uuid import UUID

import boto3
from botocore.exceptions import ClientError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from database.models import (
    CrawlError,
    IngestionChunk,
    IngestionJob,
    User,
    WebhookDelivery,
)
from database.session import session_scope

from .config import get_pipeline_settings
from .csv_ingestion import CsvChunk, CsvInputError, parse_csv_chunk


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class RetryableIngestionError(RuntimeError):
    """A failure that should leave the SQS message available for retry."""


class PermanentIngestionError(RuntimeError):
    """A file or message failure that should be recorded and acknowledged."""


@dataclass(frozen=True, slots=True)
class WorkItem:
    job_id: UUID
    bucket: str
    key: str
    version_id: str | None
    chunk_number: int
    start_byte_offset: int
    start_row_number: int
    initial_chunk: bool
    headers: list[str] | None


@dataclass(frozen=True, slots=True)
class FollowUp:
    continuation: dict[str, Any] | None = None
    delivery_id: UUID | None = None


def handle_ingestion(event: dict[str, Any], context: object) -> dict[str, list[dict[str, str]]]:
    """Process SQS records and retry only temporary failures."""

    del context
    failures: list[dict[str, str]] = []
    for record in event.get("Records", []):
        message_id = record.get("messageId") or record.get("receiptHandle")
        try:
            process_sqs_record(record)
        except PermanentIngestionError as exc:
            LOGGER.warning("Permanent ingestion failure: %s", str(exc))
        except Exception:
            LOGGER.exception("Retryable ingestion failure")
            if message_id:
                failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}


def process_sqs_record(record: dict[str, Any]) -> None:
    """Process one S3 notification or one continuation message."""

    work_item = parse_work_item(record.get("body", ""))
    follow_up = _process_work_item(work_item)
    _publish_follow_up(follow_up, work_item)


def parse_work_item(body: str) -> WorkItem:
    """Parse either an S3 notification or an internal continuation message."""

    try:
        payload = json.loads(body)
        if isinstance(payload.get("Message"), str):
            payload = json.loads(payload["Message"])
    except (TypeError, json.JSONDecodeError) as exc:
        raise PermanentIngestionError("SQS message is not valid JSON") from exc

    if "Records" in payload:
        records = payload.get("Records") or []
        if len(records) != 1:
            raise PermanentIngestionError(
                "S3 notification must contain exactly one object record"
            )
        s3_record = records[0].get("s3", {})
        bucket = s3_record.get("bucket", {}).get("name")
        object_data = s3_record.get("object", {})
        key = object_data.get("key")
        if not bucket or not key:
            raise PermanentIngestionError("S3 notification is missing bucket or key")

        key = unquote_plus(key)
        job_id = _job_id_from_key(key)
        version_id = object_data.get("versionId")
        return WorkItem(
            job_id=job_id,
            bucket=bucket,
            key=key,
            version_id=version_id,
            chunk_number=0,
            start_byte_offset=0,
            start_row_number=2,
            initial_chunk=True,
            headers=None,
        )

    try:
        return WorkItem(
            job_id=UUID(str(payload["job_id"])),
            bucket=str(payload["bucket"]),
            key=str(payload["key"]),
            version_id=payload.get("version_id"),
            chunk_number=int(payload["chunk_number"]),
            start_byte_offset=int(payload["start_byte_offset"]),
            start_row_number=int(payload["start_row_number"]),
            initial_chunk=bool(payload.get("initial_chunk", False)),
            headers=list(payload["headers"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise PermanentIngestionError(
            "Continuation message is missing valid ingestion fields"
        ) from exc


def _job_id_from_key(key: str) -> UUID:
    parts = key.split("/")
    if len(parts) < 3 or parts[0] != "incoming":
        raise PermanentIngestionError("S3 object key is not an ingestion key")
    try:
        return UUID(parts[1])
    except ValueError as exc:
        raise PermanentIngestionError("S3 object key contains an invalid job ID") from exc


def _process_work_item(work_item: WorkItem) -> FollowUp:
    # Claim the chunk in the database before reading S3. This makes duplicate
    # S3 events and duplicate SQS deliveries harmless.
    if _claim_chunk(work_item) is None:
        return _follow_up_for_existing_work_item(work_item)

    if not work_item.initial_chunk and not work_item.headers:
        with session_scope() as session:
            job = session.get(IngestionJob, work_item.job_id)
            if job is None or not job.csv_headers:
                raise PermanentIngestionError("Ingestion job headers are unavailable")
            work_item = replace(work_item, headers=list(job.csv_headers))

    try:
        get_object_params: dict[str, Any] = {
            "Bucket": work_item.bucket,
            "Key": work_item.key,
        }
        if work_item.version_id:
            get_object_params["VersionId"] = work_item.version_id
        if work_item.start_byte_offset > 0:
            get_object_params["Range"] = f"bytes={work_item.start_byte_offset}-"
        # Range reads let us process large files across several Lambda calls.
        response = _s3_client().get_object(**get_object_params)
        body = response["Body"]
        try:
            chunk = parse_csv_chunk(
                body,
                start_byte_offset=work_item.start_byte_offset,
                start_row_number=work_item.start_row_number,
                max_rows=get_pipeline_settings().ingestion_batch_rows,
                initial_chunk=work_item.initial_chunk,
                headers=work_item.headers,
            )
        finally:
            body.close()
    except ClientError as exc:
        if _is_permanent_s3_error(exc):
            message = _safe_client_error_message(exc)
            _mark_permanent_failure(work_item, message)
            return FollowUp()
        raise RetryableIngestionError("S3 read failed") from exc
    except CsvInputError as exc:
        _mark_permanent_failure(work_item, str(exc))
        return FollowUp()

    follow_up = _persist_chunk(work_item, chunk)
    return follow_up


def _claim_chunk(work_item: WorkItem) -> IngestionChunk | None:
    """Lock the job and mark this chunk as being processed.

    Returning ``None`` means another delivery already finished this work.
    """

    settings = get_pipeline_settings()
    now = datetime.now(timezone.utc)
    lease_until = now + timedelta(seconds=settings.webhook_lock_seconds)

    with session_scope() as session:
        job = session.scalar(
            select(IngestionJob)
            .where(IngestionJob.id == work_item.job_id)
            .with_for_update()
        )
        if job is None:
            raise PermanentIngestionError("Ingestion job does not exist")
        if job.s3_bucket != work_item.bucket or job.s3_key != work_item.key:
            raise PermanentIngestionError("S3 object does not belong to ingestion job")
        if job.s3_version_id and job.s3_version_id != work_item.version_id:
            raise PermanentIngestionError("S3 object version does not match the job")
        if job.s3_version_id is None:
            job.s3_version_id = work_item.version_id

        if job.status == "failed" or job.status == "completed":
            return None

        if work_item.chunk_number < job.next_chunk_number:
            return None
        if (
            work_item.chunk_number != job.next_chunk_number
            or work_item.start_byte_offset != job.next_byte_offset
        ):
            raise RetryableIngestionError("Ingestion chunks arrived out of order")

        chunk = session.scalar(
            select(IngestionChunk)
            .where(
                IngestionChunk.ingestion_job_id == work_item.job_id,
                IngestionChunk.chunk_number == work_item.chunk_number,
            )
            .with_for_update()
        )
        if chunk is not None and chunk.status == "completed":
            return None
        if (
            chunk is not None
            and chunk.status == "processing"
            and chunk.lease_until is not None
            and chunk.lease_until > now
        ):
            raise RetryableIngestionError("Ingestion chunk is already being processed")

        if chunk is None:
            chunk = IngestionChunk(
                ingestion_job_id=work_item.job_id,
                chunk_number=work_item.chunk_number,
                start_byte_offset=work_item.start_byte_offset,
                start_row_number=work_item.start_row_number,
            )
            session.add(chunk)

        chunk.status = "processing"
        chunk.lease_until = lease_until
        job.status = "processing"
        session.flush()
        return chunk


def _persist_chunk(work_item: WorkItem, csv_chunk: CsvChunk) -> FollowUp:
    """Save one CSV chunk and decide what message should happen next."""

    source_url = f"s3://{work_item.bucket}/{work_item.key}"
    with session_scope() as session:
        job = session.scalar(
            select(IngestionJob)
            .where(IngestionJob.id == work_item.job_id)
            .with_for_update()
        )
        chunk_record = session.scalar(
            select(IngestionChunk)
            .where(
                IngestionChunk.ingestion_job_id == work_item.job_id,
                IngestionChunk.chunk_number == work_item.chunk_number,
            )
            .with_for_update()
        )
        if job is None or chunk_record is None:
            raise RetryableIngestionError(
                "Ingestion state disappeared during processing"
            )
        if chunk_record.status == "completed" or job.status == "completed":
            return _follow_up_for_completed_job(session, job)

        inserted_sources: set[str] = set()

        if csv_chunk.rows:
            values = [
                {**row, "ingestion_job_id": work_item.job_id}
                for row in csv_chunk.rows
            ]
            statement = (
                pg_insert(User)
                .values(values)
                .on_conflict_do_nothing(
                    constraint="uq_users_ingestion_job_source_user_id"
                )
                .returning(User.source_user_id)
            )
            inserted_sources = set(session.scalars(statement).all())

        # A row can be a duplicate because it already exists in the database,
        # or because the same source_user_id appears twice in this batch.
        duplicate_errors: list[dict[str, object]] = []
        seen_sources: set[object] = set()
        for row in csv_chunk.rows:
            source_user_id = row["source_user_id"]
            if (
                source_user_id in seen_sources
                or source_user_id not in inserted_sources
            ):
                duplicate_errors.append(
                    {
                        "row_number": None,
                        "source_user_id": source_user_id,
                        "message": "duplicate source user_id within this ingestion job",
                    }
                )
            seen_sources.add(source_user_id)

        rejected_rows = csv_chunk.errors + duplicate_errors
        session.add_all(
            [
                CrawlError(
                    ingestion_job_id=work_item.job_id,
                    source_url=source_url,
                    stage="validation",
                    source_row_number=row["row_number"],
                    source_user_id=row["source_user_id"],
                    error_message=row["message"],
                )
                for row in rejected_rows
            ]
        )

        total_rows_in_chunk = len(csv_chunk.rows) + len(csv_chunk.errors)
        job.processed_rows += total_rows_in_chunk
        job.rejected_rows += len(rejected_rows)
        job.csv_headers = csv_chunk.headers
        job.next_byte_offset = csv_chunk.next_byte_offset
        job.next_row_number = csv_chunk.next_row_number
        job.next_chunk_number = work_item.chunk_number + 1
        job.updated_at = datetime.now(timezone.utc)

        chunk_record.processed_rows = total_rows_in_chunk
        chunk_record.rejected_rows = len(rejected_rows)
        chunk_record.next_byte_offset = csv_chunk.next_byte_offset
        chunk_record.next_row_number = csv_chunk.next_row_number
        chunk_record.status = "completed"
        chunk_record.lease_until = None
        chunk_record.completed_at = datetime.now(timezone.utc)

        if csv_chunk.has_more:
            return FollowUp(
                continuation={
                    "job_id": str(work_item.job_id),
                    "bucket": work_item.bucket,
                    "key": work_item.key,
                    "version_id": work_item.version_id,
                    "chunk_number": work_item.chunk_number + 1,
                    "start_byte_offset": csv_chunk.next_byte_offset,
                    "start_row_number": csv_chunk.next_row_number,
                    "initial_chunk": False,
                    "headers": csv_chunk.headers,
                }
            )

        job.status = "completed"
        job.total_rows = job.processed_rows
        job.completed_at = datetime.now(timezone.utc)
        delivery = session.scalar(
            select(WebhookDelivery).where(
                WebhookDelivery.ingestion_job_id == work_item.job_id,
                WebhookDelivery.event_type == "ingestion.completed",
            )
        )
        if delivery is None:
            delivery = WebhookDelivery(
                ingestion_job_id=work_item.job_id,
                event_type="ingestion.completed",
                payload={"job_id": str(work_item.job_id)},
            )
            session.add(delivery)
            session.flush()

        if delivery.status == "sent":
            return FollowUp()
        return FollowUp(delivery_id=delivery.id)


def _follow_up_for_completed_job(session: Any, job: IngestionJob) -> FollowUp:
    delivery = session.scalar(
        select(WebhookDelivery).where(
            WebhookDelivery.ingestion_job_id == job.id,
            WebhookDelivery.event_type == "ingestion.completed",
        )
    )
    if delivery is None or delivery.status == "sent":
        return FollowUp()
    return FollowUp(delivery_id=delivery.id)


def _follow_up_for_existing_work_item(work_item: WorkItem) -> FollowUp:
    with session_scope() as session:
        job = session.get(IngestionJob, work_item.job_id)
        if job is None or job.status != "completed":
            chunk = session.scalar(
                select(IngestionChunk).where(
                    IngestionChunk.ingestion_job_id == work_item.job_id,
                    IngestionChunk.chunk_number == work_item.chunk_number,
                )
            )
            if chunk is None or not chunk.next_byte_offset:
                return FollowUp()
            headers = (
                list(job.csv_headers)
                if job is not None and job.csv_headers
                else list(work_item.headers or [])
            )
            return FollowUp(
                continuation={
                    "job_id": str(work_item.job_id),
                    "bucket": work_item.bucket,
                    "key": work_item.key,
                    "version_id": work_item.version_id,
                    "chunk_number": work_item.chunk_number + 1,
                    "start_byte_offset": chunk.next_byte_offset,
                    "start_row_number": chunk.next_row_number,
                    "initial_chunk": False,
                    "headers": headers,
                }
            )
        return _follow_up_for_completed_job(session, job)


def _publish_follow_up(follow_up: FollowUp, work_item: WorkItem) -> None:
    settings = get_pipeline_settings()
    if follow_up.continuation is not None:
        _sqs_client().send_message(
            QueueUrl=settings.ingestion_queue_url,
            MessageBody=json.dumps(follow_up.continuation),
        )
    elif follow_up.delivery_id is not None:
        _sqs_client().send_message(
            QueueUrl=settings.matching_queue_url,
            MessageBody=json.dumps(
                {
                    "delivery_id": str(follow_up.delivery_id),
                    "job_id": str(work_item.job_id),
                }
            ),
        )


def _mark_permanent_failure(work_item: WorkItem, message: str) -> None:
    source_url = f"s3://{work_item.bucket}/{work_item.key}"
    with session_scope() as session:
        job = session.scalar(
            select(IngestionJob)
            .where(IngestionJob.id == work_item.job_id)
            .with_for_update()
        )
        if job is None or job.status == "completed":
            return
        job.status = "failed"
        job.error_message = message[:4000]
        job.updated_at = datetime.now(timezone.utc)
        chunk = session.scalar(
            select(IngestionChunk).where(
                IngestionChunk.ingestion_job_id == work_item.job_id,
                IngestionChunk.chunk_number == work_item.chunk_number,
            )
        )
        if chunk is not None:
            chunk.status = "failed"
            chunk.error_message = message[:4000]
            chunk.lease_until = None
        session.add(
            CrawlError(
                ingestion_job_id=work_item.job_id,
                source_url=source_url,
                stage="extraction",
                error_message=message[:4000],
            )
        )


def _is_permanent_s3_error(exc: ClientError) -> bool:
    code = str(exc.response.get("Error", {}).get("Code", ""))
    return code in {"NoSuchKey", "NoSuchBucket", "AccessDenied", "InvalidObjectState"}


def _safe_client_error_message(exc: ClientError) -> str:
    code = str(exc.response.get("Error", {}).get("Code", "S3Error"))
    return f"S3 object could not be read ({code})"


def _s3_client() -> Any:
    return boto3.client("s3")


def _sqs_client() -> Any:
    return boto3.client("sqs")
