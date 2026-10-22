"""Add resumable ingestion, row tracking, and webhook delivery state."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0005_ingestion_pipeline_schema"
down_revision: Union[str, None] = "0004_add_uuid_server_defaults"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()

    bind.execute(
        sa.text(
            """
            ALTER TABLE ingestion_jobs
                ADD COLUMN IF NOT EXISTS s3_version_id VARCHAR(255),
                ADD COLUMN IF NOT EXISTS csv_headers JSONB,
                ADD COLUMN IF NOT EXISTS processed_rows INTEGER NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS rejected_rows INTEGER NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS next_byte_offset BIGINT NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS next_row_number BIGINT NOT NULL DEFAULT 2,
                ADD COLUMN IF NOT EXISTS next_chunk_number INTEGER NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            """
        )
    )
    bind.execute(
        sa.text(
            """
            ALTER TABLE users
                ADD COLUMN IF NOT EXISTS full_name VARCHAR(255)
            """
        )
    )

    duplicate_user = bind.execute(
        sa.text(
            """
            SELECT ingestion_job_id, source_user_id, COUNT(*) AS duplicate_count
            FROM users
            GROUP BY ingestion_job_id, source_user_id
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        )
    ).first()
    if duplicate_user is not None:
        raise RuntimeError(
            "Cannot add uq_users_ingestion_job_source_user_id because duplicate "
            "source_user_id values already exist within an ingestion job. "
            "Reconcile those users before rerunning this migration."
        )

    bind.execute(
        sa.text(
            """
            ALTER TABLE crawl_errors
                ADD COLUMN IF NOT EXISTS ingestion_job_id UUID,
                ADD COLUMN IF NOT EXISTS source_row_number BIGINT,
                ADD COLUMN IF NOT EXISTS source_user_id VARCHAR(255)
            """
        )
    )
    bind.execute(
        sa.text(
            """
            ALTER TABLE crawl_errors
                ADD CONSTRAINT fk_crawl_errors_ingestion_job_id_ingestion_jobs
                FOREIGN KEY (ingestion_job_id) REFERENCES ingestion_jobs(id)
                ON DELETE CASCADE
            """
        )
    )
    bind.execute(
        sa.text(
            """
            CREATE INDEX IF NOT EXISTS ix_crawl_errors_ingestion_job_id
            ON crawl_errors (ingestion_job_id)
            """
        )
    )

    bind.execute(
        sa.text(
            """
            ALTER TABLE users
                ADD CONSTRAINT uq_users_ingestion_job_source_user_id
                UNIQUE (ingestion_job_id, source_user_id)
            """
        )
    )

    bind.execute(
        sa.text(
            """
            CREATE TABLE ingestion_chunks (
                id UUID NOT NULL DEFAULT gen_random_uuid(),
                ingestion_job_id UUID NOT NULL,
                chunk_number INTEGER NOT NULL,
                start_byte_offset BIGINT NOT NULL,
                next_byte_offset BIGINT,
                start_row_number BIGINT NOT NULL,
                next_row_number BIGINT,
                processed_rows INTEGER NOT NULL DEFAULT 0,
                rejected_rows INTEGER NOT NULL DEFAULT 0,
                status VARCHAR(16) NOT NULL DEFAULT 'pending',
                error_message TEXT,
                lease_until TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMPTZ,
                CONSTRAINT pk_ingestion_chunks PRIMARY KEY (id),
                CONSTRAINT uq_ingestion_chunks_job_chunk_number
                    UNIQUE (ingestion_job_id, chunk_number),
                CONSTRAINT ck_ingestion_chunks_ingestion_chunks_status
                    CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
                CONSTRAINT ck_ingestion_chunks_ingestion_chunks_row_counts_non_negative
                    CHECK (processed_rows >= 0 AND rejected_rows >= 0),
                CONSTRAINT fk_ingestion_chunks_ingestion_job_id_ingestion_jobs
                    FOREIGN KEY (ingestion_job_id) REFERENCES ingestion_jobs(id)
                    ON DELETE CASCADE
            )
            """
        )
    )
    bind.execute(
        sa.text(
            """
            CREATE INDEX ix_ingestion_chunks_ingestion_job_id
            ON ingestion_chunks (ingestion_job_id)
            """
        )
    )

    bind.execute(
        sa.text(
            """
            CREATE TABLE webhook_deliveries (
                id UUID NOT NULL DEFAULT gen_random_uuid(),
                ingestion_job_id UUID NOT NULL,
                event_type VARCHAR(64) NOT NULL DEFAULT 'ingestion.completed',
                payload JSONB NOT NULL,
                status VARCHAR(16) NOT NULL DEFAULT 'pending',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                locked_until TIMESTAMPTZ,
                last_http_status INTEGER,
                last_error TEXT,
                delivered_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT pk_webhook_deliveries PRIMARY KEY (id),
                CONSTRAINT uq_webhook_deliveries_job_event_type
                    UNIQUE (ingestion_job_id, event_type),
                CONSTRAINT ck_webhook_deliveries_webhook_deliveries_status
                    CHECK (status IN ('pending', 'sending', 'sent', 'failed')),
                CONSTRAINT fk_webhook_deliveries_ingestion_job_id_ingestion_jobs
                    FOREIGN KEY (ingestion_job_id) REFERENCES ingestion_jobs(id)
                    ON DELETE CASCADE
            )
            """
        )
    )
    bind.execute(
        sa.text(
            """
            CREATE INDEX ix_webhook_deliveries_ingestion_job_id
            ON webhook_deliveries (ingestion_job_id)
            """
        )
    )
    bind.execute(
        sa.text(
            """
            CREATE INDEX ix_webhook_deliveries_status
            ON webhook_deliveries (status)
            """
        )
    )


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("DROP TABLE webhook_deliveries"))
    bind.execute(sa.text("DROP TABLE ingestion_chunks"))
    bind.execute(
        sa.text(
            """
            ALTER TABLE users
                DROP CONSTRAINT uq_users_ingestion_job_source_user_id
            """
        )
    )
    bind.execute(
        sa.text(
            """
            ALTER TABLE crawl_errors
                DROP CONSTRAINT fk_crawl_errors_ingestion_job_id_ingestion_jobs
            """
        )
    )
    bind.execute(
        sa.text(
            """
            ALTER TABLE crawl_errors
                DROP COLUMN source_user_id,
                DROP COLUMN source_row_number,
                DROP COLUMN ingestion_job_id
            """
        )
    )
    bind.execute(
        sa.text(
            """
            ALTER TABLE users DROP COLUMN full_name
            """
        )
    )
    bind.execute(
        sa.text(
            """
            ALTER TABLE ingestion_jobs
                DROP COLUMN updated_at,
                DROP COLUMN next_chunk_number,
                DROP COLUMN next_row_number,
                DROP COLUMN next_byte_offset,
                DROP COLUMN rejected_rows,
                DROP COLUMN processed_rows,
                DROP COLUMN csv_headers,
                DROP COLUMN s3_version_id
            """
        )
    )
