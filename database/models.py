"""SQLAlchemy declarative base and ORM models for the application schema."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    BigInteger,
    Integer,
    MetaData,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PostgreSQLUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Base class that all SQLAlchemy ORM models should inherit from."""
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class IngestionJob(Base):
    """A CSV import job and its resulting user records."""

    __tablename__ = "ingestion_jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'processing', 'completed', 'failed')",
            name="ingestion_jobs_status",
        ),
        CheckConstraint(
            "total_rows >= 0",
            name="ingestion_jobs_total_rows_non_negative",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        server_default=text("gen_random_uuid()"),
    )
    s3_bucket: Mapped[str] = mapped_column(String(63), nullable=False)
    s3_key: Mapped[str] = mapped_column(Text, nullable=False)
    s3_version_id: Mapped[str | None] = mapped_column(
        String(255), nullable=True
    )
    csv_headers: Mapped[list[str] | None] = mapped_column(
        JSONB, nullable=True
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="pending",
        server_default=text("'pending'"),
        index=True,
    )
    total_rows: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    processed_rows: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    rejected_rows: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    next_byte_offset: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0, server_default=text("0")
    )
    next_row_number: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=2, server_default=text("2")
    )
    next_chunk_number: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=func.now(),
    )
    users: Mapped[list["User"]] = relationship(
        back_populates="ingestion_job",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    chunks: Mapped[list["IngestionChunk"]] = relationship(
        back_populates="ingestion_job",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    crawl_errors: Mapped[list["CrawlError"]] = relationship(
        back_populates="ingestion_job",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    webhook_deliveries: Mapped[list["WebhookDelivery"]] = relationship(
        back_populates="ingestion_job",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class User(Base):
    """A user imported from an ingestion job."""

    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint(
            "ingestion_job_id",
            "source_user_id",
            name="uq_users_ingestion_job_source_user_id",
        ),
        CheckConstraint(
            "monthly_income >= 0",
            name="users_monthly_income_non_negative",
        ),
        CheckConstraint(
            "credit_score >= 0",
            name="users_credit_score_non_negative",
        ),
        CheckConstraint("age >= 0", name="users_age_non_negative"),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        server_default=text("gen_random_uuid()"),
    )
    source_user_id: Mapped[str] = mapped_column(String(255), nullable=False)
    ingestion_job_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("ingestion_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    full_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    email: Mapped[str | None] = mapped_column(String(320), nullable=True)
    monthly_income: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2), nullable=True
    )
    credit_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    employment_status: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    age: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=func.now(),
    )

    ingestion_job: Mapped[IngestionJob] = relationship(
        back_populates="users"
    )
    matches: Mapped[list["Match"]] = relationship(
        back_populates="user",
        cascade="all, delete",
        passive_deletes=True,
    )


class LoanProduct(Base):
    """A loan product and the criteria used to evaluate it."""

    __tablename__ = "loan_products"
    __table_args__ = (
        UniqueConstraint(
            "lender_name",
            "product_name",
            name="uq_loan_products_lender_name_product_name",
        ),
        CheckConstraint(
            "min_monthly_income >= 0",
            name="loan_products_min_monthly_income_non_negative",
        ),
        CheckConstraint(
            "min_credit_score >= 0",
            name="loan_products_min_credit_score_non_negative",
        ),
        CheckConstraint(
            "min_age >= 0",
            name="loan_products_min_age_non_negative",
        ),
        CheckConstraint(
            "max_age >= 0",
            name="loan_products_max_age_non_negative",
        ),
        CheckConstraint(
            "min_age IS NULL OR max_age IS NULL OR min_age <= max_age",
            name="loan_products_age_range_valid",
        ),
        CheckConstraint(
            "interest_rate_min >= 0 AND interest_rate_min <= 100",
            name="loan_products_interest_rate_min_percent",
        ),
        CheckConstraint(
            "interest_rate_max >= 0 AND interest_rate_max <= 100",
            name="loan_products_interest_rate_max_percent",
        ),
        CheckConstraint(
            "interest_rate_min IS NULL OR interest_rate_max IS NULL "
            "OR interest_rate_min <= interest_rate_max",
            name="loan_products_interest_rate_range_valid",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        server_default=text("gen_random_uuid()"),
    )
    lender_name: Mapped[str] = mapped_column(String(255), nullable=False)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)
    min_monthly_income: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2), nullable=True
    )
    min_credit_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_age: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_age: Mapped[int | None] = mapped_column(Integer, nullable=True)
    interest_rate_min: Mapped[Decimal | None] = mapped_column(
        Numeric(5, 2), nullable=True
    )
    interest_rate_max: Mapped[Decimal | None] = mapped_column(
        Numeric(5, 2), nullable=True
    )
    eligibility_criteria: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    source_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    last_verified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=text("true")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=func.now(),
    )

    matches: Mapped[list["Match"]] = relationship(
        back_populates="loan_product",
        cascade="all, delete",
        passive_deletes=True,
    )


class Match(Base):
    """The current eligibility result for one user and loan product."""

    __tablename__ = "matches"
    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "loan_product_id",
            name="matches_user_id_loan_product_id",
        ),
        CheckConstraint(
            "eligibility_status IN "
            "('likely_eligible', 'not_eligible', 'needs_review')",
            name="matches_eligibility_status",
        ),
        CheckConstraint(
            "match_score >= 0 AND match_score <= 100",
            name="matches_score_percent",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    loan_product_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("loan_products.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    eligibility_status: Mapped[str] = mapped_column(
        String(24), nullable=False, index=True
    )
    match_score: Mapped[Decimal | None] = mapped_column(
        Numeric(5, 2), nullable=True
    )
    match_reasons: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )
    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )

    user: Mapped[User] = relationship(back_populates="matches")
    loan_product: Mapped[LoanProduct] = relationship(
        back_populates="matches"
    )
    notifications: Mapped[list["Notification"]] = relationship(
        back_populates="match",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class Notification(Base):
    """An email notification attempt associated with a match."""

    __tablename__ = "notifications"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'sending', 'sent', 'failed')",
            name="notifications_status",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        server_default=text("gen_random_uuid()"),
    )
    match_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("matches.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    recipient_email: Mapped[str] = mapped_column(
        String(320), nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="pending",
        server_default=text("'pending'"),
        index=True,
    )
    provider_message_id: Mapped[str | None] = mapped_column(
        String(255), nullable=True
    )
    sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )

    match: Mapped[Match] = relationship(back_populates="notifications")


class CrawlError(Base):
    """An error recorded while crawling or processing a source URL."""

    __tablename__ = "crawl_errors"

    ingestion_job_id: Mapped[UUID | None] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("ingestion_jobs.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    source_row_number: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    source_user_id: Mapped[str | None] = mapped_column(
        String(255), nullable=True
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    stage: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    ingestion_job: Mapped[IngestionJob | None] = relationship(
        back_populates="crawl_errors"
    )


class IngestionChunk(Base):
    """A resumable bounded portion of an S3 CSV ingestion job."""

    __tablename__ = "ingestion_chunks"
    __table_args__ = (
        UniqueConstraint(
            "ingestion_job_id",
            "chunk_number",
            name="uq_ingestion_chunks_job_chunk_number",
        ),
        CheckConstraint(
            "status IN ('pending', 'processing', 'completed', 'failed')",
            name="ingestion_chunks_status",
        ),
        CheckConstraint(
            "processed_rows >= 0 AND rejected_rows >= 0",
            name="ingestion_chunks_row_counts_non_negative",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        server_default=text("gen_random_uuid()"),
    )
    ingestion_job_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("ingestion_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    chunk_number: Mapped[int] = mapped_column(Integer, nullable=False)
    start_byte_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    next_byte_offset: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    start_row_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    next_row_number: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    processed_rows: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    rejected_rows: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="pending",
        server_default=text("'pending'"),
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    ingestion_job: Mapped[IngestionJob] = relationship(
        back_populates="chunks"
    )


class WebhookDelivery(Base):
    """Durable delivery state for the post-ingestion n8n trigger."""

    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        UniqueConstraint(
            "ingestion_job_id",
            "event_type",
            name="uq_webhook_deliveries_job_event_type",
        ),
        CheckConstraint(
            "status IN ('pending', 'sending', 'sent', 'failed')",
            name="webhook_deliveries_status",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        server_default=text("gen_random_uuid()"),
    )
    ingestion_job_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("ingestion_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    event_type: Mapped[str] = mapped_column(
        String(64), nullable=False, default="ingestion.completed"
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="pending",
        server_default=text("'pending'"),
        index=True,
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    locked_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=func.now(),
    )

    ingestion_job: Mapped[IngestionJob] = relationship(
        back_populates="webhook_deliveries"
    )
