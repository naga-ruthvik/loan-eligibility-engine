"""Create the initial PostgreSQL schema."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            CREATE TABLE ingestion_jobs (
                id UUID NOT NULL,
                s3_bucket VARCHAR(63) NOT NULL,
                s3_key TEXT NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                total_rows INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMPTZ,
                CONSTRAINT pk_ingestion_jobs PRIMARY KEY (id),
                CONSTRAINT ck_ingestion_jobs_ingestion_jobs_status
                    CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
                CONSTRAINT ck_ingestion_jobs_ingestion_jobs_total_rows_non_negative
                    CHECK (total_rows >= 0)
            )
            """
        )
    )
    op.execute(
        sa.text(
            "CREATE INDEX ix_ingestion_jobs_status ON ingestion_jobs (status)"
        )
    )

    op.execute(
        sa.text(
            """
            CREATE TABLE loan_products (
                id UUID NOT NULL,
                lender_name VARCHAR(255) NOT NULL,
                product_name VARCHAR(255) NOT NULL,
                min_monthly_income NUMERIC(12, 2),
                min_credit_score INTEGER,
                min_age INTEGER,
                max_age INTEGER,
                interest_rate_min NUMERIC(5, 2),
                interest_rate_max NUMERIC(5, 2),
                eligibility_criteria JSONB NOT NULL DEFAULT '{}'::jsonb,
                source_url VARCHAR(2048),
                last_verified_at TIMESTAMPTZ,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT pk_loan_products PRIMARY KEY (id),
                CONSTRAINT ck_loan_products_loan_products_min_monthly_income_non_negative
                    CHECK (min_monthly_income >= 0),
                CONSTRAINT ck_loan_products_loan_products_min_credit_score_non_negative
                    CHECK (min_credit_score >= 0),
                CONSTRAINT ck_loan_products_loan_products_min_age_non_negative
                    CHECK (min_age >= 0),
                CONSTRAINT ck_loan_products_loan_products_max_age_non_negative
                    CHECK (max_age >= 0),
                CONSTRAINT ck_loan_products_loan_products_age_range_valid
                    CHECK (min_age IS NULL OR max_age IS NULL OR min_age <= max_age),
                CONSTRAINT ck_loan_products_loan_products_interest_rate_min_percent
                    CHECK (interest_rate_min >= 0 AND interest_rate_min <= 100),
                CONSTRAINT ck_loan_products_loan_products_interest_rate_max_percent
                    CHECK (interest_rate_max >= 0 AND interest_rate_max <= 100),
                CONSTRAINT ck_loan_products_loan_products_interest_rate_range_valid
                    CHECK (interest_rate_min IS NULL OR interest_rate_max IS NULL
                        OR interest_rate_min <= interest_rate_max)
            )
            """
        )
    )

    op.execute(
        sa.text(
            """
            CREATE TABLE users (
                id UUID NOT NULL,
                source_user_id VARCHAR(255) NOT NULL,
                ingestion_job_id UUID NOT NULL,
                email VARCHAR(320),
                monthly_income NUMERIC(12, 2),
                credit_score INTEGER,
                employment_status VARCHAR(64),
                age INTEGER,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT pk_users PRIMARY KEY (id),
                CONSTRAINT ck_users_users_monthly_income_non_negative
                    CHECK (monthly_income >= 0),
                CONSTRAINT ck_users_users_credit_score_non_negative
                    CHECK (credit_score >= 0),
                CONSTRAINT ck_users_users_age_non_negative
                    CHECK (age >= 0),
                CONSTRAINT fk_users_ingestion_job_id_ingestion_jobs
                    FOREIGN KEY (ingestion_job_id) REFERENCES ingestion_jobs (id)
                    ON DELETE CASCADE
            )
            """
        )
    )
    op.execute(
        sa.text("CREATE INDEX ix_users_ingestion_job_id ON users (ingestion_job_id)")
    )

    op.execute(
        sa.text(
            """
            CREATE TABLE matches (
                id UUID NOT NULL,
                user_id UUID NOT NULL,
                loan_product_id UUID NOT NULL,
                eligibility_status VARCHAR(24) NOT NULL,
                match_score NUMERIC(5, 2),
                match_reasons JSONB,
                evaluated_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT pk_matches PRIMARY KEY (id),
                CONSTRAINT matches_user_id_loan_product_id
                    UNIQUE (user_id, loan_product_id),
                CONSTRAINT ck_matches_matches_eligibility_status
                    CHECK (eligibility_status IN
                        ('likely_eligible', 'not_eligible', 'needs_review')),
                CONSTRAINT ck_matches_matches_score_percent
                    CHECK (match_score >= 0 AND match_score <= 100),
                CONSTRAINT fk_matches_user_id_users
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                CONSTRAINT fk_matches_loan_product_id_loan_products
                    FOREIGN KEY (loan_product_id) REFERENCES loan_products (id)
                    ON DELETE CASCADE
            )
            """
        )
    )
    op.execute(sa.text("CREATE INDEX ix_matches_user_id ON matches (user_id)"))
    op.execute(
        sa.text(
            "CREATE INDEX ix_matches_loan_product_id ON matches (loan_product_id)"
        )
    )
    op.execute(
        sa.text(
            "CREATE INDEX ix_matches_eligibility_status ON matches (eligibility_status)"
        )
    )

    op.execute(
        sa.text(
            """
            CREATE TABLE notifications (
                id UUID NOT NULL,
                match_id UUID NOT NULL,
                recipient_email VARCHAR(320) NOT NULL,
                status VARCHAR(16) NOT NULL DEFAULT 'pending',
                provider_message_id VARCHAR(255),
                sent_at TIMESTAMPTZ,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT pk_notifications PRIMARY KEY (id),
                CONSTRAINT ck_notifications_notifications_status
                    CHECK (status IN ('pending', 'sending', 'sent', 'failed')),
                CONSTRAINT fk_notifications_match_id_matches
                    FOREIGN KEY (match_id) REFERENCES matches (id) ON DELETE CASCADE
            )
            """
        )
    )
    op.execute(
        sa.text("CREATE INDEX ix_notifications_match_id ON notifications (match_id)")
    )
    op.execute(
        sa.text("CREATE INDEX ix_notifications_status ON notifications (status)")
    )


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE notifications"))
    op.execute(sa.text("DROP TABLE matches"))
    op.execute(sa.text("DROP TABLE users"))
    op.execute(sa.text("DROP TABLE loan_products"))
    op.execute(sa.text("DROP TABLE ingestion_jobs"))
