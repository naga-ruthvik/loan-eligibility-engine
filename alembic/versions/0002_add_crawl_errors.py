"""Add crawl error tracking."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0002_add_crawl_errors"
down_revision: Union[str, None] = "0001_initial_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            CREATE TABLE crawl_errors (
                id SERIAL PRIMARY KEY,
                source_url TEXT,
                stage TEXT,
                error_message TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            )
            """
        )
    )


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE crawl_errors"))
