"""Add PostgreSQL-side UUID defaults for raw SQL inserts."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0004_add_uuid_server_defaults"
down_revision: Union[str, None] = "0003_loan_product_name_unique"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


UUID_TABLES = (
    "ingestion_jobs",
    "users",
    "loan_products",
    "matches",
    "notifications",
)


def upgrade() -> None:
    """Allow SQL clients such as n8n to omit UUID primary keys."""

    op.execute(sa.text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))

    for table_name in UUID_TABLES:
        op.execute(
            sa.text(
                f"ALTER TABLE {table_name} "
                "ALTER COLUMN id SET DEFAULT gen_random_uuid()"
            )
        )


def downgrade() -> None:
    """Remove database-side defaults but retain the pgcrypto extension."""

    for table_name in UUID_TABLES:
        op.execute(
            sa.text(
                f"ALTER TABLE {table_name} "
                "ALTER COLUMN id DROP DEFAULT"
            )
        )
