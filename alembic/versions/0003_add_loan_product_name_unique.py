"""Enforce unique lender/product names for loan products."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0003_loan_product_name_unique"
down_revision: Union[str, None] = "0002_add_crawl_errors"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


CONSTRAINT_NAME = "uq_loan_products_lender_name_product_name"


def upgrade() -> None:
    """Add the arbiter required by ON CONFLICT (lender_name, product_name).

    Do not silently delete or merge existing products during a migration. If
    duplicates exist, stop with an actionable error so their matches can be
    reconciled intentionally before the constraint is added.
    """

    bind = op.get_bind()
    duplicate = bind.execute(
        sa.text(
            """
            SELECT lender_name, product_name, COUNT(*) AS duplicate_count
            FROM loan_products
            GROUP BY lender_name, product_name
            HAVING COUNT(*) > 1
            ORDER BY lender_name, product_name
            LIMIT 1
            """
        )
    ).first()

    if duplicate is not None:
        raise RuntimeError(
            "Cannot add "
            f"{CONSTRAINT_NAME}: duplicate loan_products exist for "
            f"lender_name={duplicate.lender_name!r}, "
            f"product_name={duplicate.product_name!r}. "
            "Run the reviewed duplicate cleanup first, then rerun this migration."
        )

    op.create_unique_constraint(
        CONSTRAINT_NAME,
        "loan_products",
        ["lender_name", "product_name"],
    )


def downgrade() -> None:
    op.drop_constraint(
        CONSTRAINT_NAME,
        "loan_products",
        type_="unique",
    )
