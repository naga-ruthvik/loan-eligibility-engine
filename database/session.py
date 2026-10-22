"""Process-wide SQLAlchemy engine and transaction-scoped sessions."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .config import get_database_settings


_settings = get_database_settings()

# Construct this once per warm Lambda execution environment. Reusing the
# engine lets SQLAlchemy reuse healthy connections instead of creating a pool
# for every request.
engine: Engine = create_engine(_settings.url, **_settings.engine_options())

# Sessions are deliberately short-lived and must never be shared globally
# between requests or concurrent work.
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    expire_on_commit=False,
)


@contextmanager
def session_scope() -> Iterator[Session]:
    """Provide one transaction-scoped session and clean it up reliably.

    Successful blocks are committed. Exceptions roll the transaction back and
    are re-raised without logging the database URL or connection details.
    The session is closed in both cases.
    """

    with SessionLocal.begin() as session:
        yield session
