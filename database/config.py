"""Environment-backed settings for the database layer.

Local development may use a project ``.env`` file. Deployed environments are
expected to provide the same values through the process environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache

from dotenv import load_dotenv


# Do not override values injected by Lambda, CI, or a local shell.
load_dotenv(override=False)


def _get_int(name: str, default: int, *, minimum: int = 0) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default

    try:
        parsed = int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc

    if parsed < minimum:
        raise RuntimeError(f"{name} must be greater than or equal to {minimum}")

    return parsed


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False

    raise RuntimeError(f"{name} must be a boolean")


def _normalize_database_url(url: str) -> str:
    """Make the PostgreSQL driver explicit for SQLAlchemy's sync engine."""

    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url.removeprefix("postgres://")
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url.removeprefix("postgresql://")
    return url


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    """Settings used to construct the process-wide SQLAlchemy engine."""

    # Do not include the URL in the dataclass repr: it can contain credentials.
    url: str = field(repr=False)
    pool_size: int = 1
    max_overflow: int = 0
    pool_timeout: int = 5
    pool_recycle: int = 300
    pool_pre_ping: bool = True
    echo: bool = False

    def engine_options(self) -> dict[str, object]:
        """Return safe engine options for the configured database URL."""

        options: dict[str, object] = {
            "echo": self.echo,
            "pool_pre_ping": self.pool_pre_ping,
            "pool_recycle": self.pool_recycle,
        }

        # These options are appropriate for the network database used by the
        # Lambda runtime. Avoid passing them to SQLite's in-memory pool so the
        # layer remains easy to use for local tooling and future checks.
        if not self.url.endswith(":memory:"):
            options.update(
                {
                    "pool_size": self.pool_size,
                    "max_overflow": self.max_overflow,
                    "pool_timeout": self.pool_timeout,
                }
            )

        return options


@lru_cache(maxsize=1)
def get_database_settings() -> DatabaseSettings:
    """Load and validate database settings once per process."""

    raw_url = os.getenv("DATABASE_URL")
    if raw_url is None or not raw_url.strip():
        raise RuntimeError("DATABASE_URL environment variable is required")

    url = _normalize_database_url(raw_url.strip())
    if "://" not in url:
        raise RuntimeError("DATABASE_URL must be a valid SQLAlchemy database URL")

    return DatabaseSettings(
        url=url,
        pool_size=_get_int("DB_POOL_SIZE", 1, minimum=1),
        max_overflow=_get_int("DB_MAX_OVERFLOW", 0),
        pool_timeout=_get_int("DB_POOL_TIMEOUT_SECONDS", 5, minimum=1),
        pool_recycle=_get_int("DB_POOL_RECYCLE_SECONDS", 300, minimum=0),
        pool_pre_ping=_get_bool("DB_POOL_PRE_PING", True),
        echo=_get_bool("DB_ECHO", False),
    )
