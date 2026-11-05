# src/load/postgres.py
"""PostgreSQL connection utilities for the PUBG data pipeline.

Reads connection parameters from environment variables (via dotenv).
Provides a context-manager-based connection and a helper to run SQL files.
"""

import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from dotenv import load_dotenv
from contextlib import contextmanager

load_dotenv()
logger = logging.getLogger(__name__)


def get_connection():
    """Create and return a new PostgreSQL connection."""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "pubg"),
        user=os.getenv("POSTGRES_USER", "pubg"),
        password=os.getenv("POSTGRES_PASSWORD", "pubg_secret"),
        cursor_factory=RealDictCursor,
    )
    return conn


@contextmanager
def get_cursor(autocommit=False):
    """Yield a cursor inside a managed connection.

    Usage::

        with get_cursor() as cur:
            cur.execute("SELECT 1")
    """
    conn = get_connection()
    conn.autocommit = autocommit
    try:
        with conn.cursor() as cur:
            yield cur
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def run_migration(sql_path: str) -> None:
    """Execute a .sql file against the database."""
    logger.info("Running migration: %s", sql_path)
    with open(sql_path, "r", encoding="utf-8") as fh:
        sql = fh.read()
    with get_cursor() as cur:
        cur.execute(sql)
    logger.info("Migration complete: %s", sql_path)
