# src/load/postgres.py
"""Postgres connection utilities.
Placeholder – replace with real connection handling (psycopg2 or SQLAlchemy).
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_connection():
    """Create and return a new PostgreSQL connection using env vars.
    Expected env vars: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    """
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "pubg"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        cursor_factory=RealDictCursor,
    )
    return conn
