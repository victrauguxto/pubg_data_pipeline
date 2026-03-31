# src/load/upsert.py
"""Utility functions for upserting data into PostgreSQL.
Placeholder – implement actual upsert logic using INSERT ... ON CONFLICT.
"""

def upsert_match(conn, match):
    """Upsert a match record into the database.
    Args:
        conn: psycopg2 connection.
        match: dict or dataclass instance representing a match.
    """
    # TODO: implement upsert query
    pass

def upsert_player(conn, player):
    """Upsert a player record.
    Args:
        conn: psycopg2 connection.
        player: dict or dataclass instance.
    """
    # TODO: implement upsert query
    pass
