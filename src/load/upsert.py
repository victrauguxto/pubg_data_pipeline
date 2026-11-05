# src/load/upsert.py
"""Upsert functions for each layer of the Medallion architecture.

Bronze – raw JSONB payloads (idempotent via ON CONFLICT).
Silver – normalized relational rows.
"""

import json
import logging
from psycopg2.extras import Json

logger = logging.getLogger(__name__)


# =========================================================================
# BRONZE
# =========================================================================

def upsert_raw_player(cur, player_name: str, payload: dict) -> None:
    """Insert or update a raw player payload into bronze.raw_players."""
    cur.execute(
        """
        INSERT INTO bronze.raw_players (player_name, payload, ingested_at)
        VALUES (%s, %s, now())
        ON CONFLICT (player_name) DO UPDATE
            SET payload     = EXCLUDED.payload,
                ingested_at = now()
        """,
        (player_name, Json(payload)),
    )
    logger.debug("Upserted bronze.raw_players: %s", player_name)


def upsert_raw_match(cur, match_id: str, payload: dict) -> None:
    """Insert a raw match payload into bronze.raw_matches (skip if exists)."""
    cur.execute(
        """
        INSERT INTO bronze.raw_matches (match_id, payload, ingested_at)
        VALUES (%s, %s, now())
        ON CONFLICT (match_id) DO NOTHING
        """,
        (match_id, Json(payload)),
    )
    logger.debug("Upserted bronze.raw_matches: %s", match_id)


def match_exists_in_bronze(cur, match_id: str) -> bool:
    """Check if a match already exists in the bronze layer."""
    cur.execute(
        "SELECT 1 FROM bronze.raw_matches WHERE match_id = %s",
        (match_id,),
    )
    return cur.fetchone() is not None


# =========================================================================
# SILVER
# =========================================================================

def upsert_match(cur, match: dict) -> None:
    """Upsert a normalized match row into silver.matches."""
    cur.execute(
        """
        INSERT INTO silver.matches
            (match_id, game_mode, map_name, duration_sec, created_at,
             is_custom, shard_id, match_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (match_id) DO NOTHING
        """,
        (
            match["id"],
            match.get("gameMode"),
            match.get("mapName"),
            match.get("duration"),
            match.get("createdAt"),
            match.get("isCustomMatch"),
            match.get("shardId"),
            match.get("matchType"),
        ),
    )


def upsert_roster(cur, match_id: str, roster: dict) -> None:
    """Upsert a roster row into silver.rosters."""
    cur.execute(
        """
        INSERT INTO silver.rosters
            (roster_id, match_id, rank, team_id, won)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (roster_id) DO NOTHING
        """,
        (
            roster["roster_id"],
            match_id,
            roster.get("rank"),
            roster.get("teamId"),
            roster.get("won"),
        ),
    )


def upsert_player_match_stats(cur, match_id: str, roster_id: str,
                               participant: dict) -> None:
    """Upsert a player's per-match stats into silver.player_match_stats."""
    stats = participant.get("stats", {})
    cur.execute(
        """
        INSERT INTO silver.player_match_stats
            (match_id, roster_id, player_id, player_name,
             kills, assists, dbnos, damage_dealt, death_type,
             headshot_kills, heals, boosts, revives,
             time_survived, walk_distance, ride_distance, swim_distance,
             longest_kill, win_place, kill_place,
             kill_streaks, road_kills, team_kills,
             vehicle_destroys, weapons_acquired)
        VALUES (%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s)
        ON CONFLICT (match_id, player_id) DO NOTHING
        """,
        (
            match_id,
            roster_id,
            participant.get("playerId"),
            participant.get("name"),
            stats.get("kills"),
            stats.get("assists"),
            stats.get("DBNOs"),
            stats.get("damageDealt"),
            stats.get("deathType"),
            stats.get("headshotKills"),
            stats.get("heals"),
            stats.get("boosts"),
            stats.get("revives"),
            stats.get("timeSurvived"),
            stats.get("walkDistance"),
            stats.get("rideDistance"),
            stats.get("swimDistance"),
            stats.get("longestKill"),
            stats.get("winPlace"),
            stats.get("killPlace"),
            stats.get("killStreaks"),
            stats.get("roadKills"),
            stats.get("teamKills"),
            stats.get("vehicleDestroys"),
            stats.get("weaponsAcquired"),
        ),
    )
