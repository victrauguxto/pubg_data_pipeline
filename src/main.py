# src/main.py
"""Local entry point for the PUBG data pipeline.

Full workflow (Medallion architecture):
  1. Extract player profile from API → upsert into bronze.raw_players
  2. For each match ID in the player profile:
       a. Respect the API rate limit
       b. Fetch match payload → upsert into bronze.raw_matches
  3. Normalize bronze → silver (matches, rosters, player_match_stats)
  4. Refresh gold materialized views
"""

import os
import sys
import time
import logging
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s]: %(message)s",
)
logger = logging.getLogger("pipeline")
load_dotenv()

# Ensure src/ is on the path so submodule imports work when run directly
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from ingestion.extract_players import extract_players
from ingestion.extract_matches import extract_matches
from load.postgres import get_connection
from load.upsert import upsert_raw_player, upsert_raw_match, match_exists_in_bronze
from processing.normalize_matches import normalize_from_bronze, refresh_gold

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_raw_limit = os.getenv("API_CALLS_LIMIT")
API_RATE_LIMIT: int = int(_raw_limit) if _raw_limit and _raw_limit.isdigit() else 10


def _throttle() -> None:
    """Sleep to respect the API rate limit (calls per minute)."""
    time.sleep(60.0 / max(API_RATE_LIMIT, 1))


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def step_1_extract_player(conn, player_name: str) -> list[str]:
    """Fetch player from API → bronze.  Returns list of match IDs."""
    logger.info("[1/4] Extracting player: %s", player_name)
    _throttle()

    player_data = extract_players(player_name)
    if player_data is None:
        logger.error("Failed to fetch player '%s' from API.", player_name)
        return []

    with conn.cursor() as cur:
        upsert_raw_player(cur, player_name, player_data)
    conn.commit()

    match_ids = player_data.get("matches", [])
    logger.info("Player '%s' → %d matches found.", player_name, len(match_ids))
    max_matches = os.getenv("MAX_MATCHES_PER_PLAYER", "0")
    if max_matches.isdigit() and int(max_matches) > 0:
        match_ids = match_ids[: int(max_matches)]
        logger.info("Limited to the %d most recent matches.", len(match_ids))
    return match_ids


def step_2_extract_matches(conn, match_ids: list[str]) -> int:
    """Fetch each match from API → bronze.  Returns count of new matches."""
    total = len(match_ids)
    new_count = 0
    logger.info("[2/4] Fetching %d matches …", total)

    for idx, mid in enumerate(match_ids, start=1):
        # Skip if already in bronze (idempotent)
        with conn.cursor() as cur:
            if match_exists_in_bronze(cur, mid):
                logger.debug("[%d/%d] Already in bronze: %s", idx, total, mid)
                continue

        logger.info("[%d/%d] Fetching match: %s", idx, total, mid)
        _throttle()

        match_data = extract_matches(mid)
        if match_data is None:
            logger.warning("Failed to fetch match %s – skipping.", mid)
            continue

        with conn.cursor() as cur:
            upsert_raw_match(cur, mid, match_data)
        conn.commit()
        new_count += 1

    logger.info("Bronze ingestion: %d new matches out of %d.", new_count, total)
    return new_count


def step_3_normalize(conn) -> int:
    """Bronze JSONB → Silver tables."""
    logger.info("[3/4] Normalizing bronze → silver …")
    with conn.cursor() as cur:
        count = normalize_from_bronze(cur)
    conn.commit()
    return count


def step_4_refresh_gold(conn) -> None:
    """Refresh gold materialized views."""
    logger.info("[4/4] Refreshing gold layer …")
    with conn.cursor() as cur:
        refresh_gold(cur)
    conn.commit()


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run(player_name: str) -> None:
    """Run the full pipeline for a single player name."""
    logger.info("=" * 60)
    logger.info("PUBG Pipeline – player: %s", player_name)
    logger.info("=" * 60)

    conn = get_connection()
    try:
        match_ids = step_1_extract_player(conn, player_name)
        if not match_ids:
            logger.warning("No matches for '%s'. Done.", player_name)
            return

        step_2_extract_matches(conn, match_ids)
        step_3_normalize(conn)
        step_4_refresh_gold(conn)

        logger.info("Pipeline complete for '%s'.", player_name)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    player_names_raw = os.getenv("PLAYER_NAMES", "v1t1n")
    for name in player_names_raw.split(","):
        name = name.strip()
        if name:
            run(name)
