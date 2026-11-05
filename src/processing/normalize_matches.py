# src/processing/normalize_matches.py
"""Read raw match JSONB from bronze and write normalized rows to silver.

This module is the core of the Silver layer transformation. It:
  1. Queries bronze.raw_matches for un-processed matches
  2. For each match payload:
     a. Inserts into silver.matches
     b. Inserts each roster into silver.rosters
     c. Resolves participant → roster mapping and inserts into
        silver.player_match_stats
"""

import logging
from load.upsert import upsert_match, upsert_roster, upsert_player_match_stats

logger = logging.getLogger(__name__)


def normalize_from_bronze(cur) -> int:
    """Read un-processed matches from bronze and write to silver.

    A match is considered "un-processed" if its match_id doesn't yet exist
    in silver.matches.

    Args:
        cur: psycopg2 cursor (within an open transaction).

    Returns:
        Number of matches normalized.
    """
    cur.execute(
        """
        SELECT match_id, payload
        FROM   bronze.raw_matches
        WHERE  match_id NOT IN (SELECT match_id FROM silver.matches)
        ORDER  BY ingested_at
        """
    )
    rows = cur.fetchall()
    total = len(rows)

    if total == 0:
        logger.info("No new matches to normalize.")
        return 0

    logger.info("Normalizing %d new matches from bronze → silver …", total)

    for idx, row in enumerate(rows, start=1):
        match_id = row["match_id"]
        payload = row["payload"]

        # -- 1. Match metadata ------------------------------------------------
        match_info = payload.get("match", {})
        upsert_match(cur, match_info)

        # -- 2. Build a participant_id → roster_id lookup ----------------------
        rosters = payload.get("rosters", [])
        participant_to_roster = {}

        for roster in rosters:
            upsert_roster(cur, match_id, roster)
            for pid in roster.get("participant_ids", []):
                participant_to_roster[pid] = roster["roster_id"]

        # -- 3. Player match stats ---------------------------------------------
        participants = payload.get("participants", {})
        for pid, participant in participants.items():
            roster_id = participant_to_roster.get(pid)
            if roster_id is None:
                logger.warning(
                    "Participant %s has no roster in match %s – skipping.",
                    pid, match_id,
                )
                continue
            upsert_player_match_stats(cur, match_id, roster_id, participant)

        if idx % 10 == 0 or idx == total:
            logger.info("  [%d/%d] normalized", idx, total)

    logger.info("Silver normalization complete: %d matches.", total)
    return total


def refresh_gold(cur) -> None:
    """Refresh the gold.player_career materialized view."""
    logger.info("Refreshing gold.player_career …")
    cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY gold.player_career")
    logger.info("Gold layer refreshed.")
