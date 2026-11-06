# src/ingestion/extract_matches.py
"""Extract match data from the PUBG API.

Returns a structured dict with three sections:
  - "match"       : basic match metadata (id, mode, map, duration, etc.)
  - "rosters"     : list of teams with rank, teamId, won flag, and participant IDs
  - "participants": lookup map { participant_id -> { playerId, name, stats } }

"rosters" and "participants" are intentionally kept as raw-but-clean dicts so
that normalize_matches.py can consume only what it needs without any
coupling to the ingestion layer.
"""

import os
import requests
from dotenv import load_dotenv
from typing import Dict, List, Optional

load_dotenv()
API_KEY = os.getenv("PUBG_API_KEY")

# Esports matches live under /shards/tournament/; public ones under /shards/steam/
SHARD = os.getenv("PUBG_SHARD", "steam")


def extract_matches(match_id: str) -> Optional[Dict]:
    """Fetch a single match and return a structured dict ready for processing.

    Returned structure:
    {
        "match": {
            "id": str,
            "gameMode": str,
            "mapName": str,
            "duration": int,        # seconds
            "createdAt": str,       # ISO 8601
            "isCustomMatch": bool,
            "shardId": str,
            "titleId": str,
            "matchType": str
        },
        "rosters": [
            {
                "roster_id": str,
                "rank": int,
                "teamId": int,
                "won": bool,
                "participant_ids": [str, ...]   # keys into "participants" below
            },
            ...
        ],
        "participants": {
            "<participant_id>": {
                "playerId": str,
                "name": str,
                "stats": { ... }    # full raw V2 stats object
            },
            ...
        }
    }

    Args:
        match_id: PUBG match UUID.
    Returns:
        Structured dict, or None if the request fails.
    """
    url = f"https://api.pubg.com/shards/{SHARD}/matches/{match_id}"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/vnd.api+json",
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        payload = response.json()

        # ------------------------------------------------------------------
        # 1. Basic match metadata
        # ------------------------------------------------------------------
        match_data = payload["data"]
        attrs = match_data.get("attributes", {})

        match_info = {
            "id": match_data.get("id"),
            "gameMode": attrs.get("gameMode"),
            "mapName": attrs.get("mapName"),
            "duration": attrs.get("duration"),
            "createdAt": attrs.get("createdAt"),
            "isCustomMatch": attrs.get("isCustomMatch"),
            "shardId": attrs.get("shardId"),
            "titleId": attrs.get("titleId"),
            "matchType": attrs.get("matchType"),
        }

        # ------------------------------------------------------------------
        # 2. Split ``included`` items into participants and rosters
        # ------------------------------------------------------------------
        included: List[Dict] = payload.get("included", [])
        raw_participants: Dict[str, Dict] = {}
        raw_rosters: List[Dict] = []

        for item in included:
            if item.get("type") == "participant":
                pid = item.get("id")
                p_stats = item.get("attributes", {}).get("stats", {})
                raw_participants[pid] = {
                    "playerId": p_stats.get("playerId"),
                    "name": p_stats.get("name"),
                    "stats": p_stats,       # full V2 stats – normalize later
                }

            elif item.get("type") == "roster":
                raw_rosters.append(item)

        # ------------------------------------------------------------------
        # 3. Build clean roster records
        # ------------------------------------------------------------------
        rosters: List[Dict] = []
        for roster in raw_rosters:
            r_attrs = roster.get("attributes", {})
            r_stats = r_attrs.get("stats", {})
            participant_ids = [
                p["id"]
                for p in roster
                    .get("relationships", {})
                    .get("participants", {})
                    .get("data", [])
            ]

            rosters.append({
                "roster_id": roster.get("id"),
                "rank": r_stats.get("rank"),
                "teamId": r_stats.get("teamId"),
                "won": r_attrs.get("won") == "true",    # API sends string "true"/"false"
                "participant_ids": participant_ids,
            })

        # Rank 1 first for easier debugging
        rosters.sort(key=lambda r: r["rank"] if r["rank"] is not None else 9999)

        return {
            "match": match_info,
            "rosters": rosters,
            "participants": raw_participants,
        }

    except Exception as exc:
        print(f"[extract_matches] error for {match_id}: {exc}")
        return None
