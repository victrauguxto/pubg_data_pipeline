# src/ingestion/extract_players.py
"""Extract player information and match IDs from the PUBG API.
The function returns a Python ``dict`` that can be directly serialized to JSON
by ``json.dump`` in ``src/main.py``.
"""

import os
import json
import requests
from dotenv import load_dotenv
from typing import Dict, List, Optional

# Load environment variables once when the module is imported
load_dotenv()
API_KEY = os.getenv("PUBG_API_KEY")


def extract_players(player_names: str) -> Optional[Dict]:
    """Fetch a player's basic data and the IDs of matches they have played.

    Args:
        player_names: A single player name or a comma‑separated list of names.

    Returns:
        ``{"general": {...}, "matches": ["match_id1", "match_id2", ...]}``
        or ``None`` if the request fails.
    """
    url = f"https://api.pubg.com/shards/steam/players?filter[playerNames]={player_names}"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/vnd.api+json",
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        payload = response.json()["data"][0]

        # -------------------------------------------------------------------
        # General player attributes
        # -------------------------------------------------------------------
        attributes = payload.get("attributes", {})
        general = {
            "account_id": payload.get("id"),
            "type": payload.get("type"),
            "clanId": attributes.get("clanId"),
            "name": attributes.get("name"),
            "stats": attributes.get("stats"),
            "titleId": attributes.get("titleId"),
            "shardId": attributes.get("shardId"),
            "patchVersion": attributes.get("patchVersion"),
            "banType": attributes.get("banType"),
        }

        # -------------------------------------------------------------------
        # Extract match IDs from the ``relationships`` section
        # -------------------------------------------------------------------
        relationships = payload.get("relationships", {})
        matches_data = relationships.get("matches", {}).get("data", [])
        match_ids: List[str] = [m.get("id") for m in matches_data if "id" in m]

        return {"general": general, "matches": match_ids}

    except Exception as exc:
        # Simple console output – in production replace with a logger
        print(f"[extract_players] error: {exc}")
        return None
