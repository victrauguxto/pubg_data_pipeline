# src/main.py
"""Entry point for the PUBG data pipeline.
Handles environment loading, rate‑limit waiting, player extraction and JSON
output. Uses sensible defaults when environment variables are missing.
"""

import os
import json
import time
import logging
from dotenv import load_dotenv

from ingestion.extract_players import extract_players

# ---------------------------------------------------------------------------
# Environment & configuration
# ---------------------------------------------------------------------------
load_dotenv()

# Default API rate limit (calls per minute) if not provided in .env
DEFAULT_RATE_LIMIT = 60  # 60 calls per minute → 1 call per second
api_rate_limit_raw = os.getenv("API_RATE_LIMIT")
if api_rate_limit_raw is None or not api_rate_limit_raw.isdigit():
    logging.warning(
        "API_RATE_LIMIT not set or invalid; falling back to default %s",
        DEFAULT_RATE_LIMIT,
    )
    api_rate_limit = DEFAULT_RATE_LIMIT
else:
    api_rate_limit = int(api_rate_limit_raw)

# ---------------------------------------------------------------------------
# Rate‑limit helpers
# ---------------------------------------------------------------------------
def seconds_per_call(rate_limit: int) -> float:
    """Return how many seconds to wait between calls.
    ``rate_limit`` is expressed as *calls per minute*.
    """
    return 60.0 / max(rate_limit, 1)  # avoid division by zero

def wait_for_rate_limit(rate_limit: int) -> None:
    """Sleep for the appropriate interval based on the API rate limit."""
    time.sleep(seconds_per_call(rate_limit))

# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def main(player_name: str) -> None:
    logging.info("Waiting for API rate limit")
    wait_for_rate_limit(api_rate_limit)

    logging.info("Extracting player data for %s", player_name)
    data = extract_players(player_name)
    if data is None:
        logging.error("Failed to retrieve player data for %s", player_name)
        return

    filepath = os.path.join("data", "raw", f"{player_name}.json")
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    logging.info("Saving player data to %s", filepath)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    logging.info("Player data saved successfully")


if __name__ == "__main__":
    # Example usage – replace with any PUBG player name
    main("v1t1n")
