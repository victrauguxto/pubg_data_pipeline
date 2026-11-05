# airflow/dags/pubg_pipeline_dag.py
"""Airflow DAG for the PUBG data pipeline (Medallion architecture).

Tasks:
  1. extract_player       – API → bronze.raw_players  (returns match IDs via XCom)
  2. extract_matches      – API → bronze.raw_matches   (for each match ID)
  3. normalize_to_silver  – bronze JSONB → silver tables
  4. refresh_gold         – REFRESH MATERIALIZED VIEW gold.player_career
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Make the src/ package importable inside the Airflow container.
# docker-compose mounts ./src → /opt/airflow/dags/src
# ---------------------------------------------------------------------------
_src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

logger = logging.getLogger("pubg_dag")

# ---------------------------------------------------------------------------
# Config (read from env vars injected by docker-compose)
# ---------------------------------------------------------------------------
PLAYER_NAMES = os.getenv("PLAYER_NAMES", "v1t1n")
_raw_limit = os.getenv("API_CALLS_LIMIT", "10")
API_RATE_LIMIT = int(_raw_limit) if _raw_limit.isdigit() else 10


def _throttle():
    time.sleep(60.0 / max(API_RATE_LIMIT, 1))


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def task_extract_player(**context):
    """Fetch player profile from the PUBG API and store in bronze."""
    from ingestion.extract_players import extract_players
    from load.postgres import get_connection
    from load.upsert import upsert_raw_player

    all_match_ids = []

    conn = get_connection()
    try:
        for player_name in PLAYER_NAMES.split(","):
            player_name = player_name.strip()
            if not player_name:
                continue

            logger.info("Extracting player: %s", player_name)
            _throttle()

            player_data = extract_players(player_name)
            if player_data is None:
                logger.error("Failed to fetch player: %s", player_name)
                continue

            with conn.cursor() as cur:
                upsert_raw_player(cur, player_name, player_data)
            conn.commit()

            match_ids = player_data.get("matches", [])
            all_match_ids.extend(match_ids)
            logger.info("Player '%s' → %d matches", player_name, len(match_ids))
    finally:
        conn.close()

    # Push match IDs to XCom so the next task can pick them up
    context["ti"].xcom_push(key="match_ids", value=all_match_ids)
    return all_match_ids


def task_extract_matches(**context):
    """Fetch each match from the API and store in bronze."""
    from ingestion.extract_matches import extract_matches
    from load.postgres import get_connection
    from load.upsert import upsert_raw_match, match_exists_in_bronze

    match_ids = context["ti"].xcom_pull(
        task_ids="extract_player", key="match_ids"
    )
    if not match_ids:
        logger.warning("No match IDs received – nothing to do.")
        return

    total = len(match_ids)
    logger.info("Processing %d matches …", total)

    conn = get_connection()
    try:
        for idx, mid in enumerate(match_ids, start=1):
            with conn.cursor() as cur:
                if match_exists_in_bronze(cur, mid):
                    continue

            logger.info("[%d/%d] Fetching match: %s", idx, total, mid)
            _throttle()

            match_data = extract_matches(mid)
            if match_data is None:
                logger.warning("Failed to fetch match %s", mid)
                continue

            with conn.cursor() as cur:
                upsert_raw_match(cur, mid, match_data)
            conn.commit()
    finally:
        conn.close()


def task_normalize(**context):
    """Transform bronze JSONB into silver relational tables."""
    from load.postgres import get_connection
    from processing.normalize_matches import normalize_from_bronze

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            normalize_from_bronze(cur)
        conn.commit()
    finally:
        conn.close()


def task_refresh_gold(**context):
    """Refresh the gold materialized view."""
    from load.postgres import get_connection
    from processing.normalize_matches import refresh_gold

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            refresh_gold(cur)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "pubg-pipeline",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pubg_pipeline",
    default_args=default_args,
    description="PUBG stats pipeline: API → Bronze → Silver → Gold",
    schedule_interval="@daily",
    catchup=False,
    tags=["pubg", "data-pipeline"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_player",
        python_callable=task_extract_player,
    )

    t2 = PythonOperator(
        task_id="extract_matches",
        python_callable=task_extract_matches,
    )

    t3 = PythonOperator(
        task_id="normalize_to_silver",
        python_callable=task_normalize,
    )

    t4 = PythonOperator(
        task_id="refresh_gold",
        python_callable=task_refresh_gold,
    )

    t1 >> t2 >> t3 >> t4
