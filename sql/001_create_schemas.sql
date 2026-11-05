-- =========================================================================
-- PUBG Data Pipeline – Database Schema
-- Medallion architecture: bronze → silver → gold
-- =========================================================================

-- -------------------------------------------------------------------------
-- BRONZE LAYER – raw JSONB from the PUBG API
-- -------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_players (
    id            SERIAL PRIMARY KEY,
    player_name   TEXT NOT NULL UNIQUE,
    payload       JSONB NOT NULL,
    ingested_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.raw_matches (
    id            SERIAL PRIMARY KEY,
    match_id      TEXT NOT NULL UNIQUE,
    payload       JSONB NOT NULL,
    ingested_at   TIMESTAMPTZ DEFAULT now()
);

-- -------------------------------------------------------------------------
-- SILVER LAYER – normalized relational tables
-- -------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.matches (
    match_id       TEXT PRIMARY KEY,
    game_mode      TEXT,
    map_name       TEXT,
    duration_sec   INT,
    created_at     TIMESTAMPTZ,
    is_custom      BOOLEAN,
    shard_id       TEXT,
    match_type     TEXT
);

CREATE TABLE IF NOT EXISTS silver.rosters (
    roster_id      TEXT PRIMARY KEY,
    match_id       TEXT NOT NULL REFERENCES silver.matches(match_id),
    rank           INT,
    team_id        INT,
    won            BOOLEAN
);

CREATE TABLE IF NOT EXISTS silver.player_match_stats (
    id              SERIAL PRIMARY KEY,
    match_id        TEXT NOT NULL REFERENCES silver.matches(match_id),
    roster_id       TEXT NOT NULL REFERENCES silver.rosters(roster_id),
    player_id       TEXT,
    player_name     TEXT,
    kills           INT,
    assists         INT,
    dbnos           INT,
    damage_dealt    FLOAT,
    death_type      TEXT,
    headshot_kills  INT,
    heals           INT,
    boosts          INT,
    revives         INT,
    time_survived   INT,
    walk_distance   FLOAT,
    ride_distance   FLOAT,
    swim_distance   FLOAT,
    longest_kill    FLOAT,
    win_place       INT,
    kill_place      INT,
    kill_streaks    INT,
    road_kills      INT,
    team_kills      INT,
    vehicle_destroys INT,
    weapons_acquired INT,
    UNIQUE(match_id, player_id)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player
    ON silver.player_match_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_player_match_stats_name
    ON silver.player_match_stats(player_name);
CREATE INDEX IF NOT EXISTS idx_rosters_match
    ON silver.rosters(match_id);

-- -------------------------------------------------------------------------
-- GOLD LAYER – analytics materialized views
-- -------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS gold;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.player_career AS
SELECT
    pms.player_name,
    pms.player_id,
    COUNT(DISTINCT pms.match_id)                        AS matches_played,
    ROUND(AVG(pms.kills)::numeric, 2)                   AS avg_kills,
    ROUND(AVG(pms.damage_dealt)::numeric, 2)             AS avg_damage,
    ROUND(AVG(pms.assists)::numeric, 2)                  AS avg_assists,
    SUM(pms.kills)                                      AS total_kills,
    SUM(pms.assists)                                    AS total_assists,
    SUM(pms.dbnos)                                      AS total_dbnos,
    SUM(CASE WHEN r.won THEN 1 ELSE 0 END)             AS wins,
    ROUND(AVG(pms.time_survived)::numeric, 0)            AS avg_survival_sec,
    ROUND(AVG(pms.walk_distance)::numeric, 2)            AS avg_walk_distance,
    ROUND(AVG(pms.ride_distance)::numeric, 2)            AS avg_ride_distance,
    MAX(pms.kills)                                      AS best_kills,
    MAX(pms.damage_dealt)                               AS best_damage
FROM silver.player_match_stats pms
JOIN silver.rosters r ON r.roster_id = pms.roster_id
GROUP BY pms.player_name, pms.player_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_player_career_player_id
    ON gold.player_career(player_id);
