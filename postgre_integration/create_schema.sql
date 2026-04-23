-- Premier League 2020/21 Pipeline — PostgreSQL Schema
-- Run this script to create all tables.
-- Usage: psql -U postgres -d pl_analytics -f create_schema.sql

-- Drop tables if they exist (in reverse dependency order)
DROP TABLE IF EXISTS tm_match_events CASCADE;
DROP TABLE IF EXISTS us_match_stats CASCADE;
DROP TABLE IF EXISTS tm_match_details CASCADE;
DROP TABLE IF EXISTS us_player_stats CASCADE;
DROP TABLE IF EXISTS fifa_player_attributes CASCADE;
DROP TABLE IF EXISTS tm_player_profiles CASCADE;
DROP TABLE IF EXISTS player_source_map CASCADE;
DROP TABLE IF EXISTS match_source_map CASCADE;
DROP TABLE IF EXISTS team_source_map CASCADE;
DROP TABLE IF EXISTS matches CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS teams CASCADE;

-- CORE UNIFIED TABLES

CREATE TABLE teams (
    team_id         SERIAL PRIMARY KEY,
    team_name       VARCHAR(100) NOT NULL UNIQUE,
    country         VARCHAR(50) DEFAULT 'England',
    league          VARCHAR(50) DEFAULT 'Premier League',
    season          VARCHAR(20) DEFAULT '2020/2021'
);

CREATE TABLE players (
    player_id       SERIAL PRIMARY KEY,
    team_id         INT REFERENCES teams(team_id),
    player_name     VARCHAR(150) NOT NULL,
    primary_position VARCHAR(50),
    season          VARCHAR(20) DEFAULT '2020/2021'
);

CREATE TABLE matches (
    match_id        SERIAL PRIMARY KEY,
    home_team_id    INT REFERENCES teams(team_id),
    away_team_id    INT REFERENCES teams(team_id),
    match_date      DATE,
    home_goals      INT,
    away_goals      INT,
    season          VARCHAR(20) DEFAULT '2020/2021'
);

-- TRANSFERMARKT SOURCE TABLES

CREATE TABLE tm_player_profiles (
    tm_player_profile_id SERIAL PRIMARY KEY,
    player_id       INT REFERENCES players(player_id),
    tm_player_id    VARCHAR(20),
    citizenship     VARCHAR(100),
    position_tm     VARCHAR(50),
    preferred_foot  VARCHAR(10),
    date_of_birth   VARCHAR(20),
    height_cm       INT,
    contract_expires VARCHAR(20),
    international_caps INT,
    international_goals INT
);

CREATE TABLE tm_match_details (
    tm_match_detail_id SERIAL PRIMARY KEY,
    match_id        INT REFERENCES matches(match_id),
    tm_game_id      INT,
    matchday        INT,
    stadium         VARCHAR(150),
    attendance      INT,
    referee         VARCHAR(100),
    home_manager    VARCHAR(100),
    away_manager    VARCHAR(100)
);

CREATE TABLE tm_match_events (
    tm_event_id     SERIAL PRIMARY KEY,
    match_id        INT REFERENCES matches(match_id),
    team_id         INT REFERENCES teams(team_id),
    event_type      VARCHAR(20),
    minute          INT,
    extra_minute    INT,
    player_href     VARCHAR(200),
    assist_player_href VARCHAR(200),
    subbed_in_player_href VARCHAR(200),
    event_description TEXT,
    score_after_event VARCHAR(10)
);

-- UNDERSTAT SOURCE TABLES

CREATE TABLE us_player_stats (
    us_player_stat_id SERIAL PRIMARY KEY,
    player_id       INT REFERENCES players(player_id),
    us_player_id    VARCHAR(20),
    games           INT,
    minutes_played  INT,
    goals           INT,
    xg              DECIMAL(10,4),
    assists         INT,
    xa              DECIMAL(10,4),
    shots           INT,
    key_passes      INT,
    npg             INT,
    npxg            DECIMAL(10,4),
    xg_chain        DECIMAL(10,4),
    xg_buildup      DECIMAL(10,4)
);

CREATE TABLE us_match_stats (
    us_match_stat_id SERIAL PRIMARY KEY,
    match_id        INT REFERENCES matches(match_id),
    us_match_id     VARCHAR(20),
    home_xg         DECIMAL(10,4),
    away_xg         DECIMAL(10,4),
    forecast_win    DECIMAL(6,4),
    forecast_draw   DECIMAL(6,4),
    forecast_loss   DECIMAL(6,4)
);

-- FIFA SOURCE TABLE

CREATE TABLE fifa_player_attributes (
    fifa_player_attr_id SERIAL PRIMARY KEY,
    player_id       INT REFERENCES players(player_id),
    sofifa_id       INT,
    overall         INT,
    potential       INT,
    value_eur       BIGINT,
    wage_eur        BIGINT,
    player_positions VARCHAR(50),
    preferred_foot  VARCHAR(10),
    pace            DECIMAL(5,1),
    shooting        DECIMAL(5,1),
    passing         DECIMAL(5,1),
    dribbling       DECIMAL(5,1),
    defending       DECIMAL(5,1),
    physic          DECIMAL(5,1),
    attacking_finishing  DECIMAL(5,1),
    mentality_composure  DECIMAL(5,1),
    mentality_positioning DECIMAL(5,1),
    power_shot_power DECIMAL(5,1)
);

-- SOURCE MAPPING TABLES (crosswalk / lineage)

CREATE TABLE player_source_map (
    player_source_map_id SERIAL PRIMARY KEY,
    player_id       INT REFERENCES players(player_id),
    tm_player_id    VARCHAR(20),
    us_player_id    VARCHAR(20),
    sofifa_id       INT,
    match_method    VARCHAR(20),
    tm_match_score  DECIMAL(5,1),
    fifa_match_score DECIMAL(5,1)
);

CREATE TABLE match_source_map (
    match_source_map_id SERIAL PRIMARY KEY,
    match_id        INT REFERENCES matches(match_id),
    tm_game_id      INT,
    us_match_id     VARCHAR(20),
    match_method    VARCHAR(50)
);

CREATE TABLE team_source_map (
    team_source_map_id SERIAL PRIMARY KEY,
    team_id         INT REFERENCES teams(team_id),
    source_name     VARCHAR(20),
    source_team_id  VARCHAR(50),
    source_team_name VARCHAR(100),
    match_method    VARCHAR(50)
);

-- INDEXES for query performance

CREATE INDEX idx_players_team ON players(team_id);
CREATE INDEX idx_matches_home ON matches(home_team_id);
CREATE INDEX idx_matches_away ON matches(away_team_id);
CREATE INDEX idx_matches_date ON matches(match_date);
CREATE INDEX idx_us_player_stats_player ON us_player_stats(player_id);
CREATE INDEX idx_fifa_attrs_player ON fifa_player_attributes(player_id);
CREATE INDEX idx_tm_profiles_player ON tm_player_profiles(player_id);
CREATE INDEX idx_tm_match_details_match ON tm_match_details(match_id);
CREATE INDEX idx_us_match_stats_match ON us_match_stats(match_id);
CREATE INDEX idx_tm_events_match ON tm_match_events(match_id);
CREATE INDEX idx_player_source_map_player ON player_source_map(player_id);

-- Done
SELECT 'Schema created successfully.' AS status;
