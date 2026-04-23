"""
=============================================================================
Load Cleaned Data into PostgreSQL
=============================================================================
Reads the cleaned JSON files from the cleaning notebook output and inserts
them into the PostgreSQL tables defined in create_schema.sql.

Prerequisites:
    1. PostgreSQL installed and running
    2. Database created:  createdb pl_analytics
    3. Schema created:    psql -d pl_analytics -f create_schema.sql
    4. Install driver:    pip install psycopg2-binary

Usage:
    python load_into_postgres.py

=============================================================================
"""

import json
import math
import os
import re
import sys

import psycopg2
from psycopg2.extras import execute_values

# CONFIGURATION — Update these to match your setup
DB_CONFIG = {
    "dbname":   "pl_analytics",
    "user":     "postgres",
    "password": "msba305",      
    "host":     "localhost",
    "port":     5432,
}

# Path to cleaned data output from cleaning_transformation.py
CLEAN_DIR = "data/cleaned"

# HELPERS

def load_jsonl(filepath):
    """Load a JSONL file into a list of dicts."""
    with open(filepath, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def get_connection():
    """Connect to PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        print(f"[OK] Connected to PostgreSQL: {DB_CONFIG['dbname']}")
        return conn
    except Exception as e:
        print(f"[ERROR] Cannot connect to PostgreSQL: {e}")
        print(f"[INFO] Make sure the database exists: createdb {DB_CONFIG['dbname']}")
        print(f"[INFO] And the schema is created: psql -d {DB_CONFIG['dbname']} -f create_schema.sql")
        sys.exit(1)


# STEP 1: Load teams from clubs.json

def load_teams(conn, clubs):
    """Insert teams and return a mapping of team_name -> team_id."""
    cur = conn.cursor()
    team_map = {}

    for club in clubs:
        name = club.get("club_name_std") or club.get("club_name")
        cur.execute(
            "INSERT INTO teams (team_name) VALUES (%s) RETURNING team_id",
            (name,)
        )
        team_id = cur.fetchone()[0]
        team_map[name] = team_id

    conn.commit()
    print(f"[OK] teams: {len(team_map)} rows inserted")
    return team_map


# STEP 2: Load players from unified_players.json

def load_players(conn, unified_players, team_map):
    """
    Insert players into the core players table.
    Returns a mapping of (player_name, team) -> player_id.
    """
    cur = conn.cursor()
    player_map = {}

    for p in unified_players:
        team = p.get("team")
        team_id = team_map.get(team)
        if not team_id:
            continue

        name = p.get("player_name")
        position = p.get("position_us") or p.get("position_tm")

        cur.execute(
            "INSERT INTO players (team_id, player_name, primary_position) "
            "VALUES (%s, %s, %s) RETURNING player_id",
            (team_id, name, position)
        )
        player_id = cur.fetchone()[0]
        player_map[(name, team)] = {
            "player_id": player_id,
            "data": p,
        }

    conn.commit()
    print(f"[OK] players: {len(player_map)} rows inserted")
    return player_map


# STEP 3: Load matches from unified_matches.json

def load_matches(conn, unified_matches, team_map):
    """
    Insert matches into the core matches table.
    Returns a mapping of (date, home_team, away_team) -> match_id + data.
    """
    cur = conn.cursor()
    match_map = {}

    for m in unified_matches:
        home = m.get("home_club_std")
        away = m.get("away_club_std")
        home_id = team_map.get(home)
        away_id = team_map.get(away)

        if not home_id or not away_id:
            continue

        date = m.get("date")
        home_goals = m.get("home_goals")
        away_goals = m.get("away_goals")

        cur.execute(
            "INSERT INTO matches (home_team_id, away_team_id, match_date, home_goals, away_goals) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING match_id",
            (home_id, away_id, date, home_goals, away_goals)
        )
        match_id = cur.fetchone()[0]
        match_map[(date, home, away)] = {
            "match_id": match_id,
            "data": m,
        }

    conn.commit()
    print(f"[OK] matches: {len(match_map)} rows inserted")
    return match_map


# STEP 4: Load Transfermarkt player profiles


def safe_int(value):
    """Convert value to int if safe for PostgreSQL INTEGER, else return None."""
    if value in (None, "", "null", "None"):
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    s = str(value).strip()
    if s.lower() == "nan":
        return None

    try:
        num = int(float(s))
    except (ValueError, TypeError):
        return None

    if num < -2147483648 or num > 2147483647:
        return None

    return num


import re

def extract_int(value):
    """Extract an integer from messy text like 'Attendance: 2.000'."""
    if value in (None, "", "null", "None"):
        return None

    s = str(value).strip()

    if s.lower() == "nan":
        return None

    # keep digits only
    digits = re.sub(r"[^\d]", "", s)

    if not digits:
        return None

    num = int(digits)

    if num < -2147483648 or num > 2147483647:
        return None

    return num


def load_tm_player_profiles(conn, player_map):
    """Insert TM profile data for players that have it."""
    cur = conn.cursor()
    count = 0

    for (name, team), info in player_map.items():
        p = info["data"]
        player_id = info["player_id"]
        tm_id = p.get("tm_player_id")

        if not tm_id:
            continue

        height_cm = safe_int(p.get("height_cm"))
        international_caps = safe_int(p.get("international_caps"))
        international_goals = safe_int(p.get("international_goals"))

        if height_cm is None and p.get("height_cm") not in (None, "", "null", "None"):
            print("Bad height_cm:", name, team, p.get("height_cm"))

        if international_caps is None and p.get("international_caps") not in (None, "", "null", "None"):
            print("Bad international_caps:", name, team, p.get("international_caps"))

        if international_goals is None and p.get("international_goals") not in (None, "", "null", "None"):
            print("Bad international_goals:", name, team, p.get("international_goals"))

        cur.execute(
            "INSERT INTO tm_player_profiles "
            "(player_id, tm_player_id, citizenship, position_tm, preferred_foot, "
            "date_of_birth, height_cm, contract_expires, international_caps, international_goals) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                player_id,
                str(tm_id),
                p.get("citizenship"),
                p.get("position_tm"),
                p.get("foot"),
                p.get("date_of_birth"),
                height_cm,
                p.get("contract_expires"),
                international_caps,
                international_goals,
            )
        )
        count += 1

    conn.commit()
    print(f"[OK] tm_player_profiles: {count} rows inserted")


# STEP 5: Load Understat player stats

def load_us_player_stats(conn, player_map):
    """Insert Understat xG stats for each player."""
    cur = conn.cursor()
    count = 0

    for (name, team), info in player_map.items():
        p = info["data"]
        player_id = info["player_id"]
        us_id = p.get("us_player_id")

        cur.execute(
            "INSERT INTO us_player_stats "
            "(player_id, us_player_id, games, minutes_played, goals, xg, "
            "assists, xa, shots, key_passes, npg, npxg, xg_chain, xg_buildup) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                player_id, us_id,
                p.get("games"), p.get("minutes"), p.get("goals"),
                p.get("xG"), p.get("assists"), p.get("xA"),
                p.get("shots"), p.get("key_passes"), p.get("npg") if "npg" in p else None,
                p.get("npxG"), p.get("xGChain"), p.get("xGBuildup"),
            )
        )
        count += 1

    conn.commit()
    print(f"[OK] us_player_stats: {count} rows inserted")


# STEP 6: Load FIFA player attributes

def load_fifa_player_attributes(conn, player_map):
    """Insert FIFA 21 ratings and attributes for matched players."""
    cur = conn.cursor()
    count = 0

    for (name, team), info in player_map.items():
        p = info["data"]
        player_id = info["player_id"]

        sofifa_id = safe_int(p.get("sofifa_id"))
        if sofifa_id is None:
            continue

        cur.execute(
            "INSERT INTO fifa_player_attributes "
            "(player_id, sofifa_id, overall, potential, value_eur, wage_eur, "
            "player_positions, preferred_foot, pace, shooting, passing, dribbling, "
            "defending, physic, attacking_finishing, mentality_composure, "
            "mentality_positioning, power_shot_power) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                player_id,
                sofifa_id,
                safe_int(p.get("overall")),
                safe_int(p.get("potential")),
                safe_int(p.get("value_eur")),
                safe_int(p.get("wage_eur")),
                None,
                None,
                safe_int(p.get("fifa_pace")),
                safe_int(p.get("fifa_shooting")),
                safe_int(p.get("fifa_passing")),
                safe_int(p.get("fifa_dribbling")),
                safe_int(p.get("fifa_defending")),
                safe_int(p.get("fifa_physic")),
                safe_int(p.get("attacking_finishing")),
                safe_int(p.get("mentality_composure")),
                safe_int(p.get("mentality_positioning")),
                safe_int(p.get("power_shot_power")),
            )
        )
        count += 1

    conn.commit()
    print(f"[OK] fifa_player_attributes: {count} rows inserted")


# STEP 7: Load TM match details

def load_tm_match_details(conn, match_map):
    """Insert Transfermarkt match detail data (stadium, referee, managers)."""
    cur = conn.cursor()
    count = 0

    for (date, home, away), info in match_map.items():
        m = info["data"]
        match_id = info["match_id"]

        tm_game_id = extract_int(m.get("game_id"))
        matchday = extract_int(m.get("matchday"))
        attendance = extract_int(m.get("attendance"))

        raw_attendance = m.get("attendance")
        if attendance is None and raw_attendance not in (None, "", "null", "None", "nan"):
            print("Bad attendance:", date, home, away, raw_attendance)

        cur.execute(
            "INSERT INTO tm_match_details "
            "(match_id, tm_game_id, matchday, stadium, attendance, referee, "
            "home_manager, away_manager) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                match_id,
                tm_game_id,
                matchday,
                m.get("stadium"),
                attendance,
                m.get("referee"),
                m.get("home_manager"),
                m.get("away_manager"),
            )
        )
        count += 1

    conn.commit()
    print(f"[OK] tm_match_details: {count} rows inserted")


# STEP 8: Load Understat match stats

def load_us_match_stats(conn, match_map):
    """Insert Understat xG data per match."""
    cur = conn.cursor()
    count = 0

    for (date, home, away), info in match_map.items():
        m = info["data"]
        match_id = info["match_id"]
        us_match_id = m.get("us_match_id")

        if not us_match_id:
            continue

        cur.execute(
            "INSERT INTO us_match_stats "
            "(match_id, us_match_id, home_xg, away_xg, "
            "forecast_win, forecast_draw, forecast_loss) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                match_id, str(us_match_id),
                m.get("home_xG"), m.get("away_xG"),
                m.get("forecast_win"), m.get("forecast_draw"), m.get("forecast_loss"),
            )
        )
        count += 1

    conn.commit()
    print(f"[OK] us_match_stats: {count} rows inserted")


# STEP 9: Load TM match events (goals, cards, subs)

def load_tm_match_events(conn, match_map, team_map):
    """Insert match events extracted from TM games."""
    cur = conn.cursor()
    count = 0

    for (date, home, away), info in match_map.items():
        m = info["data"]
        match_id = info["match_id"]

        # Process goals
        for evt in m.get("goals", []):
            club = evt.get("club", "")
            team_id = team_map.get(club)
            cur.execute(
                "INSERT INTO tm_match_events "
                "(match_id, team_id, event_type, minute, extra_minute, "
                "player_href, assist_player_href, event_description, score_after_event) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    match_id, team_id, "Goal",
                    evt.get("minute"), evt.get("extra_time"),
                    evt.get("scorer_href"), evt.get("assist_href"),
                    evt.get("description"), evt.get("score_after"),
                )
            )
            count += 1

        # Process cards
        for evt in m.get("cards", []):
            club = evt.get("club", "")
            team_id = team_map.get(club)
            cur.execute(
                "INSERT INTO tm_match_events "
                "(match_id, team_id, event_type, minute, "
                "player_href, event_description) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    match_id, team_id, "Card",
                    evt.get("minute"),
                    evt.get("player_href"), evt.get("description"),
                )
            )
            count += 1

        # Process substitutions
        for evt in m.get("substitutions", []):
            club = evt.get("club", "")
            team_id = team_map.get(club)
            cur.execute(
                "INSERT INTO tm_match_events "
                "(match_id, team_id, event_type, minute, extra_minute, "
                "player_href, subbed_in_player_href, event_description) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    match_id, team_id, "Substitution",
                    evt.get("minute"), evt.get("extra_time"),
                    evt.get("player_out_href"), evt.get("player_in_href"),
                    evt.get("reason"),
                )
            )
            count += 1

    conn.commit()
    print(f"[OK] tm_match_events: {count} rows inserted")


# STEP 10: Load source mapping tables

def load_player_source_map(conn, player_map):
    """Insert player crosswalk linking unified player_id to source IDs."""
    cur = conn.cursor()
    count = 0

    for (name, team), info in player_map.items():
        p = info["data"]
        player_id = info["player_id"]

        tm_id = p.get("tm_player_id")
        us_id = p.get("us_player_id")
        sofifa_id = safe_int(p.get("sofifa_id"))

        methods = []
        if tm_id:
            methods.append("tm")
        if sofifa_id is not None:
            methods.append("fifa")
        method = "+".join(methods) if methods else "understat_only"

        cur.execute(
            "INSERT INTO player_source_map "
            "(player_id, tm_player_id, us_player_id, sofifa_id, "
            "match_method, tm_match_score, fifa_match_score) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                player_id,
                tm_id,
                us_id,
                sofifa_id,
                method,
                p.get("tm_match_score"),
                p.get("fifa_match_score"),
            )
        )
        count += 1

    conn.commit()
    print(f"[OK] player_source_map: {count} rows inserted")


def load_match_source_map(conn, match_map):
    """Insert match crosswalk linking unified match_id to source IDs."""
    cur = conn.cursor()
    count = 0

    for (date, home, away), info in match_map.items():
        m = info["data"]
        match_id = info["match_id"]

        tm_game_id = extract_int(m.get("game_id"))
        us_match_id = m.get("us_match_id")

        method = "date+teams"
        if tm_game_id and us_match_id:
            method = "date+teams (tm+us matched)"

        cur.execute(
            "INSERT INTO match_source_map "
            "(match_id, tm_game_id, us_match_id, match_method) "
            "VALUES (%s, %s, %s, %s)",
            (match_id, tm_game_id, str(us_match_id) if us_match_id else None, method)
        )
        count += 1

    conn.commit()
    print(f"[OK] match_source_map: {count} rows inserted")


def load_team_source_map(conn, team_map):
    """Insert team name mappings for each source."""
    cur = conn.cursor()
    count = 0

    # TM team names (add FC suffix etc.)
    tm_names = {
        "Arsenal": "Arsenal FC", "Chelsea": "Chelsea FC", "Everton": "Everton FC",
        "Fulham": "Fulham FC", "Liverpool": "Liverpool FC", "Burnley": "Burnley FC",
        "Southampton": "Southampton FC", "Leeds": "Leeds United",
        "Leicester": "Leicester City", "Tottenham": "Tottenham Hotspur",
        "West Ham": "West Ham United", "Brighton": "Brighton & Hove Albion",
    }

    # FIFA team names (similar to TM but some differ)
    fifa_names = {
        "Leeds": "Leeds United", "Leicester": "Leicester City",
        "Tottenham": "Tottenham Hotspur", "West Ham": "West Ham United",
        "Brighton": "Brighton & Hove Albion",
    }

    for canonical_name, team_id in team_map.items():
        # Transfermarkt
        tm_name = tm_names.get(canonical_name, canonical_name)
        cur.execute(
            "INSERT INTO team_source_map (team_id, source_name, source_team_name, match_method) "
            "VALUES (%s, %s, %s, %s)",
            (team_id, "transfermarkt", tm_name, "manual_mapping")
        )
        count += 1

        # Understat (uses canonical names directly)
        cur.execute(
            "INSERT INTO team_source_map (team_id, source_name, source_team_name, match_method) "
            "VALUES (%s, %s, %s, %s)",
            (team_id, "understat", canonical_name, "manual_mapping")
        )
        count += 1

        # FIFA
        fifa_name = fifa_names.get(canonical_name, canonical_name)
        cur.execute(
            "INSERT INTO team_source_map (team_id, source_name, source_team_name, match_method) "
            "VALUES (%s, %s, %s, %s)",
            (team_id, "fifa", fifa_name, "manual_mapping")
        )
        count += 1

    conn.commit()
    print(f"[OK] team_source_map: {count} rows inserted")


# STEP 11: Verify

def verify(conn):
    """Run count queries on all tables to verify the load."""
    cur = conn.cursor()
    tables = [
        "teams", "players", "matches",
        "tm_player_profiles", "us_player_stats", "fifa_player_attributes",
        "tm_match_details", "us_match_stats", "tm_match_events",
        "player_source_map", "match_source_map", "team_source_map",
    ]

    print("\n" + "=" * 50)
    print("VERIFICATION — Row counts")
    print("=" * 50)

    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  {table:30s}: {count:>6} rows")


# MAIN

def main():
    print("=" * 60)
    print("LOADING CLEANED DATA INTO POSTGRESQL")
    print("=" * 60)

    # Load cleaned JSON files
    print("\n[INFO] Loading cleaned files from", CLEAN_DIR)
    clubs = load_jsonl(os.path.join(CLEAN_DIR, "clubs.json"))
    unified_players = load_jsonl(os.path.join(CLEAN_DIR, "unified_players.json"))
    unified_matches = load_jsonl(os.path.join(CLEAN_DIR, "unified_matches.json"))

    print(f"  clubs.json:            {len(clubs)} records")
    print(f"  unified_players.json:  {len(unified_players)} records")
    print(f"  unified_matches.json:  {len(unified_matches)} records")

    # Connect
    conn = get_connection()

    try:
        # Core tables (order matters — teams first, then players/matches)
        print("\n--- Core tables ---")
        team_map = load_teams(conn, clubs)
        player_map = load_players(conn, unified_players, team_map)
        match_map = load_matches(conn, unified_matches, team_map)

        # Source-specific detail tables
        print("\n--- Transfermarkt detail ---")
        load_tm_player_profiles(conn, player_map)
        load_tm_match_details(conn, match_map)
        load_tm_match_events(conn, match_map, team_map)

        print("\n--- Understat detail ---")
        load_us_player_stats(conn, player_map)
        load_us_match_stats(conn, match_map)

        print("\n--- FIFA detail ---")
        load_fifa_player_attributes(conn, player_map)

        # Source maps
        print("\n--- Source mapping tables ---")
        load_player_source_map(conn, player_map)
        load_match_source_map(conn, match_map)
        load_team_source_map(conn, team_map)

        # Verify
        verify(conn)

        print("\n[DONE] All data loaded into PostgreSQL successfully.")

    except Exception as e:
        conn.rollback()
        print(f"\n[ERROR] {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
