"""
=============================================================================
Source 2: Understat — Premier League 2020/21
=============================================================================
Scrapes advanced performance metrics from understat.com using the 'understat'
Python library, which wraps Understat's async API.

Data Collected:
    - Players: ~524 records — xG, xA, npxG, xGChain, xGBuildup, goals, assists,
                               shots, key_passes, yellow/red cards, position, team
    - Teams:   ~532 records — same stats as players, grouped per team (includes
                               players who transferred mid-season with comma-separated teams)
    - Matches: 380 records  — home/away teams, goals, xG per side, datetime,
                               win/draw/loss forecast probabilities

Format: JSONL (one JSON object per line)
Access Method: HTTP scraping via aiohttp (data embedded in HTML script tags)
Season: 2020/2021 (season parameter = 2020)

Notes:
    - Understat only tracks players who received actual playing time. Players who
      were registered but never appeared will not be in this dataset (524 vs 783 in TM).
    - Some players have comma-separated team names (e.g., "Arsenal,West Bromwich Albion")
      if they transferred mid-season. These need to be handled during cleaning.
    - The join key to Transfermarkt is player_name + team_title, requiring fuzzy matching
      due to naming convention differences (e.g., "Son Heung-min" vs "Heung-Min Son").

Requirements:
    pip install understat aiohttp

Usage:
    python ingest_understat.py

=============================================================================
"""

import asyncio
import json
import os

import aiohttp
from understat import Understat


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SEASON = 2020                               # Understat uses start year (2020 = 2020/21)
LEAGUE = "EPL"
OUTPUT_DIR = "data/raw/understat"

# 20 PL teams for 2020/21 (must match Understat's exact naming)
TEAMS_2020_21 = [
    "Manchester City", "Manchester United", "Liverpool", "Chelsea",
    "Leicester", "West Ham", "Tottenham", "Arsenal",
    "Leeds", "Everton", "Aston Villa", "Newcastle United",
    "Wolverhampton Wanderers", "Crystal Palace", "Southampton",
    "Brighton", "Burnley", "Fulham", "West Bromwich Albion",
    "Sheffield United"
]


def setup_output_dir():
    """Create output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[INFO] Output directory: {OUTPUT_DIR}")


def save_jsonl(records, filepath, label):
    """
    Save a list of records as JSONL (one JSON object per line).

    Args:
        records: list of dicts
        filepath: output path
        label: description for logging
    """
    with open(filepath, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[OK] {label}: {len(records)} records -> {filepath}")


async def scrape_all():
    """
    Main scraping coroutine. Collects player stats, team-level stats,
    and match results from Understat for PL 2020/21.
    """
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)

        # -----------------------------------------------------------------
        # 1. LEAGUE-LEVEL PLAYER STATS
        # -----------------------------------------------------------------
        # Returns aggregated season stats for every player who appeared
        # in at least one EPL match during 2020/21
        print("[INFO] Scraping league player stats...")
        players = await understat.get_league_players(LEAGUE, SEASON)
        print(f"  -> {len(players)} players found")

        # Add metadata for traceability
        for p in players:
            p["source"] = "understat"
            p["season"] = "2020/2021"
            p["league"] = LEAGUE

        save_jsonl(
            players,
            os.path.join(OUTPUT_DIR, "understat_players_2020_2021.json"),
            "Players"
        )

        # -----------------------------------------------------------------
        # 2. TEAM-LEVEL PLAYER STATS
        # -----------------------------------------------------------------
        # Same player stats but fetched per team — useful for team-level
        # aggregations and catches players the league endpoint may miss
        print("[INFO] Scraping team player stats...")
        all_team_players = []

        for team in TEAMS_2020_21:
            print(f"  -> {team}...")
            try:
                team_players = await understat.get_team_players(team, SEASON)
                for tp in team_players:
                    tp["source"] = "understat"
                    tp["season"] = "2020/2021"
                    tp["league"] = LEAGUE
                all_team_players.extend(team_players)
            except Exception as e:
                print(f"     [ERROR] {team}: {e}")

        print(f"  -> {len(all_team_players)} team-player records total")
        save_jsonl(
            all_team_players,
            os.path.join(OUTPUT_DIR, "understat_teams_2020_2021.json"),
            "Teams"
        )

        # -----------------------------------------------------------------
        # 3. MATCH RESULTS WITH xG
        # -----------------------------------------------------------------
        # All 380 PL matches with home/away xG and win/draw/loss probabilities
        print("[INFO] Scraping match results...")
        matches = await understat.get_league_results(LEAGUE, SEASON)
        print(f"  -> {len(matches)} matches found")

        for m in matches:
            m["source"] = "understat"
            m["season"] = "2020/2021"
            m["league"] = LEAGUE

        save_jsonl(
            matches,
            os.path.join(OUTPUT_DIR, "understat_matches_2020_2021.json"),
            "Matches"
        )


def validate_outputs():
    """Validate all output files exist and have expected record counts."""
    print("\n" + "=" * 60)
    print("VALIDATION")
    print("=" * 60)

    expected = {
        "understat_players_2020_2021.json": ("Players", 500),
        "understat_teams_2020_2021.json": ("Teams", 500),
        "understat_matches_2020_2021.json": ("Matches", 380),
    }

    for filename, (label, min_count) in expected.items():
        filepath = os.path.join(OUTPUT_DIR, filename)
        if not os.path.exists(filepath):
            print(f"[ERROR] Missing: {filepath}")
            continue

        with open(filepath, "r", encoding="utf-8") as f:
            records = [json.loads(line) for line in f if line.strip()]

        count = len(records)
        status = "OK" if count >= min_count else "WARN"
        print(f"[{status}] {label}: {count} records (expected >= {min_count})")

        if records:
            keys = list(records[0].keys())
            print(f"       Fields: {keys}")


def main():
    """
    Main ingestion pipeline for Understat.
    Scrapes player stats, team stats, and match results for PL 2020/21.
    """
    print("=" * 60)
    print("UNDERSTAT INGESTION — Premier League 2020/21")
    print("=" * 60)

    setup_output_dir()
    asyncio.run(scrape_all())
    validate_outputs()

    print("\n[DONE] Understat ingestion complete.")


if __name__ == "__main__":
    main()
