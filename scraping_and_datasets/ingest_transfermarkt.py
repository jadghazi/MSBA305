"""
=============================================================================
Source 1: Transfermarkt — Premier League 2020/21
=============================================================================
Scrapes club metadata, player profiles, and match data from Transfermarkt.co.uk
using the open-source 'transfermarkt-scraper' tool (github.com/dcaribou/transfermarkt-scraper).

Data Collected:
    - Clubs:   20 records  — squad size, avg age, stadium, coach, net transfer record
    - Players: 783 records — name, age, height, nationality, position, foot, contract,
                              international caps (market values blocked by Transfermarkt)
    - Games:   380 records — result, half-time score, matchday, date, stadium, referee,
                              managers, events (goals with assist + minute, cards, subs)

Format: JSONL (one JSON object per line)
Access Method: Web scraping via Crawlee/Playwright headless browser
Season: 2020/2021 (--season 2020 flag)

Notes:
    - Market value fields (current_market_value, highest_market_value, market_value_history)
      return null for all players. Transfermarkt protects this data behind CAPTCHAs and
      JavaScript rendering. This is a known limitation documented in the scraper's GitHub
      issues. We compensate by using FIFA 21 value_eur as an alternative valuation source.
    - The scraper requires the 'crawlee' Python package with Playwright for browser automation.
    - Output is piped to stdout and redirected to JSON files.

Usage (run from transfermarkt-scraper directory):
    # Step 1: Create a pl_competition.json file with the Premier League entry
    # Step 2: Scrape clubs for 2020/21 season
    python -m tfmkt clubs -p pl_competition.json --season 2020 > premier_league_clubs_2020_2021.json
    # Step 3: Scrape players from those clubs
    python -m tfmkt players -p premier_league_clubs_2020_2021.json --season 2020 > premier_league_players_2020_2021.json
    # Step 4: Scrape games for 2020/21 season
    python -m tfmkt games -p pl_competition.json --season 2020 > premier_league_games_2020_2021.json

Requirements:
    pip install crawlee[playwright]
    playwright install chromium

=============================================================================
"""

import json
import os
import sys
import subprocess


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRAPER_DIR = "transfermarkt-scraper"       # path to cloned repo
SEASON = "2020"                             # Transfermarkt uses start year
OUTPUT_DIR = "data/raw/transfermarkt"

# Premier League competition entry (extracted from competitions.json)
PL_COMPETITION = {
    "type": "competition",
    "parent": {
        "type": "confederation",
        "href": "/wettbewerbe/europa",
        "seasoned_href": "https://www.transfermarkt.co.uk/wettbewerbe/europa"
    },
    "country_id": "189",
    "country_name": "England",
    "country_code": "GB1",
    "total_clubs": "20",
    "total_players": "528",
    "average_age": "26.4",
    "foreigner_percentage": "72.0 %",
    "average_market_value": None,
    "total_value": "\u20ac12.58bn",
    "competition_type": "first_tier",
    "competition_name": "Premier League",
    "href": "/premier-league/startseite/wettbewerb/GB1"
}


def setup_output_dir():
    """Create output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[INFO] Output directory: {OUTPUT_DIR}")


def write_competition_file():
    """Write the PL competition entry to a temp file for the scraper."""
    filepath = os.path.join(OUTPUT_DIR, "pl_competition.json")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(json.dumps(PL_COMPETITION, ensure_ascii=False) + "\n")
    print(f"[INFO] Competition file written: {filepath}")
    return filepath


def run_scraper(crawler, parent_file, output_file):
    """
    Run the transfermarkt-scraper for a given crawler.

    Args:
        crawler: str — the crawler name (clubs, players, games)
        parent_file: str — path to the parent JSON file
        output_file: str — path for the output JSON file
    """
    cmd = [
        sys.executable, "-m", "tfmkt", crawler,
        "-p", parent_file,
        "--season", SEASON
    ]
    print(f"[INFO] Running: {' '.join(cmd)}")
    print(f"[INFO] Output: {output_file}")

    with open(output_file, "w", encoding="utf-8") as out:
        result = subprocess.run(
            cmd,
            stdout=out,
            stderr=subprocess.PIPE,
            text=True,
            cwd=SCRAPER_DIR
        )

    if result.returncode != 0:
        print(f"[ERROR] Scraper failed:\n{result.stderr}")
        return False

    # Count output lines
    with open(output_file, "r", encoding="utf-8") as f:
        count = sum(1 for line in f if line.strip())
    print(f"[OK] Scraped {count} records -> {output_file}")
    return True


def validate_output(filepath, expected_min):
    """
    Validate that a scraped file has the expected number of records.

    Args:
        filepath: str — path to the JSONL file
        expected_min: int — minimum expected record count
    """
    with open(filepath, "r", encoding="utf-8") as f:
        records = [json.loads(line) for line in f if line.strip()]

    count = len(records)
    if count < expected_min:
        print(f"[WARN] {filepath}: only {count} records (expected >= {expected_min})")
    else:
        print(f"[OK] {filepath}: {count} records validated")

    # Show sample record keys
    if records:
        keys = [k for k in records[0].keys() if k != "parent"]
        print(f"       Fields: {keys}")

    return count


def main():
    """
    Main ingestion pipeline for Transfermarkt.
    Scrapes clubs, players, and games for PL 2020/21.
    """
    print("=" * 60)
    print("TRANSFERMARKT INGESTION — Premier League 2020/21")
    print("=" * 60)

    setup_output_dir()

    # Step 1: Write competition file
    comp_file = write_competition_file()

    # Step 2: Scrape clubs
    clubs_file = os.path.join(OUTPUT_DIR, "premier_league_clubs_2020_2021.json")
    if not run_scraper("clubs", comp_file, clubs_file):
        print("[FATAL] Club scraping failed. Aborting.")
        return

    # Step 3: Scrape players (uses clubs as parent)
    players_file = os.path.join(OUTPUT_DIR, "premier_league_players_2020_2021.json")
    if not run_scraper("players", clubs_file, players_file):
        print("[FATAL] Player scraping failed. Aborting.")
        return

    # Step 4: Scrape games
    games_file = os.path.join(OUTPUT_DIR, "premier_league_games_2020_2021.json")
    if not run_scraper("games", comp_file, games_file):
        print("[FATAL] Games scraping failed. Aborting.")
        return

    # Step 5: Validate
    print("\n" + "=" * 60)
    print("VALIDATION")
    print("=" * 60)
    validate_output(clubs_file, expected_min=20)
    validate_output(players_file, expected_min=700)
    validate_output(games_file, expected_min=380)

    print("\n[DONE] Transfermarkt ingestion complete.")


if __name__ == "__main__":
    main()
