"""
=============================================================================
Source 3: Kaggle FIFA 21 — Premier League Players
=============================================================================
Downloads the FIFA 21 Complete Player Dataset from Kaggle and filters it to
Premier League players only, converting from CSV to JSONL format.

Data Collected:
    - Players: ~654 records — 106 attributes per player including:
        Identity:   sofifa_id, short_name, long_name, age, dob, nationality, club_name
        Ratings:    overall, potential, value_eur, wage_eur, international_reputation
        Skills:     pace, shooting, passing, dribbling, defending, physic
        Sub-attrs:  attacking_finishing, attacking_short_passing, skill_ball_control,
                    mentality_vision, mentality_composure, power_shot_power, etc.
        Profile:    preferred_foot, weak_foot, skill_moves, work_rate, body_type,
                    height_cm, weight_kg, player_positions

Format: CSV (input) -> JSONL (output, one JSON object per line)
Access Method: Direct download from Kaggle (requires Kaggle account)
Season: FIFA 21 corresponds to the 2020/2021 football season

Download Instructions:
    1. Go to: https://www.kaggle.com/datasets/stefanoleone992/fifa-21-complete-player-dataset
    2. Download players_21.csv
    3. Place it in data/raw/kaggle/
    4. Run this script

    OR use the Kaggle CLI:
        pip install kaggle
        kaggle datasets download stefanoleone992/fifa-21-complete-player-dataset
        unzip fifa-21-complete-player-dataset.zip -d data/raw/kaggle/

Notes:
    - FIFA ratings are produced by EA Sports using a network of 6,000+ real scouts,
      making them a widely-used proxy for player ability in football analytics.
    - value_eur and wage_eur fill the gap left by Transfermarkt's blocked market values.
    - Name matching with other sources requires fuzzy matching since FIFA uses shortened
      names (e.g., "H. Kane") while Understat uses full names ("Harry Kane").
    - We filter by league_name == "English Premier League" to keep only PL players.

Requirements:
    pip install pandas

Usage:
    python ingest_kaggle_fifa.py

=============================================================================
"""

import json
import os
import sys

import pandas as pd


# Configuration
INPUT_DIR = "data/raw/kaggle"
INPUT_FILE = "players_21.csv"
OUTPUT_DIR = "data/raw/kaggle"
OUTPUT_FILE = "fifa21_pl_players.json"

LEAGUE_FILTER = "English Premier League"

# Expected 20 PL clubs for 2020/21 season
EXPECTED_CLUBS = [
    "Arsenal", "Aston Villa", "Brighton & Hove Albion", "Burnley",
    "Chelsea", "Crystal Palace", "Everton", "Fulham",
    "Leeds United", "Leicester City", "Liverpool",
    "Manchester City", "Manchester United", "Newcastle United",
    "Sheffield United", "Southampton", "Tottenham Hotspur",
    "West Bromwich Albion", "West Ham United", "Wolverhampton Wanderers"
]


def load_and_filter(filepath):
    """
    Load the full FIFA 21 CSV and filter to Premier League players only.

    Args:
        filepath: path to players_21.csv

    Returns:
        DataFrame with only English Premier League players
    """
    print(f"[INFO] Loading {filepath}...")
    df = pd.read_csv(filepath, low_memory=False)
    print(f"[INFO] Total FIFA 21 players: {len(df)}")
    print(f"[INFO] Total columns: {len(df.columns)}")

    # Filter to Premier League only
    pl_df = df[df["league_name"] == LEAGUE_FILTER].copy()
    print(f"[INFO] Premier League players: {len(pl_df)}")

    # Validate clubs
    clubs = sorted(pl_df["club_name"].unique())
    print(f"[INFO] Clubs found: {len(clubs)}")

    missing = set(EXPECTED_CLUBS) - set(clubs)
    extra = set(clubs) - set(EXPECTED_CLUBS)
    if missing:
        print(f"[WARN] Missing clubs: {missing}")
    if extra:
        print(f"[WARN] Unexpected clubs: {extra}")

    return pl_df


def convert_to_jsonl(df, output_path):
    """
    Convert DataFrame to JSONL format (one JSON object per line).
    Handles NaN values by converting them to None (null in JSON).

    Args:
        df: pandas DataFrame
        output_path: path for output JSONL file
    """
    records = df.where(df.notna(), None).to_dict(orient="records")

    with open(output_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    print(f"[OK] Saved {len(records)} records -> {output_path}")


def print_summary(df):
    """Print a summary of the filtered dataset for documentation."""
    print("\n" + "=" * 60)
    print("DATASET SUMMARY")
    print("=" * 60)
    print(f"Records:  {len(df)}")
    print(f"Columns:  {len(df.columns)}")
    print(f"Clubs:    {df['club_name'].nunique()}")
    print(f"\nRating distribution:")
    print(f"  Overall: min={df['overall'].min()}, max={df['overall'].max()}, "
          f"mean={df['overall'].mean():.1f}")
    print(f"  Potential: min={df['potential'].min()}, max={df['potential'].max()}, "
          f"mean={df['potential'].mean():.1f}")
    print(f"\nValue (EUR):")
    print(f"  Min: €{df['value_eur'].min():,.0f}")
    print(f"  Max: €{df['value_eur'].max():,.0f}")
    print(f"  Mean: €{df['value_eur'].mean():,.0f}")
    print(f"\nNull counts (top 10):")
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0].sort_values(ascending=False).head(10)
    for col, count in nulls.items():
        print(f"  {col}: {count} ({count/len(df)*100:.1f}%)")


def validate_output(filepath):
    """Validate the output JSONL file."""
    print("\n" + "=" * 60)
    print("VALIDATION")
    print("=" * 60)

    with open(filepath, "r", encoding="utf-8") as f:
        records = [json.loads(line) for line in f if line.strip()]

    print(f"[OK] {len(records)} records in output file")
    print(f"     Fields per record: {len(records[0])}")

    # Show key fields from first record
    sample = records[0]
    key_fields = ["short_name", "long_name", "club_name", "overall",
                  "potential", "value_eur", "wage_eur", "pace", "shooting",
                  "passing", "dribbling", "defending", "physic"]
    print(f"\n     Sample record:")
    for k in key_fields:
        if k in sample:
            print(f"       {k}: {sample[k]}")


def main():
    """
    Main ingestion pipeline for FIFA 21 Kaggle dataset.
    Loads CSV, filters to PL, converts to JSONL.
    """
    print("=" * 60)
    print("FIFA 21 KAGGLE INGESTION — Premier League 2020/21")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    input_path = os.path.join(INPUT_DIR, INPUT_FILE)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    # Check input file exists
    if not os.path.exists(input_path):
        print(f"[ERROR] Input file not found: {input_path}")
        print(f"[INFO] Download from: https://www.kaggle.com/datasets/"
              f"stefanoleone992/fifa-21-complete-player-dataset")
        sys.exit(1)

    # Load and filter
    pl_df = load_and_filter(input_path)

    # Summary stats
    print_summary(pl_df)

    # Convert to JSONL
    convert_to_jsonl(pl_df, output_path)

    # Validate
    validate_output(output_path)

    print("\n[DONE] FIFA 21 Kaggle ingestion complete.")


if __name__ == "__main__":
    main()
