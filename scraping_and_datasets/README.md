# Data Ingestion Scripts — Premier League 2020/21 Pipeline

## Overview

Three ingestion scripts collect data from three distinct sources, each in a different format
and access method, for the 2020/21 Premier League season.

| # | Source | Script | Access Method | Input Format | Output |
|---|--------|--------|---------------|--------------|--------|
| 1 | Transfermarkt | `ingest_transfermarkt.py` | Web scraping (Crawlee/Playwright) | HTML → JSONL | 3 files (clubs, players, games) |
| 2 | Understat | `ingest_understat.py` | HTTP scraping (aiohttp) | HTML/JS → JSONL | 3 files (players, teams, matches) |
| 3 | Kaggle FIFA 21 | `ingest_kaggle_fifa.py` | Direct download (CSV) | CSV → JSONL | 1 file (PL players) |

## Output Files

All outputs are in **JSONL format** (one JSON object per line) stored in `data/raw/<source>/`.

```
data/raw/
├── transfermarkt/
│   ├── premier_league_clubs_2020_2021.json     (20 records)
│   ├── premier_league_players_2020_2021.json   (783 records)
│   └── premier_league_games_2020_2021.json     (380 records)
├── understat/
│   ├── understat_players_2020_2021.json        (524 records)
│   ├── understat_teams_2020_2021.json          (532 records)
│   └── understat_matches_2020_2021.json        (380 records)
└── kaggle/
    ├── players_21.csv                          (18,944 records — full FIFA 21)
    └── fifa21_pl_players.json                  (654 records — PL only)
```

## Setup & Execution

### 1. Transfermarkt
```bash
# Clone the scraper
git clone https://github.com/dcaribou/transfermarkt-scraper.git
cd transfermarkt-scraper
pip install crawlee[playwright]
playwright install chromium

# Run ingestion
cd ..
python ingest_transfermarkt.py
```

### 2. Understat
```bash
pip install understat aiohttp
python ingest_understat.py
```

### 3. Kaggle FIFA 21
```bash
# Download players_21.csv from Kaggle first:
# https://www.kaggle.com/datasets/stefanoleone992/fifa-21-complete-player-dataset
pip install pandas
python ingest_kaggle_fifa.py
```

## Cross-Source Join Keys

| Join | Key Fields | Challenge |
|------|-----------|-----------|
| TM ↔ Understat (players) | player name + team | Fuzzy matching needed (naming conventions differ) |
| TM ↔ Understat (matches) | date + home/away team | Team name standardization needed |
| TM ↔ FIFA 21 | player name + club | FIFA uses short names (e.g., "H. Kane") |
| Understat ↔ FIFA 21 | player name + club | Different naming + club name formats |

## Known Data Quality Issues

1. **Transfermarkt market values:** All null — blocked by Transfermarkt's anti-scraping measures
2. **Name mismatches:** Each source uses different naming conventions
3. **Team name differences:** e.g., "Tottenham" (Understat) vs "Tottenham Hotspur" (TM/FIFA)
4. **Mid-season transfers:** Understat shows comma-separated teams for transferred players
5. **Coverage gaps:** Understat only has players with minutes; TM has full squads including reserves
