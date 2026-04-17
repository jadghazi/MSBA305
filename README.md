[README.md](https://github.com/user-attachments/files/26826938/README.md)
# Premier League 2020/21 — Analytics Pipeline

**MSBA 305 — Data Processing Framework | Spring 2025/2026**
**American University of Beirut | Dr. Ahmad El-Hajj**

**Team:** Jad Ghazi, Haytham Duwaji, Sadek Sadek, Mohamad Ardroumli, Borhane Abdul Samad

---

## Project Overview

A complete data pipeline for football analytics, built from the perspective of a sports consulting firm. The pipeline ingests data from **3 external sources** in **2 formats**, cleans and integrates them via fuzzy matching, loads into a **PostgreSQL** relational database (12 tables), and delivers insights through **5 analytical SQL queries** and an **interactive Streamlit dashboard**.

**Domain:** Premier League 2020/21 season
**Focus:** Team and player performance analysis using expected goals (xG), FIFA ratings, and transfer valuations

### Pipeline Architecture

```
Transfermarkt (scraping) ──┐
Understat (HTTP API)   ────┼── 7 raw JSON files ── cleaning notebook ── 8 cleaned files ── PostgreSQL (12 tables) ── SQL queries ── Streamlit dashboard
Kaggle FIFA 21 (CSV)   ───┘
```

---

## Repository Structure

```
├── scraping_and_datasets/              # Raw data & ingestion scripts
│   ├── ingest_transfermarkt.py         # Crawlee/Playwright scraper wrapper
│   ├── ingest_understat.py             # Understat async API collector
│   ├── ingest_kaggle_fifa.py           # FIFA 21 CSV → filtered JSON
│   ├── premier_league_clubs_2020_2021.json     # TM raw: 20 clubs
│   ├── premier_league_players_2020_2021.json   # TM raw: 783 players
│   ├── premier_league_games_2020_2021.json     # TM raw: 380 matches
│   ├── understat_players_2020_2021.json        # US raw: 524 players
│   ├── understat_teams_2020_2021.json          # US raw: 532 team-player records
│   ├── understat_matches_2020_2021.json        # US raw: 380 matches
│   └── fifa21_pl_players.json                  # FIFA raw: 654 PL players
│
├── postgre_integration/                # Database setup & queries
│   ├── create_schema.sql               # CREATE TABLE for all 12 tables (with FKs, indexes)
│   ├── load_into_postgres.py           # Reads cleaned JSON → inserts into PostgreSQL
│   └── queries_outputs          # 5 queries and their outputs with increasing complexity ran in SQLshell
│
├── cleaning_transformation_pipeline.ipynb  # Full cleaning notebook (Jupyter)
├── eda_visualizations.py               # 7 EDA charts (matplotlib)
└── README.md                           # This file
```

---

## Data Sources

| Source | Type | Collection Method | Records | Key Data |
|--------|------|-------------------|---------|----------|
| **Transfermarkt** | Web scraping | `dcaribou/transfermarkt-scraper` (Crawlee + Playwright) | 20 clubs, 783 players, 380 matches | Profiles, match events, stadium, referee |
| **Understat** | HTTP API | `understat` Python library | 524 players, 380 matches | xG, xA, npxG, xGChain, xGBuildup, shots |
| **Kaggle FIFA 21** | CSV download | `stefanoleone992/fifa-21-complete-player-dataset` | 654 PL players (filtered from 18,944) | Overall rating, potential, value_eur, 6 primary + 29 sub-attributes |

---

## Database Schema

**12 tables** organized into 4 layers:

- **Core** — `teams` (20), `players` (524), `matches` (380)
- **Transfermarkt** — `tm_player_profiles`, `tm_match_details`, `tm_match_events`
- **Understat** — `us_player_stats`, `us_match_stats`
- **FIFA** — `fifa_player_attributes`
- **Crosswalks** — `player_source_map`, `match_source_map`, `team_source_map`

All foreign keys enforced. Crosswalk tables include fuzzy match confidence scores for lineage tracking.

---

## Analytical Queries

| # | Question | Complexity | Sources |
|---|----------|------------|---------|
| Q1 | Are the league standings deserved based on xG? | Simple | US + Core |
| Q2 | Who are the most clinical and wasteful finishers? | Medium | US + Core |
| Q3 | Do FIFA ratings predict real xG output? | Medium-Complex | US + FIFA + Core |
| Q4 | Best value players by position (role-normalized)? | Complex | US + FIFA + Core |
| Q5 | Complete team scouting report across all dimensions | Advanced | All 3 + Core |

---

## Setup Instructions

### Prerequisites

- **Python 3.10+**
- **PostgreSQL 14+** installed and running
- **Node.js 18+** (only if re-running Transfermarkt scraper)

### 1. Clone the repository

```bash
git clone https://github.com/jadghazi/MSBA305-PL-Analytics.git
cd MSBA305-PL-Analytics
```

### 2. Install Python dependencies

```bash
pip install pandas numpy thefuzz psycopg2-binary matplotlib streamlit plotly
```

For the Transfermarkt scraper (optional — raw data already included):
```bash
pip install crawlee[playwright]
playwright install chromium
```

For the Understat scraper (optional — raw data already included):
```bash
pip install understat aiohttp
```

### 3. Run the cleaning notebook

Open `cleaning_transformation_pipeline.ipynb` in Jupyter and run all cells. This reads the 7 raw files from `scraping_and_datasets/` and outputs 8 cleaned JSON files to `data/cleaned/`.

```bash
jupyter notebook cleaning_transformation_pipeline.ipynb
```

### 4. Create the PostgreSQL database

```bash
psql -U postgres -c "CREATE DATABASE pl_analytics;"
psql -U postgres -d pl_analytics -f postgre_integration/create_schema.sql
```

### 5. Load cleaned data into PostgreSQL

Update the database connection string in `load_into_postgres.py` if needed, then run:

```bash
python postgre_integration/load_into_postgres.py
```

### 6. Verify the load

```bash
psql -U postgres -d pl_analytics -f postgre_integration/health_check.sql
```

All 8 checks should pass (row counts, team verification, FK integrity, null checks).

### 7. Run analytical queries

```bash
psql -U postgres -d pl_analytics -f postgre_integration/analytical_queries.sql
```

### 8. Launch the dashboard

```bash
streamlit run dashboard.py
```

Opens at `http://localhost:8501` with 5 interactive tabs.

### 9. Generate EDA charts (optional)

```bash
python eda_visualizations.py
```

Outputs 7 PNG files to the working directory.

---

## Key Findings

- **Manchester City** won the league deservedly — best actual and expected goal difference
- **Brighton** was the most unlucky team: xG difference ranked 6th but they finished 16th (-14 goals vs xG)
- **Son Heung-Min** was the season's most clinical finisher (+5.98 goals above xG)
- **Timo Werner** was the most wasteful (-7.43 below xG on 80 shots, 7.5% conversion)
- **Leeds** achieved the best efficiency: 0.64 points per €M invested (7x Man City's ratio)
- **FIFA ratings** correlate with real xG output, but mid-range tiers offer 61% of elite output at 9% of the cost

---

## Cross-Source Match Rates

| Source Pair | Match Rate |
|-------------|------------|
| Understat ↔ Transfermarkt (players) | 89.0% |
| Understat ↔ FIFA 21 (players) | 83.6% |
| Understat ↔ Transfermarkt (matches) | 100% |

Fuzzy matching via `thefuzz` library (Levenshtein distance, threshold 75, constrained within same team). FIFA match rate improved from 70.8% → 83.6% by dual-indexing `short_name` and `long_name`.

---

## AI Usage


## License

Academic project — not for commercial use. Data sources retain their original licenses:
- Transfermarkt: scraped under academic fair use
- Understat: public API, no license restrictions
- FIFA 21 Kaggle: CC BY-NC-SA 4.0
