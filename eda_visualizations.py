"""
EDA Visualizations — Premier League 2020/21 Unified Player Dataset
Run: python eda_visualizations.py
Outputs: 7 PNG files in the same directory
"""

import json
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from collections import Counter

# ── Load data ──
data = []
with open("/mnt/user-data/uploads/unified_players.json", "r") as f:
    for line in f:
        line = line.strip()
        if line:
            data.append(json.loads(line))

print(f"Loaded {len(data)} players")

# ── Style setup ──
BG      = "#0f1117"
CARD    = "#1a1d27"
GRID    = "#2a2d37"
TEXT    = "#e0e0e0"
ACCENT1 = "#4fc3f7"   # light blue
ACCENT2 = "#81c784"   # green
ACCENT3 = "#ff8a65"   # orange
ACCENT4 = "#ce93d8"   # purple
ACCENT5 = "#fff176"   # yellow

plt.rcParams.update({
    "figure.facecolor": BG,
    "axes.facecolor": CARD,
    "axes.edgecolor": GRID,
    "axes.labelcolor": TEXT,
    "xtick.color": TEXT,
    "ytick.color": TEXT,
    "text.color": TEXT,
    "grid.color": GRID,
    "grid.alpha": 0.4,
    "font.family": "sans-serif",
    "font.size": 11,
})

OUT = "/mnt/user-data/outputs"

# ══════════════════════════════════════════════════════════════
# 1. PLAYER DISTRIBUTION BY POSITION
# ══════════════════════════════════════════════════════════════
pos_map = {"F": "Forward", "M": "Midfielder", "D": "Defender", "GK": "Goalkeeper", "S": "Sub/Other"}
positions = [pos_map.get(p.get("position_us", ""), p.get("position_us", "Unknown")) for p in data]
pos_counts = Counter(positions)
# Sort by count
labels, counts = zip(*sorted(pos_counts.items(), key=lambda x: -x[1]))
colors = [ACCENT1, ACCENT2, ACCENT3, ACCENT4, ACCENT5][:len(labels)]

fig, ax = plt.subplots(figsize=(8, 5))
bars = ax.barh(labels[::-1], counts[::-1], color=colors[::-1], edgecolor="none", height=0.6)
for bar, val in zip(bars, counts[::-1]):
    ax.text(bar.get_width() + 3, bar.get_y() + bar.get_height()/2, str(val),
            va="center", fontsize=12, fontweight="bold", color=TEXT)
ax.set_xlabel("Number of Players")
ax.set_title("Player Distribution by Position", fontsize=15, fontweight="bold", pad=15)
ax.set_xlim(0, max(counts) * 1.15)
ax.grid(axis="x", linestyle="--")
plt.tight_layout()
plt.savefig(f"{OUT}/eda_1_position_distribution.png", dpi=180, bbox_inches="tight")
plt.close()
print("✓ 1/7  Position distribution")

# ══════════════════════════════════════════════════════════════
# 2. xG DISTRIBUTION (HISTOGRAM)
# ══════════════════════════════════════════════════════════════
xg_vals = [p["xG"] for p in data if p.get("xG") is not None]

fig, ax = plt.subplots(figsize=(8, 5))
n, bins, patches = ax.hist(xg_vals, bins=30, color=ACCENT1, edgecolor=CARD, alpha=0.85)
ax.axvline(np.median(xg_vals), color=ACCENT3, linestyle="--", linewidth=2, label=f"Median: {np.median(xg_vals):.2f}")
ax.axvline(np.mean(xg_vals), color=ACCENT5, linestyle="--", linewidth=2, label=f"Mean: {np.mean(xg_vals):.2f}")
ax.set_xlabel("Expected Goals (xG)")
ax.set_ylabel("Number of Players")
ax.set_title("Distribution of Season xG Across Players", fontsize=15, fontweight="bold", pad=15)
ax.legend(facecolor=CARD, edgecolor=GRID, fontsize=10)
ax.grid(axis="y", linestyle="--")
plt.tight_layout()
plt.savefig(f"{OUT}/eda_2_xg_distribution.png", dpi=180, bbox_inches="tight")
plt.close()
print("✓ 2/7  xG distribution")

# ══════════════════════════════════════════════════════════════
# 3. GOALS VS xG SCATTER
# ══════════════════════════════════════════════════════════════
goals = [p["goals"] for p in data if p.get("goals") is not None and p.get("xG") is not None and p["goals"] > 0]
xg_scatter = [p["xG"] for p in data if p.get("goals") is not None and p.get("xG") is not None and p["goals"] > 0]
names_scatter = [p["player_name"] for p in data if p.get("goals") is not None and p.get("xG") is not None and p["goals"] > 0]

fig, ax = plt.subplots(figsize=(9, 7))
ax.scatter(xg_scatter, goals, c=ACCENT1, s=40, alpha=0.6, edgecolors="none")

# Diagonal reference
max_val = max(max(goals), max(xg_scatter)) + 2
ax.plot([0, max_val], [0, max_val], color=ACCENT3, linestyle="--", linewidth=1.5, alpha=0.7, label="Goals = xG (perfect efficiency)")

# Label top overperformers and underperformers
diffs = [(g - x, i) for i, (g, x) in enumerate(zip(goals, xg_scatter))]
diffs.sort(key=lambda x: x[0], reverse=True)
# Top 5 clinical
for _, idx in diffs[:5]:
    ax.annotate(names_scatter[idx], (xg_scatter[idx], goals[idx]),
                fontsize=8, color=ACCENT2, fontweight="bold",
                xytext=(5, 5), textcoords="offset points")
# Top 5 wasteful
for _, idx in diffs[-5:]:
    ax.annotate(names_scatter[idx], (xg_scatter[idx], goals[idx]),
                fontsize=8, color=ACCENT3, fontweight="bold",
                xytext=(5, -10), textcoords="offset points")

ax.set_xlabel("Expected Goals (xG)")
ax.set_ylabel("Actual Goals")
ax.set_title("Goals vs. Expected Goals — Clinical & Wasteful Finishers", fontsize=14, fontweight="bold", pad=15)
ax.legend(facecolor=CARD, edgecolor=GRID, fontsize=10, loc="upper left")
ax.grid(True, linestyle="--")
plt.tight_layout()
plt.savefig(f"{OUT}/eda_3_goals_vs_xg.png", dpi=180, bbox_inches="tight")
plt.close()
print("✓ 3/7  Goals vs xG scatter")

# ══════════════════════════════════════════════════════════════
# 4. FIFA OVERALL RATING DISTRIBUTION
# ══════════════════════════════════════════════════════════════
ratings = [p["overall"] for p in data if p.get("overall") is not None]

fig, ax = plt.subplots(figsize=(8, 5))
ax.hist(ratings, bins=20, color=ACCENT2, edgecolor=CARD, alpha=0.85)
ax.axvline(np.median(ratings), color=ACCENT3, linestyle="--", linewidth=2, label=f"Median: {np.median(ratings):.0f}")
ax.axvline(np.mean(ratings), color=ACCENT5, linestyle="--", linewidth=2, label=f"Mean: {np.mean(ratings):.1f}")
ax.set_xlabel("FIFA 21 Overall Rating")
ax.set_ylabel("Number of Players")
ax.set_title("FIFA Overall Rating Distribution (PL Players)", fontsize=15, fontweight="bold", pad=15)
ax.legend(facecolor=CARD, edgecolor=GRID, fontsize=10)
ax.grid(axis="y", linestyle="--")
plt.tight_layout()
plt.savefig(f"{OUT}/eda_4_fifa_rating_distribution.png", dpi=180, bbox_inches="tight")
plt.close()
print("✓ 4/7  FIFA rating distribution")

# ══════════════════════════════════════════════════════════════
# 5. CORRELATION HEATMAP
# ══════════════════════════════════════════════════════════════
corr_fields = ["xG", "xA", "goals", "assists", "overall", "value_eur", "shots", "key_passes", "minutes"]
corr_labels = ["xG", "xA", "Goals", "Assists", "FIFA OVR", "Value (€)", "Shots", "Key Passes", "Minutes"]

# Build matrix
matrix = []
for p in data:
    row = []
    valid = True
    for f in corr_fields:
        v = p.get(f)
        if v is None:
            valid = False
            break
        row.append(float(v))
    if valid:
        matrix.append(row)

matrix = np.array(matrix)
corr = np.corrcoef(matrix.T)

fig, ax = plt.subplots(figsize=(9, 7.5))
im = ax.imshow(corr, cmap="RdYlBu_r", vmin=-0.2, vmax=1, aspect="auto")
ax.set_xticks(range(len(corr_labels)))
ax.set_yticks(range(len(corr_labels)))
ax.set_xticklabels(corr_labels, rotation=45, ha="right", fontsize=10)
ax.set_yticklabels(corr_labels, fontsize=10)

# Annotate cells
for i in range(len(corr_labels)):
    for j in range(len(corr_labels)):
        color = "white" if abs(corr[i, j]) > 0.6 else TEXT
        ax.text(j, i, f"{corr[i,j]:.2f}", ha="center", va="center", fontsize=9, color=color, fontweight="bold")

ax.set_title("Correlation Matrix — Key Performance & Valuation Metrics", fontsize=14, fontweight="bold", pad=15)
plt.colorbar(im, ax=ax, shrink=0.8, label="Pearson Correlation")
plt.tight_layout()
plt.savefig(f"{OUT}/eda_5_correlation_heatmap.png", dpi=180, bbox_inches="tight")
plt.close()
print("✓ 5/7  Correlation heatmap")

# ══════════════════════════════════════════════════════════════
# 6. PLAYERS PER TEAM
# ══════════════════════════════════════════════════════════════
teams = Counter(p["team"] for p in data)
t_labels, t_counts = zip(*sorted(teams.items(), key=lambda x: -x[1]))

fig, ax = plt.subplots(figsize=(10, 6))
bar_colors = [ACCENT1 if c >= np.mean(t_counts) else ACCENT4 for c in t_counts]
bars = ax.bar(range(len(t_labels)), t_counts, color=bar_colors, edgecolor="none", width=0.7)
ax.set_xticks(range(len(t_labels)))
ax.set_xticklabels(t_labels, rotation=55, ha="right", fontsize=9)
for bar, val in zip(bars, t_counts):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3, str(val),
            ha="center", fontsize=9, fontweight="bold", color=TEXT)
ax.axhline(np.mean(t_counts), color=ACCENT3, linestyle="--", linewidth=1.5, alpha=0.7, label=f"Mean: {np.mean(t_counts):.1f}")
ax.set_ylabel("Number of Players")
ax.set_title("Squad Size by Club (Players with Appearances)", fontsize=14, fontweight="bold", pad=15)
ax.legend(facecolor=CARD, edgecolor=GRID, fontsize=10)
ax.grid(axis="y", linestyle="--")
plt.tight_layout()
plt.savefig(f"{OUT}/eda_6_players_per_team.png", dpi=180, bbox_inches="tight")
plt.close()
print("✓ 6/7  Players per team")

# ══════════════════════════════════════════════════════════════
# 7. AGE DISTRIBUTION
# ══════════════════════════════════════════════════════════════
ages = [int(p["age"]) for p in data if p.get("age") not in (None, "", " ") and str(p["age"]).isdigit()]

fig, ax = plt.subplots(figsize=(8, 5))
bins_age = range(min(ages), max(ages) + 2)
n, bins_out, patches = ax.hist(ages, bins=bins_age, color=ACCENT4, edgecolor=CARD, alpha=0.85, align="left")

# Color peak bins
peak = max(n)
for patch, count in zip(patches, n):
    if count >= peak * 0.85:
        patch.set_facecolor(ACCENT5)

ax.axvline(np.median(ages), color=ACCENT3, linestyle="--", linewidth=2, label=f"Median: {np.median(ages):.0f}")
ax.axvline(np.mean(ages), color=ACCENT1, linestyle="--", linewidth=2, label=f"Mean: {np.mean(ages):.1f}")
ax.set_xlabel("Age")
ax.set_ylabel("Number of Players")
ax.set_title("Age Distribution of Premier League Players (2020/21)", fontsize=14, fontweight="bold", pad=15)
ax.legend(facecolor=CARD, edgecolor=GRID, fontsize=10)
ax.grid(axis="y", linestyle="--")
plt.tight_layout()
plt.savefig(f"{OUT}/eda_7_age_distribution.png", dpi=180, bbox_inches="tight")
plt.close()
print("✓ 7/7  Age distribution")

print(f"\n{'='*50}")
print(f"All 7 EDA charts saved to {OUT}/")
print(f"{'='*50}")
