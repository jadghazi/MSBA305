from io import StringIO

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="Premier League xG Story Dashboard",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================
# Embedded data from query outputs
# =========================

Q1_CSV = """pos,team_name,games,points,gf,ga,gd,total_xg,xga,xg_diff,goals_vs_xg
1,Manchester City,38,86,83,32,51,77.71,30.62,47.09,5
2,Manchester United,38,74,73,44,29,63.17,41.92,21.25,10
3,Liverpool,38,69,68,42,26,72.21,47.30,24.91,-4
4,Chelsea,38,67,58,36,22,68.65,30.91,37.74,-11
5,Leicester,38,66,68,50,18,58.80,47.06,11.74,9
6,West Ham,38,65,62,47,15,60.34,49.86,10.48,2
7,Tottenham,38,62,68,45,23,56.68,52.55,4.13,11
8,Arsenal,38,61,55,39,16,52.25,43.23,9.02,3
9,Leeds,38,59,62,54,8,59.26,63.02,-3.76,3
10,Everton,38,59,47,48,-1,49.24,50.16,-0.92,-2
11,Aston Villa,38,55,55,46,9,56.72,53.26,3.46,-2
12,Newcastle United,38,45,46,62,-16,43.96,60.02,-16.06,2
13,Wolverhampton Wanderers,38,45,36,52,-16,38.62,52.19,-13.57,-3
14,Crystal Palace,38,44,41,66,-25,35.29,61.90,-26.61,6
15,Southampton,38,43,47,68,-21,45.29,58.55,-13.26,2
16,Brighton,38,41,40,46,-6,53.82,39.91,13.91,-14
17,Burnley,38,39,33,55,-22,38.13,59.06,-20.93,-5
18,Fulham,38,28,27,53,-26,41.05,57.64,-16.59,-14
19,West Bromwich Albion,38,26,35,76,-41,34.97,74.04,-39.07,0
20,Sheffield United,38,23,20,63,-43,33.16,66.11,-32.95,-13
"""

Q2_OVER_CSV = """player_name,team_name,goals,xg,goals_minus_xg,shots,conversion_rate_pct,games,minutes_played
Son Heung-Min,Tottenham,17,11.02,5.98,68,25.0,37,3139
Gareth Bale,Tottenham,11,5.80,5.20,38,28.9,20,909
James Maddison,Leicester,8,3.83,4.17,75,10.7,31,2123
Matheus Pereira,West Bromwich Albion,11,6.95,4.05,65,16.9,33,2594
Nicolas Pepe,Arsenal,10,6.00,4.00,47,21.3,29,1602
Jesse Lingard,West Ham,9,5.25,3.75,42,21.4,16,1434
Danny Ings,Southampton,12,8.28,3.72,57,21.1,29,2200
Stuart Dallas,Leeds,8,4.46,3.54,48,16.7,38,3412
Ilkay Gundogan,Manchester City,13,9.57,3.43,54,24.1,28,2033
Kelechi Iheanacho,Leicester,12,9.05,2.95,59,20.3,25,1452
"""

Q2_UNDER_CSV = """player_name,team_name,goals,xg,goals_minus_xg,shots,conversion_rate_pct,games,minutes_played
Timo Werner,Chelsea,6,13.43,-7.43,80,7.5,35,2605
Neal Maupay,Brighton,8,13.77,-5.77,71,11.3,33,2526
Jamie Vardy,Leicester,15,19.94,-4.94,82,18.3,34,2848
Michail Antonio,West Ham,10,14.14,-4.14,63,15.9,26,1993
Kevin De Bruyne,Manchester City,6,9.98,-3.98,81,7.4,25,2008
Roberto Firmino,Liverpool,9,12.86,-3.86,83,10.8,36,2882
Sadio Mane,Liverpool,11,14.83,-3.83,94,11.7,35,2805
Richarlison,Everton,7,10.67,-3.67,81,8.6,34,2883
Anthony Martial,Manchester United,4,7.41,-3.41,42,9.5,22,1494
Mbaye Diagne,West Bromwich Albion,3,6.25,-3.25,25,12.0,16,1205
"""

# Partial sample only, because source extract was truncated.
Q3A_CSV = """player_name,team_name,fifa_overall,fifa_shooting,fifa_finishing,goals,xg,xg_per_90,xa_per_90,minutes_played
Harry Kane,Tottenham,88,91.0,94.0,23,22.17,0.644,0.220,3097
Kevin De Bruyne,Manchester City,91,86.0,82.0,6,9.98,0.447,0.491,2008
Mohamed Salah,Liverpool,90,86.0,91.0,22,20.25,0.591,0.190,3085
James Rodriguez,Everton,82,86.0,84.0,6,3.36,0.168,0.272,1802
Pierre-Emerick Aubameyang,Arsenal,87,86.0,91.0,10,10.45,0.403,0.094,2333
Sadio Mane,Liverpool,90,85.0,90.0,11,14.83,0.476,0.250,2805
Timo Werner,Chelsea,85,85.0,88.0,6,13.43,0.464,0.230,2605
Jamie Vardy,Leicester,86,85.0,92.0,15,19.94,0.630,0.161,2848
Gareth Bale,Tottenham,83,84.0,81.0,11,5.80,0.574,0.182,909
Marcus Rashford,Manchester United,85,83.0,83.0,11,9.58,0.293,0.128,2941
Anthony Martial,Manchester United,84,83.0,85.0,4,7.41,0.446,0.158,1494
Bruno Fernandes,Manchester United,87,83.0,77.0,18,16.02,0.463,0.331,3117
Gabriel Jesus,Manchester City,83,82.0,85.0,9,9.65,0.423,0.156,2053
Alexandre Lacazette,Arsenal,83,82.0,83.0,13,12.03,0.558,0.103,1939
Paul Pogba,Manchester United,86,81.0,75.0,3,2.99,0.142,0.098,1893
Danny Ings,Southampton,80,81.0,85.0,12,8.28,0.339,0.095,2200
Raheem Sterling,Manchester City,88,81.0,85.0,10,12.05,0.427,0.235,2539
Kai Havertz,Chelsea,85,81.0,85.0,4,6.35,0.375,0.092,1523
Sebastien Haller,West Ham,81,80.0,84.0,3,2.55,0.246,0.109,933
Roberto Firmino,Liverpool,87,80.0,80.0,9,12.86,0.402,0.191,2882
Steven Bergwijn,Tottenham,83,79.0,77.0,1,2.09,0.155,0.060,1218
"""

Q3B_CSV = """fifa_tier,players,avg_goals,avg_xg,avg_xg_per_90,avg_xa_per_90,avg_value_millions
Elite (85+),32,5.4,6.18,0.217,0.139,50.16
Top (80-84),73,3.1,2.76,0.135,0.094,20.66
Average (70-74),42,3.1,2.96,0.132,0.082,4.70
Good (75-79),134,2.3,2.60,0.114,0.079,10.00
Below avg (<70),16,0.9,1.20,0.081,0.068,1.15
"""

# Partial extract only, because source extract was truncated.
Q4_CSV = """player_name,team_name,primary_position,role_bucket,fifa_rating,value_millions,adjusted_value_m,minutes_played,games,goals,assists,xg,xa,xg_plus_xa,xg_plus_xa_per_90,availability_factor,blended_value_score,role_value_rank
Patrick Bamford,Leeds,F S,Forward,71,2.8,5.0,3085,38,17,7,18.40,3.78,22.18,0.647,1.000,0.1294,1
Che Adams,Southampton,F S,Forward,74,7.5,7.5,2685,36,9,5,12.12,5.23,17.35,0.581,0.895,0.0734,2
Pedro Neto,Wolverhampton Wanderers,F M S,Forward,72,5.5,5.5,2570,31,5,6,5.50,5.74,11.23,0.393,0.857,0.0663,3
Michail Antonio,West Ham,F S,Forward,78,10.0,10.0,1993,26,10,5,14.14,3.19,17.33,0.783,0.664,0.0652,4
Christian Benteke,Crystal Palace,F S,Forward,72,3.1,5.0,1824,30,10,1,7.46,0.70,8.16,0.402,0.608,0.0646,5
David McGoldrick,Sheffield United,F M S,Forward,74,4.5,5.0,2409,35,8,1,7.61,1.12,8.72,0.326,0.803,0.0588,6
Callum Wilson,Newcastle United,F S,Forward,78,10.5,10.5,2079,26,12,5,13.59,3.05,16.64,0.720,0.693,0.0580,7
Jack Harrison,Leeds,M S,Midfielder,74,7.5,7.5,2871,36,8,8,6.25,7.52,13.78,0.432,0.957,0.0564,1
Ollie Watkins,Aston Villa,F,Forward,76,10.5,10.5,3330,37,14,5,16.28,5.31,21.59,0.584,1.000,0.0556,8
Neal Maupay,Brighton,F M S,Forward,77,12.0,12.0,2526,33,8,2,13.77,4.28,18.05,0.643,0.842,0.0494,9
Mateusz Klich,Leeds,M S,Midfielder,73,3.7,5.0,2404,35,4,5,3.00,3.65,6.65,0.249,0.801,0.0449,2
"""

Q5_CSV = """league_pos,team_name,w_d_l,points,total_goals,team_xg,goals_vs_xg,avg_fifa_rating,squad_value_m,points_per_m,avg_pace,avg_shooting,avg_passing,avg_defending,avg_height_cm,avg_intl_caps,nationalities
1,Manchester City,27-5-6,86,82,78.96,3,82.2,666.3,0.13,75.9,65.9,76.6,66.9,181.7,62.1,12
2,Manchester United,21-11-6,74,69,63.13,6,80.0,471.0,0.16,74.6,66.0,72.7,65.1,182.9,38.8,14
3,Liverpool,20-9-9,69,65,72.97,-8,80.0,767.7,0.09,75.8,64.1,73.2,65.1,182.4,57.7,16
4,Chelsea,19-10-9,67,57,72.57,-16,80.4,536.2,0.12,72.7,63.0,72.3,63.6,183.0,48.7,13
5,Leicester,20-6-12,66,64,58.22,6,77.3,318.2,0.21,70.0,61.7,68.5,63.4,181.7,40.1,16
6,West Ham,19-8-11,65,60,62.23,-2,77.0,198.4,0.33,69.6,60.8,67.8,63.9,182.2,33.7,15
7,Tottenham,18-8-12,62,65,56.59,8,81.3,461.4,0.13,74.1,65.6,74.2,68.5,183.5,66.3,13
8,Arsenal,18-7-13,61,59,56.72,2,77.6,330.6,0.18,74.1,62.3,68.3,59.3,182.2,38.1,15
9,Everton,17-8-13,59,47,51.69,-5,77.2,301.8,0.20,70.3,66.3,70.3,60.0,182.7,38.4,16
9,Leeds,18-5-15,59,60,57.69,2,71.5,91.8,0.64,72.8,56.2,63.8,55.2,181.8,20.4,13
11,Aston Villa,16-7-15,55,50,56.11,-6,73.5,145.6,0.38,71.4,60.4,66.7,58.1,182.7,32.1,12
12,Wolverhampton Wanderers,12-9-17,45,33,37.59,-5,76.4,209.0,0.22,72.1,60.1,68.9,59.7,182.0,31.9,14
12,Newcastle United,12-9-17,45,35,40.03,-5,76.0,217.3,0.21,69.9,61.0,65.9,59.9,181.3,28.3,15
14,Crystal Palace,12-8-18,44,38,35.14,3,75.6,154.3,0.29,63.2,59.4,64.7,63.6,185.0,28.7,11
15,Southampton,12-7-19,43,42,41.60,0,72.5,143.3,0.30,70.1,56.4,61.6,54.1,182.2,20.8,13
16,Brighton,9-14-15,41,39,53.01,-14,73.7,142.8,0.29,69.3,58.2,66.9,58.6,181.2,23.3,16
17,Burnley,10-9-19,39,31,37.61,-7,73.6,148.1,0.26,63.5,59.8,66.2,61.9,184.3,23.8,9
18,Fulham,5-13-20,28,24,35.25,-11,71.8,108.0,0.26,72.4,56.6,63.4,57.7,182.8,27.1,17
19,West Bromwich Albion,5-11-22,26,33,34.40,-1,71.9,85.2,0.31,68.8,55.0,62.8,56.9,184.7,27.5,14
20,Sheffield United,7-2-29,23,18,31.11,-13,73.9,140.6,0.16,67.2,55.0,64.1,63.0,182.4,17.2,9
"""


@st.cache_data
def load_data():
    q1 = pd.read_csv(StringIO(Q1_CSV))
    q2_over = pd.read_csv(StringIO(Q2_OVER_CSV))
    q2_under = pd.read_csv(StringIO(Q2_UNDER_CSV))
    q3a = pd.read_csv(StringIO(Q3A_CSV))
    q3b = pd.read_csv(StringIO(Q3B_CSV))
    q4 = pd.read_csv(StringIO(Q4_CSV))
    q5 = pd.read_csv(StringIO(Q5_CSV))

    q1["performance_label"] = q1["goals_vs_xg"].apply(
        lambda x: "Overperforming attack" if x > 0 else ("Underperforming attack" if x < 0 else "On expectation")
    )

    q5["efficiency_band"] = pd.cut(
        q5["points_per_m"],
        bins=[-1, 0.15, 0.25, 1],
        labels=["Low value", "Mid value", "High value"],
    )

    combined = pd.concat(
        [
            q2_over.assign(segment="Clinical finishers"),
            q2_under.assign(segment="Wasteful finishers"),
        ],
        ignore_index=True,
    )
    combined["minutes_90s"] = combined["minutes_played"] / 90
    combined["label"] = combined["player_name"] + " (" + combined["team_name"] + ")"
    q4["label"] = q4["player_name"] + " (" + q4["team_name"] + ")"

    return q1, q2_over, q2_under, combined, q3a, q3b, q4, q5


q1, q2_over, q2_under, q2_combined, q3a, q3b, q4, q5 = load_data()

# =========================
# Styling
# =========================

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.0rem; padding-bottom: 2rem;}
    .insight-card {
        border: 1px solid rgba(128,128,128,0.25);
        border-radius: 14px;
        padding: 1rem 1rem 0.8rem 1rem;
        background: rgba(250,250,250,0.02);
        margin-bottom: 0.8rem;
    }
    .small-note {
        font-size: 0.9rem;
        color: #8a8f98;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================
# Sidebar
# =========================

st.sidebar.title("Dashboard sections")
page = st.sidebar.radio(
    "Section",
    [
        "Executive summary",
        "1. League table vs xG",
        "2. Finishing efficiency",
        "3. FIFA ratings vs output",
        "4. Value-for-money players",
        "5. Team scouting report",
    ],
)

team_filter = st.sidebar.multiselect(
    "Optional team filter",
    sorted(q1["team_name"].unique().tolist()),
)

selected_teams = team_filter if team_filter else sorted(q1["team_name"].unique().tolist())

q1_f = q1[q1["team_name"].isin(selected_teams)].copy()
q5_f = q5[q5["team_name"].isin(selected_teams)].copy()
q2_f = q2_combined[q2_combined["team_name"].isin(selected_teams)].copy()
q4_f = q4[q4["team_name"].isin(selected_teams)].copy()
q3a_f = q3a[q3a["team_name"].isin(selected_teams)].copy()

# =========================
# Helpers
# =========================

def format_fig(fig, height=480):
    fig.update_layout(
        height=height,
        margin=dict(l=20, r=20, t=50, b=20),
        legend_title_text="",
        hovermode="closest",
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(150,150,150,0.15)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(150,150,150,0.15)")
    return fig


best_xg_team = q1.loc[q1["xg_diff"].idxmax()]
most_lucky_team = q1.loc[q1["goals_vs_xg"].idxmax()]
most_unlucky_team = q1.loc[q1["goals_vs_xg"].idxmin()]
most_clinical = q2_over.loc[q2_over["goals_minus_xg"].idxmax()]
most_wasteful = q2_under.loc[q2_under["goals_minus_xg"].idxmin()]
best_value_team = q5.loc[q5["points_per_m"].idxmax()]

# =========================
# Pages
# =========================

if page == "Executive summary":
    st.title("Premier League xG dashboard")
    st.caption("This dashboard uses the query outputs to compare league results with xG, finishing, player quality, and squad value.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Strongest xG profile", best_xg_team["team_name"], f"xG diff {best_xg_team['xg_diff']:.2f}")
    c2.metric("Top overperformer vs xG", most_clinical["player_name"], f"{most_clinical['goals_minus_xg']:+.2f} goals vs xG")
    c3.metric("Largest underperformance vs xG", most_wasteful["player_name"], f"{most_wasteful['goals_minus_xg']:+.2f} goals vs xG")
    c4.metric("Best points return per €m", best_value_team["team_name"], f"{best_value_team['points_per_m']:.2f} points / €m")

    st.markdown("### Key findings")
    left, right = st.columns([1.05, 1])

    with left:
        st.markdown(
            f"""
            <div class="insight-card">
            <b>1) Manchester City's title was backed by the underlying numbers.</b><br>
            They finished first and also recorded the best xG difference in the league. The larger gaps show up elsewhere: <b>{most_lucky_team['team_name']}</b> scored {int(most_lucky_team['goals_vs_xg'])} more goals than expected,
            while <b>{most_unlucky_team['team_name']}</b> scored {abs(int(most_unlucky_team['goals_vs_xg']))} fewer.
            </div>
            <div class="insight-card">
            <b>2) Finishing had a clear effect on outcomes.</b><br>
            <b>{most_clinical['player_name']}</b> was the biggest overperformer against xG in this sample at {most_clinical['goals_minus_xg']:+.2f},
            while <b>{most_wasteful['player_name']}</b> had the largest shortfall at {most_wasteful['goals_minus_xg']:+.2f}.
            </div>
            <div class="insight-card">
            <b>3) Spending mattered, but value varied sharply across clubs.</b><br>
            <b>{best_value_team['team_name']}</b> returned {best_value_team['points_per_m']:.2f} points per €m of squad value,
            which stands out against several more expensive teams. League position alone does not show how efficiently resources were used.
            </div>
            """,
            unsafe_allow_html=True,
        )

    with right:
        fig = px.scatter(
            q5,
            x="squad_value_m",
            y="points",
            size="points_per_m",
            color="points_per_m",
            hover_name="team_name",
            text="team_name",
            labels={"squad_value_m": "Squad value (€m)", "points": "League points", "points_per_m": "Points per €m"},
        )
        fig.update_traces(textposition="top center")
        st.plotly_chart(format_fig(fig, 520), use_container_width=True)

    st.markdown("### Supporting charts")
    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            q1.sort_values("goals_vs_xg"),
            x="goals_vs_xg",
            y="team_name",
            orientation="h",
            color="goals_vs_xg",
            labels={"goals_vs_xg": "Goals minus xG", "team_name": ""},
        )
        st.plotly_chart(format_fig(fig, 520), use_container_width=True)

    with col2:
        fig = px.bar(
            q2_combined.sort_values("goals_minus_xg"),
            x="goals_minus_xg",
            y="player_name",
            color="segment",
            orientation="h",
            hover_data=["team_name", "shots", "conversion_rate_pct", "minutes_played"],
            labels={"goals_minus_xg": "Goals minus xG", "player_name": ""},
        )
        st.plotly_chart(format_fig(fig, 520), use_container_width=True)

    st.markdown(
        '<div class="small-note">Query 3A and Query 4 are based on partial extracts.</div>',
        unsafe_allow_html=True,
    )

elif page == "1. League table vs xG":
    st.title("League table vs xG")
    st.markdown("This section compares league results with chance quality to show which teams were backed by their underlying numbers and which moved away from them.")

    c1, c2, c3 = st.columns(3)
    c1.metric("Highest points total", q1.loc[q1["points"].idxmax(), "team_name"], int(q1["points"].max()))
    c2.metric("Largest goal surplus vs xG", most_lucky_team["team_name"], f"{int(most_lucky_team['goals_vs_xg']):+d}")
    c3.metric("Largest goal deficit vs xG", most_unlucky_team["team_name"], f"{int(most_unlucky_team['goals_vs_xg']):+d}")

    left, right = st.columns(2)

    with left:
        fig = px.scatter(
            q1_f,
            x="xg_diff",
            y="points",
            color="goals_vs_xg",
            size="gf",
            text="team_name",
            hover_name="team_name",
            labels={"xg_diff": "xG difference", "points": "Points", "gf": "Goals scored", "goals_vs_xg": "Goals - xG"},
        )
        fig.update_traces(textposition="top center")
        st.plotly_chart(format_fig(fig, 540), use_container_width=True)
        st.caption("Teams in the upper-right combined strong chance difference with strong league results. Large color swings point to teams whose goal totals moved away from what xG would suggest.")

    with right:
        dumbbell_df = q1_f.sort_values("goals_vs_xg")
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=dumbbell_df["total_xg"],
                y=dumbbell_df["team_name"],
                mode="markers",
                name="Expected goals",
                marker=dict(size=10),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=dumbbell_df["gf"],
                y=dumbbell_df["team_name"],
                mode="markers",
                name="Actual goals",
                marker=dict(size=10),
            )
        )
        for _, row in dumbbell_df.iterrows():
            fig.add_shape(
                type="line",
                x0=row["total_xg"],
                x1=row["gf"],
                y0=row["team_name"],
                y1=row["team_name"],
                line=dict(width=2, color="rgba(160,160,160,0.45)"),
            )
        fig.update_layout(
            xaxis_title="Goals / xG",
            yaxis_title="",
            height=540,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption("A larger gap between expected goals and actual goals indicates a bigger finishing difference.")

    fig = px.bar(
        q1_f.sort_values("goals_vs_xg"),
        x="goals_vs_xg",
        y="team_name",
        orientation="h",
        color="performance_label",
        labels={"goals_vs_xg": "Goals minus xG", "team_name": ""},
    )
    st.plotly_chart(format_fig(fig, 620), use_container_width=True)

    st.markdown("### Interpretation")
    st.markdown(
        f"**{best_xg_team['team_name']}** combine the best xG difference with the highest points total, so their finish looks justified by performance. "
        f"**{most_lucky_team['team_name']}** scored {int(most_lucky_team['goals_vs_xg'])} more goals than expected, which suggests their final goal return was lifted by strong finishing. "
        f"**{most_unlucky_team['team_name']}** scored {abs(int(most_unlucky_team['goals_vs_xg']))} fewer than expected, so their process was better than the final goal total suggests."
    )

    with st.expander("View team table"):
        st.dataframe(q1_f.sort_values("pos").reset_index(drop=True), use_container_width=True, hide_index=True)

elif page == "2. Finishing efficiency":
    st.title("Finishing efficiency")
    st.markdown("Comparing goals with xG highlights which players finished above expectation and which left chances behind.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Most clinical", most_clinical["player_name"], f"{most_clinical['goals_minus_xg']:+.2f}")
    c2.metric("Highest conversion", q2_over.loc[q2_over["conversion_rate_pct"].idxmax(), "player_name"], f"{q2_over['conversion_rate_pct'].max():.1f}%")
    c3.metric("Most wasteful", most_wasteful["player_name"], f"{most_wasteful['goals_minus_xg']:+.2f}")
    c4.metric("Lowest conversion", q2_under.loc[q2_under["conversion_rate_pct"].idxmin(), "player_name"], f"{q2_under['conversion_rate_pct'].min():.1f}%")

    left, right = st.columns([1, 1])

    with left:
        fig = px.bar(
            q2_f.sort_values("goals_minus_xg"),
            x="goals_minus_xg",
            y="player_name",
            orientation="h",
            color="segment",
            hover_data=["team_name", "shots", "conversion_rate_pct", "minutes_played"],
            labels={"goals_minus_xg": "Goals minus xG", "player_name": ""},
        )
        st.plotly_chart(format_fig(fig, 620), use_container_width=True)

    with right:
        fig = px.scatter(
            q2_f,
            x="shots",
            y="conversion_rate_pct",
            size="minutes_played",
            color="goals_minus_xg",
            text="player_name",
            hover_name="label",
            labels={"shots": "Shots", "conversion_rate_pct": "Conversion rate (%)", "goals_minus_xg": "Goals - xG"},
        )
        fig.update_traces(textposition="top center")
        st.plotly_chart(format_fig(fig, 620), use_container_width=True)

    st.markdown("### Interpretation")
    spurs_delta = int(q1.loc[q1["team_name"] == "Tottenham", "goals_vs_xg"].iloc[0])
    st.markdown(
        f"- **{most_clinical['player_name']}** is the clearest positive finisher in the sample.\n"
        f"- **{most_wasteful['player_name']}** generated chances at an elite rate but converted far below expectation.\n"
        f"- Tottenham place two players in the overperformer top 10, which helps explain why their attack beat team xG by {spurs_delta} goals."
    )

    with st.expander("View player tables"):
        a, b = st.columns(2)
        with a:
            st.subheader("Top overperformers vs xG")
            st.dataframe(q2_over, use_container_width=True, hide_index=True)
        with b:
            st.subheader("Top underperformers vs xG")
            st.dataframe(q2_under, use_container_width=True, hide_index=True)

elif page == "3. FIFA ratings vs output":
    st.title("FIFA ratings and attacking output")
    st.markdown("This section checks whether higher FIFA ratings are associated with stronger attacking numbers, rather than assuming the ratings are automatically correct.")

    c1, c2, c3 = st.columns(3)
    c1.metric("Highest xG/90 in visible sample", q3a.loc[q3a["xg_per_90"].idxmax(), "player_name"], f"{q3a['xg_per_90'].max():.3f}")
    c2.metric("Highest FIFA shooting", q3a.loc[q3a["fifa_shooting"].idxmax(), "player_name"], f"{q3a['fifa_shooting'].max():.0f}")
    c3.metric("Average value of elite tier", "Elite (85+)", f"€{q3b.loc[q3b['fifa_tier'] == 'Elite (85+)', 'avg_value_millions'].iloc[0]:.2f}m")

    left, right = st.columns([1.1, 0.9])

    with left:
        fig = px.scatter(
            q3a_f,
            x="fifa_shooting",
            y="xg_per_90",
            size="minutes_played",
            color="xa_per_90",
            text="player_name",
            hover_name="player_name",
            labels={"fifa_shooting": "FIFA shooting rating", "xg_per_90": "xG per 90", "xa_per_90": "xA per 90"},
        )
        fig.update_traces(textposition="top center")
        st.plotly_chart(format_fig(fig, 580), use_container_width=True)
        st.caption("This scatter uses the visible rows from the partial Query 3A extract.")

    with right:
        metric_choice = st.radio(
            "Metric by rating tier",
            ["avg_xg_per_90", "avg_xa_per_90", "avg_value_millions", "avg_goals"],
            horizontal=True,
        )
        pretty = {
            "avg_xg_per_90": "Average xG per 90",
            "avg_xa_per_90": "Average xA per 90",
            "avg_value_millions": "Average value (€m)",
            "avg_goals": "Average goals",
        }
        fig = px.bar(
            q3b,
            x="fifa_tier",
            y=metric_choice,
            text="players",
            labels={"fifa_tier": "FIFA tier", metric_choice: pretty[metric_choice], "players": "Players"},
        )
        st.plotly_chart(format_fig(fig, 580), use_container_width=True)
        st.caption("The numbers above the bars show how many players are included in each rating tier.")

    st.markdown("### Interpretation")
    st.markdown(
        "- Elite players post the highest average attacking process and the highest market value.\n"
        "- The separation between Top (80–84) and Average/Good tiers is real, but not linear in price.\n"
        "- That gap is useful for recruitment: mid-tier players can preserve a fair amount of output at a fraction of elite cost."
    )

    with st.expander("View source tables"):
        st.subheader("Query 3A visible rows")
        st.dataframe(q3a_f, use_container_width=True, hide_index=True)
        st.subheader("Query 3B full tier summary")
        st.dataframe(q3b, use_container_width=True, hide_index=True)

elif page == "4. Value-for-money players":
    st.title("Value-for-money players")
    st.markdown("This score combines attacking output per 90, availability, and adjusted market value. It is meant to highlight efficient contributors within each role, not to rank the best players overall.")
    st.caption("This section is based on the visible top rows from Query 4 because the source extract is partial.")

    role_choice = st.selectbox("Role", sorted(q4_f["role_bucket"].unique().tolist()))
    q4_role = q4_f[q4_f["role_bucket"] == role_choice].sort_values("blended_value_score", ascending=False)

    c1, c2, c3 = st.columns(3)
    c1.metric("Highest visible value score", q4_f.loc[q4_f["blended_value_score"].idxmax(), "player_name"], f"{q4_f['blended_value_score'].max():.4f}")
    c2.metric(
        "Top midfielder in visible sample",
        q4_f[q4_f["role_bucket"] == "Midfielder"].sort_values("blended_value_score", ascending=False).iloc[0]["player_name"],
        f"{q4_f[q4_f['role_bucket'] == 'Midfielder']['blended_value_score'].max():.4f}",
    )
    c3.metric("Leeds players in visible top group", int((q4_f["team_name"] == "Leeds").sum()))

    left, right = st.columns([0.9, 1.1])

    with left:
        fig = px.bar(
            q4_role.sort_values("blended_value_score"),
            x="blended_value_score",
            y="player_name",
            orientation="h",
            color="team_name",
            hover_data=["value_millions", "xg_plus_xa_per_90", "availability_factor", "role_value_rank"],
            labels={"blended_value_score": "Blended value score", "player_name": ""},
        )
        st.plotly_chart(format_fig(fig, 560), use_container_width=True)

    with right:
        fig = px.scatter(
            q4_f,
            x="adjusted_value_m",
            y="xg_plus_xa_per_90",
            size="availability_factor",
            color="role_bucket",
            text="player_name",
            hover_name="label",
            labels={"adjusted_value_m": "Adjusted value (€m)", "xg_plus_xa_per_90": "xG + xA per 90", "availability_factor": "Availability"},
        )
        fig.update_traces(textposition="top center")
        st.plotly_chart(format_fig(fig, 560), use_container_width=True)

    st.markdown("### Interpretation")
    st.markdown(
        "- The top of the visible list is dominated by cheaper forwards with strong output volume.\n"
        "- Leeds appear repeatedly, which aligns with the club's strong team-level value efficiency.\n"
        "- The score rewards availability, so part-time impact players are naturally discounted against durable starters."
    )

    with st.expander("View visible Query 4 extract"):
        st.dataframe(q4_f.sort_values("blended_value_score", ascending=False), use_container_width=True, hide_index=True)

elif page == "5. Team scouting report":
    st.title("Team scouting report")
    st.markdown("This section combines league results, squad value, FIFA profile, and physical profile to compare how clubs were built and how efficiently they performed.")

    default_compare = [t for t in ["Manchester City", "Leeds", "Brighton"] if t in q5_f["team_name"].tolist()]
    compare_teams = st.multiselect(
        "Clubs to compare in the radar chart",
        sorted(q5_f["team_name"].tolist()),
        default=default_compare if default_compare else q5_f["team_name"].head(3).tolist(),
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Highest points total", q5_f.loc[q5_f["points"].idxmax(), "team_name"], int(q5_f["points"].max()))
    c2.metric("Best points return per €m", q5_f.loc[q5_f["points_per_m"].idxmax(), "team_name"], f"{q5_f['points_per_m'].max():.2f}")
    c3.metric("Highest squad value", q5_f.loc[q5_f["squad_value_m"].idxmax(), "team_name"], f"€{q5_f['squad_value_m'].max():.1f}m")
    c4.metric("Largest goal deficit vs xG", q5_f.loc[q5_f["goals_vs_xg"].idxmin(), "team_name"], int(q5_f["goals_vs_xg"].min()))

    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        fig = px.scatter(
            q5_f,
            x="squad_value_m",
            y="points",
            size="points_per_m",
            color="efficiency_band",
            text="team_name",
            hover_name="team_name",
            labels={"squad_value_m": "Squad value (€m)", "points": "Points"},
        )
        fig.update_traces(textposition="top center")
        st.plotly_chart(format_fig(fig, 560), use_container_width=True)

    with row1_col2:
        fig = px.bar(
            q5_f.sort_values("points_per_m"),
            x="points_per_m",
            y="team_name",
            orientation="h",
            color="points_per_m",
            hover_data=["points", "squad_value_m", "avg_fifa_rating"],
            labels={"points_per_m": "Points per €m", "team_name": ""},
        )
        st.plotly_chart(format_fig(fig, 560), use_container_width=True)

    radar_metrics = ["avg_pace", "avg_shooting", "avg_passing", "avg_defending", "avg_height_cm", "avg_intl_caps"]
    radar_df = q5_f[q5_f["team_name"].isin(compare_teams)].copy()

    if not radar_df.empty:
        scaled = radar_df.set_index("team_name")[radar_metrics].copy()
        scaled = (scaled - scaled.min()) / (scaled.max() - scaled.min()).replace(0, 1)

        fig = go.Figure()
        categories = ["Pace", "Shooting", "Passing", "Defending", "Height", "Intl caps"]

        for team in scaled.index:
            values = scaled.loc[team].tolist()
            fig.add_trace(
                go.Scatterpolar(
                    r=values + [values[0]],
                    theta=categories + [categories[0]],
                    fill="toself",
                    name=team,
                )
            )

        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            height=560,
            margin=dict(l=20, r=20, t=50, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Radar values are scaled from 0 to 1 within the clubs currently selected.")

    st.markdown("### Interpretation")
    st.markdown(
        f"- **{best_value_team['team_name']}** is the clearest efficiency outlier: modest squad cost, strong points return.\n"
        f"- **Brighton** profile as the classic under-rewarded side: decent process, poor finishing, and only 41 points despite a positive xG difference.\n"
        f"- **Liverpool** and **Chelsea** both look expensive relative to output in this season snapshot, especially when compared with their chance creation."
    )

    with st.expander("View full team table"):
        st.dataframe(q5_f.sort_values(["league_pos", "team_name"]), use_container_width=True, hide_index=True)