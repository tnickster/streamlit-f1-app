import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import time
from google.oauth2 import service_account
from google.cloud import bigquery

st.set_page_config(
    page_title="F1 Race Replay",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  .main { background-color: #0f0f0f; }
  .stApp { background-color: #0f0f0f; color: #f0f0f0; }
  .metric-card {
    background: #1a1a1a;
    border: 1px solid #333;
    border-radius: 8px;
    padding: 10px 14px;
    margin: 4px 0;
  }
  .driver-name { font-size: 13px; font-weight: 600; }
  .driver-detail { font-size: 11px; color: #888; }
  h1, h2, h3 { color: #f0f0f0; }
  .stSelectbox label, .stSlider label { color: #ccc; }
</style>
""", unsafe_allow_html=True)

# ── BigQuery auth ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=creds, project=st.secrets["gcp_service_account"]["project_id"])

@st.cache_data(ttl=3600)
def load_sessions():
    client = get_bq_client()
    query = """
        SELECT DISTINCT
            meeting_key,
            session_key,
            MIN(date) AS session_start
        FROM `openf1-pipeline.marts.fct_race_replay`
        GROUP BY meeting_key, session_key
        ORDER BY session_start DESC
        LIMIT 20
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_replay_data(session_key: int):
    client = get_bq_client()
    query = f"""
        SELECT
            date,
            x, y,
            driver_number,
            full_name,
            name_acronym,
            team_name,
            team_colour,
            session_key,
            meeting_key
        FROM `openf1-pipeline.marts.fct_race_replay`
        WHERE session_key = {session_key}
        ORDER BY date
    """
    df = client.query(query).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])
    return df

@st.cache_data(ttl=3600)
def load_lap_data(session_key: int):
    client = get_bq_client()
    query = f"""
        SELECT
            driver_number,
            lap_number,
            lap_duration,
            date_start
        FROM `openf1-pipeline.marts.fct_laps`
        WHERE session_key = {session_key}
        ORDER BY date_start
    """
    return client.query(query).to_dataframe()

def hex_to_rgb(hex_colour):
    hex_colour = hex_colour.lstrip("#")
    if len(hex_colour) != 6:
        return "rgb(128,128,128)"
    r, g, b = int(hex_colour[0:2], 16), int(hex_colour[2:4], 16), int(hex_colour[4:6], 16)
    return f"rgb({r},{g},{b})"

def get_lap_number(driver_number, timestamp, lap_df):
    if lap_df is None or lap_df.empty:
        return "—"
    d = lap_df[lap_df["driver_number"] == driver_number].copy()
    d["date_start"] = pd.to_datetime(d["date_start"])
    past = d[d["date_start"] <= timestamp]
    if past.empty:
        return 1
    return int(past["lap_number"].max())

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏎️ F1 Race Replay")
    st.markdown("---")

    try:
        sessions_df = load_sessions()
    except Exception as e:
        st.error(f"BigQuery connection failed: {e}")
        st.stop()

    if sessions_df.empty:
        st.warning("No sessions found in fct_race_replay.")
        st.stop()

    session_labels = {
        row["session_key"]: f"Session {row['session_key']} (Meeting {row['meeting_key']})"
        for _, row in sessions_df.iterrows()
    }
    selected_session = st.selectbox(
        "Session",
        options=list(session_labels.keys()),
        format_func=lambda k: session_labels[k],
    )

    st.markdown("---")
    st.markdown("### Playback")
    speed = st.select_slider(
        "Speed",
        options=[1, 2, 5, 10, 20, 50],
        value=10,
        help="How many seconds of race data to advance per frame",
    )
    frame_delay = st.slider("Frame delay (s)", 0.05, 0.5, 0.1, step=0.05)

    st.markdown("---")
    play_col, stop_col = st.columns(2)
    play_btn = play_col.button("▶ Play", use_container_width=True)
    stop_btn = stop_col.button("⏹ Stop", use_container_width=True)

    st.markdown("---")
    st.caption("Data: OpenF1 API  •  Pipeline: BigQuery + dbt")

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading session data…"):
    try:
        df = load_replay_data(selected_session)
        lap_df = load_lap_data(selected_session)
    except Exception as e:
        st.error(f"Failed to load session {selected_session}: {e}")
        st.stop()

if df.empty:
    st.warning("No location data for this session.")
    st.stop()

# ── Driver info ───────────────────────────────────────────────────────────────
drivers = df.groupby("driver_number").first()[
    ["full_name", "name_acronym", "team_name", "team_colour"]
].reset_index()

colour_map = {
    row["driver_number"]: f"#{row['team_colour']}" if not str(row["team_colour"]).startswith("#") else row["team_colour"]
    for _, row in drivers.iterrows()
}

# ── Time range ────────────────────────────────────────────────────────────────
t_min = df["date"].min()
t_max = df["date"].max()
timestamps = sorted(df["date"].unique())

# ── Session state ─────────────────────────────────────────────────────────────
if "playing" not in st.session_state:
    st.session_state.playing = False
if "frame_idx" not in st.session_state:
    st.session_state.frame_idx = 0

if play_btn:
    st.session_state.playing = True
if stop_btn:
    st.session_state.playing = False

# ── Layout ────────────────────────────────────────────────────────────────────
col_map, col_info = st.columns([3, 1])

with col_map:
    st.markdown(f"#### Session {selected_session} — Track Map")
    chart_placeholder = st.empty()

with col_info:
    st.markdown("#### Drivers")
    info_placeholder = st.empty()

scrubber = st.empty()

# ── Build a single frame ──────────────────────────────────────────────────────
def build_frame(frame_idx):
    t = pd.Timestamp(timestamps[frame_idx])
    # Get latest position for each driver up to time t
    snapshot = (
        df[df["date"] <= t]
        .sort_values("date")
        .groupby("driver_number")
        .last()
        .reset_index()
    )

    # Track path (full session faintly)
    fig = go.Figure()

    # Faint full track outline
    for drv_num, grp in df.groupby("driver_number"):
        colour = colour_map.get(drv_num, "#888888")
        fig.add_trace(go.Scatter(
            x=grp["x"], y=grp["y"],
            mode="lines",
            line=dict(color=colour, width=0.3),
            opacity=0.08,
            showlegend=False,
            hoverinfo="skip",
        ))

    # Driver markers
    for _, row in snapshot.iterrows():
        drv = row["driver_number"]
        colour = colour_map.get(drv, "#888888")
        fig.add_trace(go.Scatter(
            x=[row["x"]], y=[row["y"]],
            mode="markers+text",
            marker=dict(size=14, color=colour, line=dict(color="white", width=1.5)),
            text=[row["name_acronym"]],
            textposition="top center",
            textfont=dict(size=9, color="white"),
            name=row["full_name"],
            showlegend=False,
            hovertemplate=f"<b>{row['full_name']}</b><br>{row['team_name']}<extra></extra>",
        ))

    elapsed = (t - t_min).total_seconds()
    mins, secs = divmod(int(elapsed), 60)

    fig.update_layout(
        paper_bgcolor="#0f0f0f",
        plot_bgcolor="#0f0f0f",
        font=dict(color="#f0f0f0"),
        margin=dict(l=10, r=10, t=40, b=10),
        title=dict(
            text=f"⏱ {mins:02d}:{secs:02d}",
            font=dict(size=16, color="#f0f0f0"),
            x=0.02,
        ),
        xaxis=dict(visible=False, scaleanchor="y", scaleratio=1),
        yaxis=dict(visible=False),
        height=580,
    )
    return fig, snapshot, t

def build_driver_cards(snapshot, t):
    cards_html = ""
    for _, row in snapshot.iterrows():
        drv = row["driver_number"]
        colour = colour_map.get(drv, "#888888")
        lap = get_lap_number(drv, t, lap_df)
        cards_html += f"""
        <div class="metric-card" style="border-left: 3px solid {colour};">
            <div class="driver-name" style="color:{colour}">{row['name_acronym']} &nbsp;<span style="color:#ccc;font-weight:400">{row['full_name']}</span></div>
            <div class="driver-detail">{row['team_name']} &nbsp;·&nbsp; Lap {lap}</div>
        </div>
        """
    return cards_html

# ── Initial render ────────────────────────────────────────────────────────────
fig, snapshot, t = build_frame(st.session_state.frame_idx)
chart_placeholder.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
info_placeholder.markdown(build_driver_cards(snapshot, t), unsafe_allow_html=True)
scrubber.slider(
    "Timeline",
    min_value=0,
    max_value=len(timestamps) - 1,
    value=st.session_state.frame_idx,
    key="scrubber_display",
    disabled=True,
)

# ── Animation loop ────────────────────────────────────────────────────────────
if st.session_state.playing:
    n = len(timestamps)
    step = max(1, speed)  # advance `speed` seconds worth of frames per tick

    while st.session_state.playing and st.session_state.frame_idx < n - 1:
        st.session_state.frame_idx = min(st.session_state.frame_idx + step, n - 1)

        fig, snapshot, t = build_frame(st.session_state.frame_idx)
        chart_placeholder.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        info_placeholder.markdown(build_driver_cards(snapshot, t), unsafe_allow_html=True)
        scrubber.slider(
            "Timeline",
            min_value=0,
            max_value=n - 1,
            value=st.session_state.frame_idx,
            key=f"scrubber_{st.session_state.frame_idx}",
            disabled=True,
        )
        time.sleep(frame_delay)

    st.session_state.playing = False
    st.rerun()