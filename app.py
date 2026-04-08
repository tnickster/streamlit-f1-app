import streamlit as st
import pandas as pd
import json
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit.components.v1 as components

st.set_page_config(
    page_title="F1 Race Replay",
    page_icon="🏎️",
    layout="wide"
)

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=["https://www.googleapis.com/auth/bigquery"]
)
client = bigquery.Client(credentials=credentials, project="openf1-pipeline")

@st.cache_data(ttl=3600)
def get_meetings():
    query = """
        SELECT DISTINCT meeting_key, meeting_name, country_name, year
        FROM `openf1-pipeline.raw.meetings`
        ORDER BY year DESC, meeting_key DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def get_track_outline(meeting_key):
    query = f"""
        WITH numbered AS (
            SELECT x, y,
                ROW_NUMBER() OVER (PARTITION BY driver_number ORDER BY date) as rn
            FROM `openf1-pipeline.marts.fct_race_replay`
            WHERE meeting_key = {meeting_key}
            AND driver_number = (
                SELECT MIN(driver_number)
                FROM `openf1-pipeline.marts.fct_race_replay`
                WHERE meeting_key = {meeting_key}
            )
        )
        SELECT x, y FROM numbered
        WHERE MOD(rn, 3) = 0
        ORDER BY rn
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def get_race_data(meeting_key):
    query = f"""
        WITH numbered AS (
            SELECT
                UNIX_MILLIS(TIMESTAMP(date)) as ts,
                x, y, driver_number, full_name, name_acronym, team_colour,
                ROW_NUMBER() OVER (PARTITION BY driver_number ORDER BY date) as rn
            FROM `openf1-pipeline.marts.fct_race_replay`
            WHERE meeting_key = {meeting_key}
        )
        SELECT ts, x, y, driver_number, full_name, name_acronym, team_colour
        FROM numbered
        WHERE MOD(rn, 5) = 0
        ORDER BY driver_number, ts
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def get_laps_data(meeting_key):
    query = f"""
        SELECT
            UNIX_MILLIS(TIMESTAMP(date_start)) as ts,
            lap_number, driver_number, lap_duration,
            full_name, name_acronym, team_colour
        FROM `openf1-pipeline.marts.fct_laps`
        WHERE meeting_key = {meeting_key}
        AND date_start IS NOT NULL
        ORDER BY driver_number, lap_number
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def get_starting_grid(meeting_key):
    query = f"""
        SELECT position, driver_number, full_name, name_acronym, team_colour
        FROM `openf1-pipeline.marts.fct_starting_grid`
        WHERE meeting_key = {meeting_key}
        ORDER BY position
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def get_car_data(meeting_key):
    query = f"""
        WITH numbered AS (
            SELECT
                UNIX_MILLIS(TIMESTAMP(date)) as ts,
                driver_number, rpm, speed, n_gear, throttle, brake, drs,
                ROW_NUMBER() OVER (PARTITION BY driver_number ORDER BY date) as rn
            FROM `openf1-pipeline.raw.car_data`
            WHERE meeting_key = {meeting_key}
        )
        SELECT ts, driver_number, rpm, speed, n_gear, throttle, brake, drs
        FROM numbered
        WHERE MOD(rn, 10) = 0
        ORDER BY driver_number, ts
    """
    return client.query(query).to_dataframe()

def format_color(team_colour):
    if not team_colour or str(team_colour) == 'nan':
        return '#FFFFFF'
    c = str(team_colour).strip()
    return f"#{c}" if not c.startswith('#') else c

# --- Sidebar ---
st.sidebar.title("🏎️ F1 Race Replay")

meetings_df = get_meetings()
if meetings_df.empty:
    st.error("No meetings data found.")
    st.stop()

meeting_options = {
    f"{row['year']} - {row['meeting_name']} ({row['country_name']})": row['meeting_key']
    for _, row in meetings_df.iterrows()
}

selected_meeting_label = st.sidebar.selectbox("Select Race", list(meeting_options.keys()))
selected_meeting_key = meeting_options[selected_meeting_label]

if st.sidebar.button("▶ Load Race"):
    st.session_state['loaded'] = True
    st.session_state['meeting_key'] = selected_meeting_key

if 'loaded' not in st.session_state:
    st.title("🏎️ F1 Race Replay")
    st.info("Select a race from the sidebar and click Load Race to begin.")
    st.stop()

meeting_key = st.session_state['meeting_key']

with st.spinner("Loading race data..."):
    location_df = get_race_data(meeting_key)
    track_df = get_track_outline(meeting_key)
    laps_df = get_laps_data(meeting_key)
    grid_df = get_starting_grid(meeting_key)
    car_df = get_car_data(meeting_key)

if location_df.empty:
    st.error("No location data available for this race.")
    st.stop()

# --- Prepare data for JS ---
drivers_info = {}
driver_positions = {}

for driver_num in location_df['driver_number'].unique():
    driver_data = location_df[location_df['driver_number'] == driver_num].copy()
    row = driver_data.iloc[0]
    color = format_color(row['team_colour'])
    drivers_info[int(driver_num)] = {
        'full_name': str(row['full_name']),
        'name_acronym': str(row['name_acronym']),
        'color': color
    }
    driver_positions[int(driver_num)] = [
        {'ts': int(r['ts']), 'x': float(r['x']), 'y': float(r['y'])}
        for _, r in driver_data.iterrows()
    ]

track_points = [
    {'x': float(r['x']), 'y': float(r['y'])}
    for _, r in track_df.iterrows()
]

driver_laps = {}
for driver_num in laps_df['driver_number'].unique():
    d = laps_df[laps_df['driver_number'] == driver_num].copy()
    driver_laps[int(driver_num)] = [
        {
            'ts': int(r['ts']) if not pd.isna(r['ts']) else 0,
            'lap_number': int(r['lap_number'])
        }
        for _, r in d.iterrows()
    ]

driver_car = {}
for driver_num in car_df['driver_number'].unique():
    d = car_df[car_df['driver_number'] == driver_num].copy()
    driver_car[int(driver_num)] = [
        {
            'ts': int(r['ts']),
            'rpm': int(r['rpm']) if not pd.isna(r['rpm']) else 0,
            'speed': int(r['speed']) if not pd.isna(r['speed']) else 0,
            'n_gear': int(r['n_gear']) if not pd.isna(r['n_gear']) else 0,
            'throttle': int(r['throttle']) if not pd.isna(r['throttle']) else 0,
            'brake': int(r['brake']) if not pd.isna(r['brake']) else 0,
            'drs': int(r['drs']) if not pd.isna(r['drs']) else 0
        }
        for _, r in d.iterrows()
    ]

grid_positions = {}
for _, row in grid_df.iterrows():
    grid_positions[int(row['driver_number'])] = int(row['position'])

all_x = [p['x'] for pts in driver_positions.values() for p in pts] + [p['x'] for p in track_points]
all_y = [p['y'] for pts in driver_positions.values() for p in pts] + [p['y'] for p in track_points]
all_ts = [p['ts'] for pts in driver_positions.values() for p in pts]

min_x = float(min(all_x))
max_x = float(max(all_x))
min_y = float(min(all_y))
max_y = float(max(all_y))
min_ts = int(min(all_ts))
max_ts = int(max(all_ts))

drivers_json = json.dumps(drivers_info)
positions_json = json.dumps(driver_positions)
track_json = json.dumps(track_points)
laps_json = json.dumps(driver_laps)
car_json = json.dumps(driver_car)
grid_json = json.dumps(grid_positions)

st.subheader(f"🏁 {selected_meeting_label}")

html = f"""
<style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ background: #1a1a2e; font-family: sans-serif; color: white; }}
    #wrapper {{ display: flex; gap: 12px; width: 100%; }}
    #left {{ flex: 1; min-width: 0; }}
    #right {{ width: 200px; flex-shrink: 0; overflow-y: auto; max-height: 600px; }}
    canvas {{ width: 100%; background: #16213e; border-radius: 8px; display: block; }}
    #controls {{ display: flex; align-items: center; gap: 12px; padding: 8px 0; }}
    button.ctrl {{
        background: #e10600; color: white; border: none;
        padding: 8px 16px; border-radius: 4px; cursor: pointer;
        font-size: 13px; font-weight: bold;
    }}
    button.ctrl:hover {{ background: #ff1801; }}
    #progress {{
        flex: 1; height: 6px; background: #333;
        border-radius: 3px; cursor: pointer;
    }}
    #progress-bar {{
        height: 100%; background: #e10600;
        border-radius: 3px; width: 0%;
        pointer-events: none;
    }}
    #time-label {{
        color: #aaa; font-size: 12px; font-family: monospace;
        min-width: 70px; text-align: right;
    }}
    .section-title {{
        font-size: 11px; color: #aaa; margin-bottom: 6px;
        text-transform: uppercase; letter-spacing: 1px;
    }}
    .driver-row {{
        display: flex; align-items: center; gap: 6px;
        padding: 5px 6px; margin-bottom: 3px;
        background: #16213e; border-radius: 5px;
        cursor: pointer;
        border-left: 3px solid transparent;
    }}
    .driver-row:hover {{ background: #1f2f4d; }}
    .driver-row.active {{ background: #1f2f4d; border-left-color: white; }}
    .pos {{ font-size: 10px; color: #888; width: 16px; text-align: center; flex-shrink: 0; }}
    .dot {{ width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }}
    .acronym {{ font-size: 11px; font-weight: bold; flex: 1; }}
    .lap-badge {{ font-size: 10px; color: #666; }}
    #telemetry-panel {{
        margin-top: 10px; padding: 8px;
        background: #16213e; border-radius: 6px;
    }}
    .tel-title {{
        font-size: 11px; color: #aaa; margin-bottom: 6px;
        text-transform: uppercase; letter-spacing: 1px;
    }}
    .tel-row {{
        display: flex; justify-content: space-between;
        padding: 3px 0; border-bottom: 1px solid #2a2a4a;
        font-size: 11px;
    }}
    .tel-row:last-child {{ border-bottom: none; }}
    .tel-label {{ color: #888; }}
    .tel-val {{ font-weight: bold; }}
    .drs-on {{ color: #00ff88; }}
    .drs-off {{ color: #ff4444; }}
</style>

<div id="wrapper">
    <div id="left">
        <canvas id="raceCanvas" height="480"></canvas>
        <div id="controls">
            <button class="ctrl" id="playBtn">&#9654; Play</button>
            <div id="progress">
                <div id="progress-bar"></div>
            </div>
            <span id="time-label">0:00</span>
        </div>
    </div>
    <div id="right">
        <div class="section-title">&#127942; Standings</div>
        <div id="standings-list"></div>
        <div id="telemetry-panel">
            <div class="tel-title" id="tel-name">Select a driver</div>
            <div class="tel-row"><span class="tel-label">Speed</span><span class="tel-val" id="t-speed">—</span></div>
            <div class="tel-row"><span class="tel-label">RPM</span><span class="tel-val" id="t-rpm">—</span></div>
            <div class="tel-row"><span class="tel-label">Gear</span><span class="tel-val" id="t-gear">—</span></div>
            <div class="tel-row"><span class="tel-label">Throttle</span><span class="tel-val" id="t-throttle">—</span></div>
            <div class="tel-row"><span class="tel-label">Brake</span><span class="tel-val" id="t-brake">—</span></div>
            <div class="tel-row"><span class="tel-label">DRS</span><span class="tel-val" id="t-drs">—</span></div>
        </div>
    </div>
</div>

<script>
    const driversInfo = {drivers_json};
    const driverPositions = {positions_json};
    const trackPoints = {track_json};
    const driverLaps = {laps_json};
    const driverCar = {car_json};
    const gridPositions = {grid_json};

    const minX = {min_x}, maxX = {max_x};
    const minY = {min_y}, maxY = {max_y};
    const minTs = {min_ts}, maxTs = {max_ts};
    const duration = maxTs - minTs;

    const canvas = document.getElementById('raceCanvas');
    const ctx = canvas.getContext('2d');
    canvas.width = canvas.parentElement.offsetWidth || 700;
    canvas.height = 480;
    const padding = 60;

    let selectedDriver = null;
    let currentTs = minTs;
    let playing = false;
    let lastRealTime = null;
    let lastReorderTs = minTs;
    const playbackSpeed = 1;

    const driverNums = Object.keys(driversInfo).map(Number);
    const standingsList = document.getElementById('standings-list');

    // Build standings rows ONCE
    driverNums.forEach(driverNum => {{
        const info = driversInfo[driverNum];
        const row = document.createElement('div');
        row.className = 'driver-row';
        row.id = 'row-' + driverNum;
        row.style.borderLeftColor = info.color;
        row.innerHTML = `
            <span class="pos" id="pos-${{driverNum}}">—</span>
            <span class="dot" style="background:${{info.color}}"></span>
            <span class="acronym">${{info.name_acronym}}</span>
            <span class="lap-badge" id="lap-${{driverNum}}">L1</span>
        `;
        row.addEventListener('click', () => {{
            selectedDriver = driverNum;
            document.querySelectorAll('.driver-row').forEach(r => r.classList.remove('active'));
            row.classList.add('active');
            updateTelemetry(driverNum, currentTs);
        }});
        standingsList.appendChild(row);
    }});

    function toCanvas(x, y) {{
        const cx = padding + (x - minX) / (maxX - minX) * (canvas.width - padding * 2);
        const cy = padding + (y - minY) / (maxY - minY) * (canvas.height - padding * 2);
        return [cx, cy];
    }}

    function binarySearch(pts, ts) {{
        if (!pts || pts.length === 0) return null;
        if (ts <= pts[0].ts) return [pts[0].x, pts[0].y];
        if (ts >= pts[pts.length-1].ts) return [pts[pts.length-1].x, pts[pts.length-1].y];
        let lo = 0, hi = pts.length - 1;
        while (lo < hi - 1) {{
            const mid = Math.floor((lo + hi) / 2);
            if (pts[mid].ts <= ts) lo = mid; else hi = mid;
        }}
        const t0 = pts[lo], t1 = pts[hi];
        const f = (ts - t0.ts) / (t1.ts - t0.ts);
        return [t0.x + (t1.x - t0.x) * f, t0.y + (t1.y - t0.y) * f];
    }}

    function getCurrentLapInfo(driverNum, ts) {{
        const laps = driverLaps[driverNum];
        let lap = 1;
        let lapStart = minTs;
        if (laps && laps.length > 0) {{
            for (const l of laps) {{
                if (l.ts <= ts) {{ lap = l.lap_number; lapStart = l.ts; }}
                else break;
            }}
        }}
        return {{ lap, lapStart }};
    }}

    function getCarTel(driverNum, ts) {{
        const car = driverCar[driverNum];
        if (!car || car.length === 0) return null;
        let result = car[0];
        for (const c of car) {{
            if (c.ts <= ts) result = c;
            else break;
        }}
        return result;
    }}

    function updateTelemetry(driverNum, ts) {{
        const info = driversInfo[driverNum];
        document.getElementById('tel-name').textContent = info.name_acronym + ' — Telemetry';
        const tel = getCarTel(driverNum, ts);
        if (!tel) return;
        document.getElementById('t-speed').textContent = tel.speed + ' km/h';
        document.getElementById('t-rpm').textContent = tel.rpm.toLocaleString();
        document.getElementById('t-gear').textContent = tel.n_gear;
        document.getElementById('t-throttle').textContent = tel.throttle + '%';
        document.getElementById('t-brake').textContent = tel.brake + '%';
        const drsOn = tel.drs >= 10;
        const drsEl = document.getElementById('t-drs');
        drsEl.textContent = drsOn ? 'ON' : 'OFF';
        drsEl.className = 'tel-val ' + (drsOn ? 'drs-on' : 'drs-off');
    }}

    function updateStandings(ts) {{
        const lapMap = {{}};
        const lapStartMap = {{}};

        driverNums.forEach(d => {{
            const info = getCurrentLapInfo(d, ts);
            lapMap[d] = info.lap;
            lapStartMap[d] = info.lapStart;
        }});

        const sorted = [...driverNums].sort((a, b) => {{
            if (lapMap[b] !== lapMap[a]) return lapMap[b] - lapMap[a];
            return lapStartMap[a] - lapStartMap[b];
        }});

        // Reorder DOM every 5 seconds of race time
        if (ts - lastReorderTs > 5000) {{
            lastReorderTs = ts;
            sorted.forEach(driverNum => {{
                const row = document.getElementById('row-' + driverNum);
                if (row) standingsList.appendChild(row);
            }});
        }}

        sorted.forEach((driverNum, idx) => {{
            const posEl = document.getElementById('pos-' + driverNum);
            const lapEl = document.getElementById('lap-' + driverNum);
            if (posEl) posEl.textContent = idx + 1;
            if (lapEl) lapEl.textContent = 'L' + lapMap[driverNum];
        }});
    }}

    function draw(ts) {{
        ctx.clearRect(0, 0, canvas.width, canvas.height);

        if (trackPoints.length > 1) {{
            ctx.beginPath();
            trackPoints.forEach((p, i) => {{
                const [cx, cy] = toCanvas(p.x, p.y);
                if (i === 0) ctx.moveTo(cx, cy); else ctx.lineTo(cx, cy);
            }});
            ctx.strokeStyle = '#555';
            ctx.lineWidth = 8;
            ctx.lineCap = 'round';
            ctx.lineJoin = 'round';
            ctx.stroke();
        }}

        driverNums.forEach(driverNum => {{
            const info = driversInfo[driverNum];
            const pos = binarySearch(driverPositions[driverNum], ts);
            if (!pos) return;
            const [cx, cy] = toCanvas(pos[0], pos[1]);
            const isSelected = selectedDriver === driverNum;

            ctx.beginPath();
            ctx.arc(cx, cy, isSelected ? 10 : 7, 0, Math.PI * 2);
            ctx.fillStyle = info.color;
            ctx.fill();
            ctx.strokeStyle = isSelected ? 'white' : 'rgba(255,255,255,0.4)';
            ctx.lineWidth = isSelected ? 2 : 1;
            ctx.stroke();

            ctx.fillStyle = 'white';
            ctx.font = isSelected ? 'bold 10px sans-serif' : 'bold 8px sans-serif';
            ctx.textAlign = 'center';
            ctx.fillText(info.name_acronym, cx, cy - 13);
        }});

        const pct = (ts - minTs) / duration * 100;
        document.getElementById('progress-bar').style.width = pct + '%';
        const elapsed = Math.floor((ts - minTs) / 1000);
        const mins = Math.floor(elapsed / 60);
        const secs = elapsed % 60;
        document.getElementById('time-label').textContent = mins + ':' + String(secs).padStart(2, '0');

        updateStandings(ts);
        if (selectedDriver !== null) updateTelemetry(selectedDriver, ts);
    }}

    function animate(realTime) {{
        if (!playing) return;
        if (lastRealTime !== null) {{
            const delta = realTime - lastRealTime;
            currentTs += delta * playbackSpeed;
            if (currentTs >= maxTs) {{
                currentTs = maxTs;
                playing = false;
                document.getElementById('playBtn').innerHTML = '&#9654; Play';
            }}
        }}
        lastRealTime = realTime;
        draw(currentTs);
        if (playing) requestAnimationFrame(animate);
    }}

    document.getElementById('playBtn').addEventListener('click', () => {{
        if (playing) {{
            playing = false;
            lastRealTime = null;
            document.getElementById('playBtn').innerHTML = '&#9654; Play';
        }} else {{
            if (currentTs >= maxTs) currentTs = minTs;
            playing = true;
            document.getElementById('playBtn').innerHTML = '&#9646;&#9646; Pause';
            requestAnimationFrame(animate);
        }}
    }});

    document.getElementById('progress').addEventListener('click', function(e) {{
        const rect = this.getBoundingClientRect();
        const pct = (e.clientX - rect.left) / rect.width;
        currentTs = minTs + pct * duration;
        draw(currentTs);
    }});

    draw(currentTs);
</script>
"""

components.html(html, height=700)