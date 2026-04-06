"""
Streamlit UI for browsing ``ingestion_runs`` (Phase 7).

Run from repo root::

    pip install -r requirements-dashboard.txt
    streamlit run scripts/dashboard_runs.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import streamlit as st

from metadata.run_queries import list_ingestion_runs, parse_iso_datetime

st.set_page_config(page_title="IngestFlow runs", layout="wide")
st.title("IngestFlow — ingestion runs")

with st.sidebar:
    st.header("Connection & filters")
    db_path = st.text_input(
        "DuckDB file",
        value="warehouse.duckdb",
        help="Path to the DuckDB file (same as `runs list --db`).",
    )
    limit = st.slider("Max rows", 1, 500, 50)
    status = st.text_input("Status (optional)", placeholder="success")
    config_contains = st.text_input("Config path contains (optional)", "")
    since_s = st.text_input("Since — finished_at ≥ (ISO date, optional)", "")
    until_s = st.text_input("Until — finished_at ≤ (ISO date, optional)", "")

p = Path(db_path).expanduser()
if not p.is_file():
    st.warning(f"Database file not found: {p.resolve()}")
    st.info("Run a pipeline first, or point the sidebar to your `target.db_path` file.")
    st.stop()

since = until = None
if since_s.strip():
    try:
        since = parse_iso_datetime(since_s)
    except ValueError:
        st.error(f"Invalid --since value: {since_s!r}")
        st.stop()
if until_s.strip():
    try:
        until = parse_iso_datetime(until_s)
    except ValueError:
        st.error(f"Invalid --until value: {until_s!r}")
        st.stop()

try:
    df = list_ingestion_runs(
        str(p.resolve()),
        limit=limit,
        status=status.strip() or None,
        config_path_contains=config_contains.strip() or None,
        since=since,
        until=until,
    )
except Exception as e:
    st.error(f"Query failed: {e}")
    st.stop()

if df.empty:
    st.info("No rows match the filters (or `ingestion_runs` is empty).")
else:
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(f"{len(df)} row(s) shown (limit {limit}).")
