#!/usr/bin/env python3
"""
MD Case Scraper — Streamlit App
Run with: .venv/bin/streamlit run app.py
"""

import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

import db

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="MD Case Scraper",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Hide the sidebar collapse arrow so it stays pinned
st.markdown(
    """
    <style>
    [data-testid="collapsedControl"] { display: none; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# DB helpers (cached)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=60)
def get_available_dates() -> list[str]:
    conn = db.get_conn(read_only=True)
    dates = db.available_dates(conn)
    conn.close()
    return dates


@st.cache_data(ttl=60)
def load_date(file_date: str) -> pd.DataFrame:
    conn = db.get_conn(read_only=True)
    df = db.query_date(conn, file_date)
    conn.close()
    return df


@st.cache_data(ttl=60)
def load_trends() -> dict:
    conn = db.get_conn(read_only=True)
    data = db.query_trends(conn)
    conn.close()
    return data


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("MD Case Scraper")

report_dates = get_available_dates()

if not report_dates:
    st.warning("No data found. Use the sidebar to run the scraper.")
    st.stop()

if "selected_date" not in st.session_state or st.session_state.selected_date not in report_dates:
    st.session_state.selected_date = report_dates[0]

st.sidebar.markdown("### Reports")
st.sidebar.markdown(
    """
    <style>
    div[data-testid="stSidebar"] button {
        width: 100%;
        text-align: left;
        border-radius: 4px;
        margin-bottom: 2px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

for d in report_dates:
    label = f"  {d}" if d == st.session_state.selected_date else d
    if st.sidebar.button(label, key=f"btn_{d}", use_container_width=True):
        st.session_state.selected_date = d
        st.rerun()

df_raw = load_date(st.session_state.selected_date)

st.sidebar.divider()
st.sidebar.markdown(f"**Records:** {len(df_raw):,}")
st.sidebar.markdown(f"**Counties:** {df_raw['county'].replace('', pd.NA).dropna().nunique()}")
st.sidebar.markdown(f"**Case Types:** {df_raw['case_type'].replace('', pd.NA).dropna().nunique()}")

# Run Scraper — bottom of sidebar
st.sidebar.divider()
st.sidebar.markdown("### Run Scraper")
sidebar_date = st.sidebar.date_input(
    "date",
    value=date.today() - timedelta(days=1),
    min_value=date(2020, 1, 1),
    max_value=date.today(),
    key="sidebar_scrape_date",
    label_visibility="collapsed",
)
if st.sidebar.button("Download & Parse", use_container_width=True, type="primary"):
    with st.sidebar:
        with st.spinner(f"Running scraper for {sidebar_date}..."):
            result = subprocess.run(
                [sys.executable, "scraper.py", str(sidebar_date)],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent,
            )
    if result.returncode == 0:
        get_available_dates.clear()
        load_date.clear()
        load_trends.clear()
        st.session_state.selected_date = str(sidebar_date)
        st.rerun()
    else:
        st.sidebar.error("Scraper failed.")
        st.sidebar.code(result.stderr, language=None)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_overview, tab_data, tab_analytics, tab_trends, tab_scraper = st.tabs([
    "Overview", "Data", "Analytics", "Trends", "Run Scraper"
])

# ===========================================================================
# TAB 1 — Overview
# ===========================================================================

with tab_overview:
    total = len(df_raw)
    has_address = (df_raw["address_street"] != "").sum()
    unique_defendants = df_raw["defendant_name"].nunique()
    unique_counties = df_raw["county"].replace("", pd.NA).dropna().nunique()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Cases", f"{total:,}")
    c2.metric("With Address", f"{has_address:,}", f"{has_address/total*100:.1f}%")
    c3.metric("Unique Defendants", f"{unique_defendants:,}")
    c4.metric("Counties", unique_counties)

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Cases by County")
        county_counts = (
            df_raw[df_raw["county"] != ""]["county"]
            .value_counts()
            .reset_index()
            .rename(columns={"count": "Cases"})
        )
        fig = px.bar(
            county_counts,
            x="Cases",
            y="county",
            orientation="h",
            labels={"county": "County"},
            height=600,
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"}, margin=dict(l=0))
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Cases by Type")
        type_counts = (
            df_raw[df_raw["case_type"] != ""]["case_type"]
            .value_counts()
            .reset_index()
            .rename(columns={"count": "Cases"})
        )
        fig2 = px.pie(
            type_counts,
            names="case_type",
            values="Cases",
            hole=0.4,
        )
        fig2.update_traces(textposition="inside", textinfo="percent+label")
        fig2.update_layout(showlegend=False, margin=dict(l=0, r=0))
        st.plotly_chart(fig2, use_container_width=True)


# ===========================================================================
# TAB 2 — Data Table
# ===========================================================================

with tab_data:
    f1, f2, f3 = st.columns(3)

    with f1:
        county_opts = ["All"] + sorted(df_raw["county"].replace("", pd.NA).dropna().unique().tolist())
        county_filter = st.selectbox("County", county_opts)

    with f2:
        type_opts = ["All"] + sorted(df_raw["case_type"].replace("", pd.NA).dropna().unique().tolist())
        type_filter = st.selectbox("Case Type", type_opts)

    with f3:
        addr_filter = st.selectbox("Address", ["All", "Has Address", "Missing Address"])

    search = st.text_input("Search defendant name", placeholder="e.g. Smith")

    df_filtered = df_raw.copy()

    if county_filter != "All":
        df_filtered = df_filtered[df_filtered["county"] == county_filter]
    if type_filter != "All":
        df_filtered = df_filtered[df_filtered["case_type"] == type_filter]
    if addr_filter == "Has Address":
        df_filtered = df_filtered[df_filtered["address_street"] != ""]
    elif addr_filter == "Missing Address":
        df_filtered = df_filtered[df_filtered["address_street"] == ""]
    if search:
        df_filtered = df_filtered[
            df_filtered["defendant_name"].str.contains(search, case=False, na=False)
        ]

    st.caption(f"{len(df_filtered):,} of {len(df_raw):,} records shown")

    display_cols = [
        "case_number", "file_date", "county", "court_location",
        "defendant_name", "case_type",
        "address_street", "address_city", "address_state", "address_zip",
        "addr_verdict", "addr_corrected_street", "addr_corrected_city",
        "addr_corrected_state", "addr_corrected_zip",
        "charges",
    ]
    available_cols = [c for c in display_cols if c in df_filtered.columns]

    st.dataframe(
        df_filtered[available_cols].reset_index(drop=True),
        use_container_width=True,
        height=550,
        column_config={
            "case_number":          st.column_config.TextColumn("Case #",          width="small"),
            "file_date":            st.column_config.TextColumn("Filed",            width="small"),
            "county":               st.column_config.TextColumn("County",           width="medium"),
            "court_location":       st.column_config.TextColumn("Court",            width="medium"),
            "defendant_name":       st.column_config.TextColumn("Defendant",        width="medium"),
            "case_type":            st.column_config.TextColumn("Type",             width="medium"),
            "address_street":       st.column_config.TextColumn("Street",           width="medium"),
            "address_city":         st.column_config.TextColumn("City",             width="small"),
            "address_state":        st.column_config.TextColumn("ST",               width="small"),
            "address_zip":          st.column_config.TextColumn("ZIP",              width="small"),
            "addr_verdict":         st.column_config.TextColumn("Verified",         width="small"),
            "addr_corrected_street":st.column_config.TextColumn("Corrected Street", width="medium"),
            "addr_corrected_city":  st.column_config.TextColumn("Corrected City",   width="small"),
            "addr_corrected_state": st.column_config.TextColumn("Corr. ST",         width="small"),
            "addr_corrected_zip":   st.column_config.TextColumn("Corr. ZIP",        width="small"),
            "charges":              st.column_config.TextColumn("Charges",          width="large"),
        },
    )

    csv_bytes = df_filtered[available_cols].to_csv(index=False).encode()
    st.download_button(
        "Download filtered CSV",
        data=csv_bytes,
        file_name=f"filtered_cases_{st.session_state.selected_date}.csv",
        mime="text/csv",
    )


# ===========================================================================
# TAB 3 — Analytics (single-day)
# ===========================================================================

with tab_analytics:
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Address Coverage by Case Type")
        cov = (
            df_raw.groupby("case_type")
            .apply(lambda g: pd.Series({
                "Total": len(g),
                "With Address": (g["address_street"] != "").sum(),
            }), include_groups=False)
            .reset_index()
        )
        cov["Coverage %"] = (cov["With Address"] / cov["Total"] * 100).round(1)
        cov = cov.sort_values("Coverage %", ascending=True)

        fig3 = px.bar(
            cov,
            x="Coverage %",
            y="case_type",
            orientation="h",
            text="Coverage %",
            labels={"case_type": "Case Type"},
            range_x=[0, 105],
            height=500,
            color="Coverage %",
            color_continuous_scale=["#EF4444", "#F59E0B", "#22C55E"],
            color_continuous_midpoint=50,
        )
        fig3.update_traces(texttemplate="%{text}%", textposition="outside")
        fig3.update_layout(coloraxis_showscale=False, margin=dict(l=0))
        st.plotly_chart(fig3, use_container_width=True)

    with col_b:
        st.subheader("Top 20 Charges")
        charges_exploded = (
            df_raw["charges"]
            .dropna()
            .str.split(" | ", regex=False)
            .explode()
            .str.strip()
        )
        charges_exploded = charges_exploded[charges_exploded != ""]
        top_charges = (
            charges_exploded.value_counts()
            .head(20)
            .reset_index()
            .rename(columns={"count": "Count"})
        )
        fig4 = px.bar(
            top_charges,
            x="Count",
            y="charges",
            orientation="h",
            labels={"charges": "Charge"},
            height=500,
        )
        fig4.update_layout(yaxis={"categoryorder": "total ascending"}, margin=dict(l=0))
        st.plotly_chart(fig4, use_container_width=True)

    st.divider()

    st.subheader("Defendant State Distribution")
    state_counts = (
        df_raw[df_raw["address_state"] != ""]["address_state"]
        .value_counts()
        .head(20)
        .reset_index()
        .rename(columns={"count": "Count"})
    )
    fig5 = px.bar(state_counts, x="address_state", y="Count", labels={"address_state": "State"})
    st.plotly_chart(fig5, use_container_width=True)


# ===========================================================================
# TAB 4 — Trends (multi-day, DB-powered)
# ===========================================================================

with tab_trends:
    if len(report_dates) < 2:
        st.info("Trends require data from multiple dates. Run the scraper for additional days to unlock this tab.")
    else:
        trends = load_trends()

        # --- Case volume by county over time ---
        st.subheader("Daily Case Volume by County")
        vol = trends["volume"]
        top_counties = vol.groupby("county")["cases"].sum().nlargest(10).index.tolist()
        vol_top = vol[vol["county"].isin(top_counties)]
        fig_vol = px.line(
            vol_top,
            x="date",
            y="cases",
            color="county",
            labels={"date": "Date", "cases": "Cases", "county": "County"},
            height=400,
        )
        st.plotly_chart(fig_vol, use_container_width=True)

        st.divider()

        col_t1, col_t2 = st.columns(2)

        with col_t1:
            # --- Case type mix over time ---
            st.subheader("Case Type Mix Over Time")
            mix = trends["type_mix"]
            fig_mix = px.area(
                mix,
                x="date",
                y="cases",
                color="case_type",
                labels={"date": "Date", "cases": "Cases", "case_type": "Type"},
                height=400,
            )
            fig_mix.update_layout(legend=dict(orientation="h", y=-0.2))
            st.plotly_chart(fig_mix, use_container_width=True)

        with col_t2:
            # --- Top charges across all dates ---
            st.subheader("Top Charges (All Time)")
            tc = trends["top_charges"]
            fig_tc = px.bar(
                tc.head(20),
                x="occurrences",
                y="charge_text",
                orientation="h",
                labels={"charge_text": "Charge", "occurrences": "Count"},
                height=400,
            )
            fig_tc.update_layout(yaxis={"categoryorder": "total ascending"}, margin=dict(l=0))
            st.plotly_chart(fig_tc, use_container_width=True)

        st.divider()

        # --- Repeat offenders ---
        st.subheader("Repeat Offenders")
        ro = trends["repeat_offenders"]
        if ro.empty:
            st.write("No repeat offenders found across the loaded dates.")
        else:
            st.caption(f"{len(ro):,} defendants appeared on more than one filing date")
            st.dataframe(
                ro.rename(columns={
                    "defendant_name": "Defendant",
                    "filing_days":    "Days Filed",
                    "total_cases":    "Total Cases",
                    "first_seen":     "First Seen",
                    "last_seen":      "Last Seen",
                }).drop(columns=["last_name", "first_name"]),
                use_container_width=True,
                hide_index=True,
                height=400,
            )


# ===========================================================================
# TAB 5 — Run Scraper (detailed)
# ===========================================================================

with tab_scraper:
    st.subheader("Download & Parse New Report")
    st.write(
        "The MD courts publish a new case filing PDF each day. "
        "Select a date and click Run to download and parse it into the database."
    )

    default_date = date.today() - timedelta(days=1)
    scrape_date = st.date_input(
        "Report date",
        value=default_date,
        min_value=date(2020, 1, 1),
        max_value=date.today(),
    )

    conn_check = db.get_conn(read_only=True)
    already_in_db = scrape_date.strftime("%Y-%m-%d") in db.available_dates(conn_check)
    conn_check.close()
    if already_in_db:
        st.info(f"{scrape_date} is already in the database — running again will re-parse and upsert.")

    if st.button("Run Scraper", type="primary"):
        with st.spinner(f"Running scraper for {scrape_date}..."):
            result = subprocess.run(
                [sys.executable, "scraper.py", str(scrape_date)],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent,
            )

        if result.returncode == 0:
            st.success("Done!")
            st.code(result.stderr, language=None)
            get_available_dates.clear()
            load_date.clear()
            load_trends.clear()
            st.rerun()
        else:
            st.error("Scraper failed.")
            st.code(result.stderr, language=None)
            if result.stdout:
                st.code(result.stdout, language=None)

    st.divider()
    st.subheader("Database Summary")
    conn_summary = db.get_conn(read_only=True)
    dates_in_db = db.available_dates(conn_summary)
    total_in_db = conn_summary.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
    total_charges = conn_summary.execute("SELECT COUNT(*) FROM case_charges").fetchone()[0]
    conn_summary.close()

    s1, s2, s3 = st.columns(3)
    s1.metric("Total Cases in DB", f"{total_in_db:,}")
    s2.metric("Total Charges in DB", f"{total_charges:,}")
    s3.metric("Days of Data", len(dates_in_db))

    if dates_in_db:
        st.dataframe(
            pd.DataFrame({"Date": dates_in_db}),
            use_container_width=True,
            hide_index=True,
        )
