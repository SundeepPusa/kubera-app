# -*- coding: utf-8 -*-
"""
Kubera ML Features Dashboard

Reads from:
    - ml.v_kubera_features
    - ml.v_kubera_delivery_freq
    - ml.v_starter_delivery_watchlist  (Delivery Radar tab)

Purpose:
    - Validate that CASH + FO features are joined correctly
    - Visualise:
        * Delivery breakouts + delivery frequency (10d / 20d / 60d)
        * Volume vs 5D
        * Delivery Accumulation Strength Tag (✅ / 🟡 / ❌)
        * Futures + CE/PE OI + turnover
        * Simple PCR
        * FO OI regimes (Long Buildup / Short Covering / etc.)
    - Delivery Radar:
        * Use ml.v_starter_delivery_watchlist
        * Rank frequent-delivery stocks by delivery_score
    - Provide a 2nd tab to export an ML training CSV
      from ml.v_kubera_features (with all derived columns)

Run (from repo root):

    python -m reports.kubera_ml_dashboard
"""

import os
from functools import lru_cache

import psycopg2
import pandas as pd

import dash
from dash import Dash, html, dcc, dash_table, Input, Output, State, no_update
import dash_bootstrap_components as dbc


# -------------------------------------------------------------------
# ENV / DB CONNECTION
# -------------------------------------------------------------------

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print(
        "[kubera_ml_dashboard] python-dotenv not installed; "
        "ensure DB env vars are set in the OS."
    )


def get_pg_conn():
    """
    Simple Postgres connection helper.

    Priority of env vars:

        1) OSA_DB_HOST / OSA_DB_NAME / OSA_DB_USER / OSA_DB_PASSWORD / OSA_DB_PORT
        2) DB_HOST / DB_NAME / DB_USER / DB_PASSWORD / DB_PORT
        3) PGHOST / PGDATABASE / PGUSER / PGPASSWORD / PGPORT
    """

    host = (
        os.getenv("OSA_DB_HOST")
        or os.getenv("DB_HOST")
        or os.getenv("PGHOST")
        or "localhost"
    )
    dbname = (
        os.getenv("OSA_DB_NAME")
        or os.getenv("DB_NAME")
        or os.getenv("PGDATABASE")
        or "osa_db"
    )
    user = (
        os.getenv("OSA_DB_USER")
        or os.getenv("DB_USER")
        or os.getenv("PGUSER")
        or "osa_admin"
    )
    password = (
        os.getenv("OSA_DB_PASSWORD")
        or os.getenv("DB_PASSWORD")
        or os.getenv("PGPASSWORD")
        or ""
    )
    port = int(
        os.getenv("OSA_DB_PORT")
        or os.getenv("DB_PORT")
        or os.getenv("PGPORT")
        or "5432"
    )

    print(f"[kubera_ml_dashboard] Connecting to Postgres host={host} db={dbname}")

    if not password:
        print(
            "[kubera_ml_dashboard] WARNING: password env is empty. "
            "Set OSA_DB_PASSWORD or DB_PASSWORD or PGPASSWORD in your .env / OS."
        )

    return psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
    )


# -------------------------------------------------------------------
# DATA FETCH
# -------------------------------------------------------------------

def _fetch_trade_dates_raw():
    """Uncached: fetch distinct trade_dates from ml.v_kubera_features."""
    sql = """
        SELECT DISTINCT trade_date
        FROM ml.v_kubera_features
        ORDER BY trade_date DESC;
    """
    try:
        with get_pg_conn() as conn:
            df = pd.read_sql(sql, conn)
    except Exception as exc:
        print(f"[kubera_ml_dashboard] _fetch_trade_dates_raw() error: {exc}")
        return []

    if df.empty:
        print("[kubera_ml_dashboard] _fetch_trade_dates_raw(): query returned 0 rows")
        return []

    dates = df["trade_date"].tolist()
    print(f"[kubera_ml_dashboard] _fetch_trade_dates_raw(): got {len(dates)} dates")
    return dates


@lru_cache(maxsize=1)
def fetch_trade_dates():
    """Cached wrapper so Dash doesn't keep hitting the DB for date list."""
    return tuple(_fetch_trade_dates_raw())


def _fetch_features_for_date_raw(trade_date: str) -> pd.DataFrame:
    """
    Raw fetch for ML features for a given trade_date.

    Pulls from ml.v_kubera_features and LEFT JOINs ml.v_kubera_delivery_freq
    to get rolling delivery-breakout frequencies.
    """
    sql = """
        SELECT
            vf.trade_date,
            vf.symbol,
            vf.series,
            vf.close_price,
            vf.prev_close,
            vf.ret_1d_pct,
            vf.total_traded_qty,
            vf.avg_vol_5d,
            vf.vol_vs_5d,
            vf.deliverable_qty,
            vf.delivery_ratio,
            vf.avg_deliv_5d,
            vf.deliv_vs_5d,
            vf.is_delivery_breakout,

            -- rolling delivery BO counts
            df.deliv_bo_count_10d,
            df.deliv_bo_count_20d,
            df.deliv_bo_count_60d,

            vf.fut_oi,
            vf.fut_oi_chg,
            vf.fut_contracts,
            vf.fut_traded_value,

            vf.ce_oi,
            vf.pe_oi,
            vf.ce_oi_chg,
            vf.pe_oi_chg,
            vf.ce_contracts,
            vf.pe_contracts,
            vf.ce_traded_value,
            vf.pe_traded_value
        FROM ml.v_kubera_features AS vf
        LEFT JOIN ml.v_kubera_delivery_freq AS df
          ON df.trade_date = vf.trade_date
         AND df.symbol     = vf.symbol
         AND df.series     = vf.series
        WHERE vf.trade_date = %s
        ORDER BY vf.total_traded_qty DESC, vf.symbol;
    """
    with get_pg_conn() as conn:
        df = pd.read_sql(sql, conn, params=[trade_date])

    print(
        f"[kubera_ml_dashboard] _fetch_features_for_date_raw({trade_date}) "
        f"-> {len(df)} rows"
    )
    return df


@lru_cache(maxsize=64)
def fetch_features_for_date(trade_date: str) -> pd.DataFrame:
    """
    Cached wrapper around the raw DB fetch.

    NOTE:
        - The cache key is just (trade_date).
        - The returned DataFrame must NOT be mutated in place by callers;
          always use df.copy() inside enrichment.
    """
    return _fetch_features_for_date_raw(trade_date)


def _fetch_delivery_watchlist_for_date_raw(trade_date: str) -> pd.DataFrame:
    """
    Fetch Delivery Radar rows from ml.v_starter_delivery_watchlist for a date.
    """
    sql = """
        SELECT
            trade_date,
            symbol,
            series,
            close_price,
            prev_close,
            ret_5d_pct,
            total_traded_qty,
            avg_vol_5d,
            vol_vs_5d,
            deliverable_qty,
            delivery_ratio,
            avg_deliv_5d,
            deliv_vs_5d,
            is_delivery_breakout,
            deliv_bo_count_10d,
            deliv_bo_count_20d,
            deliv_bo_count_60d,
            delivery_accum_tag,
            delivery_score
        FROM ml.v_starter_delivery_watchlist
        WHERE trade_date = %s
        ORDER BY delivery_score DESC, vol_vs_5d DESC, symbol;
    """
    with get_pg_conn() as conn:
        df = pd.read_sql(sql, conn, params=[trade_date])

    print(
        f"[kubera_ml_dashboard] _fetch_delivery_watchlist_for_date_raw({trade_date}) "
        f"-> {len(df)} rows"
    )
    return df


@lru_cache(maxsize=64)
def fetch_delivery_watchlist_for_date(trade_date: str) -> pd.DataFrame:
    """
    Cached fetch for Delivery Radar data.
    """
    return _fetch_delivery_watchlist_for_date_raw(trade_date)


# -------------------------------------------------------------------
# DERIVED FEATURES
# -------------------------------------------------------------------

def _classify_fut_oi_regime(ret_1d_pct, fut_oi, fut_oi_chg):
    """
    Futures regime bucket:

        Long Buildup    : Price ↑ , OI ↑
        Short Buildup   : Price ↓ , OI ↑
        Short Covering  : Price ↑ , OI ↓
        Long Unwinding  : Price ↓ , OI ↓
    """
    if fut_oi is None or fut_oi == 0:
        return ""

    if ret_1d_pct is None:
        ret_1d_pct = 0.0
    if fut_oi_chg is None:
        fut_oi_chg = 0.0

    if fut_oi_chg == 0 and abs(ret_1d_pct) < 0.1:
        return ""

    if ret_1d_pct > 0 and fut_oi_chg > 0:
        return "Long Buildup"
    if ret_1d_pct < 0 and fut_oi_chg > 0:
        return "Short Buildup"
    if ret_1d_pct > 0 and fut_oi_chg < 0:
        return "Short Covering"
    if ret_1d_pct < 0 and fut_oi_chg < 0:
        return "Long Unwinding"

    return ""


def _trend_label(delta_val):
    """Convert OI change to 'Up' / 'Down' / ''."""
    if delta_val is None:
        return ""
    if delta_val > 0:
        return "Up"
    if delta_val < 0:
        return "Down"
    return ""


def _classify_option_oi_regime(ce_oi_chg, pe_oi_chg):
    """
    Option OI regime from CE/PE OI change:

        Bullish OI Skew     : CE↑, PE↓ or CE↑, PE≈0
        Bearish OI Skew     : PE↑, CE↓ or PE↑, CE≈0
        Two-side Buildup    : CE↑, PE↑
        Two-side Unwinding  : CE↓, PE↓
        Mixed/Choppy/Empty  : everything else
    """
    ce = ce_oi_chg or 0
    pe = pe_oi_chg or 0

    if ce == 0 and pe == 0:
        return ""

    if ce > 0 and pe <= 0:
        return "Bullish OI Skew"
    if pe > 0 and ce <= 0:
        return "Bearish OI Skew"
    if ce > 0 and pe > 0:
        return "Two-side Buildup"
    if ce < 0 and pe < 0:
        return "Two-side Unwinding"

    return "Mixed / Choppy"


def _classify_delivery_accum(deliv20, vol_vs_5d, ret_5d):
    """
    Delivery Accumulation Strength Tag, using ONLY:
        - deliv_bo_count_20d  (frequency of delivery BOs)
        - vol_vs_5d          (today's volume vs 5D avg)
        - ret_5d_pct         (5-day price change %, or proxy)

    TAGS:
        ✅ Ready to buy
        🟡 Watch for breakout
        ❌ Likely distribution
    """
    if deliv20 is None or pd.isna(deliv20) or deliv20 < 3:
        return ""  # not in the 'frequent delivery' club

    if vol_vs_5d is None or pd.isna(vol_vs_5d):
        vol_vs_5d = 1.0
    if ret_5d is None or pd.isna(ret_5d):
        ret_5d = 0.0

    # Strong, repeated accumulation + supportive recent price + good volume
    if deliv20 >= 6 and vol_vs_5d >= 1.5 and ret_5d >= 0:
        return "✅ Ready to buy"

    # Repeated accumulation but price still under mild pressure / consolidating
    if deliv20 >= 3 and vol_vs_5d >= 1.0 and -8 <= ret_5d < 0:
        return "🟡 Watch for breakout"

    # Frequent delivery but price trend clearly negative → likely distribution
    if deliv20 >= 3 and vol_vs_5d >= 1.0 and ret_5d < -8:
        return "❌ Likely distribution"

    # Default bucket for moderate setups
    return "🟡 Watch for breakout"


def _delivery_score(deliv20, vol_vs_5d, ret_5d):
    """
    Simple delivery score (0–100) aligned with ml.v_starter_delivery_watchlist.
    """
    if deliv20 is None or pd.isna(deliv20) or deliv20 < 3:
        return 0

    if vol_vs_5d is None or pd.isna(vol_vs_5d):
        vol_vs_5d = 1.0
    if ret_5d is None or pd.isna(ret_5d):
        ret_5d = 0.0

    if deliv20 >= 6 and vol_vs_5d >= 1.5 and ret_5d >= 0:
        return 90

    if deliv20 >= 3 and vol_vs_5d >= 1.0 and -8 <= ret_5d < 0:
        return 75

    if deliv20 >= 3 and vol_vs_5d >= 1.0 and ret_5d < -8:
        return 40

    return 60


def enrich_features_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived columns:

        - ret_5d_pct (proxy if missing)
        - pcr_oi
        - _fo_any_notnull
        - fo_active
        - fut_oi_regime
        - ce_oi_trend / pe_oi_trend
        - option_oi_regime
        - delivery_accum_tag
        - delivery_score
    """
    if df.empty:
        return df.copy()

    df = df.copy()

    # Ensure we always have a ret_5d_pct column
    if "ret_5d_pct" not in df.columns:
        if "ret_1d_pct" in df.columns:
            df["ret_5d_pct"] = df["ret_1d_pct"]
        else:
            df["ret_5d_pct"] = 0.0

    # ---------- FO FLAGS ----------
    fo_cols_list = [
        "fut_oi",
        "ce_oi",
        "pe_oi",
        "fut_contracts",
        "ce_contracts",
        "pe_contracts",
        "fut_traded_value",
        "ce_traded_value",
        "pe_traded_value",
    ]
    # Select only columns that exist (for safety)
    fo_cols_existing = [c for c in fo_cols_list if c in df.columns]

    if fo_cols_existing:
        fo_raw = df[fo_cols_existing]

        # FO Universe: any FO column is non-null for that row
        df["_fo_any_notnull"] = fo_raw.notna().any(axis=1)

        # FO Active: at least one FO metric has non-zero magnitude
        # Use numeric coercion to avoid FutureWarning on mixed dtypes
        fo_numeric = fo_raw.apply(pd.to_numeric, errors="coerce")
        fo_filled = fo_numeric.fillna(0)
        df["fo_active"] = (fo_filled.abs() > 0).any(axis=1)
    else:
        df["_fo_any_notnull"] = False
        df["fo_active"] = False

    # ---------- PCR (OI based) ----------
    def _calc_pcr(row):
        ce = row.get("ce_oi")
        pe = row.get("pe_oi")
        if ce in (0, None) or pe is None:
            return None
        try:
            return float(pe) / float(ce)
        except Exception:
            return None

    df["pcr_oi"] = pd.to_numeric(
        df.apply(_calc_pcr, axis=1),
        errors="coerce",
    )

    # ---------- Futures OI regime ----------
    df["fut_oi_regime"] = df.apply(
        lambda row: _classify_fut_oi_regime(
            row.get("ret_1d_pct"),
            row.get("fut_oi"),
            row.get("fut_oi_chg"),
        ),
        axis=1,
    )

    # CE / PE OI trends
    df["ce_oi_trend"] = (
        df["ce_oi_chg"].map(_trend_label) if "ce_oi_chg" in df.columns else ""
    )
    df["pe_oi_trend"] = (
        df["pe_oi_chg"].map(_trend_label) if "pe_oi_chg" in df.columns else ""
    )

    # Combined option OI regime
    df["option_oi_regime"] = df.apply(
        lambda row: _classify_option_oi_regime(
            row.get("ce_oi_chg"),
            row.get("pe_oi_chg"),
        ),
        axis=1,
    )

    # ---------- Delivery Accumulation Strength Tag ----------
    df["delivery_accum_tag"] = df.apply(
        lambda row: _classify_delivery_accum(
            row.get("deliv_bo_count_20d"),
            row.get("vol_vs_5d"),
            row.get("ret_5d_pct"),
        ),
        axis=1,
    )

    # ---------- Delivery Score (for ML export / diagnostics) ----------
    df["delivery_score"] = df.apply(
        lambda row: _delivery_score(
            row.get("deliv_bo_count_20d"),
            row.get("vol_vs_5d"),
            row.get("ret_5d_pct"),
        ),
        axis=1,
    )

    return df


# -------------------------------------------------------------------
# DASH APP
# -------------------------------------------------------------------

external_stylesheets = [dbc.themes.BOOTSTRAP]

app: Dash = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets,
    title="Kubera ML Features",
)

app.layout = dbc.Container(
    fluid=True,
    children=[
        html.H2("Kubera ML Feature Console", className="mt-3 mb-2"),

        # Short legend for Delivery Accumulation Strength Tag
        html.Div(
            [
                html.Span("Rank your Frequent Delivery list into: "),
                html.Ul(
                    [
                        html.Li("✅ Ready to buy"),
                        html.Li("🟡 Watch for breakout"),
                        html.Li("❌ Likely distribution"),
                    ],
                    style={"marginBottom": "0.5rem"},
                ),
                html.Div(
                    "Logic uses Ret_5D (proxy) + Deliv_BO_20d + Vol_vs_5D "
                    "to classify accumulation strength.",
                    className="text-muted",
                    style={"fontSize": "0.8rem"},
                ),
            ],
            className="mb-2",
        ),

        # --------- GLOBAL CONTROLS ----------
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Label("Trade Date"),
                        dcc.Dropdown(
                            id="date-dropdown",
                            options=[],
                            placeholder="Select a trade date",
                            clearable=False,
                        ),
                    ],
                    md=3,
                ),
                dbc.Col(
                    [
                        html.Label("Top N (for Feature View table)"),
                        dcc.Dropdown(
                            id="topn-dropdown",
                            options=[
                                {"label": "Top 10", "value": 10},
                                {"label": "Top 25", "value": 25},
                                {"label": "Top 50", "value": 50},
                                {"label": "Top 100", "value": 100},
                            ],
                            value=25,
                            clearable=False,
                        ),
                    ],
                    md=2,
                ),
                dbc.Col(
                    [
                        html.Label("Filter (Feature View)"),
                        dcc.Dropdown(
                            id="filter-dropdown",
                            options=[
                                {"label": "All", "value": "all"},
                                {
                                    "label": "Delivery Breakouts Only",
                                    "value": "delivery",
                                },
                                {
                                    "label": "Frequent Delivery (20d ≥ 3 BOs)",
                                    "value": "delivery_freq_20d",
                                },
                            ],
                            value="all",
                            clearable=False,
                        ),
                    ],
                    md=3,
                ),
                dbc.Col(
                    [
                        html.Label("Universe (Feature View)"),
                        dcc.RadioItems(
                            id="universe-radio",
                            options=[
                                {"label": "All Stocks", "value": "all"},
                                {
                                    "label": "FO Active (selected date)",
                                    "value": "fo_active_today",
                                },
                                {
                                    "label": "FO Universe (has any FO data)",
                                    "value": "fo_universe",
                                },
                            ],
                            value="all",
                            inline=True,
                        ),
                    ],
                    md=4,
                ),
            ],
            className="mb-3",
        ),

        # ---------------------- TABS ----------------------
        dbc.Tabs(
            [
                # 1) Feature View (Cash + FO)
                dbc.Tab(
                    label="Feature View",
                    tab_id="tab-features",
                    children=[
                        dbc.Row(
                            dbc.Col(
                                [
                                    html.Div(
                                        id="summary-banner",
                                        className="mb-2",
                                    ),
                                    dash_table.DataTable(
                                        id="features-table",
                                        columns=[
                                            {"name": "Symbol", "id": "symbol"},
                                            {"name": "Series", "id": "series"},
                                            {
                                                "name": "Close",
                                                "id": "close_price",
                                                "type": "numeric",
                                                "format": {"specifier": ".2f"},
                                            },
                                            {
                                                "name": "Ret 1D %",
                                                "id": "ret_1d_pct",
                                                "type": "numeric",
                                                "format": {"specifier": ".2f"},
                                            },
                                            {
                                                "name": "Ret 5D % (proxy)",
                                                "id": "ret_5d_pct",
                                                "type": "numeric",
                                                "format": {"specifier": ".2f"},
                                            },
                                            {
                                                "name": "Tot Qty",
                                                "id": "total_traded_qty",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "Vol vs 5D",
                                                "id": "vol_vs_5d",
                                                "type": "numeric",
                                                "format": {"specifier": ".2f"},
                                            },
                                            {
                                                "name": "Delivery %",
                                                "id": "delivery_ratio",
                                                "type": "numeric",
                                                "format": {"specifier": ".2f"},
                                            },
                                            {
                                                "name": "Deliv vs 5D",
                                                "id": "deliv_vs_5d",
                                                "type": "numeric",
                                                "format": {"specifier": ".2f"},
                                            },
                                            {
                                                "name": "Deliv BO",
                                                "id": "is_delivery_breakout",
                                            },
                                            {
                                                "name": "Deliv BO 10d",
                                                "id": "deliv_bo_count_10d",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "Deliv BO 20d",
                                                "id": "deliv_bo_count_20d",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "Deliv BO 60d",
                                                "id": "deliv_bo_count_60d",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "Deliv Accum Tag",
                                                "id": "delivery_accum_tag",
                                            },
                                            {
                                                "name": "Deliv Score",
                                                "id": "delivery_score",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "Fut OI",
                                                "id": "fut_oi",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "Fut OI Δ",
                                                "id": "fut_oi_chg",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "Fut Turnover",
                                                "id": "fut_traded_value",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "Fut Regime",
                                                "id": "fut_oi_regime",
                                            },
                                            {
                                                "name": "CE OI",
                                                "id": "ce_oi",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "PE OI",
                                                "id": "pe_oi",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "CE OI Δ",
                                                "id": "ce_oi_chg",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "PE OI Δ",
                                                "id": "pe_oi_chg",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "CE Turnover",
                                                "id": "ce_traded_value",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "PE Turnover",
                                                "id": "pe_traded_value",
                                                "type": "numeric",
                                            },
                                            {
                                                "name": "CE Trend",
                                                "id": "ce_oi_trend",
                                            },
                                            {
                                                "name": "PE Trend",
                                                "id": "pe_oi_trend",
                                            },
                                            {
                                                "name": "Option OI Regime",
                                                "id": "option_oi_regime",
                                            },
                                            {
                                                "name": "PCR (OI)",
                                                "id": "pcr_oi",
                                                "type": "numeric",
                                                "format": {"specifier": ".2f"},
                                            },
                                            {
                                                "name": "FO Active",
                                                "id": "fo_active",
                                            },
                                        ],
                                        data=[],
                                        page_size=25,
                                        sort_action="native",
                                        filter_action="native",
                                        style_table={"overflowX": "auto"},
                                        style_cell={
                                            "fontFamily": "monospace",
                                            "fontSize": 12,
                                            "padding": "4px",
                                        },
                                        style_header={"fontWeight": "bold"},
                                    ),
                                ],
                                md=12,
                            )
                        ),
                    ],
                ),

                # 2) Delivery Radar (Starter Delivery Watchlist)
                dbc.Tab(
                    label="Delivery Radar",
                    tab_id="tab-delivery-radar",
                    children=[
                        html.Br(),
                        html.P(
                            "Delivery Radar shows only frequent-delivery stocks "
                            "from ml.v_starter_delivery_watchlist "
                            "(deliv_bo_count_20d ≥ 3), ranked by delivery_score.",
                            className="text-muted",
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.Label("Tag Filter"),
                                        dcc.Dropdown(
                                            id="delivery-radar-tag-dropdown",
                                            options=[
                                                {"label": "All", "value": "all"},
                                                {
                                                    "label": "✅ Ready to buy",
                                                    "value": "ready",
                                                },
                                                {
                                                    "label": "🟡 Watch for breakout",
                                                    "value": "watch",
                                                },
                                                {
                                                    "label": "❌ Likely distribution",
                                                    "value": "dist",
                                                },
                                            ],
                                            value="all",
                                            clearable=False,
                                        ),
                                    ],
                                    md=3,
                                ),
                                dbc.Col(
                                    [
                                        html.Label("Min Delivery Score"),
                                        dcc.Slider(
                                            id="delivery-radar-min-score",
                                            min=0,
                                            max=100,
                                            step=5,
                                            value=75,
                                            marks={
                                                0: "0",
                                                40: "40",
                                                60: "60",
                                                75: "75",
                                                90: "90",
                                            },
                                        ),
                                    ],
                                    md=5,
                                ),
                            ],
                            className="mb-3",
                        ),
                        html.Div(
                            id="delivery-radar-summary",
                            className="mb-2",
                        ),
                        dash_table.DataTable(
                            id="delivery-radar-table",
                            columns=[
                                {"name": "Symbol", "id": "symbol"},
                                {"name": "Series", "id": "series"},
                                {
                                    "name": "Close",
                                    "id": "close_price",
                                    "type": "numeric",
                                    "format": {"specifier": ".2f"},
                                },
                                {
                                    "name": "Ret 5D % (proxy)",
                                    "id": "ret_5d_pct",
                                    "type": "numeric",
                                    "format": {"specifier": ".2f"},
                                },
                                {
                                    "name": "Vol vs 5D",
                                    "id": "vol_vs_5d",
                                    "type": "numeric",
                                    "format": {"specifier": ".2f"},
                                },
                                {
                                    "name": "Delivery %",
                                    "id": "delivery_ratio",
                                    "type": "numeric",
                                    "format": {"specifier": ".2f"},
                                },
                                {
                                    "name": "Deliv vs 5D",
                                    "id": "deliv_vs_5d",
                                    "type": "numeric",
                                    "format": {"specifier": ".2f"},
                                },
                                {
                                    "name": "Deliv BO 10d",
                                    "id": "deliv_bo_count_10d",
                                    "type": "numeric",
                                },
                                {
                                    "name": "Deliv BO 20d",
                                    "id": "deliv_bo_count_20d",
                                    "type": "numeric",
                                },
                                {
                                    "name": "Deliv BO 60d",
                                    "id": "deliv_bo_count_60d",
                                    "type": "numeric",
                                },
                                {
                                    "name": "Deliv BO (Today)",
                                    "id": "is_delivery_breakout",
                                },
                                {
                                    "name": "Deliv Accum Tag",
                                    "id": "delivery_accum_tag",
                                },
                                {
                                    "name": "Delivery Score",
                                    "id": "delivery_score",
                                    "type": "numeric",
                                },
                            ],
                            data=[],
                            page_size=50,
                            sort_action="native",
                            filter_action="native",
                            style_table={"overflowX": "auto"},
                            style_cell={
                                "fontFamily": "monospace",
                                "fontSize": 12,
                                "padding": "4px",
                            },
                            style_header={"fontWeight": "bold"},
                        ),
                    ],
                ),

                # 3) ML Training Export
                dbc.Tab(
                    label="ML Training Export",
                    tab_id="tab-export",
                    children=[
                        html.Br(),
                        html.P(
                            "Download a full ML training CSV for the selected "
                            "trade_date after applying the same universe, "
                            "filters and derived FO regimes.",
                        ),
                        dbc.Row(
                            dbc.Col(
                                [
                                    dbc.Button(
                                        "Download Training CSV",
                                        id="btn-download-training",
                                        color="primary",
                                        className="mb-2",
                                    ),
                                    html.Div(
                                        id="export-summary",
                                        className="text-muted small",
                                    ),
                                    dcc.Download(id="download-training"),
                                ],
                                md=4,
                            )
                        ),
                    ],
                ),
            ],
            id="main-tabs",
        ),
    ],
)


# -------------------------------------------------------------------
# CALLBACKS
# -------------------------------------------------------------------

@app.callback(
    Output("date-dropdown", "options"),
    Output("date-dropdown", "value"),
    Input("date-dropdown", "id"),  # dummy trigger on page load
)
def init_date_dropdown(_):
    """Populate date dropdown on app startup."""
    dates = list(fetch_trade_dates())
    options = [{"label": str(d), "value": str(d)} for d in dates]
    default_value = options[0]["value"] if options else None

    print(
        f"[kubera_ml_dashboard] init_date_dropdown -> "
        f"{len(options)} options, default={default_value}"
    )
    return options, default_value


@app.callback(
    Output("features-table", "data"),
    Output("features-table", "page_size"),
    Output("summary-banner", "children"),
    Input("date-dropdown", "value"),
    Input("topn-dropdown", "value"),
    Input("filter-dropdown", "value"),
    Input("universe-radio", "value"),
)
def update_table(selected_date, topn, filter_mode, universe_mode):
    """
    Core logic for Feature View:
        - Fetch + enrich data for selected date
        - Compute FO diagnostics BEFORE any filters / TopN
        - Apply universe (All / FO Active / FO Universe)
        - Apply filter (All / Delivery breakouts / Delivery freq 20d)
        - Take Top N
    """
    if not selected_date:
        return [], 25, "No trade_date selected."

    base_df = fetch_features_for_date(selected_date)
    if base_df.empty:
        return [], 25, f"No rows in ml.v_kubera_features for {selected_date}."

    df = enrich_features_df(base_df)

    # Stats BEFORE any universe/filter/TopN
    universe_rows = len(df)
    universe_delivery = int(
        df["is_delivery_breakout"].fillna(0).astype(int).astype(bool).sum()
    )
    universe_fo_any = int(df["_fo_any_notnull"].fillna(False).astype(bool).sum())
    universe_fo_active = int(df["fo_active"].fillna(False).astype(bool).sum())

    print(
        f"[kubera_ml_dashboard] {selected_date} -> "
        f"FO-any-notnull={universe_fo_any}, FO-active(>0)={universe_fo_active}"
    )

    # ----------------- UNIVERSE SELECTION -----------------
    if universe_mode == "fo_active_today":
        df = df[df["fo_active"] == True]
    elif universe_mode == "fo_universe":
        df = df[df["_fo_any_notnull"] == True]

    if df.empty:
        universe_label = (
            "All Stocks"
            if universe_mode == "all"
            else (
                "FO Active (selected date)"
                if universe_mode == "fo_active_today"
                else "FO Universe (has any FO data)"
            )
        )
        banner = (
            f"{selected_date} -> universe: {universe_rows} rows "
            f"(delivery={universe_delivery}, "
            f"FO_any={universe_fo_any}, FO_active={universe_fo_active}) | "
            f"table: 0 rows after universe='{universe_label}'."
        )
        return [], 25, banner

    # ----------------- FILTER INSIDE UNIVERSE -----------------
    if filter_mode == "delivery":
        df = df[df["is_delivery_breakout"].fillna(0).astype(int) == 1]
    elif filter_mode == "delivery_freq_20d":
        # frequent delivery winners – at least 3 breakouts in last 20 days
        df = df[df["deliv_bo_count_20d"].fillna(0) >= 3]

    if df.empty:
        universe_label = (
            "All Stocks"
            if universe_mode == "all"
            else (
                "FO Active (selected date)"
                if universe_mode == "fo_active_today"
                else "FO Universe (has any FO data)"
            )
        )
        filter_label = (
            "All"
            if filter_mode == "all"
            else (
                "Delivery"
                if filter_mode == "delivery"
                else "Delivery freq 20d"
            )
        )
        banner = (
            f"{selected_date} -> universe: {universe_rows} rows "
            f"(delivery={universe_delivery}, "
            f"FO_any={universe_fo_any}, FO_active={universe_fo_active}) | "
            f"table: 0 rows after universe='{universe_label}', "
            f"filter='{filter_label}'."
        )
        return [], 25, banner

    # ----------------- SORT & TOP N -----------------
    # NOTE: TopN is only applied AFTER universe + filter, so it does
    # NOT affect FO_any / FO_active counters shown above.
    df = df.sort_values(
        ["fo_active", "total_traded_qty"],
        ascending=[False, False],
    ).head(topn)

    # Stats after universe + filter + TopN
    n_rows = len(df)
    n_delivery = int(
        df["is_delivery_breakout"].fillna(0).astype(int).astype(bool).sum()
    )
    n_fo_any = int(df["_fo_any_notnull"].fillna(False).astype(bool).sum())
    n_fo_active = int(df["fo_active"].fillna(False).astype(bool).sum())

    universe_label = (
        "All Stocks"
        if universe_mode == "all"
        else (
            "FO Active (selected date)"
            if universe_mode == "fo_active_today"
            else "FO Universe (has any FO data)"
        )
    )
    filter_label = (
        "All"
        if filter_mode == "all"
        else (
            "Delivery breakouts only"
            if filter_mode == "delivery"
            else "Frequent Delivery (20d ≥ 3 BOs)"
        )
    )

    banner = (
        f"{selected_date} -> universe: {universe_rows} rows "
        f"(delivery={universe_delivery}, "
        f"FO_any={universe_fo_any}, FO_active={universe_fo_active}) | "
        f"table: {n_rows} rows "
        f"[universe={universe_label}, filter={filter_label}, "
        f"delivery={n_delivery}, FO_any={n_fo_any}, FO_active={n_fo_active}]"
    )

    # Display-friendly bools
    df["is_delivery_breakout"] = df["is_delivery_breakout"].map(
        lambda x: "Yes" if x else ""
    )
    df["fo_active"] = df["fo_active"].map(lambda x: "Yes" if x else "")

    # Round numeric metrics for display
    if "pcr_oi" in df.columns:
        df["pcr_oi"] = df["pcr_oi"].round(2)
    if "delivery_score" in df.columns:
        df["delivery_score"] = df["delivery_score"].round(0)

    for col in [
        "fut_traded_value",
        "ce_traded_value",
        "pe_traded_value",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(0)

    return df.to_dict("records"), int(topn), banner


@app.callback(
    Output("delivery-radar-table", "data"),
    Output("delivery-radar-summary", "children"),
    Input("date-dropdown", "value"),
    Input("delivery-radar-tag-dropdown", "value"),
    Input("delivery-radar-min-score", "value"),
)
def update_delivery_radar(selected_date, tag_filter, min_score):
    """
    Delivery Radar:
        - Reads from ml.v_starter_delivery_watchlist
        - Filters by tag + min delivery_score
    """
    if not selected_date:
        return [], "No trade_date selected."

    df = fetch_delivery_watchlist_for_date(selected_date)
    if df.empty:
        return (
            [],
            f"No rows in ml.v_starter_delivery_watchlist for {selected_date}.",
        )

    df = df.copy()

    # Tag filter
    tag_map = {
        "ready": "✅ Ready to buy",
        "watch": "🟡 Watch for breakout",
        "dist": "❌ Likely distribution",
    }
    if tag_filter != "all":
        wanted_tag = tag_map.get(tag_filter)
        df = df[df["delivery_accum_tag"] == wanted_tag]

    # Min score filter
    if min_score is not None:
        df = df[df["delivery_score"].fillna(0) >= float(min_score)]

    total_rows = len(fetch_delivery_watchlist_for_date(selected_date))
    filtered_rows = len(df)

    # Display-friendly bool for today's breakout
    df["is_delivery_breakout"] = df["is_delivery_breakout"].map(
        lambda x: "Yes" if x else ""
    )

    # Round numeric for cleaner display
    for col in [
        "close_price",
        "ret_5d_pct",
        "vol_vs_5d",
        "delivery_ratio",
        "deliv_vs_5d",
        "delivery_score",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    # Tag counts after filters
    tag_counts = (
        df["delivery_accum_tag"]
        .fillna("")
        .value_counts()
        .to_dict()
    )
    tag_summary_parts = [
        f"{k}: {v}" for k, v in tag_counts.items() if k
    ]
    tag_summary = (
        ", ".join(tag_summary_parts) if tag_summary_parts else "No tagged rows"
    )

    summary = (
        f"Delivery Radar for {selected_date}: total={total_rows}, "
        f"after filters={filtered_rows} "
        f"(TagFilter={tag_filter}, MinScore={min_score}) | {tag_summary}"
    )

    return df.to_dict("records"), summary


@app.callback(
    Output("download-training", "data"),
    Output("export-summary", "children"),
    Input("btn-download-training", "n_clicks"),
    State("date-dropdown", "value"),
    State("filter-dropdown", "value"),
    State("universe-radio", "value"),
    prevent_initial_call=True,
)
def download_training_csv(n_clicks, selected_date, filter_mode, universe_mode):
    """
    Export full ML training set for the selected date, respecting
    the same universe + filter logic (no TopN).
    """
    if not selected_date:
        return no_update, "Please select a trade_date first."

    base_df = fetch_features_for_date(selected_date)
    if base_df.empty:
        return no_update, f"No rows in ml.v_kubera_features for {selected_date}."

    df = enrich_features_df(base_df)

    # Universe
    if universe_mode == "fo_active_today":
        df = df[df["fo_active"] == True]
    elif universe_mode == "fo_universe":
        df = df[df["_fo_any_notnull"] == True]

    # Filter
    if filter_mode == "delivery":
        df = df[df["is_delivery_breakout"].fillna(0).astype(int) == 1]
    elif filter_mode == "delivery_freq_20d":
        df = df[df["deliv_bo_count_20d"].fillna(0) >= 3]

    if df.empty:
        universe_label = (
            "All Stocks"
            if universe_mode == "all"
            else (
                "FO Active (selected date)"
                if universe_mode == "fo_active_today"
                else "FO Universe (has any FO data)"
            )
        )
        return no_update, (
            f"No rows to export after applying universe='{universe_label}' "
            f"and filter='{filter_mode}' for {selected_date}."
        )

    filename = f"kubera_ml_training_{selected_date}.csv"

    universe_label = (
        "All Stocks"
        if universe_mode == "all"
        else (
            "FO Active (selected date)"
            if universe_mode == "fo_active_today"
            else "FO Universe (has any FO data)"
        )
    )
    filter_label = (
        "All"
        if filter_mode == "all"
        else (
            "Delivery breakouts only"
            if filter_mode == "delivery"
            else "Frequent Delivery (20d ≥ 3 BOs)"
        )
    )

    summary = (
        f"Prepared training CSV for {selected_date} "
        f"({len(df)} rows after universe='{universe_label}', "
        f"filter='{filter_label}')."
    )

    return dcc.send_data_frame(df.to_csv, filename, index=False), summary


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

if __name__ == "__main__":
    _dates = list(fetch_trade_dates())
    print("[kubera_ml_dashboard] initial dates (first 5):", _dates[:5])
    app.run(debug=False, host="0.0.0.0", port=8050)
