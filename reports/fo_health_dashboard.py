# -*- coding: utf-8 -*-
"""
Kubera EOD — F&O Loader Health Dashboard (fo_load_summary)

This Dash app gives you:

1) Per-day table health for the F&O loader using eod.fo_load_summary
   - Shows table_name, status, rows_affected, run_id for a selected trade_date.

2) "Failed / Missing FO Days" report for the last N business days
   - Detects:
        * NO_RUN      → no rows in fo_load_summary for that trade_date
        * FAILED      → at least one table with status = 'ERR'
        * INCOMPLETE  → at least one table with status = 'MISS'
        * OK          → all tables OK (not shown in gaps table)

3) Backfill validator (gap finder)
   - Same gap report doubles as your "which days should I backfill or re-run"
   - You can change the lookback window in the dropdown.

Run from repo root (same as other reports):

    python -m reports.fo_health_dashboard
"""

import os
import logging
import datetime as dt
from functools import lru_cache

import psycopg2
import psycopg2.extras
import pandas as pd

import dash
from dash import Dash, html, dcc, dash_table, Input, Output
import dash_bootstrap_components as dbc

# ------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------
LOG = logging.getLogger("fo_health_dashboard")

# ------------------------------------------------------------
# DB CONNECTION HELPER (aligned with other loaders)
# ------------------------------------------------------------
try:
    # Preferred: reuse shared ETL helper if available
    from reports import etl_utils  # type: ignore
except Exception:
    etl_utils = None  # type: ignore


def _get_conn_from_env():
    """
    Fallback connection using env/.env variables.

    Uses:
        PGPASSWORD / DB_PASSWORD / OSA_DB_PASSWORD
        PGHOST / DB_HOST / OSA_DB_HOST
        PGUSER / PGDATABASE / PGPORT
    """
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        # .env optional
        pass

    password = (
        os.getenv("PGPASSWORD")
        or os.getenv("DB_PASSWORD")
        or os.getenv("OSA_DB_PASSWORD")
    )
    if not password:
        raise RuntimeError(
            "No DB password found. Set PGPASSWORD / DB_PASSWORD / OSA_DB_PASSWORD."
        )

    dbname = os.getenv("PGDATABASE", "osa_db")
    user = os.getenv("PGUSER", "osa_admin")
    host = (
        os.getenv("PGHOST")
        or os.getenv("DB_HOST")
        or os.getenv("OSA_DB_HOST")
        or "osa-db.c12ccwc88y0j.eu-north-1.rds.amazonaws.com"
    )
    port = int(os.getenv("PGPORT", "5432"))

    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )


@lru_cache(maxsize=1)
def get_db_conn() -> psycopg2.extensions.connection:
    """
    Cached helper returning a live psycopg2 connection.

    - First tries reports.etl_utils.get_pg_conn() if available.
    - Falls back to env-based connection.
    """
    if etl_utils is not None:
        try:
            conn = etl_utils.get_pg_conn()
            LOG.info("Using etl_utils.get_pg_conn() for fo_health_dashboard")
            return conn
        except Exception as exc:
            LOG.warning(
                "etl_utils.get_pg_conn() failed (%s); falling back to env connection.",
                exc,
            )

    LOG.info("Using env-based DB connection for fo_health_dashboard")
    return _get_conn_from_env()


def _fetch_dataframe(sql: str, params=None) -> pd.DataFrame:
    """
    Small helper to run a SELECT and return a pandas DataFrame.
    Ensures connection is in good state (rollback on error).
    """
    if params is None:
        params = []

    conn = get_db_conn()
    try:
        df = pd.read_sql(sql, conn, params=params)
        return df
    except Exception:
        conn.rollback()
        raise


# ------------------------------------------------------------
# DATA QUERIES
# ------------------------------------------------------------

def get_available_dates() -> pd.Series:
    """
    Get distinct trade_date values from eod.fo_load_summary, newest first.
    Used to populate the date dropdown / default selection.
    """
    sql = """
        SELECT DISTINCT trade_date
        FROM eod.fo_load_summary
        ORDER BY trade_date DESC
    """
    df = _fetch_dataframe(sql)
    return df["trade_date"] if not df.empty else pd.Series([], dtype="datetime64[ns]")


def get_summary_for_date(trade_date: dt.date) -> pd.DataFrame:
    """
    Return a per-table summary for a given trade_date from eod.fo_load_summary.

    Columns:
        table_name, status, rows_affected, run_id, created_at
    """
    sql = """
        SELECT
            table_name,
            status,
            rows_affected,
            run_id,
            created_at
        FROM eod.fo_load_summary
        WHERE trade_date = %s
        ORDER BY table_name
    """
    df = _fetch_dataframe(sql, [trade_date])
    return df


def get_gap_report(days_back: int = 60) -> pd.DataFrame:
    """
    Gap / backfill report over the last N *business* days.

    Logic:
      - Build a calendar of business days (Mon–Fri) from (current_date - days_back) to current_date
      - Left join eod.fo_load_summary on trade_date
      - For each date:
            total_rows  = count(*) in fo_load_summary
            err_tables  = rows with status = 'ERR'
            miss_tables = rows with status = 'MISS'
      - day_status:
            NO_RUN     → total_rows = 0
            FAILED     → err_tables  > 0
            INCOMPLETE → miss_tables > 0 (and NO ERR)
            OK         → everything else

      - We only return rows where status is NOT OK
        (these are your "attention / backfill" days).
    """
    sql = """
        WITH bounds AS (
            SELECT
                (CURRENT_DATE - %s::int) AS start_date,
                CURRENT_DATE             AS end_date
        ),
        calendar AS (
            SELECT d::date AS trade_date
            FROM bounds,
                 generate_series(start_date, end_date, INTERVAL '1 day') AS d
            WHERE EXTRACT(ISODOW FROM d) NOT IN (6, 7)  -- Mon–Fri only
        ),
        day_status AS (
            SELECT
                c.trade_date,
                COUNT(ls.*)                                            AS total_rows,
                SUM(CASE WHEN ls.status = 'ERR'  THEN 1 ELSE 0 END)   AS err_tables,
                SUM(CASE WHEN ls.status = 'MISS' THEN 1 ELSE 0 END)   AS miss_tables,
                SUM(CASE WHEN ls.status = 'OK'   THEN 1 ELSE 0 END)   AS ok_tables
            FROM calendar c
            LEFT JOIN eod.fo_load_summary ls
                ON ls.trade_date = c.trade_date
            GROUP BY c.trade_date
        )
        SELECT
            trade_date,
            total_rows,
            err_tables,
            miss_tables,
            ok_tables,
            CASE
                WHEN total_rows = 0 THEN 'NO_RUN'
                WHEN err_tables  > 0 THEN 'FAILED'
                WHEN miss_tables > 0 THEN 'INCOMPLETE'
                ELSE 'OK'
            END AS day_status
        FROM day_status
        WHERE
            total_rows = 0           -- never ran
            OR err_tables  > 0       -- at least one ERR
            OR miss_tables > 0       -- at least one MISS
        ORDER BY trade_date DESC;
    """
    df = _fetch_dataframe(sql, [days_back])
    return df


def get_backfill_only_missing(days_back: int = 60) -> pd.DataFrame:
    """
    Optional: stricter backfill validator that returns ONLY days where
    the pipeline was never run (NO_RUN), ignoring FAILED / INCOMPLETE.

    This is useful if you want a clean list of days to schedule fresh runs for.
    """
    df = get_gap_report(days_back=days_back)
    if df.empty:
        return df
    return df[df["day_status"] == "NO_RUN"].copy()


# ------------------------------------------------------------
# DASH APP LAYOUT
# ------------------------------------------------------------

external_stylesheets = [dbc.themes.BOOTSTRAP]

app: Dash = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets,
    title="Kubera F&O Loader Health",
)

# Pre-load available dates for initial controls
available_dates = get_available_dates()
if not available_dates.empty:
    default_date = available_dates.iloc[0].date()
else:
    default_date = dt.date.today()

app.layout = dbc.Container(
    [
        html.H2("Kubera F&O Loader Health — fo_load_summary", className="mt-3 mb-4"),

        # Controls row
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Label("Trade Date (fo_load_summary)"),
                        dcc.DatePickerSingle(
                            id="trade-date-picker",
                            date=default_date,
                            display_format="YYYY-MM-DD",
                            style={"width": "100%"},
                        ),
                        html.Small(
                            "Pick a date to see per-table status (fo_load_summary).",
                            className="text-muted",
                        ),
                    ],
                    md=4,
                ),
                dbc.Col(
                    [
                        html.Label("Gap / Backfill Lookback (business days)"),
                        dcc.Dropdown(
                            id="lookback-dropdown",
                            options=[
                                {"label": "Last 30 days", "value": 30},
                                {"label": "Last 60 days", "value": 60},
                                {"label": "Last 90 days", "value": 90},
                                {"label": "Last 180 days", "value": 180},
                            ],
                            value=60,
                            clearable=False,
                        ),
                        html.Small(
                            "Used for the failed / missing days & backfill validator.",
                            className="text-muted",
                        ),
                    ],
                    md=4,
                ),
            ],
            className="mb-4",
        ),

        # Summary cards row
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H6("Selected Date Summary", className="card-title"),
                                html.Div(id="selected-date-summary"),
                            ]
                        ),
                        className="mb-4",
                    ),
                    md=6,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            [
                                html.H6("Backfill / Gaps Overview", className="card-title"),
                                html.Div(id="gap-summary"),
                            ]
                        ),
                        className="mb-4",
                    ),
                    md=6,
                ),
            ]
        ),

        # Per-table detail for selected date
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H5("Per-table Status for Selected Date"),
                        dash_table.DataTable(
                            id="table-detail",
                            columns=[
                                {"name": "Table Name", "id": "table_name"},
                                {"name": "Status", "id": "status"},
                                {"name": "Rows", "id": "rows_affected", "type": "numeric"},
                                {"name": "Run ID", "id": "run_id"},
                                {"name": "Logged At", "id": "created_at"},
                            ],
                            style_table={"overflowX": "auto"},
                            style_cell={
                                "padding": "4px",
                                "fontSize": 12,
                                "fontFamily": "monospace",
                            },
                            style_header={
                                "fontWeight": "bold",
                                "backgroundColor": "#f8f9fa",
                            },
                            style_data_conditional=[
                                {
                                    "if": {"filter_query": "{status} = 'OK'"},
                                    "backgroundColor": "#e7f7ec",
                                },
                                {
                                    "if": {"filter_query": "{status} = 'MISS'"},
                                    "backgroundColor": "#fff3cd",
                                },
                                {
                                    "if": {"filter_query": "{status} = 'ERR'"},
                                    "backgroundColor": "#f8d7da",
                                },
                            ],
                            page_size=20,
                        ),
                    ],
                    width=12,
                )
            ],
            className="mb-4",
        ),

        # Gap / backfill validator table
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H5("Failed / Missing FO Days (Gap / Backfill Report)"),
                        dash_table.DataTable(
                            id="gap-table",
                            columns=[
                                {"name": "Trade Date", "id": "trade_date"},
                                {"name": "Status", "id": "day_status"},
                                {"name": "Total Rows", "id": "total_rows", "type": "numeric"},
                                {"name": "ERR Tables", "id": "err_tables", "type": "numeric"},
                                {"name": "MISS Tables", "id": "miss_tables", "type": "numeric"},
                                {"name": "OK Tables", "id": "ok_tables", "type": "numeric"},
                            ],
                            style_table={"overflowX": "auto"},
                            style_cell={
                                "padding": "4px",
                                "fontSize": 12,
                                "fontFamily": "monospace",
                            },
                            style_header={
                                "fontWeight": "bold",
                                "backgroundColor": "#f8f9fa",
                            },
                            style_data_conditional=[
                                {
                                    "if": {"filter_query": "{day_status} = 'NO_RUN'"},
                                    "backgroundColor": "#f8d7da",
                                },
                                {
                                    "if": {"filter_query": "{day_status} = 'FAILED'"},
                                    "backgroundColor": "#f8d7da",
                                },
                                {
                                    "if": {"filter_query": "{day_status} = 'INCOMPLETE'"},
                                    "backgroundColor": "#fff3cd",
                                },
                            ],
                            page_size=15,
                        ),
                    ],
                    width=12,
                )
            ],
            className="mb-4",
        ),
    ],
    fluid=True,
)


# ------------------------------------------------------------
# CALLBACKS
# ------------------------------------------------------------

@app.callback(
    Output("table-detail", "data"),
    Output("selected-date-summary", "children"),
    Input("trade-date-picker", "date"),
)
def update_selected_date_view(date_value):
    if not date_value:
        return [], "No date selected."

    trade_date = dt.datetime.strptime(date_value, "%Y-%m-%d").date()
    df = get_summary_for_date(trade_date)

    if df.empty:
        return [], f"No rows found in fo_load_summary for {trade_date} (NO_RUN)."

    # Summary metrics
    total_tables = len(df)
    ok = (df["status"] == "OK").sum()
    miss = (df["status"] == "MISS").sum()
    err = (df["status"] == "ERR").sum()
    run_ids = sorted(df["run_id"].unique())

    summary_text = (
        f"{trade_date}: {total_tables} tables "
        f"(OK={ok}, MISS={miss}, ERR={err}) | "
        f"run_id(s): {', '.join(run_ids)}"
    )

    # Format DataTable rows
    df_display = df.copy()
    df_display["created_at"] = df_display["created_at"].astype(str)

    return df_display.to_dict("records"), summary_text


@app.callback(
    Output("gap-table", "data"),
    Output("gap-summary", "children"),
    Input("lookback-dropdown", "value"),
)
def update_gap_table(days_back):
    if not days_back:
        days_back = 60

    df = get_gap_report(days_back=int(days_back))

    if df.empty:
        return [], f"No gaps / failed days in last {days_back} business days. ✅"

    # Small textual summary
    counts = df["day_status"].value_counts().to_dict()
    parts = [f"{k}={v}" for k, v in sorted(counts.items())]
    summary_text = (
        f"In last {days_back} business days: "
        + ", ".join(parts)
        + " (only non-OK days are listed below)."
    )

    # Format trade_date as string for DataTable
    df_display = df.copy()
    df_display["trade_date"] = df_display["trade_date"].astype(str)

    return df_display.to_dict("records"), summary_text


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    LOG.info("Starting Kubera F&O Loader Health Dashboard...")
    # You can change host/port as needed (for EC2 / Render etc.)
    app.run_server(host="0.0.0.0", port=8052, debug=True)


if __name__ == "__main__":
    main()
