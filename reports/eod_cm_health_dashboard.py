# -*- coding: utf-8 -*-
"""
Kubera EOD — NSE CM Reports Health Dashboard (Phase-1)

Goal:
    For a selected trade_date, check which NSE CM reports
    are present in the DB and show their row counts + basic status.

Covers (Phase-1):
    - eod.nse_sec_bhav_full
    - eod.nse_delivery_eod
    - eod.cm_short_selling
    - eod.cm_daily_volatility
    - eod.cm_52wk_stats
    - eod.cm_sme_bhav
    - eod.cm_security_master
    - eod.cm_pe_ratio
    - eod.cm_csqr_m_raw
    - eod.cm_var_margin_rates
    - eod.index_eod_prices
    - eod.cm_pr_family_raw (split by file_tag: TT / GL / HL / MCAP / ETF / CORPBOND)

Run (from repo root):

    python -m reports.eod_cm_health_dashboard
"""

import os
from functools import lru_cache

import psycopg2
import pandas as pd

import dash
from dash import Dash, html, dcc, dash_table, Input, Output
import dash_bootstrap_components as dbc


# -------------------------------------------------------------------
# DB CONNECTION HELPER (reuse etl_utils if available)
# -------------------------------------------------------------------

try:
    from reports import etl_utils  # type: ignore

    def get_db_conn():
        return etl_utils.get_pg_conn()

except Exception:
    # Fallback: direct psycopg2 connection with optional .env
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass

    def get_db_conn():
        """
        Simple psycopg2 connector using the same env pattern as other scripts.

        Priority for password:
            - PGPASSWORD
            - DB_PASSWORD
            - OSA_DB_PASSWORD
        """
        password = (
            os.getenv("PGPASSWORD")
            or os.getenv("DB_PASSWORD")
            or os.getenv("OSA_DB_PASSWORD")
        )

        if not password:
            raise RuntimeError(
                "No DB password set. Please export PGPASSWORD or DB_PASSWORD or "
                "OSA_DB_PASSWORD in your environment / .env."
            )

        return psycopg2.connect(
            dbname=os.getenv("PGDATABASE", "osa_db"),
            user=os.getenv("PGUSER", "osa_admin"),
            password=password,
            host=os.getenv(
                "PGHOST",
                "osa-db.c12ccwc88y0j.eu-north-1.rds.amazonaws.com",
            ),
            port=int(os.getenv("PGPORT", "5432")),
        )


# -------------------------------------------------------------------
# STATIC CONFIG — REPORT DEFINITIONS
# -------------------------------------------------------------------

# Core CM tables (1 row per table per date)
CORE_REPORTS = [
    {
        "id": "nse_sec_bhav_full",
        "label": "Bhavcopy (CM Full)",
        "table": "eod.nse_sec_bhav_full",
        "category": "CORE",
    },
    {
        "id": "nse_delivery_eod",
        "label": "Delivery (MTO)",
        "table": "eod.nse_delivery_eod",
        "category": "CORE",
    },
    {
        "id": "cm_short_selling",
        "label": "Short Selling",
        "table": "eod.cm_short_selling",
        "category": "CORE",
    },
    {
        "id": "cm_daily_volatility",
        "label": "Daily Volatility",
        "table": "eod.cm_daily_volatility",
        "category": "CORE",
    },
    {
        "id": "cm_52wk_stats",
        "label": "52 Week High/Low",
        "table": "eod.cm_52wk_stats",
        "category": "CORE",
    },
    {
        "id": "cm_sme_bhav",
        "label": "SME Bhavcopy",
        "table": "eod.cm_sme_bhav",
        "category": "CORE",
    },
    {
        "id": "cm_security_master",
        "label": "CM Security Master Snapshot",
        "table": "eod.cm_security_master",
        "category": "CORE",
    },
    {
        "id": "cm_pe_ratio",
        "label": "PE Ratio",
        "table": "eod.cm_pe_ratio",
        "category": "CORE",
    },
    {
        "id": "cm_csqr_m_raw",
        "label": "CSQR M Metrics",
        "table": "eod.cm_csqr_m_raw",
        "category": "CORE",
    },
    {
        "id": "cm_var_margin_rates",
        "label": "VAR Margin Rates (C_VAR1 snapshots)",
        "table": "eod.cm_var_margin_rates",
        "category": "CORE",
    },
    {
        "id": "index_eod_prices",
        "label": "Index EOD Prices (PR)",
        "table": "eod.index_eod_prices",
        "category": "CORE",
    },
]

# PR family — multiple logical "reports" inside cm_pr_family_raw
PR_FAMILY_TAGS = [
    ("TT", "PR – Top Turnover"),
    ("GL", "PR – Gainers/Losers (GL)"),
    ("HL", "PR – 52W High/Low (HL)"),
    ("MCAP", "PR – Market Cap (MCAP)"),
    ("ETF", "PR – ETFs"),
    ("CORPBOND", "PR – Corporate Bonds"),
]

# -------------------------------------------------------------------
# DATA ACCESS
# -------------------------------------------------------------------

@lru_cache(maxsize=1)
def get_available_dates():
    """
    Get all trade_dates where at least one of the key tables has data.
    We union over a few representative tables.
    """
    sql = """
        SELECT DISTINCT trade_date FROM (
            SELECT DISTINCT trade_date FROM eod.nse_sec_bhav_full
            UNION
            SELECT DISTINCT trade_date FROM eod.cm_var_margin_rates
            UNION
            SELECT DISTINCT trade_date FROM eod.cm_pr_family_raw
        ) t
        ORDER BY trade_date DESC;
    """
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [r[0] for r in rows]


def get_core_report_counts(trade_date):
    """
    Return dict: {report_id: row_count} for CORE_REPORTS.
    """
    parts = []
    params = []
    for rpt in CORE_REPORTS:
        parts.append(
            f"SELECT '{rpt['id']}' AS report_id, COUNT(*) AS cnt "
            f"FROM {rpt['table']} WHERE trade_date = %s"
        )
        params.append(trade_date)

    sql = " UNION ALL ".join(parts)

    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    # rows: list of (report_id, cnt)
    return {rid: cnt for (rid, cnt) in rows}


def get_pr_family_counts(trade_date):
    """
    Return dict: {file_tag: row_count} for cm_pr_family_raw by file_tag.
    """
    sql = """
        SELECT file_tag, COUNT(*) AS cnt
        FROM eod.cm_pr_family_raw
        WHERE trade_date = %s
        GROUP BY file_tag;
    """
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (trade_date,))
        rows = cur.fetchall()

    return {tag: cnt for (tag, cnt) in rows}


def build_health_dataframe(trade_date):
    """
    Build a pandas DataFrame with one row per logical report:
        - id
        - label
        - category
        - table_name
        - file_tag (for PR)
        - row_count
        - status
        - note
    """
    core_counts = get_core_report_counts(trade_date)
    pr_counts = get_pr_family_counts(trade_date)

    records = []

    # CORE
    for rpt in CORE_REPORTS:
        rid = rpt["id"]
        cnt = core_counts.get(rid, 0)

        if cnt == 0:
            status = "MISSING"
            note = "No rows for this date"
        elif cnt < 10:
            status = "LOW"
            note = "Very few rows – verify file / loader"
        else:
            status = "OK"
            note = "Looks healthy"

        records.append(
            {
                "report_id": rid,
                "category": rpt["category"],
                "label": rpt["label"],
                "table_name": rpt["table"],
                "file_tag": None,
                "row_count": int(cnt),
                "status": status,
                "note": note,
            }
        )

    # PR FAMILY
    for tag, label in PR_FAMILY_TAGS:
        cnt = pr_counts.get(tag, 0)
        rid = f"cm_pr_{tag.lower()}"

        if cnt == 0:
            status = "MISSING"
            note = "No rows for this tag & date"
        elif cnt < 10:
            status = "LOW"
            note = "Very few rows – verify PR zip / parser"
        else:
            status = "OK"
            note = "Looks healthy"

        records.append(
            {
                "report_id": rid,
                "category": "PR_FAMILY",
                "label": label,
                "table_name": "eod.cm_pr_family_raw",
                "file_tag": tag,
                "row_count": int(cnt),
                "status": status,
                "note": note,
            }
        )

    df = pd.DataFrame.from_records(records)
    df.sort_values(["category", "label"], inplace=True)
    return df


# -------------------------------------------------------------------
# DASH APP
# -------------------------------------------------------------------

external_stylesheets = [dbc.themes.BOOTSTRAP]

app: Dash = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets,
    title="Kubera — CM Reports Health",
)

server = app.server

available_dates = get_available_dates()
default_date = available_dates[0] if available_dates else None

app.layout = dbc.Container(
    fluid=True,
    children=[
        html.Hr(),
        dbc.Row(
            [
                dbc.Col(
                    html.H3("Kubera EOD — NSE CM Reports Health"),
                    md=7,
                ),
                dbc.Col(
                    html.Div(
                        "Phase-1: CM core tables + PR family completeness check",
                        style={"textAlign": "right", "fontStyle": "italic"},
                    ),
                    md=5,
                ),
            ],
            align="center",
        ),
        html.Hr(),

        # Controls
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Label("Trade Date", className="fw-bold"),
                        dcc.Dropdown(
                            id="health-date-dropdown",
                            options=[
                                {"label": d.strftime("%Y-%m-%d"), "value": d.isoformat()}
                                for d in available_dates
                            ],
                            value=default_date.isoformat() if default_date else None,
                            clearable=False,
                        ),
                    ],
                    md=4,
                ),
                dbc.Col(
                    [
                        html.Label("Actions", className="fw-bold"),
                        dbc.Button(
                            "Refresh",
                            id="health-refresh-btn",
                            color="primary",
                            className="me-2",
                            n_clicks=0,
                        ),
                    ],
                    md=2,
                ),
            ],
            className="mb-4",
        ),

        # Summary cards
        dbc.Row(
            id="health-summary-row",
            className="mb-4",
        ),

        # Chart + Table
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H5("Row Counts by Report"),
                        dcc.Graph(id="health-bar-chart"),
                    ],
                    md=6,
                ),
                dbc.Col(
                    [
                        html.H5("Report Status Detail"),
                        dash_table.DataTable(
                            id="health-table",
                            columns=[
                                {"name": "Category", "id": "category"},
                                {"name": "Report", "id": "label"},
                                {"name": "Table", "id": "table_name"},
                                {"name": "PR Tag", "id": "file_tag"},
                                {"name": "Row Count", "id": "row_count", "type": "numeric"},
                                {"name": "Status", "id": "status"},
                                {"name": "Note", "id": "note"},
                            ],
                            data=[],
                            page_size=25,
                            sort_action="native",
                            filter_action="native",
                            style_table={"height": "500px", "overflowY": "auto"},
                            style_cell={
                                "fontSize": 12,
                                "fontFamily": "Monospace",
                                "whiteSpace": "normal",
                            },
                            style_header={
                                "backgroundColor": "#f8f9fa",
                                "fontWeight": "bold",
                            },
                            style_data_conditional=[
                                {
                                    "if": {"filter_query": "{status} = 'OK'"},
                                    "backgroundColor": "#e9f7ef",
                                },
                                {
                                    "if": {"filter_query": "{status} = 'LOW'"},
                                    "backgroundColor": "#fff3cd",
                                },
                                {
                                    "if": {"filter_query": "{status} = 'MISSING'"},
                                    "backgroundColor": "#f8d7da",
                                },
                            ],
                        ),
                    ],
                    md=6,
                ),
            ]
        ),

        html.Hr(),
        html.Div(
            [
                html.Span("Checks: "),
                html.Span(
                    "Bhavcopy, Delivery, Volatility, 52W, SME, Security Master, PE, CSQR, VAR, Index EOD, PR family (TT/GL/HL/MCAP/ETF/CORPBOND).",
                    style={"fontStyle": "italic", "fontSize": "0.8rem"},
                ),
            ]
        ),
        html.Hr(),
    ],
)


# -------------------------------------------------------------------
# CALLBACKS
# -------------------------------------------------------------------

@app.callback(
    [
        Output("health-table", "data"),
        Output("health-bar-chart", "figure"),
        Output("health-summary-row", "children"),
    ],
    [
        Input("health-refresh-btn", "n_clicks"),
        Input("health-date-dropdown", "value"),
    ],
)
def update_health_view(n_clicks, date_str):
    import plotly.graph_objects as go

    if not date_str:
        return [], go.Figure(), []

    trade_date = date_str  # iso format string
    df = build_health_dataframe(trade_date)

    table_data = df.to_dict("records")

    # Bar chart of row_count per label
    df_chart = df.sort_values("row_count", ascending=False)

    fig = go.Figure()
    fig.add_bar(
        x=df_chart["label"],
        y=df_chart["row_count"],
        marker_line_width=0.5,
    )
    fig.update_layout(
        margin=dict(l=40, r=20, t=40, b=150),
        xaxis_tickangle=-45,
        xaxis_title="Report",
        yaxis_title="Row Count",
        height=500,
    )

    # Summary stats
    total_reports = len(df)
    ok_count = int((df["status"] == "OK").sum())
    low_count = int((df["status"] == "LOW").sum())
    missing_count = int((df["status"] == "MISSING").sum())

    cards = dbc.Row(
        [
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Total Reports Checked", className="card-title"),
                            html.H4(str(total_reports)),
                        ]
                    ),
                    className="mb-2",
                ),
                md=3,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("OK", className="card-title"),
                            html.H4(
                                str(ok_count),
                                style={"color": "green"},
                            ),
                        ]
                    ),
                    className="mb-2",
                ),
                md=3,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Low Rows", className="card-title"),
                            html.H4(
                                str(low_count),
                                style={"color": "#856404"},
                            ),
                        ]
                    ),
                    className="mb-2",
                ),
                md=3,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Missing", className="card-title"),
                            html.H4(
                                str(missing_count),
                                style={"color": "red"},
                            ),
                        ]
                    ),
                    className="mb-2",
                ),
                md=3,
            ),
        ]
    )

    return table_data, fig, cards


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8052")),  # different from 8051 turnover app
        debug=True,
    )
