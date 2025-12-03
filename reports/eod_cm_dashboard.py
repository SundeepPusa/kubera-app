# -*- coding: utf-8 -*-
"""
Kubera EOD — NSE CM Top Turnover Dashboard (Phase-1)

This Dash app reads from:
    - eod.v_pr_top_turnover  (built from cm_pr_family_raw, file_tag = 'TT')

Features:
    - Dropdown of available trade_dates
    - Top-N selector (10 / 25 / 50 / 100)
    - Table of Top Turnover stocks
    - Bar chart of NET_TRDVAL with price % change shown in hover

Run (from repo root):

    python -m reports.eod_cm_dashboard
"""

import os
from functools import lru_cache

import psycopg2
import pandas as pd

import dash
from dash import Dash, html, dcc, dash_table, Input, Output
import dash_bootstrap_components as dbc


# -------------------------------------------------------------------
# DB CONNECTION HELPER
# -------------------------------------------------------------------

# First try to reuse the shared ETL connection helper (same as load_cm_reports)
try:
    from reports import etl_utils  # type: ignore

    def get_db_conn():
        return etl_utils.get_pg_conn()

except Exception:
    # Fallback: direct psycopg2 connection with optional .env loading
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv()
    except Exception:
        # If python-dotenv is not installed, we just rely on env vars already set
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
# DATA ACCESS LAYER
# -------------------------------------------------------------------

@lru_cache(maxsize=1)
def get_available_dates():
    """
    Fetch distinct trade_date values from v_pr_top_turnover,
    ordered DESC, cached for the session.
    """
    sql = """
        SELECT DISTINCT trade_date
        FROM eod.v_pr_top_turnover
        ORDER BY trade_date DESC;
    """
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    # rows is list of tuples like [(date,), ...]
    return [r[0] for r in rows]


def load_top_turnover(trade_date, limit_rows=25):
    """
    Load top turnover records for a given trade_date from v_pr_top_turnover.
    """
    sql = """
        SELECT
            row_no,
            security_name,
            close_price,
            prev_close,
            pct_change_close,
            net_trd_qty,
            net_trd_val
        FROM eod.v_pr_top_turnover
        WHERE trade_date = %s
        ORDER BY net_trd_val DESC, row_no ASC
        LIMIT %s;
    """
    with get_db_conn() as conn:
        df = pd.read_sql(sql, conn, params=(trade_date, limit_rows))
    return df


# -------------------------------------------------------------------
# DASH APP LAYOUT
# -------------------------------------------------------------------

external_stylesheets = [dbc.themes.BOOTSTRAP]

app: Dash = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets,
    title="Kubera — EOD NSE CM Top Turnover",
)

server = app.server  # for waitress/gunicorn if needed later

available_dates = get_available_dates()
default_date = available_dates[0] if available_dates else None

app.layout = dbc.Container(
    fluid=False,  # fixed-width container (prevents ultra-wide auto-maximise)
    style={"maxWidth": "1400px"},
    children=[
        html.Hr(),
        dbc.Row(
            [
                dbc.Col(
                    html.H3("Kubera EOD — NSE Cash Top Turnover"),
                    md=6,
                ),
                dbc.Col(
                    html.Div(
                        "Phase-1: PR TT snapshot (Top Turnover) from eod.v_pr_top_turnover",
                        style={"textAlign": "right", "fontStyle": "italic"},
                    ),
                    md=6,
                ),
            ],
            align="center",
        ),
        html.Hr(),

        # Controls row: Date + Top N
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.Label("Trade Date", className="fw-bold"),
                        dcc.Dropdown(
                            id="cm-date-dropdown",
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
                        html.Label("Top N by Turnover", className="fw-bold"),
                        dcc.Dropdown(
                            id="cm-topn-dropdown",
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
                    md=3,
                ),
                dbc.Col(
                    [
                        html.Label("Actions", className="fw-bold"),
                        dbc.Button(
                            "Refresh",
                            id="cm-refresh-btn",
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
            id="cm-summary-cards-row",
            className="mb-4",
        ),

        # Main content: chart + table
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H5("Top Turnover — Bar Chart"),
                        dcc.Graph(
                            id="cm-turnover-bar-chart",
                            style={"height": "500px"},
                        ),
                    ],
                    md=6,
                ),
                dbc.Col(
                    [
                        html.H5("Top Turnover — Detail Table"),
                        dash_table.DataTable(
                            id="cm-turnover-table",
                            columns=[
                                {"name": "Rank", "id": "row_no"},
                                {"name": "Security Name", "id": "security_name"},
                                {"name": "Close", "id": "close_price", "type": "numeric"},
                                {"name": "Prev Close", "id": "prev_close", "type": "numeric"},
                                {"name": "% Chg Close", "id": "pct_change_close", "type": "numeric"},
                                {"name": "Net Qty", "id": "net_trd_qty", "type": "numeric"},
                                {"name": "Net Trd Val", "id": "net_trd_val", "type": "numeric"},
                            ],
                            data=[],
                            page_size=20,
                            sort_action="native",
                            filter_action="native",
                            style_table={"height": "500px", "overflowY": "auto"},
                            style_cell={"fontSize": 12, "fontFamily": "Monospace"},
                            style_header={
                                "backgroundColor": "#f8f9fa",
                                "fontWeight": "bold",
                            },
                            style_data_conditional=[
                                {
                                    "if": {
                                        "filter_query": "{pct_change_close} > 0",
                                        "column_id": "pct_change_close",
                                    },
                                    "color": "green",
                                },
                                {
                                    "if": {
                                        "filter_query": "{pct_change_close} < 0",
                                        "column_id": "pct_change_close",
                                    },
                                    "color": "red",
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
            "Data source: eod.v_pr_top_turnover (NSE PR TT → cm_pr_family_raw)",
            style={"fontSize": "0.8rem", "fontStyle": "italic"},
        ),
        html.Hr(),
    ],
)


# -------------------------------------------------------------------
# CALLBACKS
# -------------------------------------------------------------------

@app.callback(
    [
        Output("cm-turnover-table", "data"),
        Output("cm-turnover-bar-chart", "figure"),
        Output("cm-summary-cards-row", "children"),
    ],
    [
        Input("cm-refresh-btn", "n_clicks"),
        Input("cm-date-dropdown", "value"),
        Input("cm-topn-dropdown", "value"),
    ],
)
def update_top_turnover(n_clicks, date_str, top_n):
    """
    Main callback:
        - Load DF for (trade_date, top_n)
        - Fill table
        - Build bar chart
        - Build summary cards
    """
    import plotly.graph_objects as go

    if not date_str:
        return [], go.Figure(), []

    trade_date = date_str  # ISO like '2025-11-28'
    df = load_top_turnover(trade_date, limit_rows=top_n or 25)

    # Prepare DataTable data
    table_data = df.to_dict("records")

    # Chart: bar of net_trd_val, x=security_name
    df_chart = df.sort_values("net_trd_val", ascending=False)

    fig = go.Figure()
    fig.add_bar(
        x=df_chart["security_name"],
        y=df_chart["net_trd_val"],
        hovertemplate=(
            "Security: %{x}<br>"
            "Net Trd Val: %{y:.2f}<br>"
            "Close: %{customdata[0]:.2f}<br>"
            "Prev Close: %{customdata[1]:.2f}<br>"
            "% Chg: %{customdata[2]:.2f}%<extra></extra>"
        ),
        customdata=df_chart[["close_price", "prev_close", "pct_change_close"]].values,
    )
    fig.update_layout(
        margin=dict(l=40, r=20, t=40, b=120),
        xaxis_tickangle=-45,
        xaxis_title="Security",
        yaxis_title="Net Traded Value",
        height=500,
    )

    # Summary cards: total turnover + avg % change + #symbols
    total_turnover = df["net_trd_val"].sum() if not df.empty else 0
    avg_pct_change = (
        df["pct_change_close"].mean()
        if "pct_change_close" in df and not df.empty
        else None
    )
    num_symbols = len(df)

    cards = dbc.Row(
        [
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Total Turnover (Top N)", className="card-title"),
                            html.H4(f"{total_turnover:,.2f}"),
                        ]
                    ),
                    className="mb-2",
                ),
                md=4,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Avg % Change (Close)", className="card-title"),
                            html.H4(
                                f"{avg_pct_change:.2f}%" if avg_pct_change is not None else "N/A",
                                style={
                                    "color": "green" if (avg_pct_change or 0) > 0 else (
                                        "red" if (avg_pct_change or 0) < 0 else "black"
                                    ),
                                },
                            ),
                        ]
                    ),
                    className="mb-2",
                ),
                md=4,
            ),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Number of Symbols", className="card-title"),
                            html.H4(str(num_symbols)),
                        ]
                    ),
                    className="mb-2",
                ),
                md=4,
            ),
        ]
    )

    return table_data, fig, cards


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

if __name__ == "__main__":
    # For local dev. Later you can run via waitress/gunicorn if needed.
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8051")),
        debug=True,
    )
