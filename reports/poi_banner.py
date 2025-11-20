# FILE: components/poi_banner.py
import os
import pandas as pd
import psycopg2
from dash import html

DB_KW = dict(
    host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)

SQL = """
SELECT *
FROM eod.vw_poi_banner
ORDER BY trade_date DESC
LIMIT 1;
"""

def _fmt(n):
    if n is None: return "—"
    return f"{int(n):,}"

def _arrow(delta):
    if delta is None: return "↔︎"
    if delta > 0:     return "↑"
    if delta < 0:     return "↓"
    return "↔︎"

def _arrow_color(delta):
    if delta is None: return "#999"
    return "#16a34a" if delta > 0 else ("#dc2626" if delta < 0 else "#999")

def card(title, net, dod):
    return html.Div([
        html.Div(title, style={"fontSize":"0.9rem","color":"#64748b","marginBottom":"4px"}),
        html.Div([
            html.Span(_fmt(net),  style={"fontSize":"1.4rem","fontWeight":700,"marginRight":"8px"}),
            html.Span(_arrow(dod), style={"fontSize":"1.2rem","color":_arrow_color(dod),"fontWeight":700})
        ]),
        html.Div(f"DoD: {_fmt(dod)}", style={"fontSize":"0.85rem","color":"#94a3b8","marginTop":"2px"})
    ], style={
        "padding":"12px 14px","border":"1px solid #e2e8f0","borderRadius":"14px",
        "boxShadow":"0 2px 8px rgba(0,0,0,0.04)","background":"#fff","minWidth":"220px"
    })

def layout():
    with psycopg2.connect(**DB_KW) as conn:
        df = pd.read_sql(SQL, conn)
    if df.empty:
        return html.Div("No POI data yet.", style={"color":"#64748b"})

    row = df.iloc[0]
    date_str = row["trade_date"].strftime("%Y-%m-%d")

    grid = html.Div([
        card("CLIENT Net", row["client_net"], row["client_dod"]),
        card("DII Net",    row["dii_net"],    row["dii_dod"]),
        card("FII Net",    row["fii_net"],    row["fii_dod"]),
        card("PRO Net",    row["pro_net"],    row["pro_dod"]),
    ], style={
        "display":"grid","gridTemplateColumns":"repeat(4, minmax(220px, 1fr))",
        "gap":"12px","alignItems":"stretch"
    })

    return html.Div([
        html.Div([
            html.Div("Participants OI — Net & DoD", style={"fontSize":"1.1rem","fontWeight":700}),
            html.Div(f"Trade date: {date_str}", style={"color":"#64748b"})
        ], style={"display":"flex","justifyContent":"space-between","marginBottom":"10px"}),
        grid
    ], style={"padding":"8px 12px","background":"#f8fafc","borderRadius":"16px"})
