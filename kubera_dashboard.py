# kubera_dashboard.py
# Kubera Insights – Alpha Console (Dash layout skeleton + demo data)

from dash import Dash, html, dcc, dash_table, Input, Output
import plotly.graph_objs as go
from datetime import datetime

app = Dash(__name__)
server = app.server  # in case you want to deploy later

app.title = "Kubera Insights – Alpha Console"

app.layout = html.Div(
    id="app-root",
    style={
        "display": "flex",
        "flexDirection": "column",
        "minHeight": "100vh",
        "background": "#020617",
        "fontFamily": "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
        "color": "#f9fafb",
    },
    children=[
        # ================= TOP BAR =================
        html.Div(
            id="top-bar",
            style={
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "space-between",
                "padding": "0.6rem 1.2rem",
                "borderBottom": "1px solid rgba(148,163,184,0.3)",
                "background": "rgba(3,7,18,0.96)",
                "backdropFilter": "blur(12px)",
                "position": "sticky",
                "top": 0,
                "zIndex": 20,
            },
            children=[
                # Left: Logo + title
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "0.6rem"},
                    children=[
                        html.Div(
                            style={
                                "width": "32px",
                                "height": "32px",
                                "borderRadius": "999px",
                                "background": "radial-gradient(circle at 30% 0, #D4AF37 0, #B38A20 30%, #4B3D18 100%)",
                                "display": "flex",
                                "alignItems": "center",
                                "justifyContent": "center",
                                "boxShadow": "0 0 0 2px rgba(212,175,55,0.35)",
                                "fontWeight": 700,
                                "color": "#020617",
                            },
                            children="K",
                        ),
                        html.Div(
                            children=[
                                html.Div(
                                    "Kubera Insights",
                                    style={
                                        "fontWeight": 600,
                                        "letterSpacing": "0.05em",
                                        "fontSize": "0.9rem",
                                        "textTransform": "uppercase",
                                    },
                                ),
                                html.Div(
                                    "Alpha Console",
                                    style={
                                        "fontSize": "0.7rem",
                                        "color": "#9ca3af",
                                        "textTransform": "uppercase",
                                        "letterSpacing": "0.18em",
                                    },
                                ),
                            ]
                        ),
                    ],
                ),

                # Center: Symbol selector & timeframe
                html.Div(
                    id="top-bar-middle",
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "gap": "0.75rem",
                    },
                    children=[
                        dcc.Dropdown(
                            id="symbol-dropdown",
                            options=[
                                {"label": "NIFTY 50", "value": "NIFTY 50"},
                                {"label": "BANKNIFTY", "value": "BANKNIFTY"},
                                {"label": "FINNIFTY", "value": "FINNIFTY"},
                            ],
                            value="NIFTY 50",
                            clearable=False,
                            style={"width": "180px", "color": "#000"},
                            placeholder="Select symbol",
                        ),
                        dcc.Dropdown(
                            id="expiry-dropdown",
                            options=[],
                            placeholder="Expiry",
                            style={"width": "150px", "color": "#000"},
                            clearable=True,
                        ),
                        html.Div(
                            id="timeframe-toggle",
                            style={
                                "display": "inline-flex",
                                "borderRadius": "999px",
                                "border": "1px solid rgba(148,163,184,0.5)",
                                "overflow": "hidden",
                                "fontSize": "0.75rem",
                            },
                            children=[
                                html.Div(
                                    "3m",
                                    id="tf-3m",
                                    style={
                                        "padding": "0.25rem 0.55rem",
                                        "background": "#111827",
                                        "cursor": "pointer",
                                    },
                                ),
                                html.Div(
                                    "5m",
                                    id="tf-5m",
                                    style={"padding": "0.25rem 0.55rem", "cursor": "pointer"},
                                ),
                                html.Div(
                                    "30m",
                                    id="tf-30m",
                                    style={"padding": "0.25rem 0.55rem", "cursor": "pointer"},
                                ),
                            ],
                        ),
                    ],
                ),

                # Right: Session + user
                html.Div(
                    id="top-bar-right",
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "gap": "0.8rem",
                        "fontSize": "0.75rem",
                        "color": "#9ca3af",
                    },
                    children=[
                        html.Div(
                            id="market-session-indicator",
                            style={"display": "flex", "alignItems": "center", "gap": "0.3rem"},
                            children=[
                                html.Div(
                                    style={
                                        "width": "7px",
                                        "height": "7px",
                                        "borderRadius": "999px",
                                        "background": "#22c55e",
                                        "boxShadow": "0 0 0 4px rgba(34,197,94,0.35)",
                                    }
                                ),
                                html.Span("Market: Unknown"),
                            ],
                        ),
                        html.Div("IST Time: --:--", id="clock-label"),
                    ],
                ),
            ],
        ),

        # ================= MAIN BODY =================
        html.Div(
            id="main-body",
            style={
                "flex": 1,
                "display": "flex",
                "minHeight": "0",
            },
            children=[
                # --------- SIDEBAR ----------
                html.Div(
                    id="sidebar",
                    style={
                        "width": "220px",
                        "borderRight": "1px solid rgba(148,163,184,0.25)",
                        "background": "#020617",
                        "padding": "0.9rem 0.6rem",
                        "display": "flex",
                        "flexDirection": "column",
                        "gap": "0.35rem",
                    },
                    children=[
                        html.Div(
                            "Navigation",
                            style={
                                "fontSize": "0.75rem",
                                "color": "#6b7280",
                                "textTransform": "uppercase",
                                "letterSpacing": "0.13em",
                                "marginBottom": "0.3rem",
                            },
                        ),
                        html.Div(
                            id="sidebar-items",
                            children=[
                                html.Div(
                                    "Dashboard",
                                    style={
                                        "padding": "0.4rem 0.6rem",
                                        "borderRadius": "0.6rem",
                                        "background": "#111827",
                                        "fontSize": "0.85rem",
                                        "cursor": "pointer",
                                    },
                                ),
                                html.Div(
                                    "Smart Money",
                                    style={
                                        "padding": "0.4rem 0.6rem",
                                        "borderRadius": "0.6rem",
                                        "fontSize": "0.85rem",
                                        "cursor": "pointer",
                                    },
                                ),
                                html.Div(
                                    "Option Chain",
                                    style={
                                        "padding": "0.4rem 0.6rem",
                                        "borderRadius": "0.6rem",
                                        "fontSize": "0.85rem",
                                        "cursor": "pointer",
                                    },
                                ),
                                html.Div(
                                    "Alpha Radar",
                                    style={
                                        "padding": "0.4rem 0.6rem",
                                        "borderRadius": "0.6rem",
                                        "fontSize": "0.85rem",
                                        "cursor": "pointer",
                                    },
                                ),
                                html.Div(
                                    "Execution Log (later)",
                                    style={
                                        "padding": "0.4rem 0.6rem",
                                        "borderRadius": "0.6rem",
                                        "fontSize": "0.85rem",
                                        "cursor": "pointer",
                                        "color": "#6b7280",
                                    },
                                ),
                            ],
                        ),
                        html.Div(style={"flex": 1}),
                        html.Div(
                            "v1.0 Alpha Console",
                            style={"fontSize": "0.7rem", "color": "#4b5563", "marginTop": "1rem"},
                        ),
                    ],
                ),

                # --------- MAIN CONTENT ----------
                html.Div(
                    id="content-area",
                    style={
                        "flex": 1,
                        "padding": "0.9rem 1rem 1.4rem",
                        "display": "flex",
                        "flexDirection": "column",
                        "gap": "0.9rem",
                        "minWidth": 0,
                    },
                    children=[
                        # 🔁 Demo refresh timer (15s)
                        dcc.Interval(
                            id="demo-interval",
                            interval=15 * 1000,  # 15 seconds
                            n_intervals=0,
                        ),

                        # ===== Banner row =====
                        html.Div(
                            id="banner-row",
                            style={
                                "display": "grid",
                                "gridTemplateColumns": "2.2fr 1.4fr",
                                "gap": "0.8rem",
                            },
                            children=[
                                # Banner left
                                html.Div(
                                    id="symbol-banner",
                                    style={
                                        "borderRadius": "14px",
                                        "padding": "0.75rem 0.85rem",
                                        "background": "radial-gradient(circle at top left, rgba(22,163,74,0.20) 0, #020617 45%, #020617 100%)",
                                        "border": "1px solid rgba(148,163,184,0.45)",
                                    },
                                    children=[
                                        html.Div(
                                            style={
                                                "display": "flex",
                                                "justifyContent": "space-between",
                                                "alignItems": "flex-start",
                                            },
                                            children=[
                                                html.Div(
                                                    children=[
                                                        html.Div(
                                                            id="banner-symbol",
                                                            children="NIFTY 50",
                                                            style={
                                                                "fontWeight": 600,
                                                                "fontSize": "1rem",
                                                                "letterSpacing": "0.06em",
                                                            },
                                                        ),
                                                        html.Div(
                                                            id="banner-subtitle",
                                                            children="Alpha View · Intraday",
                                                            style={
                                                                "fontSize": "0.75rem",
                                                                "color": "#9ca3af",
                                                            },
                                                        ),
                                                    ]
                                                ),
                                                html.Div(
                                                    children=[
                                                        html.Div(
                                                            id="banner-alpha-label",
                                                            children="Alpha Score",
                                                            style={
                                                                "fontSize": "0.7rem",
                                                                "color": "#9ca3af",
                                                                "textAlign": "right",
                                                            },
                                                        ),
                                                        html.Div(
                                                            id="banner-alpha-score",
                                                            children="-- %",
                                                            style={
                                                                "fontSize": "1.3rem",
                                                                "fontWeight": 700,
                                                                "color": "#4ade80",
                                                                "textAlign": "right",
                                                            },
                                                        ),
                                                        html.Div(
                                                            id="banner-regime",
                                                            children="Regime: Unknown",
                                                            style={
                                                                "fontSize": "0.72rem",
                                                                "color": "#9ca3af",
                                                                "textAlign": "right",
                                                            },
                                                        ),
                                                    ]
                                                ),
                                            ],
                                        ),
                                        html.Div(
                                            id="banner-tags",
                                            style={
                                                "marginTop": "0.5rem",
                                                "display": "flex",
                                                "flexWrap": "wrap",
                                                "gap": "0.4rem",
                                                "fontSize": "0.7rem",
                                            },
                                            children=[
                                                html.Div(
                                                    "Smart Money Bias: --",
                                                    style={
                                                        "padding": "0.18rem 0.5rem",
                                                        "borderRadius": "999px",
                                                        "background": "rgba(15,23,42,0.9)",
                                                        "border": "1px solid rgba(148,163,184,0.45)",
                                                    },
                                                ),
                                                html.Div(
                                                    "Gamma Zone: --",
                                                    style={
                                                        "padding": "0.18rem 0.5rem",
                                                        "borderRadius": "999px",
                                                        "background": "rgba(15,23,42,0.9)",
                                                        "border": "1px solid rgba(148,163,184,0.45)",
                                                    },
                                                ),
                                                html.Div(
                                                    "IV Regime: --",
                                                    style={
                                                        "padding": "0.18rem 0.5rem",
                                                        "borderRadius": "999px",
                                                        "background": "rgba(15,23,42,0.9)",
                                                        "border": "1px solid rgba(148,163,184,0.45)",
                                                    },
                                                ),
                                            ],
                                        ),
                                    ],
                                ),

                                # Banner right – mini stats
                                html.Div(
                                    id="mini-stat-row",
                                    style={
                                        "display": "grid",
                                        "gridTemplateColumns": "repeat(3, minmax(0,1fr))",
                                        "gap": "0.5rem",
                                    },
                                    children=[
                                        html.Div(
                                            style={
                                                "borderRadius": "12px",
                                                "padding": "0.5rem 0.6rem",
                                                "background": "#020617",
                                                "border": "1px solid rgba(148,163,184,0.4)",
                                            },
                                            children=[
                                                html.Div(
                                                    "CE OI Shift",
                                                    style={
                                                        "fontSize": "0.72rem",
                                                        "color": "#9ca3af",
                                                    },
                                                ),
                                                html.Div(
                                                    id="stat-ce-oi-shift",
                                                    children="-- %",
                                                    style={
                                                        "fontSize": "0.95rem",
                                                        "fontWeight": 600,
                                                    },
                                                ),
                                                html.Div(
                                                    id="stat-ce-note",
                                                    children="Side: --",
                                                    style={
                                                        "fontSize": "0.7rem",
                                                        "color": "#6b7280",
                                                    },
                                                ),
                                            ],
                                        ),
                                        html.Div(
                                            style={
                                                "borderRadius": "12px",
                                                "padding": "0.5rem 0.6rem",
                                                "background": "#020617",
                                                "border": "1px solid rgba(148,163,184,0.4)",
                                            },
                                            children=[
                                                html.Div(
                                                    "PE OI Shift",
                                                    style={
                                                        "fontSize": "0.72rem",
                                                        "color": "#9ca3af",
                                                    },
                                                ),
                                                html.Div(
                                                    id="stat-pe-oi-shift",
                                                    children="-- %",
                                                    style={
                                                        "fontSize": "0.95rem",
                                                        "fontWeight": 600,
                                                    },
                                                ),
                                                html.Div(
                                                    id="stat-pe-note",
                                                    children="Side: --",
                                                    style={
                                                        "fontSize": "0.7rem",
                                                        "color": "#6b7280",
                                                    },
                                                ),
                                            ],
                                        ),
                                        html.Div(
                                            style={
                                                "borderRadius": "12px",
                                                "padding": "0.5rem 0.6rem",
                                                "background": "#020617",
                                                "border": "1px solid rgba(148,163,184,0.4)",
                                            },
                                            children=[
                                                html.Div(
                                                    "IV / Gamma",
                                                    style={
                                                        "fontSize": "0.72rem",
                                                        "color": "#9ca3af",
                                                    },
                                                ),
                                                html.Div(
                                                    id="stat-iv-regime",
                                                    children="Regime: --",
                                                    style={
                                                        "fontSize": "0.86rem",
                                                        "fontWeight": 600,
                                                    },
                                                ),
                                                html.Div(
                                                    id="stat-gamma-note",
                                                    children="Pressure: --",
                                                    style={
                                                        "fontSize": "0.7rem",
                                                        "color": "#6b7280",
                                                    },
                                                ),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),

                        # ===== CHART + SIDE PANEL =====
                        html.Div(
                            id="chart-row",
                            style={
                                "display": "grid",
                                "gridTemplateColumns": "2.3fr 1.3fr",
                                "gap": "0.8rem",
                                "minHeight": "260px",
                            },
                            children=[
                                # Main chart
                                html.Div(
                                    id="main-chart-card",
                                    style={
                                        "borderRadius": "14px",
                                        "padding": "0.6rem",
                                        "background": "#020617",
                                        "border": "1px solid rgba(148,163,184,0.4)",
                                    },
                                    children=[
                                        html.Div(
                                            style={
                                                "display": "flex",
                                                "justifyContent": "space-between",
                                                "alignItems": "center",
                                                "marginBottom": "0.3rem",
                                            },
                                            children=[
                                                html.Div(
                                                    "Price + OI Path (last 60 minutes)",
                                                    style={
                                                        "fontSize": "0.8rem",
                                                        "color": "#9ca3af",
                                                    },
                                                ),
                                                dcc.Tabs(
                                                    id="chart-tabs",
                                                    value="price_oi",
                                                    style={
                                                        "fontSize": "0.75rem",
                                                        "height": "32px",
                                                    },
                                                    children=[
                                                        dcc.Tab(
                                                            label="Price / OI",
                                                            value="price_oi",
                                                        ),
                                                        dcc.Tab(
                                                            label="IV / Greeks",
                                                            value="iv_greeks",
                                                        ),
                                                        dcc.Tab(
                                                            label="PCR / Flows",
                                                            value="pcr_flows",
                                                        ),
                                                    ],
                                                ),
                                            ],
                                        ),
                                        dcc.Graph(
                                            id="main-chart",
                                            style={"height": "260px"},
                                            figure={
                                                "data": [],
                                                "layout": {
                                                    "template": "plotly_dark",
                                                    "paper_bgcolor": "#020617",
                                                    "plot_bgcolor": "#020617",
                                                    "xaxis": {"showgrid": False},
                                                    "yaxis": {"showgrid": False},
                                                    "margin": dict(l=40, r=10, t=20, b=30),
                                                },
                                            },
                                        ),
                                    ],
                                ),

                                # Side panel (remarks / summary)
                                html.Div(
                                    id="side-panel",
                                    style={
                                        "borderRadius": "14px",
                                        "padding": "0.65rem 0.7rem",
                                        "background": "#020617",
                                        "border": "1px solid rgba(148,163,184,0.4)",
                                        "display": "flex",
                                        "flexDirection": "column",
                                        "gap": "0.45rem",
                                        "fontSize": "0.8rem",
                                    },
                                    children=[
                                        html.Div(
                                            "AI Alpha Summary",
                                            style={
                                                "fontSize": "0.8rem",
                                                "fontWeight": 600,
                                            },
                                        ),
                                        html.Div(
                                            id="ai-alpha-summary",
                                            children="No signal yet. Waiting for live data…",
                                            style={"color": "#9ca3af", "lineHeight": "1.6"},
                                        ),
                                        html.Hr(style={"borderColor": "rgba(148,163,184,0.4)"}),
                                        html.Div(
                                            "Smart Money Notes",
                                            style={
                                                "fontSize": "0.78rem",
                                                "fontWeight": 600,
                                            },
                                        ),
                                        html.Ul(
                                            id="smart-money-notes",
                                            children=[
                                                html.Li(
                                                    "CE vs PE balance: --",
                                                    style={"marginBottom": "0.15rem"},
                                                ),
                                                html.Li(
                                                    "Key OI cluster: --",
                                                    style={"marginBottom": "0.15rem"},
                                                ),
                                                html.Li(
                                                    "Regime shift status: --",
                                                    style={"marginBottom": "0.15rem"},
                                                ),
                                            ],
                                            style={
                                                "color": "#9ca3af",
                                                "paddingLeft": "1.1rem",
                                                "lineHeight": "1.6",
                                            },
                                        ),
                                    ],
                                ),
                            ],
                        ),

                        # ===== OPTION CHAIN TABLE =====
                        html.Div(
                            id="option-chain-section",
                            style={
                                "borderRadius": "14px",
                                "padding": "0.6rem 0.6rem 0.9rem",
                                "background": "#020617",
                                "border": "1px solid rgba(148,163,184,0.4)",
                                "marginTop": "0.2rem",
                            },
                            children=[
                                html.Div(
                                    style={
                                        "display": "flex",
                                        "justifyContent": "space-between",
                                        "alignItems": "center",
                                        "marginBottom": "0.4rem",
                                    },
                                    children=[
                                        html.Div(
                                            children=[
                                                html.Span(
                                                    "Option Chain – Enriched Alpha View",
                                                    style={
                                                        "fontSize": "0.85rem",
                                                        "fontWeight": 600,
                                                    },
                                                ),
                                                html.Span(
                                                    "  ·  highest confidence rows first",
                                                    style={
                                                        "fontSize": "0.78rem",
                                                        "color": "#9ca3af",
                                                    },
                                                ),
                                            ]
                                        ),
                                        html.Div(
                                            id="chain-meta",
                                            style={"fontSize": "0.75rem", "color": "#6b7280"},
                                            children="Last update: --",
                                        ),
                                    ],
                                ),
                                dash_table.DataTable(
                                    id="option-chain-table",
                                    columns=[
                                        {"name": "Strike", "id": "strike"},
                                        {"name": "CE OI", "id": "ce_oi"},
                                        {"name": "CE ΔOI%", "id": "ce_oi_change_pct"},
                                        {"name": "CE IV", "id": "ce_iv"},
                                        {"name": "CE Δ", "id": "ce_delta"},
                                        {"name": "PE OI", "id": "pe_oi"},
                                        {"name": "PE ΔOI%", "id": "pe_oi_change_pct"},
                                        {"name": "PE IV", "id": "pe_iv"},
                                        {"name": "PE Δ", "id": "pe_delta"},
                                        {"name": "Score", "id": "alpha_score"},
                                        {"name": "Sentiment", "id": "sentiment"},
                                        {"name": "Remarks", "id": "remarks"},
                                    ],
                                    data=[],
                                    sort_action="native",
                                    sort_mode="multi",
                                    page_size=20,
                                    style_as_list_view=True,
                                    style_header={
                                        "backgroundColor": "#020617",
                                        "color": "#9ca3af",
                                        "fontSize": "0.75rem",
                                        "border": "1px solid rgba(55,65,81,0.8)",
                                    },
                                    style_cell={
                                        "backgroundColor": "#020617",
                                        "color": "#e5e7eb",
                                        "border": "1px solid rgba(31,41,55,0.8)",
                                        "fontSize": "0.75rem",
                                        "padding": "0.25rem 0.35rem",
                                        "whiteSpace": "normal",
                                        "height": "auto",
                                    },
                                    style_table={
                                        "maxHeight": "300px",
                                        "overflowY": "auto",
                                    },
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),
    ],
)

# =========================
# DEMO DATA FOR UI PREVIEW
# =========================

DEMO_DATA = {
    "NIFTY 50": {
        "alpha_score": "78.4 %",
        "regime": "Bullish Bias",
        "ce_oi_shift": "+18.2 %",
        "pe_oi_shift": "-12.7 %",
        "iv_regime": "Rising",
        "ce_note": "Call writers adding OI above spot; aggressive build-up on resistance band.",
        "pe_note": "Put shorts covering; downside pressure easing.",
        "gamma_note": "Gamma building around 23000–23200 strikes.",
        "summary": (
            "Smart money leaning bullish in the last 30 minutes. "
            "CE OI is rising with IV uptick, while PE side shows unwinding. "
            "Structure suggests continuation rather than a random spike."
        ),
        "notes": [
            "CE vs PE balance: Calls dominating above key resistance zone.",
            "Key OI cluster: 23000–23200 now acting as evolving support.",
            "Regime shift status: Sideways → Bullish confirmed on intraday lens.",
        ],
        "time": list(range(0, 60, 5)),
        "price": [23000, 23025, 23040, 23070, 23110, 23120, 23140, 23160, 23180, 23210, 23220, 23240],
        "ce_oi": [100, 120, 135, 150, 180, 190, 210, 230, 250, 270, 285, 300],
        "pe_oi": [220, 215, 210, 205, 200, 195, 190, 188, 186, 184, 182, 180],
        "table": [
            {
                "strike": 22800,
                "ce_oi": 9500,
                "ce_oi_change_pct": 4.2,
                "ce_iv": 13.4,
                "ce_delta": 0.18,
                "pe_oi": 18200,
                "pe_oi_change_pct": -3.1,
                "pe_iv": 14.8,
                "pe_delta": -0.82,
                "alpha_score": 62,
                "sentiment": "Support building",
                "remarks": "Fresh PE OI with mild CE unwinding – early support zone.",
            },
            {
                "strike": 23000,
                "ce_oi": 20500,
                "ce_oi_change_pct": 11.8,
                "ce_iv": 14.6,
                "ce_delta": 0.39,
                "pe_oi": 17600,
                "pe_oi_change_pct": -8.9,
                "pe_iv": 15.2,
                "pe_delta": -0.61,
                "alpha_score": 82,
                "sentiment": "Bullish",
                "remarks": "Strong one-sided CE build-up, PE unwinding – smart money breakout zone.",
            },
            {
                "strike": 23200,
                "ce_oi": 18800,
                "ce_oi_change_pct": 16.5,
                "ce_iv": 15.1,
                "ce_delta": 0.47,
                "pe_oi": 14300,
                "pe_oi_change_pct": -6.4,
                "pe_iv": 15.9,
                "pe_delta": -0.53,
                "alpha_score": 88,
                "sentiment": "High-conviction",
                "remarks": "Gamma building, IV rising, CE OI vertical – fat-tail breakout risk.",
            },
            {
                "strike": 23400,
                "ce_oi": 13200,
                "ce_oi_change_pct": 3.1,
                "ce_iv": 14.3,
                "ce_delta": 0.29,
                "pe_oi": 16100,
                "pe_oi_change_pct": 2.5,
                "pe_iv": 15.0,
                "pe_delta": -0.71,
                "alpha_score": 55,
                "sentiment": "Balanced",
                "remarks": "Both sides adding; possible short-term pause / intraday range.",
            },
        ],
    },
    "BANKNIFTY": {
        "alpha_score": "64.2 %",
        "regime": "Mild Bullish Bias",
        "ce_oi_shift": "+9.7 %",
        "pe_oi_shift": "-4.3 %",
        "iv_regime": "Stable",
        "ce_note": "CE OI rising but slower than NIFTY – moderate risk-on.",
        "pe_note": "PE unwinding small; downside still partially protected.",
        "gamma_note": "Gamma concentrated near round-number strike.",
        "summary": (
            "Bank index shows a softer bullish bias. Good for trend-following, "
            "but not a full-blown breakout profile yet."
        ),
        "notes": [
            "CE vs PE balance: Slightly in favour of calls.",
            "Key OI cluster: Round psychological strike acting as pivot.",
            "Regime shift status: Sideways → Slightly bullish, needs confirmation.",
        ],
        "time": list(range(0, 60, 5)),
        "price": [49000, 49030, 49010, 49080, 49120, 49140, 49110, 49150, 49200, 49240, 49210, 49260],
        "ce_oi": [80, 90, 95, 110, 118, 125, 130, 137, 145, 150, 156, 160],
        "pe_oi": [160, 158, 157, 155, 154, 153, 150, 149, 148, 147, 146, 145],
        "table": [],
    },
    "FINNIFTY": {
        "alpha_score": "51.0 %",
        "regime": "Sideways / No Edge",
        "ce_oi_shift": "+2.1 %",
        "pe_oi_shift": "+1.4 %",
        "iv_regime": "Flat",
        "ce_note": "Both CE and PE adding modestly – non-directional.",
        "pe_note": "No clear unwind or panic; typical range day profile.",
        "gamma_note": "No concentrated gamma pockets.",
        "summary": (
            "FINNIFTY looks like a typical range-bound session with no strong "
            "smart money tilt. Better avoided for high-conviction trades."
        ),
        "notes": [
            "CE vs PE balance: Fairly balanced OI on both sides.",
            "Key OI cluster: Multiple minor clusters, no dominant wall.",
            "Regime shift status: Still sideways; wait for structural change.",
        ],
        "time": list(range(0, 60, 5)),
        "price": [21000, 21020, 21010, 21025, 21030, 21035, 21020, 21040, 21045, 21050, 21055, 21060],
        "ce_oi": [60, 62, 64, 65, 66, 68, 69, 70, 72, 73, 74, 75],
        "pe_oi": [59, 60, 61, 62, 62, 63, 63, 64, 64, 65, 65, 66],
        "table": [],
    },
}


def build_demo_figure(symbol: str, tab: str):
    """Create a simple figure for the given symbol + tab using DEMO_DATA."""
    data = DEMO_DATA.get(symbol, DEMO_DATA["NIFTY 50"])
    x = data["time"]
    price = data["price"]
    ce_oi = data["ce_oi"]
    pe_oi = data["pe_oi"]

    fig = go.Figure()

    if tab == "iv_greeks":
        fig.add_trace(
            go.Scatter(
                x=x,
                y=[v / 100.0 for v in price],
                name="IV (approx)",
                mode="lines+markers",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=[0.2 + 0.01 * i for i in range(len(x))],
                name="Gamma (dummy)",
                mode="lines",
            )
        )
    elif tab == "pcr_flows":
        pcr = [pe / ce if ce else None for ce, pe in zip(ce_oi, pe_oi)]
        fig.add_trace(
            go.Scatter(
                x=x,
                y=pcr,
                name="PCR (PE/CE)",
                mode="lines+markers",
            )
        )
    else:
        fig.add_trace(
            go.Scatter(
                x=x,
                y=price,
                name="Price",
                mode="lines+markers",
                yaxis="y1",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=ce_oi,
                name="CE OI (scaled)",
                mode="lines",
                yaxis="y2",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=pe_oi,
                name="PE OI (scaled)",
                mode="lines",
                yaxis="y2",
            )
        )
        fig.update_layout(
            yaxis2=dict(
                overlaying="y",
                side="right",
                showgrid=False,
            )
        )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#020617",
        plot_bgcolor="#020617",
        xaxis=dict(showgrid=False, title="Time (demo index)"),
        yaxis=dict(showgrid=False),
        margin=dict(l=40, r=20, t=20, b=35),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    return fig


@app.callback(
    [
        Output("main-chart", "figure"),
        Output("option-chain-table", "data"),
        Output("banner-symbol", "children"),
        Output("banner-alpha-score", "children"),
        Output("banner-regime", "children"),
        Output("stat-ce-oi-shift", "children"),
        Output("stat-pe-oi-shift", "children"),
        Output("stat-iv-regime", "children"),
        Output("stat-ce-note", "children"),
        Output("stat-pe-note", "children"),
        Output("stat-gamma-note", "children"),
        Output("ai-alpha-summary", "children"),
        Output("smart-money-notes", "children"),
        Output("chain-meta", "children"),
    ],
    [
        Input("symbol-dropdown", "value"),
        Input("chart-tabs", "value"),
        Input("demo-interval", "n_intervals"),
    ],
)
def update_demo_dashboard(symbol, tab, n):
    """Drive the whole UI from static demo data."""
    if symbol is None:
        symbol = "NIFTY 50"
    data = DEMO_DATA.get(symbol, DEMO_DATA["NIFTY 50"])

    fig = build_demo_figure(symbol, tab)
    table_data = data["table"]

    notes_children = [
        html.Li(note, style={"marginBottom": "0.15rem"}) for note in data["notes"]
    ]

    last_update = datetime.now().strftime("%H:%M:%S")
    meta_text = f"Last update: demo · {last_update}"

    return (
        fig,
        table_data,
        symbol,
        data["alpha_score"],
        f"Regime: {data['regime']}",
        data["ce_oi_shift"],
        data["pe_oi_shift"],
        f"Regime: {data['iv_regime']}",
        f"Side: {data['ce_note']}",
        f"Side: {data['pe_note']}",
        f"Pressure: {data['gamma_note']}",
        data["summary"],
        notes_children,
        meta_text,
    )


if __name__ == "__main__":
    app.run(debug=True, port=8050)
