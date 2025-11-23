# -*- coding: utf-8 -*-
# KUBERA — Execution/Trade core + banner scoreboard & CE/PE strength + popups
# Changes (this build):
# - RR(25Δ) sentiment bar in banner center (red/gray/green ▓/░ meter).
# - Scrolling regime ticker on top: "SYMBOL — moving from Sideways to Bullish" etc.
# - Regime fusion: price slope (last 60m) + RR(25Δ) bias with stable thresholds.
# - LTP fix: fetch ce_ltp/pe_ltp then select based on inferred side in Python.
# - UI action: "Send back selected" to set status='sent_back'.
# - Banner layout: md=3 | md=6 | md=3; consistent left/center/right alignment.
# - Exit LTP lookup now uses latest ts for (symbol, expiry, strike).
# - SR Panel: 3x Support & 3x Resistance with strength%, colored shift arrows, shift text.
# - SR Panel EXTRAS: mini-sparkline + touch markers + compact S/R table.
# - LTP_LOOKBACK_HOURS env; sl_type/tsl_type columns; percent formatting for SL/TP/TSL;
#   UI_SYMBOL_FILTER (optional); popups for sent_back.
# - NEW (this update): Execution Log & Trade Monitor show ONLY today's entries (market-local date).
#   Popups/toasts also restricted to today. Fixed extra ')' in send-back callback.
# - FIX: Replaced html.Style (not available) with dcc.Markdown(<style>..., dangerously_allow_html=True).
# - CLEANUP (Render-ready): single Dash app instance + exported `server` for gunicorn.
import dash_bootstrap_components as dbc

import os
import re
import json
import time
import uuid
import threading
import collections
from urllib.parse import urlparse, parse_qs, unquote
from datetime import datetime, timezone, timedelta
from datetime import time as dtime
from decimal import Decimal
from typing import Optional, List, Tuple

import pandas as pd
import numpy as np
from pandas import Timestamp
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool, PoolError
from dotenv import load_dotenv

import dash
from dash import html, dcc, dash_table, Input, Output, State, no_update
import dash_bootstrap_components as dbc
import plotly.graph_objs as go

# ----------------------------------
# Env
# ----------------------------------
load_dotenv()

SYMBOL                 = os.getenv("SYMBOL", "NIFTY 50")
MARKET_TZ              = os.getenv("MARKET_TZ", "Asia/Kolkata")
MARKET_OPEN            = os.getenv("MARKET_OPEN", "09:15")
MARKET_CLOSE           = os.getenv("MARKET_CLOSE", "15:30")
STALE_MINUTES          = int(os.getenv("STALE_MINUTES", "8"))
DB_STATEMENT_TIMEOUT_MS= int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "5000"))
DB_IDLE_TX_TIMEOUT_MS  = int(os.getenv("DB_IDLE_TX_TIMEOUT_MS", "5000"))
DB_LOCK_TIMEOUT_MS     = int(os.getenv("DB_LOCK_TIMEOUT_MS", "1000"))
ENABLE_DB_HEALTHCHECK  = os.getenv("ENABLE_DB_HEALTHCHECK", "1").lower() in ("1","true","on","yes")

# Panel/feature defaults (env-overridable)
SR_LOOKBACK_MIN        = int(os.getenv("SR_LOOKBACK_MIN", "480"))     # 8 hours
SR_PIVOT_WIN           = int(os.getenv("SR_PIVOT_WIN", "5"))
SR_ATR_WIN             = int(os.getenv("SR_ATR_WIN", "14"))
SR_PROX_ATR            = float(os.getenv("SR_PROX_ATR", "0.20"))      # proximity band for touches
SR_TOP_K               = int(os.getenv("SR_TOP_K", "3"))
SR_SPARK_BARS          = int(os.getenv("SR_SPARK_BARS", "120"))       # bars in mini sparkline

# NEW: LTP lookback window and optional UI symbol filter
LTP_LOOKBACK_HOURS     = int(os.getenv("LTP_LOOKBACK_HOURS", "6"))
UI_SYMBOL_FILTER       = [s.strip() for s in os.getenv("UI_SYMBOL_FILTER", "").split(",") if s.strip()]

# Regime fusion knobs (can tune without .env)
REGIME_LOOKBACK_MIN    = int(os.getenv("REGIME_LOOKBACK_MIN", "60"))
REGIME_SLOPE_EPS_PCT   = float(os.getenv("REGIME_SLOPE_EPS_PCT", "0.10"))  # 0.10% of price over lookback
RR_BULL_THRES          = float(os.getenv("RR_BULL_THRES", "2.0"))
RR_BEAR_THRES          = float(os.getenv("RR_BEAR_THRES", "-2.0"))

# Dev tools
DEVLOG_ON     = os.getenv("DEVLOG_ON", "0").lower() in ("1","true","on","yes")
DEVLOG_SINK   = os.getenv("DEVLOG_SINK", "stdout").lower()        # stdout|memory|db
DEVLOG_MAX    = int(os.getenv("DEVLOG_MAX", "4000"))
DEVLOG_TABLE  = os.getenv("DEVLOG_TABLE", "public.kubera_devlog")
_DEVLOG_BUF   = collections.deque(maxlen=DEVLOG_MAX)


def _json_safe(o):
    if isinstance(o, Decimal):
        try:
            return float(o)
        except Exception:
            return str(o)
    if isinstance(o, (datetime, Timestamp)):
        return o.isoformat()
    if isinstance(o, (set,)):
        return list(o)
    if hasattr(o, "tolist"):
        return o.tolist()
    return str(o)


def _now_iso():
    try:
        return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    except Exception:
        return str(datetime.utcnow())


_DB_CTX = threading.local()
setattr(_DB_CTX, "in_conn", False)
_LAST_DB_ERR = ""


def devlog(event: str, **fields):
    if not DEVLOG_ON:
        return
    rec = {"ts": _now_iso(), "event": event}
    rec.update(fields or {})
    if DEVLOG_SINK in ("memory", "stdout"):
        _DEVLOG_BUF.append(rec)
    if DEVLOG_SINK == "stdout":
        try:
            print(json.dumps(rec, separators=(',', ':'), ensure_ascii=False, default=_json_safe))
        except Exception:
            print(f"[devlog]{rec}")
    if DEVLOG_SINK == "db":
        if getattr(_DB_CTX, "in_conn", False):
            _DEVLOG_BUF.append(rec)
            return
        tbl = DEVLOG_TABLE if re.fullmatch(r"[A-Za-z0-9_.]+", DEVLOG_TABLE or "") else "public.kubera_devlog"
        try:
            exec_sql(
                f'INSERT INTO {tbl} (ts, event, payload) VALUES (now(), %s, %s)',
                [event, json.dumps(rec, ensure_ascii=False, default=_json_safe)],
            )
        except Exception:
            pass


# ----------------------------------
# DB Setup + helpers
# ----------------------------------
def _db_params():
    def _int(x, default):
        try:
            return int(x)
        except Exception:
            return default

    url = os.getenv("DATABASE_URL")
    if url:
        p = urlparse(url)
        q = {k: v[0] for k, v in parse_qs(p.query).items()}
        user = unquote(p.username) if p.username else None
        password = unquote(p.password) if p.password else None
        user = os.getenv("DB_USER", user)
        password = os.getenv("DB_PASSWORD", password)
        return dict(
            host=p.hostname or os.getenv("DB_HOST", "127.0.0.1"),
            port=p.port or int(os.getenv("DB_PORT", 5432)),
            dbname=(p.path or "").lstrip("/") or os.getenv("DB_NAME", "osa_db"),
            user=user,
            password=password,
            sslmode=q.get("sslmode") or os.getenv("DB_SSLMODE", "disable"),
            connect_timeout=_int(q.get("connect_timeout") or os.getenv("DB_CONNECT_TIMEOUT_SEC", "3"), 3),
            application_name=os.getenv("DB_APPNAME", "kubera"),
        )
    return dict(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "osa_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        sslmode=os.getenv("DB_SSLMODE", "disable"),
        connect_timeout=int(os.getenv("DB_CONNECT_TIMEOUT_SEC", "3")),
        application_name=os.getenv("DB_APPNAME", "kubera"),
    )


_DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "40"))
_DB_POOL_WAIT_SECS = float(os.getenv("DB_POOL_WAIT_SECS", "20"))
_DB_SEMA = threading.BoundedSemaphore(_DB_POOL_MAX)
_POOL = None
_POOL_LOCK = threading.Lock()


def _reset_pool(reason: str = ""):
    global _POOL
    with _POOL_LOCK:
        try:
            if _POOL is not None:
                try:
                    _POOL.closeall()
                except Exception:
                    pass
        finally:
            _POOL = None
    devlog("db.pool.reset", reason=reason)


def _get_pool():
    global _POOL
    if _POOL is not None:
        return _POOL
    with _POOL_LOCK:
        if _POOL is None:
            try:
                _POOL = SimpleConnectionPool(minconn=1, maxconn=_DB_POOL_MAX, **_db_params())
                devlog("db.pool.init", ok=True, max=_DB_POOL_MAX)
            except Exception as e:
                _POOL = None
                devlog("db.pool.init", ok=False, err=str(e))
                raise
    return _POOL


def _conn():
    class _Ctx:
        def __enter__(self):
            self.pool = None
            self._sema = False
            t0 = time.monotonic()
            _DB_SEMA.acquire()
            self._sema = True
            while True:
                try:
                    self.pool = _get_pool()
                    self.c = self.pool.getconn()
                    try:
                        with self.c.cursor() as _ping:
                            _ping.execute("SELECT 1;")
                    except Exception:
                        try:
                            self.pool.putconn(self.c, close=True)
                        except Exception:
                            try:
                                self.c.close()
                            except Exception:
                                pass
                        _reset_pool("bad_socket_ping")
                        raise
                    break
                except Exception as e:
                    waited = time.monotonic() - t0
                    if waited >= _DB_POOL_WAIT_SECS:
                        if self._sema:
                            try:
                                _DB_SEMA.release()
                            except Exception:
                                pass
                        devlog("db.pool.acquire.timeout", waited_s=waited, err=str(e))
                        raise
                    _reset_pool(f"acquire_retry_{type(e).__name__}")
                    time.sleep(0.1)

            try:
                self.c.set_client_encoding("UTF8")
            except Exception:
                pass
            setattr(_DB_CTX, "in_conn", True)
            try:
                with self.c.cursor() as _cur:
                    _cur.execute(
                        "SET SESSION statement_timeout = %s; "
                        "SET SESSION idle_in_transaction_session_timeout = %s; "
                        "SET SESSION lock_timeout = %s;",
                        (f"{DB_STATEMENT_TIMEOUT_MS}ms", f"{DB_IDLE_TX_TIMEOUT_MS}ms", f"{DB_LOCK_TIMEOUT_MS}ms"),
                    )
            except Exception as e:
                devlog("db.session.timeout_set.fail", err=str(e))
            devlog("db.conn.open")
            return self.c

        def __exit__(self, exc_type, exc, tb):
            try:
                if exc_type:
                    try:
                        self.c.rollback()
                    except Exception:
                        pass
                else:
                    try:
                        self.c.commit()
                    except Exception:
                        pass
            finally:
                try:
                    bad = bool(exc_type)
                    self.pool.putconn(self.c, close=bad)
                except Exception:
                    try:
                        self.c.close()
                    except Exception:
                        pass
                if self._sema:
                    try:
                        _DB_SEMA.release()
                    except Exception:
                        pass
                setattr(_DB_CTX, "in_conn", False)
                devlog("db.conn.close", error=bool(exc_type))

    return _Ctx()


def db_ready(max_ms: int = 800) -> bool:
    global _LAST_DB_ERR
    if not ENABLE_DB_HEALTHCHECK:
        _LAST_DB_ERR = ""
        return True
    try:
        with _conn() as c:
            with c.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = %s;", (f"{max_ms}ms",))
                cur.execute("SELECT 1;")
        _LAST_DB_ERR = ""
        devlog("db.health.ok", budget_ms=max_ms)
        return True
    except Exception as e:
        _LAST_DB_ERR = str(e)
        devlog("db.health.fail", budget_ms=max_ms, err=_LAST_DB_ERR)
        return False


def fetch_df(sql, params=None, timeout_ms: Optional[int] = None):
    t0 = time.monotonic()
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if timeout_ms:
            try:
                cur.execute("SET LOCAL statement_timeout = %s;", (f"{int(timeout_ms)}ms",))
            except Exception:
                pass
        cur.execute(sql, params or None)
        rows = cur.fetchall() or []
        cols = [d[0] for d in (cur.description or [])]
        try:
            df = pd.DataFrame.from_records(rows, columns=cols)
        except Exception:
            df = pd.DataFrame(rows)
            if cols and (len(df.columns) != len(cols)):
                df = pd.DataFrame.from_records(rows, columns=cols)
    devlog("db.fetch_df", ms=int((time.monotonic() - t0) * 1000), rows=len(rows), cols=len(df.columns))
    return df


def fetch_rows(sql, params=None, timeout_ms: Optional[int] = None):
    t0 = time.monotonic()
    with _conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        if timeout_ms:
            try:
                cur.execute("SET LOCAL statement_timeout = %s;", (f"{int(timeout_ms)}ms",))
            except Exception:
                pass
        cur.execute(sql, params or None)
        rows = cur.fetchall() or []
    devlog("db.fetch_rows", ms=int((time.monotonic() - t0) * 1000), rows=len(rows))
    return [dict(r) for r in rows]


def exec_sql(sql, params=None):
    t0 = time.monotonic()
    with _conn() as conn, conn.cursor() as cur:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
    devlog("db.exec_sql", ms=int((time.monotonic() - t0) * 1000))


# ----------------- Market status & freshness -----------------
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


def _hhmm(s: str) -> dtime:
    h, m = s.split(":")
    return dtime(int(h), int(m))


def _now_mkt():
    if ZoneInfo:
        return datetime.now(tz=ZoneInfo(MARKET_TZ))
    return datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(hours=5, minutes=30)


def _today_local():
    """Market-local 'today' date for consistent day filtering across UI."""
    return _now_mkt().date()


def is_market_open() -> bool:
    now = _now_mkt()
    if now.weekday() >= 5:
        return False
    if ZoneInfo:
        open_dt = datetime.combine(now.date(), _hhmm(MARKET_OPEN), tzinfo=ZoneInfo(MARKET_TZ))
        close_dt = datetime.combine(now.date(), _hhmm(MARKET_CLOSE), tzinfo=ZoneInfo(MARKET_TZ))
    else:
        base = (
            datetime.combine(now.date(), dtime(0, 0)).replace(tzinfo=timezone.utc)
            + timedelta(hours=5, minutes=30)
        )
        open_dt = base + timedelta(
            hours=int(MARKET_OPEN.split(":")[0]), minutes=int(MARKET_OPEN.split(":")[1])
        )
        close_dt = base + timedelta(
            hours=int(MARKET_CLOSE.split(":")[0]), minutes=int(MARKET_CLOSE.split(":")[1])
        )
    return open_dt <= now <= close_dt


def last_tick_age_minutes(conn, symbol) -> Optional[float]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXTRACT(EPOCH FROM (now() - MAX("timestamp")))/60.0
            FROM public.option_chain_oi
            WHERE UPPER(symbol)=UPPER(%s)
        """,
            (str(symbol or "").upper(),),
        )
        v = cur.fetchone()[0]
        return float(v) if v is not None else None


def _ts_local_str(x):
    try:
        return pd.to_datetime(x).tz_localize(None).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return pd.to_datetime(x).tz_convert(None).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(x)


def _fresh_and_open(symbol: str = SYMBOL):
    try:
        with _conn() as c:
            age = last_tick_age_minutes(c, symbol)
    except Exception:
        age = None
    mkt_open = is_market_open()
    fresh = (age is not None) and (age <= STALE_MINUTES)
    return (mkt_open and fresh), (float(age) if age is not None else None), mkt_open, fresh


# ----------------------------------
# RR(25Δ) + Regime helpers
# ----------------------------------
def _fetch_latest_rr25d(symbol: str) -> Optional[dict]:
    sql = """
    WITH latest AS (
      SELECT MAX("timestamp") AS ts FROM public.option_chain_oi WHERE UPPER(symbol)=UPPER(%(sym)s)
    ),
    ce AS (
      SELECT o.expiry, o."timestamp", o.ce_iv, o.ce_delta
      FROM public.option_chain_oi o, latest l
      WHERE UPPER(o.symbol)=UPPER(%(sym)s) AND o."timestamp"=l.ts AND o.ce_iv IS NOT NULL AND o.ce_delta IS NOT NULL
      ORDER BY ABS(o.ce_delta - 0.25) ASC NULLS LAST
      LIMIT 1
    ),
    pe AS (
      SELECT o.expiry, o."timestamp", o.pe_iv, o.pe_delta
      FROM public.option_chain_oi o, latest l
      WHERE UPPER(o.symbol)=UPPER(%(sym)s) AND o."timestamp"=l.ts AND o.pe_iv IS NOT NULL AND o.pe_delta IS NOT NULL
      ORDER BY ABS(o.pe_delta + 0.25) ASC NULLS LAST
      LIMIT 1
    )
    SELECT ce.expiry,
           ce."timestamp" AS ts,
           ce.ce_iv       AS iv_25d_ce,
           pe.pe_iv       AS iv_25d_pe,
           (ce.ce_iv - pe.pe_iv) AS rr_25d
    FROM ce CROSS JOIN pe
    LIMIT 1;
    """
    try:
        row = fetch_rows(sql, {"sym": symbol}, timeout_ms=DB_STATEMENT_TIMEOUT_MS)
        if not row:
            return None
        rr = float(row[0]["rr_25d"])
        if rr > RR_BULL_THRES:
            bias = "bullish"
        elif rr < RR_BEAR_THRES:
            bias = "bearish"
        else:
            bias = "neutral"
        return {"rr": rr, "bias": bias, "ts": row[0]["ts"], "expiry": str(row[0]["expiry"])}
    except Exception as e:
        devlog("rr25d.fail", err=str(e))
        return None


def _render_rr25d_bar(rr: Optional[float]):
    from math import isnan

    if rr is None or (isinstance(rr, float) and (not np.isfinite(rr) or isnan(rr))):
        return html.Code(
            "RR(25Δ): n/a",
            style={"color": "#777", "fontFamily": "Roboto Mono, monospace", "fontSize": "14px"},
        )
    rr_clamped = max(-5.0, min(5.0, float(rr)))
    total_slots = 30
    mid = total_slots // 2
    pos = int(round((rr_clamped + 5) / 10 * total_slots))  # 0..30
    left_fill = max(0, mid - pos)
    right_fill = max(0, pos - mid)
    left_bar = "▓" * left_fill + "░" * (mid - left_fill)
    right_bar = "░" * (mid - right_fill) + "▓" * right_fill

    if rr > RR_BULL_THRES:
        color = "green"
        label = "Bullish Skew"
    elif rr < RR_BEAR_THRES:
        color = "crimson"
        label = "Bearish Skew"
    else:
        color = "#777"
        label = "Neutral Skew"

    return html.Div(
        [
            html.Code(
                f"RR(25Δ): {rr:+.1f}  {left_bar}{right_bar}  ← {label}",
                style={"fontFamily": "Roboto Mono, monospace", "fontSize": "14px", "color": color},
            )
        ]
    )


def _price_slope_regime(symbol: str, lookback_min: int = REGIME_LOOKBACK_MIN) -> Tuple[str, Optional[float], Optional[float]]:
    df = _fetch_recent_atm_rows(symbol, lookback_min)
    if df.empty:
        return "flat", None, None
    tf = _resample_last(df, "3min")
    if tf.empty or len(tf) < 3:
        return "flat", None, None
    p0 = float(tf["spot_price"].iloc[0])
    p1 = float(tf["spot_price"].iloc[-1])
    if p0 <= 0 or not np.isfinite(p0) or not np.isfinite(p1):
        return "flat", None, None
    pct = (p1 - p0) / p0 * 100.0
    if pct > REGIME_SLOPE_EPS_PCT:
        return "up", pct, p1
    if pct < -REGIME_SLOPE_EPS_PCT:
        return "down", pct, p1
    return "flat", pct, p1


def _fuse_regime(price_dir: str, rr_bias: Optional[str]) -> str:
    rr_bias = (rr_bias or "neutral").lower()
    if price_dir == "up" and rr_bias == "bullish":
        return "Bullish"
    if price_dir == "down" and rr_bias == "bearish":
        return "Bearish"
    return "Sideways"


def _transition_text(prev_regime: Optional[str], cur_regime: str) -> str:
    to_side = lambda x: {"Bullish": "bullish", "Bearish": "bearish", "Sideways": "sideways"}.get(x, "sideways")
    if not prev_regime or prev_regime == cur_regime:
        return f"{SYMBOL} — {to_side(cur_regime)}"
    return f"{SYMBOL} — moving from {to_side(prev_regime)} to {to_side(cur_regime)}"


# ----------------------------------
# Banner: DB status snapshot
# ----------------------------------
def _db_status_snapshot():
    global _LAST_DB_ERR
    s = {
        "online": False,
        "ping_ms": None,
        "user": None,
        "db": None,
        "addr": None,
        "ver": None,
        "srv_time": None,
        "market_open": is_market_open(),
        "symbol": SYMBOL,
        "age_min": None,
        "err": None,
    }
    try:
        t0 = time.monotonic()
        with _conn() as c:
            ping_ms = int((time.monotonic() - t0) * 1000)
            s["ping_ms"] = ping_ms
            with c.cursor() as cur:
                cur.execute(
                    """
                    SELECT current_user, current_database(),
                           inet_server_addr()::text, inet_server_port(),
                           version(), now()
                """
                )
                u, d, ip, port, ver, now_ts = cur.fetchone()
            s.update(
                {
                    "online": True,
                    "user": u,
                    "db": d,
                    "addr": f"{ip}:{port}",
                    "ver": str(ver).splitlines()[0],
                    "srv_time": str(now_ts),
                }
            )
            try:
                s["age_min"] = last_tick_age_minutes(c, SYMBOL)
            except Exception:
                s["age_min"] = None
        _LAST_DB_ERR = ""
    except Exception as e:
        s["err"] = str(e)
        _LAST_DB_ERR = s["err"]
    return s


def _render_db_status():
    s = _db_status_snapshot()
    color = (
        "success"
        if (s["online"] and s["age_min"] is not None and s["age_min"] <= STALE_MINUTES)
        else ("warning" if s["online"] else "danger")
    )
    title = "DB Online" if s["online"] else "DB Offline"
    age_txt = "—" if s["age_min"] is None else f"{s['age_min']:.2f}m"
    note = (
        "fresh"
        if (s["age_min"] is not None and s["age_min"] <= STALE_MINUTES)
        else ("stale" if s["age_min"] is not None else "n/a")
    )
    mkt = "Open" if s["market_open"] else "Closed"

    body = [
        html.Div(
            [
                dbc.Badge(title, color=color, pill=True, className="me-2"),
                dbc.Badge(
                    f"Market: {mkt}",
                    color=("info" if s["market_open"] else "secondary"),
                    pill=True,
                ),
            ],
            style={"marginBottom": "6px", "textAlign": "right"},
        )
    ]
    if s["online"]:
        body += [
            html.Small(
                f"{(s.get('user') or '')}@{(s.get('db') or '')} ({s.get('addr') or 'n/a'}) • {s.get('ver') or ''}",
                style={"display": "block", "textAlign": "right"},
            )
        ]
        if s.get("ping_ms") is not None:
            body += [
                html.Small(
                    f"Ping: {s['ping_ms']} ms",
                    style={"display": "block", "textAlign": "right"},
                )
            ]
        body += [
            html.Small(
                f"Last tick [{s['symbol']}]: {age_txt} ({note}, ≤{STALE_MINUTES}m)",
                style={"display": "block", "textAlign": "right"},
            )
        ]
    else:
        body += [
            html.Small(
                f"Reason: {s.get('err') or '(no error captured)'}",
                style={"display": "block", "color": "#b91c1c", "textAlign": "right"},
            )
        ]
    return dbc.Card(
        dbc.CardBody(body),
        style={
            "border": "1px solid #e5e7eb",
            "borderRadius": "10px",
            "background": "#f8fafc",
        },
    )


# ----------------------------------
# UI shells
# ----------------------------------
TABLE_STYLE = {
    "overflowX": "auto",
    "borderRadius": "14px",
    "boxShadow": "0 6px 32px #6366f120",
    "backgroundColor": "#F5F8FA",
    "marginTop": "1em",
}
STYLE_HEADER = {
    "backgroundColor": "#6366f1",
    "color": "white",
    "fontWeight": "bold",
    "textAlign": "center",
    "fontSize": "1.0em",
}
STYLE_CELL = {
    "textAlign": "center",
    "backgroundColor": "#f1f5f9",
    "fontSize": "0.95em",
    "padding": "6px",
}
STYLE_DATA_COND = [
    {"if": {"row_index": "odd"}, "backgroundColor": "#eef2ff"},
    {"if": {"row_index": "even"}, "backgroundColor": "#f8fafc"},
]

# === Regime Ticker CSS === (inject via Markdown <style> ... </style>)
ticker_css = dcc.Markdown(
    """
<style>
@keyframes rrTickerScrollLTR {
  0%   { transform: translateX(-100%); }
  100% { transform: translateX(100%); }
}
.ticker-wrap {
  position: relative;
  overflow: hidden;
  white-space: nowrap;
  background: #ffffff;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
}
.ticker {
  display: inline-block;
  padding: 6px 12px;
  font-weight: 900;              /* bold */
  color: #000;                   /* black */
  animation: rrTickerScrollLTR 12s linear infinite;  /* left ➜ right */
  will-change: transform;
  white-space: nowrap;
}
</style>
""",
    dangerously_allow_html=True,
)

# ----------------------------------
# Dash app (single instance, Render-ready)
# ----------------------------------
external_stylesheets = [dbc.themes.BOOTSTRAP]
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP],
                suppress_callback_exceptions=True)
app.title = "KUBERA"
server = app.server   # <-- use this in Render / gunicorn

topbar = dbc.Container(
    [
        ticker_css,
        # === Scrolling Regime Ticker ===
        html.Div(
            [
                html.Div(
                    id="regime-ticker",
                    className="ticker",
                    style={"fontFamily": "Roboto Mono, monospace", "fontWeight": "900"},
                ),
            ],
            className="ticker-wrap",
            style={"marginTop": "10px", "marginBottom": "8px"},
        ),
        html.Div(
            "KUBERA",
            style={
                "textAlign": "center",
                "color": "#dc267f",
                "fontWeight": "900",
                "fontSize": "3.2rem",
                "letterSpacing": "10px",
                "fontFamily": "serif",
                "marginTop": "6px",
                "marginBottom": "6px",
                "textShadow": "0 4px 28px #818cf8, 0 1px 0 #fff",
                "lineHeight": "1.08",
            },
        ),
        # Clean 12-col layout: 3 | 6 | 3
        dbc.Row(
            [
                # Left controls
                dbc.Col(
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label("Auto refresh", className="me-2"),
                                    dcc.Checklist(
                                        id="auto-enabled",
                                        options=[{"label": "Enable", "value": "on"}],
                                        value=["on"],
                                        inline=True,
                                        style={"display": "inline-block"},
                                    ),
                                ],
                                style={"marginBottom": "6px", "textAlign": "left"},
                            ),
                            html.Div(
                                [
                                    html.Label("Seconds", className="me-2"),
                                    dcc.Input(
                                        id="auto-seconds",
                                        type="number",
                                        value=10,
                                        min=3,
                                        step=1,
                                        debounce=True,
                                        style={"width": "120px"},
                                    ),
                                    html.Button(
                                        "Refresh now",
                                        id="refresh-now",
                                        n_clicks=0,
                                        style={
                                            "marginLeft": "8px",
                                            "background": "#1f7aec",
                                            "color": "white",
                                            "border": "none",
                                            "borderRadius": "6px",
                                            "padding": "6px 10px",
                                        },
                                    ),
                                ],
                                style={"textAlign": "left"},
                            ),
                        ]
                    ),
                    md=3,
                ),
                # Center KPIs + RR(25Δ) bar + SR Panel
                dbc.Col(
                    html.Div(
                        [
                            html.Div(
                                id="banner-kpis",
                                style={
                                    "border": "1px solid #e5e7eb",
                                    "borderRadius": "8px",
                                    "padding": "8px",
                                    "minHeight": "64px",
                                    "backgroundColor": "#f8fafc",
                                    "textAlign": "center",
                                },
                            ),
                            html.Div(
                                id="rr25d-bar",
                                style={"marginTop": "6px", "textAlign": "center"},
                            ),
                            dbc.Button(
                                id="toggle-popups",
                                color="primary",
                                size="sm",
                                n_clicks=0,
                                children="Popups: ON",
                                className="mt-2",
                            ),
                            # === SR PANEL (new) ===
                            html.Div(
                                id="sr-box",
                                className="mt-3",
                                style={
                                    "border": "1px solid #e5e7eb",
                                    "borderRadius": "10px",
                                    "background": "#ffffff",
                                    "padding": "8px",
                                },
                            ),
                        ]
                    ),
                    md=6,
                ),
                # Right DB status
                dbc.Col(
                    html.Div(
                        id="db-status-box",
                        style={
                            "border": "1px dashed #94a3b8",
                            "borderRadius": "8px",
                            "padding": "8px 10px",
                            "minHeight": "64px",
                            "backgroundColor": "#f8fafc",
                            "fontSize": "0.92em",
                        },
                    ),
                    md=3,
                ),
            ],
            align="center",
        ),
        dcc.Interval(id="tick", interval=10_000, disabled=False),
        html.Div(
            id="global-status",
            style={"textAlign": "center", "color": "#444", "marginBottom": "10px"},
        ),
        # popup state + toasts mount point
        dcc.Store(id="popups-enabled", data=True),
        dcc.Store(id="exec-last-seen", data={"ids": []}),
        dcc.Store(id="regime-store", data={"prev": None, "cur": None}),
        html.Div(
            id="popup-container",
            style={"position": "fixed", "top": "10px", "right": "10px", "zIndex": 9999},
        ),
    ],
    fluid=True,
)

tabs = dcc.Tabs(
    id="main-tabs",
    value="tab-exec",
    parent_className="custom-tabs",
    className="custom-tabs-container",
    children=[
        dcc.Tab(label="Execution Log", value="tab-exec"),
        dcc.Tab(label="Trade Monitor", value="tab-trade"),
        dcc.Tab(label="Dev Tools", value="tab-dev"),
    ],
)

tab_exec_body = dbc.Container(
    [
        dash_table.DataTable(
            id="exec-table",
            columns=[],
            data=[],
            page_size=30,
            sort_action="native",
            filter_action="native",
            style_table=TABLE_STYLE,
            style_header=STYLE_HEADER,
            style_cell=STYLE_CELL,
            style_as_list_view=True,
        ),
        html.Div(id="exec-status", style={"marginTop": "6px", "color": "#555"}),
    ],
    fluid=True,
)

tab_trade_body = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        [
                            html.Button(
                                "Exit selected",
                                id="exit-selected",
                                n_clicks=0,
                                style={
                                    "background": "#ef4444",
                                    "color": "white",
                                    "border": "none",
                                    "borderRadius": "6px",
                                    "padding": "8px 12px",
                                },
                            ),
                            html.Button(
                                "Send back selected",
                                id="sendback-selected",
                                n_clicks=0,
                                style={
                                    "marginLeft": "8px",
                                    "background": "#f59e0b",
                                    "color": "white",
                                    "border": "none",
                                    "borderRadius": "6px",
                                    "padding": "8px 12px",
                                },
                            ),
                        ]
                    ),
                    md=12,
                ),
            ]
        ),
        html.Br(),
        dash_table.DataTable(
            id="trade-table",
            columns=[
                {"name": "id", "id": "id", "hideable": True},
                {"name": "timestamp", "id": "timestamp"},
                {"name": "symbol", "id": "symbol"},
                {"name": "expiry", "id": "expiry"},
                {"name": "strike", "id": "strike"},
                {"name": "strike_type", "id": "strike_type"},
                {"name": "score", "id": "score"},
                {"name": "CE/PE", "id": "option_side"},
                {"name": "LTP", "id": "ltp"},
                {"name": "action", "id": "suggested_action"},
                {"name": "source", "id": "source"},
                {"name": "stop_loss", "id": "stop_loss"},
                {"name": "trailing_sl", "id": "trailing_sl"},
                {"name": "target", "id": "target"},
                {"name": "sl_type", "id": "sl_type"},
                {"name": "tsl_type", "id": "tsl_type"},
                {"name": "iv_on_entry", "id": "iv_on_entry"},
                {"name": "ma_stack_on_entry", "id": "ma_stack_on_entry"},
                {"name": "status", "id": "status"},
                {"name": "badge", "id": "tm_badge"},
                {"name": "copilot_comment", "id": "copilot_comment"},
            ],
            data=[],
            row_selectable="multi",
            selected_rows=[],
            page_size=25,
            sort_action="native",
            filter_action="native",
            style_table=TABLE_STYLE | {"height": "58vh", "overflowY": "auto"},
            style_cell=STYLE_CELL,
            style_header=STYLE_HEADER,
            style_data_conditional=STYLE_DATA_COND,
        ),
        html.Div(id="trade-status", style={"marginTop": "6px", "color": "#555"}),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        id="trade-ema-card",
                        style={
                            "padding": "8px 10px",
                            "border": "1px solid #eee",
                            "borderRadius": "8px",
                        },
                    ),
                    md=12,
                )
            ]
        ),
        dcc.Graph(
            id="trade-ema-chart",
            figure=go.Figure(),
            config={"displayModeBar": False},
        ),
    ],
    fluid=True,
)

tab_dev_body = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        [
                            html.Button(
                                "Clear In-Memory Logs",
                                id="devlog-clear",
                                n_clicks=0,
                                style={
                                    "background": "#ef4444",
                                    "color": "white",
                                    "border": "none",
                                    "borderRadius": "6px",
                                    "padding": "8px 12px",
                                },
                            )
                        ]
                    ),
                    md=3,
                ),
                dbc.Col(
                    html.Div(
                        id="devlog-status",
                        style={"paddingTop": "10px", "color": "#555"},
                    ),
                    md=9,
                ),
            ]
        ),
        dash_table.DataTable(
            id="devlog-table",
            columns=[
                {"name": "ts", "id": "ts"},
                {"name": "event", "id": "event"},
                {"name": "payload", "id": "payload"},
            ],
            data=[],
            page_size=30,
            style_table=TABLE_STYLE | {"height": "60vh", "overflowY": "auto"},
            style_header=STYLE_HEADER,
            style_cell=STYLE_CELL,
            style_data_conditional=STYLE_DATA_COND,
        ),
    ],
    fluid=True,
)

app.layout = html.Div(
    style={
        "background": "linear-gradient(135deg, #F8FAFC 0%, #e0e7ff 100%)",
        "minHeight": "100vh",
        "paddingBottom": "40px",
    },
    children=[topbar, tabs, html.Div(id="tab-content")],
)


# ----------------------------------
# Data helpers
# ----------------------------------
def _latest_ltp_subquery(hours_back: int = LTP_LOOKBACK_HOURS) -> str:
    return f"""
      SELECT DISTINCT ON (symbol, expiry, strike)
             symbol, expiry, strike,
             "timestamp" AS live_ts, spot_price, ce_ltp, pe_ltp
      FROM public.option_chain_oi
      WHERE "timestamp" > NOW() - INTERVAL '{int(hours_back)} hours'
      ORDER BY symbol, expiry, strike, "timestamp" DESC
    """


def _normalize_side(x):
    t = (str(x or "").strip().upper())
    if t in ("CE", "CALL", "C", "CALLS"):
        return "CE"
    if t in ("PE", "PUT", "P", "PUTS"):
        return "PE"
    return None


def _infer_side_text(
    suggested_action: str | None,
    remarks: str | None,
    snapshot: dict | None = None,
    strike_type: str | None = None,
) -> str | None:
    if snapshot:
        raw = snapshot.get("side") or (snapshot.get("llm_output") or {}).get("side")
        side = _normalize_side(raw)
        if side:
            return side
    side = _normalize_side(strike_type)
    if side:
        return side
    blob = f" {(suggested_action or '')} {(remarks or '')} ".upper()
    if re.search(r"\b(CE|CALL|C)\b", blob):
        return "CE"
    if re.search(r"\b(PE|PUT|P)\b", blob):
        return "PE"
    return None


def _infer_side_for_row(r: dict) -> str | None:
    st = (r.get("strike_type") or "").strip().upper()
    if st in ("CE", "PE"):
        return st
    snap = {}
    try:
        if isinstance(r.get("decision_snapshot"), (str, bytes)):
            snap = json.loads(r["decision_snapshot"] or "{}")
    except Exception:
        pass
    side = _infer_side_text(r.get("suggested_action"), r.get("remarks"), snap)
    side = (side or "").upper()
    return side if side in ("CE", "PE") else None


def open_trades_rows():
    """
    Today's (market-local) open/queued trades, optionally symbol-filtered,
    with latest CE/PE LTP joined from recent chain data.
    """
    today = _today_local()
    sql = f"""
        WITH e AS (
          SELECT e.*,
                 NULLIF(trim(lower(e.status)), '') AS status_norm
          FROM public.execution_log e
          WHERE COALESCE(NULLIF(trim(lower(e.status)),''),'open') IN ('queued','open')
            AND e.expiry >= CURRENT_DATE
            AND DATE(e."timestamp") = %s
          ORDER BY e."timestamp" DESC
          LIMIT 600
        ),
        ol AS ({_latest_ltp_subquery(LTP_LOOKBACK_HOURS)})
        SELECT
          e.id, e."timestamp", e.symbol, e.expiry, e.strike, e.strike_type,
          e.score, e.suggested_action, e.source, e.stop_loss, e.trailing_sl,
          e.target, e.sl_type, e.tsl_type,
          e.iv_on_entry, e.ma_stack_on_entry, e.status, e.remarks,
          e.decision_snapshot::text AS decision_snapshot,
          ol.ce_ltp AS ce_ltp,
          ol.pe_ltp AS pe_ltp
        FROM e
        LEFT JOIN ol
          ON UPPER(ol.symbol)=UPPER(e.symbol)
         AND ol.expiry=e.expiry
         AND ol.strike=e.strike::double precision
        ORDER BY e."timestamp" DESC
    """
    rows = fetch_rows(sql, [today])
    if not rows:
        return []
    if UI_SYMBOL_FILTER:
        allow = {x.upper() for x in UI_SYMBOL_FILTER}
        rows = [r for r in rows if str(r.get("symbol", "")).upper() in allow]
    out = []
    for r in rows:
        r["timestamp"] = _ts_local_str(r.get("timestamp"))
        exp = r.get("expiry")
        if exp is not None and not isinstance(exp, str):
            try:
                r["expiry"] = str(getattr(exp, "date", lambda: exp)())
            except Exception:
                r["expiry"] = str(exp)
        side = _infer_side_for_row(r)
        r["option_side"] = side or ""
        ce = r.pop("ce_ltp", None)
        pe = r.pop("pe_ltp", None)
        r["ltp"] = (
            float(ce)
            if (side == "CE" and ce is not None)
            else (
                float(pe)
                if (side == "PE" and pe is not None)
                else (float(ce) if ce is not None else (float(pe) if pe is not None else None))
            )
        )
        for k, caster in (
            ("strike", float),
            ("ltp", float),
            ("iv_on_entry", float),
            ("stop_loss", float),
            ("trailing_sl", float),
            ("target", float),
        ):
            v = r.get(k)
            if v is not None:
                try:
                    r[k] = caster(v)
                except Exception:
                    pass
        if r.get("score") is not None:
            try:
                r["score"] = int(float(r["score"]))
            except Exception:
                pass

        def _pct(v):
            try:
                return f"{float(v) * 100:.1f}%"
            except Exception:
                return v

        if r.get("stop_loss") is not None:
            r["stop_loss"] = _pct(r["stop_loss"])
        if r.get("trailing_sl") is not None:
            r["trailing_sl"] = _pct(r["trailing_sl"])
        if r.get("target") is not None:
            r["target"] = _pct(r["target"])
        for k in ("sl_type", "tsl_type"):
            v = r.get(k)
            if v is not None:
                r[k] = str(v)
        r.setdefault("tm_badge", "—")
        out.append(r)
    devlog("rows.open_trades", count=len(out))
    return out


def latest_option_ltp_conn(conn, symbol: str, expiry: str, strike: float, side: str):
    col = "ce_ltp" if side.upper() == "CE" else "pe_ltp"
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {col} AS ltp
            FROM public.option_chain_oi
            WHERE UPPER(symbol)=UPPER(%s) AND expiry=%s AND strike=%s
              AND "timestamp" = (
                    SELECT MAX("timestamp")
                    FROM public.option_chain_oi
                    WHERE UPPER(symbol)=UPPER(%s) AND expiry=%s AND strike=%s
              )
            LIMIT 1
        """,
            (symbol, expiry, float(strike), symbol, expiry, float(strike)),
        )
        row = cur.fetchone()
        if not row:
            return None
        try:
            return None if row[0] is None else float(row[0])
        except Exception:
            return None


def exit_trades_by_id(trade_ids: list[int]):
    if not trade_ids:
        return
    df = fetch_df(
        """
        SELECT id, symbol, expiry, strike, suggested_action, remarks, decision_snapshot::text AS snap
        FROM public.execution_log WHERE id = ANY(%s)
    """,
        [trade_ids],
    )
    if df.empty:
        return
    with _conn() as conn:
        for _, r in df.iterrows():
            sym = str(r["symbol"])
            exp = r.get("expiry")
            exp_str = str(getattr(exp, "date", lambda: exp)()) if exp is not None else None
            try:
                snap = json.loads(r.get("snap") or "{}")
            except Exception:
                snap = {}
            side = _infer_side_text(r.get("suggested_action"), r.get("remarks"), snap)
            exit_ltp = None
            if side and exp_str is not None and r.get("strike") is not None:
                try:
                    exit_ltp = latest_option_ltp_conn(
                        conn, sym, exp_str, float(r["strike"]), side
                    )
                except Exception:
                    exit_ltp = None
            exec_sql(
                """
                UPDATE public.execution_log
                SET status='exited', exit_ltp=%s
                WHERE id=%s
            """,
                [exit_ltp, int(r["id"])],
            )
            devlog(
                "trade.exit",
                id=int(r["id"]),
                sym=sym,
                exp=exp_str,
                k=float(r["strike"]),
                side=side,
                exit_ltp=exit_ltp,
            )


def sendback_trades_by_id(trade_ids: list[int]):
    if not trade_ids:
        return
    exec_sql(
        """
        UPDATE public.execution_log
        SET status='sent_back', exit_ltp=NULL
        WHERE id = ANY(%s)
    """,
        [trade_ids],
    )
    devlog("trade.send_back", ids=list(map(int, trade_ids)))


# ----------------------------------
# Banner KPIs (scoreboard + CE/PE strength)
# ----------------------------------
def _banner_kpis():
    try:
        today = _today_local()
        rows = fetch_rows(
            """
            SELECT LOWER(COALESCE(NULLIF(status,''),'open')) AS s, COUNT(*) AS n
            FROM public.execution_log
            WHERE DATE("timestamp")=%s
            GROUP BY 1
        """,
            [today],
            timeout_ms=DB_STATEMENT_TIMEOUT_MS,
        )
    except Exception:
        rows = []
    counts = {"open": 0, "queued": 0, "sent_back": 0, "exited": 0}
    for r in rows:
        s = (r.get("s") or "").lower()
        n = int(r.get("n") or 0)
        if "sent_back" in s:
            counts["sent_back"] += n
        elif "queue" in s:
            counts["queued"] += n
        elif "exit" in s:
            counts["exited"] += n
        elif "open" in s:
            counts["open"] += n

    try:
        r2 = fetch_rows(
            """
            SELECT suggested_action, remarks, strike_type, decision_snapshot::text AS snap
            FROM public.execution_log
            WHERE DATE("timestamp")=%s
        """,
            [today],
            timeout_ms=DB_STATEMENT_TIMEOUT_MS,
        )
    except Exception:
        r2 = []
    ce = pe = 0
    for r in r2:
        snap = {}
        try:
            snap = json.loads(r.get("snap") or "{}")
        except Exception:
            pass
        side = _infer_side_text(
            r.get("suggested_action"), r.get("remarks"), snap, r.get("strike_type")
        )
        if side == "CE":
            ce += 1
        elif side == "PE":
            pe += 1
    total = max(1, ce + pe)
    ce_pct = int(round(100 * ce / total))
    pe_pct = 100 - ce_pct

    chips = html.Div(
        [
            dbc.Badge(f"open {counts['open']}", color="success", className="me-2"),
            dbc.Badge(f"queued {counts['queued']}", color="info", className="me-2"),
            dbc.Badge(
                f"sent_back {counts['sent_back']}", color="danger", className="me-2"
            ),
            dbc.Badge(
                f"exited {counts['exited']}", color="secondary", className="me-3"
            ),
            html.Span(f"CE {ce_pct}%  |  PE {pe_pct}%", style={"fontWeight": "600"}),
        ],
        style={"textAlign": "center"},
    )
    return chips


# ============================
# === SR PANEL (existing) ====
# ============================
def _fetch_recent_atm_rows(symbol: str, lookback_min: int) -> pd.DataFrame:
    sql = """
    WITH base AS (
      SELECT
        symbol, expiry, strike, "timestamp",
        spot_price, ce_ltp, pe_ltp, ce_iv, pe_iv,
        ce_gamma, pe_gamma, ce_delta, pe_delta,
        ce_volume, pe_volume
      FROM public.option_chain_oi
      WHERE UPPER(symbol)=UPPER(%(symbol)s)
        AND "timestamp" >= NOW() - (%(lb)s::text || ' minutes')::interval
        AND spot_price IS NOT NULL
        AND ce_ltp IS NOT NULL AND pe_ltp IS NOT NULL
    ),
    ranked AS (
      SELECT *, ROW_NUMBER() OVER (
        PARTITION BY "timestamp" ORDER BY ABS(strike - spot_price)
      ) AS rk
      FROM base
    )
    SELECT * FROM ranked WHERE rk=1 ORDER BY "timestamp";
    """
    df = fetch_df(
        sql,
        {"symbol": symbol, "lb": str(int(max(60, lookback_min)))},
        timeout_ms=DB_STATEMENT_TIMEOUT_MS,
    )
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def _resample_last(df: pd.DataFrame, tf: str = "3min") -> pd.DataFrame:
    if df.empty:
        return df
    df = df.sort_values("timestamp").set_index("timestamp")
    cols = [
        "spot_price",
        "expiry",
        "strike",
        "ce_ltp",
        "pe_ltp",
        "ce_iv",
        "pe_iv",
        "ce_gamma",
        "pe_gamma",
        "ce_delta",
        "pe_delta",
        "ce_volume",
        "pe_volume",
    ]
    agg = {c: "last" for c in cols if c in df.columns}
    out = df.resample(tf).agg(agg).dropna(subset=["spot_price", "ce_ltp", "pe_ltp"])
    return out.reset_index()


def _atr(series: pd.Series, win: int) -> pd.Series:
    tr = series.diff().abs()
    return tr.rolling(win, min_periods=3).mean()


def _detect_pivots_center(
    spot: pd.Series, win: int
) -> Tuple[pd.Series, pd.Series]:
    s = spot.values
    n = len(s)
    lows = np.full(n, np.nan)
    highs = np.full(n, np.nan)
    for i in range(win, n - win):
        w = s[i - win : i + win + 1]
        if np.argmin(w) == win:
            lows[i] = s[i]
        if np.argmax(w) == win:
            highs[i] = s[i]
    return pd.Series(lows, index=spot.index), pd.Series(highs, index=spot.index)


def _sigmoid(x):
    x = np.clip(x, -10, 10)
    return 1.0 / (1.0 + np.exp(-x))


def _sr_strength(
    df_tf: pd.DataFrame,
    level_val: float,
    is_support: bool,
    atr_col: str = "atr",
    prox_atr: float = SR_PROX_ATR,
    lookback_bars: int = 120,
    slope: float = 0.0,
) -> int:
    if df_tf.empty or level_val is None or np.isnan(level_val):
        return 0
    s = df_tf["spot_price"].astype(float).values
    atr = df_tf[atr_col].astype(float).values
    n = len(df_tf)
    lb = max(10, min(lookback_bars, n))
    s_lb = s[-lb:]
    atr_lb = np.where(atr[-lb:] <= 0, np.nan, atr[-lb:])
    band = prox_atr * atr_lb
    dist = np.abs(s_lb - level_val)
    touches = int(np.nansum(dist <= band))
    touch_score = touches / (touches + 4.0)  # 0..1
    slope_norm = float(slope) / (
        np.nanmean(atr_lb) if np.isfinite(np.nanmean(atr_lb)) and np.nanmean(atr_lb) > 0 else 1.0
    )
    slope_score = float(_sigmoid(slope_norm))
    strength = 0.65 * touch_score + 0.35 * (slope_score - 0.5 + 0.5)
    return int(np.clip(round(100 * strength), 0, 100))


def _last_three(series: pd.Series) -> List[Tuple[pd.Timestamp, float]]:
    vals = series.dropna()
    if vals.empty:
        return []
    last_idx = list(vals.index[-SR_TOP_K:])
    return [(idx, float(vals.loc[idx])) for idx in last_idx]


def _arrow_and_color(delta: float, eps: float) -> Tuple[str, str]:
    if not np.isfinite(delta) or abs(delta) <= eps:
        return "→", "#6b7280"  # gray
    return ("↑", "#16a34a") if delta > 0 else ("↓", "#dc2626")


def _touch_points(
    df: pd.DataFrame, lvl: float, prox_atr: float
) -> List[Tuple[int, float]]:
    if df.empty or not np.isfinite(lvl):
        return []
    tail = df.tail(SR_SPARK_BARS)
    atr = tail["atr"].astype(float).values
    price = tail["spot_price"].astype(float).values
    band = prox_atr * np.where(atr <= 0, np.nan, atr)
    mask = np.isfinite(band) & (np.abs(price - lvl) <= band)
    xs = np.nonzero(mask)[0].tolist()
    return [(int(i), float(price[i])) for i in xs]


def _sparkline_fig(
    tf: pd.DataFrame,
    supports: List[Tuple[pd.Timestamp, float]],
    resist: List[Tuple[pd.Timestamp, float]],
    bars: int = SR_SPARK_BARS,
) -> go.Figure:
    if tf.empty:
        return go.Figure()
    tail = tf.tail(bars).copy()
    x = list(range(len(tail)))
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=x,
            y=tail["spot_price"],
            mode="lines",
            name="price",
            line=dict(width=2),
        )
    )
    for _i, (_t, lvl) in enumerate(supports):
        fig.add_trace(
            go.Scatter(
                x=[x[0], x[-1]],
                y=[lvl, lvl],
                mode="lines",
                name=f"S{_i+1}",
                line=dict(width=1.5, dash="dot"),
            )
        )
        pts = _touch_points(tail, lvl, SR_PROX_ATR)
        if pts:
            fig.add_trace(
                go.Scatter(
                    x=[p[0] for p in pts],
                    y=[p[1] for p in pts],
                    mode="markers",
                    marker=dict(size=5),
                    name=f"S{_i+1} touch",
                )
            )
    for _i, (_t, lvl) in enumerate(resist):
        fig.add_trace(
            go.Scatter(
                x=[x[0], x[-1]],
                y=[lvl, lvl],
                mode="lines",
                name=f"R{_i+1}",
                line=dict(width=1.5, dash="dash"),
            )
        )
        pts = _touch_points(tail, lvl, SR_PROX_ATR)
        if pts:
            fig.add_trace(
                go.Scatter(
                    x=[p[0] for p in pts],
                    y=[p[1] for p in pts],
                    mode="markers",
                    marker=dict(size=5),
                    name=f"R{_i+1} touch",
                )
            )
    fig.update_layout(
        height=90,
        margin=dict(l=6, r=6, t=6, b=6),
        showlegend=False,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig


def _sr_table(levels: List[Tuple[str, float, int, str, str]]) -> html.Table:
    head = html.Tr(
        [html.Th("Type"), html.Th("Level"), html.Th("Strength"), html.Th("Shift")]
    )
    rows = []
    for label, lvl, st, arr, col in levels:
        rows.append(
            html.Tr(
                [
                    html.Td(label),
                    html.Td(f"{lvl:,.2f}"),
                    html.Td(f"{st}%"),
                    html.Td(html.Span(arr, style={"color": col, "fontWeight": "900"})),
                ]
            )
        )
    return html.Table(
        [html.Thead(head), html.Tbody(rows)],
        style={"width": "100%", "fontSize": "0.9em", "marginTop": "6px"},
        className="table table-sm",
    )


def _render_sr_panel():
    try:
        raw = _fetch_recent_atm_rows(SYMBOL, SR_LOOKBACK_MIN)
        if raw.empty:
            return dbc.Alert(
                "SR panel: no recent ATM rows.", color="light", className="mb-0"
            )
        tf = _resample_last(raw, "3min")
        if tf.empty or len(tf) < (SR_PIVOT_WIN * 4):
            return dbc.Alert(
                "SR panel: not enough rows after resample.",
                color="light",
                className="mb-0",
            )

        tf["atr"] = _atr(tf["spot_price"].astype(float), SR_ATR_WIN)
        piv_low, piv_high = _detect_pivots_center(
            tf["spot_price"].astype(float), SR_PIVOT_WIN
        )

        last_low = prev_low = np.nan
        last_high = prev_high = np.nan
        lows_seq = []
        highs_seq = []
        for lo, hi in zip(piv_low.values, piv_high.values):
            if not np.isnan(lo):
                prev_low, last_low = last_low, lo
            if not np.isnan(hi):
                prev_high, last_high = last_high, hi
            lows_seq.append((last_low, prev_low))
            highs_seq.append((last_high, prev_high))
        tf["last_support"] = [x[0] for x in lows_seq]
        tf["prev_support"] = [x[1] for x in lows_seq]
        tf["last_resist"] = [x[0] for x in highs_seq]
        tf["prev_resist"] = [x[1] for x in highs_seq]
        tf["support_slope"] = tf["last_support"] - tf["prev_support"]
        tf["resistance_slope"] = tf["last_resist"] - tf["prev_resist"]

        supports = _last_three(piv_low)
        resist = _last_three(piv_high)

        last_row = tf.iloc[-1]
        atr_mean = float(np.nanmean(tf["atr"].tail(50)))
        eps = (SR_PROX_ATR * atr_mean) if atr_mean > 0 else 0.0

        sup_delta = float(last_row.get("support_slope", np.nan))
        res_delta = float(last_row.get("resistance_slope", np.nan))
        sup_arrow, sup_color = _arrow_and_color(sup_delta, eps)
        res_arrow, res_color = _arrow_and_color(res_delta, eps)

        sup_cards = []
        sr_table_items = []
        for i, (idx, lvl) in enumerate(reversed(supports), start=1):
            st = _sr_strength(
                tf, lvl, True, "atr", SR_PROX_ATR, SR_SPARK_BARS, sup_delta
            )
            sup_cards.append(
                dbc.Badge(
                    f"{lvl:,.2f} • {st}%",
                    color="success",
                    className="me-2 mb-2",
                    title=f"S{i}",
                )
            )
            sr_table_items.append((f"S{i}", lvl, st, sup_arrow, sup_color))

        res_cards = []
        for i, (idx, lvl) in enumerate(reversed(resist), start=1):
            st = _sr_strength(
                tf, lvl, False, "atr", SR_PROX_ATR, SR_SPARK_BARS, res_delta
            )
            res_cards.append(
                dbc.Badge(
                    f"{lvl:,.2f} • {st}%",
                    color="danger",
                    className="me-2 mb-2",
                    title=f"R{i}",
                )
            )
            sr_table_items.append((f"R{i}", lvl, st, res_arrow, res_color))

        sup_msg = "n/a"
        res_msg = "n/a"
        if np.isfinite(last_row.get("last_support", np.nan)) and np.isfinite(
            last_row.get("prev_support", np.nan)
        ):
            sup_msg = (
                f"Support shifting to {last_row['last_support']:.2f} (from {last_row['prev_support']:.2f})"
                if abs(sup_delta) > eps
                else f"Support steady near {last_row['last_support']:.2f}"
            )
        if np.isfinite(last_row.get("last_resist", np.nan)) and np.isfinite(
            last_row.get("prev_resist", np.nan)
        ):
            res_msg = (
                f"Resistance shifting to {last_row['last_resist']:.2f} (from {last_row['prev_resist']:.2f})"
                if abs(res_delta) > eps
                else f"Resistance steady near {last_row['last_resist']:.2f}"
            )

        spark = dcc.Graph(
            figure=_sparkline_fig(tf, supports, resist, SR_SPARK_BARS),
            config={"displayModeBar": False},
        )

        header = html.Div(
            [
                html.Span("S/R (3x) ", style={"fontWeight": "700", "marginRight": "8px"}),
                html.Span(
                    [
                        "Support ",
                        html.Span(
                            sup_arrow,
                            style={"color": sup_color, "fontWeight": "900"},
                        ),
                    ],
                    className="me-3",
                    style={"color": "#166534", "fontWeight": "700"},
                ),
                html.Span(
                    [
                        "Resistance ",
                        html.Span(
                            res_arrow,
                            style={"color": res_color, "fontWeight": "900"},
                        ),
                    ],
                    style={"color": "#7f1d1d", "fontWeight": "700"},
                ),
            ],
            style={"marginBottom": "6px", "textAlign": "center"},
        )

        body = dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        [
                            html.Small(
                                "Supports",
                                className="d-block mb-1",
                                style={"color": "#166534", "fontWeight": "700"},
                            ),
                            html.Div(sup_cards),
                        ]
                    ),
                    md=6,
                ),
                dbc.Col(
                    html.Div(
                        [
                            html.Small(
                                "Resistances",
                                className="d-block mb-1",
                                style={"color": "#7f1d1d", "fontWeight": "700"},
                            ),
                            html.Div(res_cards),
                        ]
                    ),
                    md=6,
                ),
            ]
        )

        shifts = html.Div(
            [
                html.Div(
                    html.Small(
                        sup_msg,
                        style={"color": sup_color, "fontWeight": "700"},
                    ),
                    className="mb-1",
                ),
                html.Div(
                    html.Small(
                        res_msg,
                        style={"color": res_color, "fontWeight": "700"},
                    )
                ),
            ],
            style={"textAlign": "center", "marginTop": "4px"},
        )

        table = _sr_table(sr_table_items)

        foot = html.Div(
            html.Small(
                f"win={SR_PIVOT_WIN} • ATR={SR_ATR_WIN} • band={SR_PROX_ATR:.2f}ATR • lookback={SR_LOOKBACK_MIN}m",
                style={"color": "#64748b"},
            ),
            style={"textAlign": "center", "marginTop": "4px"},
        )

        return html.Div([header, spark, body, table, shifts, foot])
    except Exception as e:
        devlog("sr.panel.fail", err=str(e))
        return dbc.Alert(f"SR panel error: {e}", color="warning", className="mb-0")


# ----------------------------------
# Callbacks
# ----------------------------------
@app.callback(
    Output("db-status-box", "children"),
    Output("db-status-box", "style"),
    Input("tick", "n_intervals"),
    Input("refresh-now", "n_clicks"),
    prevent_initial_call=False,
)
def render_db_status(_n, _btn):
    card = _render_db_status()
    style = {
        "border": "1px dashed #94a3b8",
        "borderRadius": "8px",
        "padding": "8px 10px",
        "minHeight": "64px",
        "backgroundColor": "#f8fafc",
        "fontSize": "0.92em",
    }
    return card, style


@app.callback(
    Output("banner-kpis","children"),
    Input("tick","n_intervals"),
    prevent_initial_call=False
)
def _update_banner_kpis(_n):
    try:
        return _banner_kpis()
    except Exception as e:
        devlog("banner.kpis.fail", err=str(e))
        return html.Div("DB not available", style={"color": "#b91c1c"})


# NEW: SR panel updater
@app.callback(
    Output("sr-box", "children"),
    Input("tick", "n_intervals"),
    Input("refresh-now", "n_clicks"),
    prevent_initial_call=False,
)
def _update_sr_box(_n, _btn):
    return _render_sr_panel()


# NEW: RR(25Δ) + Regime ticker updater
@app.callback(
    Output("rr25d-bar","children"),
    Output("regime-ticker","children"),
    Output("regime-store","data"),
    Input("tick","n_intervals"),
    Input("refresh-now","n_clicks"),
    State("regime-store","data"),
    prevent_initial_call=False
)
def _update_rr_and_regime(_n, _btn, regime_state):
    try:
        rr_data = _fetch_latest_rr25d(SYMBOL)
        rr_comp = _render_rr25d_bar(rr_data["rr"]) if rr_data else _render_rr25d_bar(None)

        price_dir, pct, lastp = _price_slope_regime(SYMBOL, REGIME_LOOKBACK_MIN)
        rr_bias = rr_data["bias"] if rr_data else None
        cur_regime = _fuse_regime(price_dir, rr_bias)
    except Exception as e:
        devlog("rr.regime.fail", err=str(e))
        # fallback: neutral bar + simple text
        rr_comp = _render_rr25d_bar(None)
        cur_regime = "Sideways"

    prev = (regime_state or {}).get("cur") or (regime_state or {}).get("prev")
    ticker_text = _transition_text(prev, cur_regime)
    last_text = (regime_state or {}).get("text")
    ticker_out = (no_update if ticker_text == last_text else ticker_text)

    return rr_comp, ticker_out, {"prev": prev, "cur": cur_regime, "text": ticker_text}


@app.callback(
    Output("popups-enabled", "data"),
    Output("toggle-popups", "children"),
    Input("toggle-popups", "n_clicks"),
    State("popups-enabled", "data"),
    prevent_initial_call=False,
)
def _toggle_popups(n, enabled):
    if n is None:
        return True, "Popups: ON"
    enabled = not bool(enabled)
    return enabled, ("Popups: ON" if enabled else "Popups: OFF")


@app.callback(
    Output("popup-container", "children"),
    Output("exec-last-seen", "data"),
    Input("tick", "n_intervals"),
    State("popups-enabled", "data"),
    State("exec-last-seen", "data"),
    prevent_initial_call=False,
)
def _exec_toasts(_n, enabled, last_seen):
    if not enabled:
        return no_update, last_seen
    try:
        today = _today_local()
        recent = fetch_rows(
            """
            SELECT id, symbol, expiry, strike, status
            FROM public.execution_log
            WHERE expiry >= CURRENT_DATE
              AND DATE("timestamp")=%s
            ORDER BY "timestamp" DESC
            LIMIT 200
        """,
            [today],
            timeout_ms=DB_STATEMENT_TIMEOUT_MS,
        )
    except Exception:
        return no_update, last_seen
    prev_ids = set((last_seen or {}).get("ids", []))
    cur_ids = [r["id"] for r in recent]
    toasts = []
    for r in recent:
        rid = r["id"]
        if rid in prev_ids:
            continue
        s = (r.get("status") or "").lower()
        if s.startswith("open") or ("exit" in s) or ("sent_back" in s):
            color = (
                "warning"
                if "sent_back" in s
                else ("success" if s.startswith("open") else "dark")
            )
            toasts.append(
                dbc.Toast(
                    [
                        html.B(f"{r['symbol']} {r['expiry']} • {r['strike']}"),
                        html.Div(s),
                    ],
                    header="Execution",
                    is_open=True,
                    dismissable=True,
                    duration=6000,
                    icon=color,
                    color=color,
                    style={"marginBottom": "8px"},
                )
            )
    return html.Div(toasts), {"ids": cur_ids}


@app.callback(
    Output("tick", "interval"),
    Output("tick", "disabled"),
    Output("global-status", "children"),
    Input("auto-enabled", "value"),
    Input("auto-seconds", "value"),
    Input("refresh-now", "n_clicks"),
    prevent_initial_call=False,
)
def tune_interval(enabled_vals, secs, _btn):
    enabled = "on" in (enabled_vals or [])
    try:
        s = max(3, int(secs or 10))
    except Exception:
        s = 10
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"{ts} — auto={'on' if enabled else 'off'} @ {s}s"
    return s * 1000, (not enabled), msg


@app.callback(Output("tab-content", "children"), Input("main-tabs", "value"))
def render_tab(tab):
    if tab == "tab-exec":
        return tab_exec_body
    if tab == "tab-trade":
        return tab_trade_body
    if tab == "tab-dev":
        return tab_dev_body
    return html.Div("Invalid tab.")


# ---- Execution Log (open/queued)
def _execution_rows_for_tab():
    rows = open_trades_rows()
    rows = [
        r
        for r in rows
        if str(r.get("status") or "").lower() in ("open", "queued", "")
    ]
    return rows


def _exec_fallback_cols():
    base = [
        "timestamp",
        "symbol",
        "expiry",
        "strike",
        "option_side",
        "ltp",
        "suggested_action",
        "copilot_comment",
        "remarks",
        "source",
    ]
    return [
        {"name": ("CE/PE" if c == "option_side" else c), "id": c}
        for c in base
    ]


@app.callback(
    Output("exec-table", "columns"),
    Output("exec-table", "data"),
    Output("exec-status", "children"),
    Input("main-tabs", "value"),
    Input("tick", "n_intervals"),
    Input("refresh-now", "n_clicks"),
    prevent_initial_call=False,
)
def refresh_exec(active_tab, _n, _btn):
    try:
        if active_tab != "tab-exec":
            return no_update, no_update, no_update
        if not db_ready():
            return _exec_fallback_cols(), [], "DB not ready (retrying…)"
        rows = _execution_rows_for_tab()
        total = len(rows)
        if total == 0:
            fresh_open, age_min, mkt_open, fresh = _fresh_and_open()
            msg = (
                f"No eligible rows. Market={'open' if mkt_open else 'closed'}; age="
                + ("n/a" if age_min is None else f"{age_min:.2f}m")
            )
            return _exec_fallback_cols(), [], msg
        df = pd.DataFrame(rows)
        order = [
            "timestamp",
            "symbol",
            "expiry",
            "strike",
            "option_side",
            "ltp",
            "suggested_action",
            "copilot_comment",
            "remarks",
            "source",
        ]
        df = df[[c for c in order if c in df.columns]]
        cols = [
            {"name": ("CE/PE" if c == "option_side" else c), "id": c}
            for c in df.columns
        ]
        status = f"rows {len(df)}/{total}"
        return cols, df.to_dict("records"), status
    except Exception as e:
        devlog("exec.refresh.fatal", err=str(e))
        return _exec_fallback_cols(), [], f"Execution Log error: {e}"


# ---- Trade Monitor
@app.callback(
    Output("trade-table", "data"),
    Output("trade-table", "selected_rows"),
    Output("trade-ema-card", "children"),
    Output("trade-ema-chart", "figure"),
    Input("main-tabs", "value"),
    Input("tick", "n_intervals"),
    Input("refresh-now", "n_clicks"),
    prevent_initial_call=False,
)
def refresh_trade(active_tab, _n, _btn):
    if active_tab != "tab-trade":
        return no_update, no_update, no_update, no_update
    if not db_ready():
        return [], [], "DB not ready (retrying…)", go.Figure()
    rows = open_trades_rows()
    card_text = "No open trades."
    fig = go.Figure()
    if rows:
        card_text = f"{len(rows)} open/queued trade(s)."
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=360)
    return rows, [], card_text, fig


@app.callback(
    Output("trade-status", "children"),
    Input("trade-table", "data"),
    prevent_initial_call=False,
)
def trade_status(data):
    try:
        n = len(data or [])
    except Exception:
        n = 0
    return f"{n} row(s)"


@app.callback(
    Output("exit-selected", "n_clicks", allow_duplicate=True),
    Input("exit-selected", "n_clicks"),
    State("trade-table", "data"),
    State("trade-table", "selected_rows"),
    prevent_initial_call=True,
)
def do_exit(n, data, selected_rows):
    if not n:
        return no_update
    if not data or not selected_rows:
        return 0
    ids = []
    for i in selected_rows:
        try:
            ids.append(int(data[i]["id"]))
        except Exception:
            pass
    if ids:
        exit_trades_by_id(ids)
    return 0


@app.callback(
    Output("sendback-selected", "n_clicks", allow_duplicate=True),
    Input("sendback-selected", "n_clicks"),
    State("trade-table", "data"),
    State("trade-table", "selected_rows"),
    prevent_initial_call=True,
)
def do_send_back(n, data, selected_rows):
    if not n:
        return no_update
    if not data or not selected_rows:
        return 0
    ids = []
    for i in selected_rows:
        try:
            ids.append(int(data[i]["id"]))
        except Exception:
            pass
    if ids:
        sendback_trades_by_id(ids)
    return 0

@server.route("/_healthz")
def healthz():
    return "ok", 200

# ---------- Final wiring -------------
if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8050"))
    debug = os.getenv("DASH_DEBUG", "0").lower() in ("1", "true", "on", "yes")
    app.run(host=host, port=port, debug=debug)