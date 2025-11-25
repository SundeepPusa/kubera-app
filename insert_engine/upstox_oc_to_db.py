# === FILE: insert_engine/upstox_oc_to_db.py ===
# -*- coding: utf-8 -*-
"""
Upstox Option Chain → PostgreSQL ingester
- Bash/Windows friendly: UTF-8 safe prints, optional ASCII logs
- Explicit, flushed logging so you can see progress in real time
- Short, separate connect/read timeouts to avoid long hangs
"""

import os
import json
import time
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional

import sys
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from insert_engine.utils_date import load_holidays, is_market_closed
import datetime, os

HOLIDAY_FILE = os.path.join(os.path.dirname(__file__), "holidays.txt")
HOLIDAYS = load_holidays(HOLIDAY_FILE)

# ------------------------------- Console Safety --------------------------------

# Force UTF-8 console where possible; never crash on emoji/Unicode
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


# Unified print with flush & safe fallback (works on Bash, PowerShell, CMD)
def P(*a, **k):
    k.setdefault("flush", True)
    try:
        print(*a, **k)
    except Exception:
        txt = " ".join(str(x) for x in a)
        print(txt.encode("ascii", "replace").decode("ascii"), **k)


USE_ASCII_LOGS = os.getenv("ASCII_LOGS", "").strip().lower() in {"1", "true", "yes", "on"}

if USE_ASCII_LOGS:
    ICON = {
        "start": "[START]",
        "warn":  "[WARN]",
        "cal":   "[CAL]",
        "stop":  "[STOP]",
        "err":   "[ERR]",
        "ok":    "[OK]",
        "net":   "[NET]",
        "db":    "[DB]",
        "sleep": "[SLEEP]",
        "run":   "[RUN]",
        "skip":  "[SKIP]",
        "dbg":   "[DEBUG]",
    }
else:
    ICON = {
        "start": "🟢",
        "warn":  "⚠️",
        "cal":   "📅",
        "stop":  "🛑",
        "err":   "❌",
        "ok":    "✅",
        "net":   "🌐",
        "db":    "🗄️",
        "sleep": "😴",
        "run":   "🏃",
        "skip":  "⏭️",
        "dbg":   "🔎",
    }

# --------------------------------- Config --------------------------------------

load_dotenv()

# Upstox – instrument & expiry file
INSTRUMENT_KEY = os.getenv("TEST_INSTRUMENT", "NSE_INDEX|Nifty 50")
WINDOW_STRIKES = int(os.getenv("ATM_WINDOW", "10"))  # how many up/down from ATM

# Expiry file
DEFAULT_EXPIRY_FILE = os.path.join(
    os.path.dirname(__file__),
    "expiry_calendars",
    "nifty50_expiry_dates.txt",
)
EXPIRY_FILE = os.getenv("EXPIRY_FILE", DEFAULT_EXPIRY_FILE)

NUM_EXPIRIES_PER_CYCLE = int(os.getenv("NUM_EXPIRIES_PER_CYCLE", "2"))  # pick next N upcoming
LOOP_SECONDS = int(os.getenv("LOOP_SECONDS", "10"))  # run every 10s

# DB
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "osa_db")
DB_USER = os.getenv("DB_USER", "osa_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Table (allow schema override via env: e.g. OC_TABLE="public.option_chain_oi")
OC_TABLE = os.getenv("OC_TABLE", "option_chain_oi")

# Requests timeouts: (connect, read)
HTTP_CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "5"))
HTTP_READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", "12"))

TOKEN_FILE = os.getenv("UPSTOX_TOKEN_FILE", "token.txt")

# --------------------------------- Helpers -------------------------------------


def _safe_date_only(expiry_val: Any) -> Optional[str]:
    if expiry_val is None:
        return None
    s = str(expiry_val)
    return s.split(" ")[0]


def _symbol_from_underlying_key(uk: str) -> str:
    # e.g. "NSE_INDEX|NIFTY 50" -> "NIFTY 50"
    if not uk or "|" not in uk:
        return uk or "NIFTY 50"
    return uk.split("|", 1)[1].strip()


def _parse_expiry_file(path: str) -> List[date]:
    """Read YYYY-MM-DD lines, ignoring empty lines and those starting with #."""
    out: List[date] = []
    if not os.path.exists(path):
        P(f"{ICON['warn']} Expiry file not found: {path}")
        return out
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                try:
                    y, m, d = [int(x) for x in s.split("-")]
                    out.append(date(y, m, d))
                except Exception:
                    P(f"{ICON['warn']} Skipping invalid expiry line: {s}")
    except Exception as e:
        P(f"{ICON['err']} Failed reading expiry file: {path} | {e}")
        return out
    # Unique + sorted
    out = sorted(set(out))
    return out


def _pick_upcoming_expiries(all_dates: List[date], n: int) -> List[str]:
    """Return next n upcoming expiries (>= today) as strings."""
    today = date.today()
    upcoming = [dt for dt in all_dates if dt >= today]
    selected = upcoming[: max(1, n)]
    # Fallback to the last available date if none upcoming
    if not selected and all_dates:
        selected = [all_dates[-1]]
    return [dt.isoformat() for dt in selected]


def load_access_token() -> str:
    """
    Load the Upstox access token.
    Priority:
      1) UPSTOX_ACCESS_TOKEN from environment (if set)
      2) token.txt (or UPSTOX_TOKEN_FILE) next to project root
    """
    env_token = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
    if env_token:
        return env_token

    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            token = f.read().strip()
    except FileNotFoundError:
        P(
            f"{ICON['err']} Token file '{TOKEN_FILE}' not found. "
            f"Run auth_app.py and complete login to generate it."
        )
        raise
    except OSError as e:
        P(f"{ICON['err']} Failed to read token file '{TOKEN_FILE}': {e}")
        raise

    if not token:
        P(
            f"{ICON['err']} Token file '{TOKEN_FILE}' is empty. "
            f"Re-run auth_app.py to fetch a fresh access token."
        )
        raise RuntimeError("Empty Upstox access token")

    return token


# ---------------------------- Upstox + Transform -------------------------------


def fetch_upstox_option_chain_raw(expiry_str: str) -> Dict[str, Any]:
    url = "https://api.upstox.com/v2/option/chain"
    token = load_access_token()
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    params = {"instrument_key": INSTRUMENT_KEY, "expiry_date": expiry_str}
    resp = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
    )
    resp.raise_for_status()
    return resp.json()


def filter_to_atm_window(raw_payload: Dict[str, Any], window: int) -> List[Dict[str, Any]]:
    data = raw_payload.get("data") or []
    if not data:
        return []

    # Spot is repeated per row—use the first non-null
    spot = None
    for r in data:
        sp = r.get("underlying_spot_price")
        if sp is not None:
            try:
                spot = float(sp)
                break
            except Exception:
                pass

    if spot is None:
        return []

    strikes = sorted(
        set(float(r.get("strike_price")) for r in data if r.get("strike_price") is not None)
    )
    if not strikes:
        return []

    # nearest strike to spot
    atm_strike = min(strikes, key=lambda x: abs(x - spot))
    idx = strikes.index(atm_strike)

    lo = max(0, idx - window)
    hi = min(len(strikes), idx + window + 1)
    selected = set(strikes[lo:hi])

    P(
        f"{ICON['dbg']} Spot: {spot:.2f} | ATM: {atm_strike:g} | "
        f"Keeping [{strikes[lo]:g} .. {strikes[hi-1]:g}] ({len(selected)} strikes)"
    )

    # Filter original rows to only selected strikes
    filtered = [r for r in data if float(r.get("strike_price")) in selected]
    filtered.sort(key=lambda r: float(r.get("strike_price")))
    return filtered


def flatten_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc)  # timestamptz in DB
    ts = now.isoformat()
    for r in rows:
        ce = (r.get("call_options") or {})
        pe = (r.get("put_options") or {})
        md_ce = ce.get("market_data") or {}
        md_pe = pe.get("market_data") or {}
        gk_ce = ce.get("option_greeks") or {}
        gk_pe = pe.get("option_greeks") or {}

        row = {
            # PK parts + identity
            "symbol": _symbol_from_underlying_key(r.get("underlying_key")),
            "expiry": _safe_date_only(r.get("expiry")),
            "strike": r.get("strike_price"),
            "timestamp": ts,

            # spot
            "spot_price": r.get("underlying_spot_price"),

            # CE market
            "ce_oi": md_ce.get("oi"),
            "ce_chng_oi": None,
            "ce_volume": md_ce.get("volume"),
            "ce_iv": gk_ce.get("iv"),
            "ce_ltp": md_ce.get("ltp"),
            "ce_chng": None,
            "ce_bid_qty": md_ce.get("bid_qty"),
            "ce_bid": md_ce.get("bid_price"),
            "ce_ask": md_ce.get("ask_price"),
            "ce_ask_qty": md_ce.get("ask_qty"),
            "ce_prev_oi": md_ce.get("prev_oi"),
            "ce_close_price": md_ce.get("close_price"),

            # PE market
            "pe_bid_qty": md_pe.get("bid_qty"),
            "pe_bid": md_pe.get("bid_price"),
            "pe_ask": md_pe.get("ask_price"),
            "pe_ask_qty": md_pe.get("ask_qty"),
            "pe_chng": None,
            "pe_ltp": md_pe.get("ltp"),
            "pe_iv": gk_pe.get("iv"),
            "pe_volume": md_pe.get("volume"),
            "pe_chng_oi": None,
            "pe_oi": md_pe.get("oi"),
            "pe_prev_oi": md_pe.get("prev_oi"),
            "pe_close_price": md_pe.get("close_price"),

            # Greeks
            "ce_delta": gk_ce.get("delta"),
            "ce_gamma": gk_ce.get("gamma"),
            "ce_theta": gk_ce.get("theta"),
            "ce_vega": gk_ce.get("vega"),
            "pe_delta": gk_pe.get("delta"),
            "pe_gamma": gk_pe.get("gamma"),
            "pe_theta": gk_pe.get("theta"),
            "pe_vega": gk_pe.get("vega"),

            # change placeholders (optional)
            "ce_chng_iv": None,
            "ce_chng_volume": None,
            "pe_chng_iv": None,
            "pe_chng_volume": None,

            # notes/signals (optional)
            "ce_signal": None,
            "pe_signal": None,
            "remarks": None,

            # EMA fields (trigger will overwrite values)
            "ce_ema_5": None,
            "ce_ema_8": None,
            "ce_ema_13": None,
            "pe_ema_5": None,
            "pe_ema_8": None,
            "pe_ema_13": None,
        }
        out.append(row)
    return out


def insert_rows(conn, rows: List[Dict[str, Any]]):
    if not rows:
        P(f"{ICON['skip']} No rows to insert.")
        return

    cols = [
        "symbol",
        "expiry",
        "strike",
        "ce_oi",
        "ce_chng_oi",
        "ce_volume",
        "ce_iv",
        "ce_ltp",
        "ce_chng",
        "ce_bid_qty",
        "ce_bid",
        "ce_ask",
        "ce_ask_qty",
        "pe_bid_qty",
        "pe_bid",
        "pe_ask",
        "pe_ask_qty",
        "pe_chng",
        "pe_ltp",
        "pe_iv",
        "pe_volume",
        "pe_chng_oi",
        "pe_oi",
        "timestamp",
        # these exist in table; trigger fills them
        "ce_ema_5",
        "ce_ema_8",
        "ce_ema_13",
        "pe_ema_5",
        "pe_ema_8",
        "pe_ema_13",
        "ce_signal",
        "pe_signal",
        "remarks",
        "ce_chng_iv",
        "ce_chng_volume",
        "pe_chng_iv",
        "pe_chng_volume",
        "ce_delta",
        "ce_gamma",
        "ce_theta",
        "ce_vega",
        "pe_delta",
        "pe_gamma",
        "pe_theta",
        "pe_vega",
        "spot_price",
        "ce_prev_oi",
        "ce_close_price",
        "pe_prev_oi",
        "pe_close_price",
    ]

    values = [tuple(r.get(c) for c in cols) for r in rows]

    with conn.cursor() as cur:
        sql = f"""
            INSERT INTO {OC_TABLE} ({",".join(cols)})
            VALUES %s
            -- PK is (symbol, expiry, strike, timestamp), we insert fresh snapshots, so no conflict clause
        """
        execute_values(cur, sql, values, page_size=500)
    conn.commit()
    P(f"{ICON['ok']} Inserted {len(values)} rows into {OC_TABLE}.")


def preview(rows: List[Dict[str, Any]], n=3):
    P("\n--- Preview (first few flattened rows) ---")
    for i, r in enumerate(rows[:n], 1):
        keep = {
            "timestamp": r["timestamp"],
            "symbol": r["symbol"],
            "expiry": r["expiry"],
            "strike": r["strike"],
            "spot_price": r["spot_price"],
            "ce_ltp": r["ce_ltp"],
            "ce_iv": r["ce_iv"],
            "ce_delta": r["ce_delta"],
            "ce_oi": r["ce_oi"],
            "pe_ltp": r["pe_ltp"],
            "pe_iv": r["pe_iv"],
            "pe_delta": r["pe_delta"],
            "pe_oi": r["pe_oi"],
        }
        P(f"Row {i}: {json.dumps(keep, indent=2)}")


# --------------------------------- Runner --------------------------------------


def run_once_for_expiry(expiry_str: str):
    P(
        f"\n=== {datetime.now(timezone.utc).isoformat()} | "
        f"Fetching & Inserting Upstox OC (±{WINDOW_STRIKES}) ==="
    )
    P(f"Instrument: {INSTRUMENT_KEY} | Expiry: {expiry_str}")

    # Fetch RAW
    try:
        P(f"{ICON['net']} GET /v2/option/chain ...", end=" ")
        raw = fetch_upstox_option_chain_raw(expiry_str)
        P("ok")
    except requests.exceptions.Timeout as e:
        P(f"{ICON['err']} Fetch timeout: {e}")
        return
    except requests.exceptions.HTTPError as e:
        # Handle 401 specially – very common for expired token
        try:
            status = e.response.status_code
            body = (e.response.text or "")[:300]
        except Exception:
            status = None
            body = str(e)

        if status == 401:
            P(
                f"{ICON['err']} HTTP 401 Unauthorized from Upstox. "
                f"Access token likely expired. Re-run auth_app.py to get a new token."
            )
        else:
            P(f"{ICON['err']} HTTP {status} | {body}")
        return
    except FileNotFoundError:
        # load_access_token() error already printed a helpful message
        return
    except Exception as e:
        P(f"{ICON['err']} Fetch error: {e}")
        return

    status = raw.get("status")
    total_rows = len(raw.get("data") or [])
    P(f"Upstox status: {status} | total rows: {total_rows}")

    # Filter to ATM window
    filtered_rows = filter_to_atm_window(raw, WINDOW_STRIKES)
    P(f"Filtered rows to insert: {len(filtered_rows)}")
    if not filtered_rows:
        P(f"{ICON['skip']} No filtered data. Skipping insert.")
        return

    # Flatten
    flat = flatten_rows(filtered_rows)
    preview(flat, n=2)

    # Insert
    try:
        P(f"{ICON['db']} Connecting ...", end=" ")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        P("ok → inserting ...", end=" ")
        insert_rows(conn, flat)
        conn.close()
        P("done.")
    except Exception as e:
        P(f"{ICON['err']} DB insert error: {e}")


def main():
    P(
        f"{ICON['start']} Upstox OC ingester loop. "
        f"Interval={LOOP_SECONDS}s | Expiry file={EXPIRY_FILE} | Table={OC_TABLE}"
    )
    last_mtime: Optional[float] = None
    cached_dates: List[date] = []

    try:
        while True:
            # 🔒 Market-closed gate (weekends + NSE holidays via holidays.txt)
            today = date.today()
            if is_market_closed(today, HOLIDAYS):
                P(
                    f"{ICON['skip']} Market closed today ({today.isoformat()}). "
                    f"Skipping OC fetch; sleeping {LOOP_SECONDS}s."
                )
                time.sleep(LOOP_SECONDS)
                continue

            # Hot-reload the expiry file if changed
            try:
                mtime = os.path.getmtime(EXPIRY_FILE)
            except Exception:
                mtime = None

            if (mtime is None) or (last_mtime is None) or (mtime != last_mtime) or not cached_dates:
                cached_dates = _parse_expiry_file(EXPIRY_FILE)
                last_mtime = mtime
                P(f"{ICON['cal']} Loaded {len(cached_dates)} expiry dates from file.")

            expiries_to_run = _pick_upcoming_expiries(cached_dates, NUM_EXPIRIES_PER_CYCLE)
            if not expiries_to_run:
                P(f"{ICON['warn']} No valid expiries found. Waiting…")
            else:
                P(f"{ICON['run']} Expiries this cycle: {expiries_to_run}")
                for ex in expiries_to_run:
                    run_once_for_expiry(ex)

            P(f"{ICON['sleep']} {LOOP_SECONDS}s")
            time.sleep(LOOP_SECONDS)
    except KeyboardInterrupt:
        P(f"\n{ICON['stop']} Stopped by user.")


if __name__ == "__main__":
    main()
