# -*- coding: utf-8 -*-
"""
Load NSE Capital Market (cash) reports for a single date
from data/nse/cm_all_reports/YYYY/MM/DD into eod.* tables.

First target date for us: 2025-11-28.

This is a *v1 skeleton*:
- Handles the core, most important reports end-to-end.
- Phase-2: captures the optional PR-family CSVs into a single raw table.

Usage (from repo root):

    python -m reports.load_cm_reports --date 2025-11-28
"""

import argparse
import csv
import gzip
import io
import zipfile
from pathlib import Path
from datetime import datetime, date

import psycopg2
from psycopg2.extras import execute_values, Json  # Json for jsonb payloads

# --- Connection helper -------------------------------------------------------
# Try to use shared ETL utility if present; otherwise connect directly to AWS RDS.
try:
    from reports import etl_utils  # type: ignore

    def get_conn():
        return etl_utils.get_pg_conn()

except Exception:  # pragma: no cover
    import os

    def get_conn():
        """
        Direct psycopg2 connection, defaulting to AWS RDS host.

        Priority for password:
        - PGPASSWORD
        - DB_PASSWORD
        - OSA_DB_PASSWORD

        We also try to load .env if python-dotenv is installed.
        """
        try:
            from dotenv import load_dotenv  # type: ignore
            load_dotenv()
        except Exception:
            pass

        password = (
            os.getenv("PGPASSWORD")
            or os.getenv("DB_PASSWORD")
            or os.getenv("OSA_DB_PASSWORD")
        )

        if not password:
            raise RuntimeError(
                "No DB password found. Set PGPASSWORD or DB_PASSWORD or OSA_DB_PASSWORD "
                "in your environment or .env file."
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


# ---------- Generic helpers ----------

REPO_ROOT = Path(__file__).resolve().parents[1]
CM_ROOT = REPO_ROOT / "data" / "nse" / "cm_all_reports"


def parse_trade_date(s: str) -> date:
    """Accept 'YYYY-MM-DD' and return date object."""
    return datetime.strptime(s, "%Y-%m-%d").date()


def day_folder(trade_date: date) -> Path:
    return CM_ROOT / f"{trade_date.year:04d}" / f"{trade_date.month:02d}" / f"{trade_date.day:02d}"


def _normalize_row(row: dict) -> dict:
    """
    Normalize CSV dict row:
    - Strip spaces from header names
    - Uppercase keys so we can rely on 'SYMBOL', 'SERIES', etc.
    """
    norm = {}
    for k, v in row.items():
        if k is None:
            continue
        norm[k.strip().upper()] = v
    return norm


def read_csv(path: Path, encoding: str = "utf-8"):
    """Yield dict rows from CSV with NORMALIZED uppercase keys."""
    with path.open("r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield _normalize_row(row)


def read_csv_from_gzip(path: Path, encoding: str = "utf-8"):
    """Yield dict rows from GZIP CSV with NORMALIZED uppercase keys."""
    with gzip.open(path, "rt", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield _normalize_row(row)


def bulk_insert(conn, table: str, cols, rows, cleanup_sql: str | None = None, cleanup_params=None):
    """
    Simple DELETE+INSERT loader for a single trade_date.
    cols: list[str]
    rows: list[tuple]
    cleanup_sql: optional SQL to run before insert (e.g. DELETE for that date)
    cleanup_params: params for cleanup_sql
    """
    if not rows:
        print(f"[{table}] No rows to insert; skipping.")
        return

    with conn.cursor() as cur:
        if cleanup_sql:
            cur.execute(cleanup_sql, cleanup_params or ())
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
        execute_values(cur, sql, rows)
    conn.commit()
    print(f"[{table}] Inserted {len(rows)} rows.")


# --- Header helpers (symbol / series aliases) --------------------------------

def get_symbol(r: dict) -> str:
    """
    Try multiple possible header names for SYMBOL/SECURITY, after _normalize_row.

    Covers:
    - Standard CM bhavcopy     : SYMBOL
    - PR / index files         : SECURITY
    - Security master unified  : TCKRSYMB, FININSTRMNM
    - Short selling newer file : SYMBOL NAME, SECURITY NAME
    - MTO DAT (2025 format)    : NAME OF SECURITY
    """
    return (
        (r.get("SYMBOL")
         or r.get("SYMBOL NAME")
         or r.get("TCKRSYMB")
         or r.get("SECURITY")
         or r.get("SECURITY NAME")
         or r.get("NAME OF SECURITY")
         or r.get("SYMB")
         or r.get("FININSTRMNM")
         or "").strip()
    )


def get_series(r: dict) -> str:
    """
    Try multiple possible header names for SERIES column.

    Covers:
    - Standard CM files        : SERIES
    - Variants                 : SERIES NAME / SERIES1
    - Security master unified  : SCTYSRS
    """
    return (
        (r.get("SERIES")
         or r.get("SERIES NAME")
         or r.get("SERIES1")
         or r.get("SCTYSRS")
         or "").strip()
    )


def debug_no_data(table_label: str, source_rows: int, first_row_keys):
    """
    Small helper to print a debug hint when a file had rows but all were filtered.
    """
    if source_rows > 0:
        keys_part = ", ".join(first_row_keys or [])
        print(
            f"[{table_label}] WARNING: parsed {source_rows} rows from file but "
            f"0 valid rows for insert. Normalized headers: [{keys_part}]"
        )


# ---------- Loaders for individual reports ----------

def load_sec_bhavdata_full(conn, folder: Path, trade_date: date):
    """
    sec_bhavdata_full_28112025.csv -> eod.nse_sec_bhav_full

    Real-world header example (28-Nov-2025):
    SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE,
    CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, NO_OF_TRADES, DELIV_QTY, DELIV_PER
    """
    pattern = f"sec_bhavdata_full_{trade_date.strftime('%d%m%Y')}.csv"
    path = folder / pattern
    if not path.exists():
        print(f"[nse_sec_bhav_full] File not found: {pattern}, skipping.")
        return

    rows = []
    source_rows = 0
    first_keys = None

    for raw in read_csv(path):
        r = raw  # already normalized keys

        if source_rows == 0:
            first_keys = sorted(r.keys())
        source_rows += 1

        symbol = get_symbol(r)
        series = get_series(r)
        if not symbol or not series:
            continue

        isin = (r.get("ISIN") or "").strip() or None

        def num_key(*fields: str):
            """Try multiple possible column names, return float or None."""
            for field in fields:
                if field is None:
                    continue
                v = r.get(field)
                if v not in (None, "", "-", "NA", " "):
                    try:
                        return float(str(v).strip())
                    except ValueError:
                        continue
            return None

        def int_key(*fields: str):
            for field in fields:
                if field is None:
                    continue
                v = r.get(field)
                if v not in (None, "", "-", "NA", " "):
                    try:
                        return int(float(str(v).strip()))
                    except ValueError:
                        continue
            return None

        # Handle both OPEN / OPEN_PRICE variants
        open_price = num_key("OPEN", "OPEN_PRICE")
        high_price = num_key("HIGH", "HIGH_PRICE")
        low_price = num_key("LOW", "LOW_PRICE")
        close_price = num_key("CLOSE", "CLOSE_PRICE")

        # PREVCLOSE vs PREV_CLOSE
        prev_close = num_key("PREVCLOSE", "PREV_CLOSE")

        last_price = num_key("LAST", "LAST_PRICE")

        # VWAP vs AVG_PRICE
        vwap = num_key("VWAP", "AVG_PRICE")

        # TOTTRDQTY vs TTL_TRD_QNTY
        total_traded_qty = int_key("TOTTRDQTY", "TTL_TRD_QNTY")

        # TOTTRDVAL vs TURNOVER_LACS (we keep as-is; decide scaling later)
        total_traded_val = num_key("TOTTRDVAL", "TURNOVER_LACS")

        # TOTALTRADES vs NO_OF_TRADES
        no_of_trades = int_key("TOTALTRADES", "NO_OF_TRADES")

        # Deliverable from this file (optional – we still also have MTO)
        deliverable_qty = int_key("DELIV_QTY")
        deliv_per = num_key("DELIV_PER")

        rows.append(
            (
                trade_date,
                symbol,
                series,
                isin,
                open_price,
                high_price,
                low_price,
                close_price,
                prev_close,
                last_price,
                vwap,
                total_traded_qty,
                total_traded_val,
                no_of_trades,
                deliverable_qty,
                deliv_per,
                False,  # index_flag
            )
        )

    if not rows:
        debug_no_data("nse_sec_bhav_full", source_rows, first_keys)

    cols = [
        "trade_date",
        "symbol",
        "series",
        "isin",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "prev_close",
        "last_price",
        "vwap",
        "total_traded_qty",
        "total_traded_val",
        "no_of_trades",
        "deliverable_qty",
        "deliv_per",
        "index_flag",
    ]

    bulk_insert(
        conn,
        "eod.nse_sec_bhav_full",
        cols,
        rows,
        cleanup_sql="DELETE FROM eod.nse_sec_bhav_full WHERE trade_date = %s",
        cleanup_params=(trade_date,),
    )


def load_mto_delivery(conn, folder: Path, trade_date: date):
    """
    MTO_28112025.DAT -> eod.nse_delivery_eod

    File is comma-separated text with record types:

        10,MTO,28112025,.....
        10,...
        Record Type,Sr No,Name of Security,Quantity Traded,Deliverable Quantity(...),
                   % of Deliverable Quantity to Traded Quantity
        20,1,1058ISEF28,N4,1100,1100,100.00
        20,2,...

    Notes:
    - Newer DATs *do not* have a SYMBOL column; instead they carry "Name of Security".
    - We ignore the free-text title lines and the 10-record metadata rows.
    - We aggregate per SYMBOL (Name of Security) to one row, since PK is (trade_date, symbol).
    """
    pattern = f"MTO_{trade_date.strftime('%d%m%Y')}.DAT"
    path = folder / pattern
    if not path.exists():
        print(f"[nse_delivery_eod] File not found: {pattern}, skipping.")
        return

    with path.open("r", encoding="utf-8", newline="") as f:
        all_lines = [ln for ln in f]

    if not all_lines:
        print("[nse_delivery_eod] Empty MTO file, skipping.")
        return

    # ------------------------------------------------------------------
    # Find the *real* header:
    # - It is comma-delimited.
    # - It contains either 'RECORD TYPE' or 'NAME OF SECURITY' (2025 format),
    #   or the classic 'SYMBOL'/'SECURITY' headers.
    # - We explicitly skip title lines like "Security Wise Delivery Position..."
    #   that have no commas.
    # ------------------------------------------------------------------
    header_idx = None
    for i, ln in enumerate(all_lines):
        upper = ln.upper()
        if ("," not in upper) and ("|" not in upper):
            # Almost always the free-text title / blank lines.
            continue
        if ("RECORD TYPE" in upper and "SECURITY" in upper) or \
           ("NAME OF SECURITY" in upper) or \
           ("SYMBOL" in upper) or \
           ("SECURITY," in upper):
            header_idx = i
            break

    if header_idx is None:
        print("[nse_delivery_eod] No header line with SYMBOL/SECURITY found; skipping.")
        return

    header_line = all_lines[header_idx].strip()
    delimiter = "|" if "|" in header_line else ","
    header = [h.strip().upper() for h in header_line.split(delimiter)]

    data_lines = "".join(all_lines[header_idx + 1 :])
    if not data_lines.strip():
        print("[nse_delivery_eod] No data lines after header; skipping.")
        return

    reader = csv.DictReader(
        io.StringIO(data_lines),
        fieldnames=header,
        delimiter=delimiter,
    )

    # --- aggregation helpers (per symbol) ------------------------------------
    def sum_int(a, b):
        if a is None and b is None:
            return None
        if a is None:
            return b
        if b is None:
            return a
        try:
            return int(a) + int(b)
        except Exception:
            return a

    def sum_float(a, b):
        if a is None and b is None:
            return None
        if a is None:
            return b
        if b is None:
            return a
        try:
            return float(a) + float(b)
        except Exception:
            return a

    def clamp_ratio(r):
        """
        Keep ratio in a realistic band; DB column is numeric(6,2) so
        it must be < 10000. Real-world MTO ratios are 0–100.
        Anything negative or > 1000 is treated as None.
        """
        if r is None:
            return None
        try:
            val = float(r)
        except Exception:
            return None
        if val < 0 or val > 1000:
            return None
        return val

    def safe_ratio(tq, dq, existing=None):
        """
        Compute (dq / tq) * 100 with safety:
        - If tq <= 0 or any value missing, return existing.
        - Clamp to realistic range using clamp_ratio.
        """
        if tq is None or dq is None:
            return existing
        try:
            tq_val = float(tq)
            dq_val = float(dq)
        except Exception:
            return existing
        if tq_val <= 0:
            return existing
        r = (dq_val / tq_val) * 100.0
        r = clamp_ratio(r)
        if r is None:
            return existing
        return r

    agg: dict[str, dict] = {}
    source_rows = 0
    first_keys = None

    for r in reader:
        r = _normalize_row(r)

        if source_rows == 0:
            first_keys = sorted(r.keys())
        source_rows += 1

        symbol = get_symbol(r)
        if not symbol or symbol.upper() == "SYMBOL":
            continue

        # Many MTO files don't carry SERIES explicitly; default to EQ if missing.
        series = get_series(r) or "EQ"

        def num(field: str):
            v = r.get(field)
            if v is None:
                return None
            v = str(v).strip()
            if v in ("", "-", "NA"):
                return None
            try:
                return float(v)
            except ValueError:
                return None

        def intl(field: str):
            v = r.get(field)
            if v is None:
                return None
            v = str(v).strip()
            if v in ("", "-", "NA"):
                return None
            try:
                return int(float(v))
            except ValueError:
                return None

        # New 2025 headers:
        #   QUANTITY TRADED
        #   DELIVERABLE QUANTITY(GROSS ACROSS CLIENT LEVEL)
        #   % OF DELIVERABLE QUANTITY TO TRADED QUANTITY
        total_traded_qty = (
            intl("TOTTRDQTY")
            or intl("TRD_QTY")
            or intl("QUANTITY TRADED")
        )
        deliverable_qty = (
            intl("DELIV_QTY")
            or intl("DELIVERABLE QUANTITY(GROSS ACROSS CLIENT LEVEL)")
        )
        raw_ratio = (
            num("DELIV_PER")
            or num("% OF DELIVERABLE QUANTITY TO TRADED QUANTITY")
        )
        delivery_ratio = clamp_ratio(raw_ratio)
        total_traded_val = num("TRD_VAL") or num("TOTTRDVAL")

        if symbol not in agg:
            agg[symbol] = {
                "series": series,
                "total_traded_qty": total_traded_qty,
                "deliverable_qty": deliverable_qty,
                "delivery_ratio": delivery_ratio,
                "total_traded_val": total_traded_val,
            }
        else:
            cur = agg[symbol]
            if not cur.get("series") and series:
                cur["series"] = series

            cur["total_traded_qty"] = sum_int(
                cur.get("total_traded_qty"),
                total_traded_qty,
            )
            cur["deliverable_qty"] = sum_int(
                cur.get("deliverable_qty"),
                deliverable_qty,
            )
            cur["total_traded_val"] = sum_float(
                cur.get("total_traded_val"),
                total_traded_val,
            )

            # Prefer recomputing ratio when we have valid totals; otherwise,
            # fall back to the per-row ratio (clamped).
            tq = cur.get("total_traded_qty")
            dq = cur.get("deliverable_qty")
            cur_ratio = cur.get("delivery_ratio")
            cur["delivery_ratio"] = safe_ratio(tq, dq, existing=cur_ratio)

            if cur["delivery_ratio"] is None and delivery_ratio is not None:
                # Use row-level ratio as last fallback (also clamped)
                cur["delivery_ratio"] = delivery_ratio

    if not agg:
        debug_no_data("nse_delivery_eod", source_rows, first_keys)

    rows = []
    for symbol, d in agg.items():
        rows.append(
            (
                trade_date,
                symbol,
                d.get("series") or "EQ",
                d.get("total_traded_qty"),
                d.get("deliverable_qty"),
                d.get("delivery_ratio"),
                d.get("total_traded_val"),
            )
        )

    cols = [
        "trade_date",
        "symbol",
        "series",
        "total_traded_qty",
        "deliverable_qty",
        "delivery_ratio",
        "total_traded_val",
    ]

    bulk_insert(
        conn,
        "eod.nse_delivery_eod",
        cols,
        rows,
        cleanup_sql="DELETE FROM eod.nse_delivery_eod WHERE trade_date = %s",
        cleanup_params=(trade_date,),
    )


def load_short_selling(conn, folder: Path, trade_date: date):
    """
    shortselling_DDMMYYYY.csv -> eod.cm_short_selling

    Supports:
    - Older format with SYMBOL, SERIES, SHORT_QTY / QTY, SHORT_VAL / VAL
    - Newer simple format: QUANTITY, SECURITY NAME / SYMBOL NAME, TRADE DATE
    """
    pattern = f"shortselling_{trade_date.strftime('%d%m%Y')}.csv"
    path = folder / pattern
    if not path.exists():
        print(f"[cm_short_selling] File not found: {pattern}, skipping.")
        return

    rows = []
    source_rows = 0
    first_keys = None

    for r in read_csv(path):
        if source_rows == 0:
            first_keys = sorted(r.keys())
        source_rows += 1

        symbol = get_symbol(r)
        if not symbol:
            continue

        # Many shortselling files don't carry SERIES, assume EQ if blank
        series = get_series(r) or "EQ"

        def num(field: str):
            v = r.get(field)
            if v is None:
                return None
            v = str(v).strip()
            if v in ("", "-", "NA"):
                return None
            try:
                return float(v)
            except ValueError:
                return None

        short_qty = num("SHORT_QTY") or num("QTY") or num("QUANTITY")
        short_val = num("SHORT_VAL") or num("VAL")  # may be None in newer simple format

        rows.append(
            (
                trade_date,
                symbol,
                series,
                short_qty,
                short_val,
            )
        )

    if not rows:
        debug_no_data("cm_short_selling", source_rows, first_keys)

    cols = ["trade_date", "symbol", "series", "short_qty", "short_val"]

    bulk_insert(
        conn,
        "eod.cm_short_selling",
        cols,
        rows,
        cleanup_sql="DELETE FROM eod.cm_short_selling WHERE trade_date = %s",
        cleanup_params=(trade_date,),
    )


def load_cmvold(conn, folder: Path, trade_date: date):
    pattern = f"CMVOLT_{trade_date.strftime('%d%m%Y')}.CSV"
    path = folder / pattern
    if not path.exists():
        print(f"[cm_daily_volatility] File not found: {pattern}, skipping.")
        return

    rows = []
    source_rows = 0
    first_keys = None

    for r in read_csv(path):
        if source_rows == 0:
            first_keys = sorted(r.keys())
        source_rows += 1

        symbol = get_symbol(r)
        if not symbol:
            continue
        series = get_series(r)
        v = r.get("VOLATILITY") or r.get("CM_VOLT") or r.get("VOL")
        if v is None:
            vol = None
        else:
            v_str = str(v).strip()
            if v_str in ("", "-", "NA"):
                vol = None
            else:
                try:
                    vol = float(v_str)
                except ValueError:
                    vol = None
        rows.append((trade_date, symbol, series, vol))

    if not rows:
        debug_no_data("cm_daily_volatility", source_rows, first_keys)

    cols = ["trade_date", "symbol", "series", "volatility"]

    bulk_insert(
        conn,
        "eod.cm_daily_volatility",
        cols,
        rows,
        cleanup_sql="DELETE FROM eod.cm_daily_volatility WHERE trade_date = %s",
        cleanup_params=(trade_date,),
    )


def load_52wk_stats(conn, folder: Path, trade_date: date):
    """
    CM_52_wk_High_low_DDMMYYYY.csv -> eod.cm_52wk_stats

    Newer format (Nov 2025):
        "SYMBOL","SERIES","Adjusted_52_Week_High","52_Week_High_Date",
        "Adjusted_52_Week_Low","52_Week_Low_DT"

    Key FIX vs old version:
        - We let csv.DictReader parse the header + quotes correctly by
          feeding it the entire text from the header line onwards,
          instead of manually splitting the header line (which left
          quotes in the fieldnames and broke get_symbol()).
    """
    pattern = f"CM_52_wk_High_low_{trade_date.strftime('%d%m%Y')}.csv"
    path = folder / pattern
    if not path.exists():
        print(f"[cm_52wk_stats] File not found: {pattern}, skipping.")
        return

    with path.open("r", encoding="utf-8", newline="") as f:
        all_lines = [ln for ln in f]

    if not all_lines:
        print("[cm_52wk_stats] Empty file; skipping.")
        return

    # Find header line containing 'SYMBOL'
    header_idx = None
    for i, ln in enumerate(all_lines):
        if "SYMBOL" in ln.upper():
            header_idx = i
            break

    if header_idx is None:
        print("[cm_52wk_stats] No header line with 'SYMBOL' found; skipping.")
        return

    # Feed everything from header onwards to DictReader so it handles quotes properly.
    data_text = "".join(all_lines[header_idx:])
    reader = csv.DictReader(io.StringIO(data_text))

    rows = []
    source_rows = 0
    first_keys = None

    def num_from_row(r: dict, field: str):
        v = r.get(field)
        if v is None:
            return None
        s = str(v).strip()
        if s in ("", "-", "NA"):
            return None
        # remove thousand separators like '25,718.15'
        s = s.replace(",", "")
        try:
            return float(s)
        except ValueError:
            return None

    def parse_dt_from_row(r: dict, field: str):
        v = r.get(field)
        if v in (None, "", "-", "NA", " "):
            return None
        s = str(v).strip()
        for fmt in ("%d-%b-%Y", "%d-%b-%y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    for raw in reader:
        r = _normalize_row(raw)

        # Skip completely empty lines
        if not any(v not in (None, "", " ") for v in r.values()):
            continue

        if source_rows == 0:
            first_keys = sorted(r.keys())
        source_rows += 1

        symbol = get_symbol(r)
        if not symbol:
            continue
        series = get_series(r)

        # Support both old and new column names:
        high_price = (
            num_from_row(r, "HIGH_52_WK")
            or num_from_row(r, "ADJUSTED_52_WEEK_HIGH")
        )
        high_date = (
            parse_dt_from_row(r, "HIGH_52_WK_DATE")
            or parse_dt_from_row(r, "52_WEEK_HIGH_DATE")
        )
        low_price = (
            num_from_row(r, "LOW_52_WK")
            or num_from_row(r, "ADJUSTED_52_WEEK_LOW")
        )
        low_date = (
            parse_dt_from_row(r, "LOW_52_WK_DATE")
            or parse_dt_from_row(r, "52_WEEK_LOW_DT")
        )

        rows.append(
            (
                trade_date,
                symbol,
                series,
                high_price,
                high_date,
                low_price,
                low_date,
            )
        )

    # Keep rows even if one side is missing; only drop total junk rows.
    rows = [
        row
        for row in rows
        if (row[3] is not None or row[5] is not None)
    ]

    if not rows:
        debug_no_data("cm_52wk_stats", source_rows, first_keys)

    cols = [
        "trade_date",
        "symbol",
        "series",
        "high_52w_price",
        "high_52w_date",
        "low_52w_price",
        "low_52w_date",
    ]

    bulk_insert(
        conn,
        "eod.cm_52wk_stats",
        cols,
        rows,
        cleanup_sql="DELETE FROM eod.cm_52wk_stats WHERE trade_date = %s",
        cleanup_params=(trade_date,),
    )


def load_sme_bhav(conn, folder: Path, trade_date: date):
    pattern = f"sme{trade_date.strftime('%d%m%Y')}.csv"
    path = folder / pattern
    if not path.exists():
        print(f"[cm_sme_bhav] File not found: {pattern}, skipping.")
        return

    rows = []
    source_rows = 0
    first_keys = None

    for r in read_csv(path):
        if source_rows == 0:
            first_keys = sorted(r.keys())
        source_rows += 1

        symbol = get_symbol(r)
        if not symbol:
            continue

        def num(field: str):
            v = r.get(field)
            if v is None:
                return None
            v = str(v).strip()
            if v in ("", "-", "NA"):
                return None
            try:
                return float(v)
            except ValueError:
                return None

        qty_raw = r.get("TOTTRDQTY")
        if qty_raw in (None, "", "-", "NA", " "):
            total_traded_qty = None
        else:
            try:
                total_traded_qty = int(float(str(qty_raw).strip()))
            except ValueError:
                total_traded_qty = None

        rows.append(
            (
                trade_date,
                symbol,
                (r.get("ISIN") or "").strip() or None,
                num("OPEN"),
                num("HIGH"),
                num("LOW"),
                num("CLOSE"),
                num("PREVCLOSE") or num("PREV_CLOSE"),
                total_traded_qty,
                num("TOTTRDVAL"),
            )
        )

    if not rows:
        debug_no_data("cm_sme_bhav", source_rows, first_keys)

    cols = [
        "trade_date",
        "symbol",
        "isin",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "prev_close",
        "total_traded_qty",
        "total_traded_val",
    ]

    bulk_insert(
        conn,
        "eod.cm_sme_bhav",
        cols,
        rows,
        cleanup_sql="DELETE FROM eod.cm_sme_bhav WHERE trade_date = %s",
        cleanup_params=(trade_date,),
    )


def load_cm_security_master(conn, folder: Path, trade_date: date):
    """
    NSE_CM_security_DDMMYYYY.csv.gz -> eod.cm_security_master

    PK: (symbol, series, as_on_date)

    Requirements:
    - We want exactly one snapshot per (symbol, series, as_on_date).
    - Loader must be idempotent: re-running for the same trade_date should not error.
    - If file has duplicate rows for same (symbol, series), we keep the first.
    """
    pattern = f"NSE_CM_security_{trade_date.strftime('%d%m%Y')}.csv.gz"
    path = folder / pattern
    if not path.exists():
        print(f"[cm_security_master] File not found: {pattern}, skipping.")
        return

    dedup: dict[tuple[str, str], tuple] = {}
    source_rows = 0
    first_keys = None

    for r in read_csv_from_gzip(path):
        if source_rows == 0:
            first_keys = sorted(r.keys())
        source_rows += 1

        symbol = get_symbol(r)
        series = get_series(r)

        # IMPORTANT: avoid blank symbol/series which caused PK duplicate ('', '', date)
        if not symbol or not series:
            continue

        key = (symbol, series)
        if key in dedup:
            # Duplicate row for same symbol+series in file; keep the first
            continue

        def num(field: str):
            v = r.get(field)
            if v is None:
                return None
            v = str(v).strip()
            if v in ("", "-", "NA"):
                return None
            try:
                return float(v)
            except ValueError:
                return None

        listing_date = None
        ld = r.get("LISTING_DATE")
        if ld not in (None, "", "-", "NA", " "):
            try:
                listing_date = datetime.strptime(str(ld).strip(), "%d-%b-%Y").date()
            except ValueError:
                listing_date = None

        row = (
            symbol,
            series,
            (r.get("ISIN") or "").strip() or None,
            (r.get("SECURITY_NAME") or "").strip() or None,
            listing_date,
            (r.get("MARKET_TYPE") or "").strip() or None,
            (r.get("STATUS") or "").strip() or None,
            int(float(r["LOT_SIZE"])) if r.get("LOT_SIZE") not in (None, "", "-", "NA", " ") else None,
            num("TICK_SIZE"),
            (r.get("SEGMENT") or "").strip() or None,
            trade_date,
        )
        dedup[key] = row

    rows = list(dedup.values())

    if not rows:
        debug_no_data("cm_security_master", source_rows, first_keys)

    cols = [
        "symbol",
        "series",
        "isin",
        "security_name",
        "listing_date",
        "market_type",
        "status",
        "lot_size",
        "tick_size",
        "segment",
        "as_on_date",
    ]

    # IMPORTANT: delete existing snapshot for this as_on_date to make loader idempotent
    bulk_insert(
        conn,
        "eod.cm_security_master",
        cols,
        rows,
        cleanup_sql="DELETE FROM eod.cm_security_master WHERE as_on_date = %s",
        cleanup_params=(trade_date,),
    )


def load_pe_ratio(conn, folder: Path, trade_date: date):
    # For 28-11-2025 the actual name is PE_281125.csv
    pattern = f"PE_{trade_date.strftime('%d%m%y')}.csv"
    path = folder / pattern
    if not path.exists():
        print(f"[cm_pe_ratio] File not found: {pattern}, skipping.")
        return

    rows = []
    source_rows = 0
    first_keys = None

    for r in read_csv(path):
        if source_rows == 0:
            first_keys = sorted(r.keys())
        source_rows += 1

        symbol = get_symbol(r)
        if not symbol:
            continue
        series = get_series(r)

        def num(field: str):
            v = r.get(field)
            if v is None:
                return None
            v = str(v).strip()
            if v in ("", "-", "NA"):
                return None
            try:
                return float(v)
            except ValueError:
                return None

        rows.append(
            (trade_date, symbol, series, num("PE"), num("ADJ_PE") or num("ADJ_PE_RATIO"))
        )

    if not rows:
        debug_no_data("cm_pe_ratio", source_rows, first_keys)

    cols = ["trade_date", "symbol", "series", "pe_ratio", "adjusted_pe"]

    bulk_insert(
        conn,
        "eod.cm_pe_ratio",
        cols,
        rows,
        cleanup_sql="DELETE FROM eod.cm_pe_ratio WHERE trade_date = %s",
        cleanup_params=(trade_date,),
    )


def load_csqr_m(conn, folder: Path, trade_date: date):
    """
    CSQR_M_DDMMYYYY.csv -> eod.cm_csqr_m_raw

    There are two real-world formats:

    1) Older, header-based CSV with SYMBOL/SERIES/METRIC_CODE/...
       (we use DictReader + get_symbol/get_series).

    2) Newer (2025) header-less numeric CSV, as seen for 26 & 28 Nov 2025:
           523483,EQ,M,2025226,207.6
           522650,EQ,M,2025224,124.92
           ...

       We interpret as:
           col0: symbol_code  -> stored in symbol (text)
           col1: series       -> EQ / BE / SM / etc.
           col2: metric_code  -> typically 'M'
           col3: metric_period (e.g. 2025226)  -> stored as extra_value
           col4: metric_value  -> stored as metric_value

    Table PK is (trade_date, symbol, metric_code), so we must DEDUP
    on (symbol, metric_code) within the file.
    """
    pattern = f"CSQR_M_{trade_date.strftime('%d%m%Y')}.csv"
    path = folder / pattern
    if not path.exists():
        print(f"[cm_csqr_m_raw] File not found: {pattern}, skipping.")
        return

    rows = []
    source_rows = 0
    first_keys = None

    with path.open("r", encoding="utf-8", newline="") as f:
        raw_rows = [r for r in csv.reader(f) if any(cell.strip() for cell in r)]

    if not raw_rows:
        print("[cm_csqr_m_raw] Empty file; skipping.")
        return

    first = raw_rows[0]
    first_cell = (first[0] if first else "").strip().upper()

    def num_val(v):
        if v is None:
            return None
        s = str(v).strip()
        if s in ("", "-", "NA"):
            return None
        s = s.replace(",", "")
        try:
            return float(s)
        except ValueError:
            return None

    # We'll dedup on (symbol, metric_code) to respect PK.
    seen_keys: set[tuple[str, str]] = set()

    # ------------------------------------------------------------------
    # Case 1: header-based format (first cell is SYMBOL / SECURITY etc.)
    # ------------------------------------------------------------------
    if first_cell in ("SYMBOL", "SECURITY", "SYMB", "TCKRSYMB"):
        header = [c.strip().upper() for c in first]
        data_rows = raw_rows[1:]

        for row in data_rows:
            r = _normalize_row(dict(zip(header, row)))
            if source_rows == 0:
                first_keys = sorted(r.keys())
            source_rows += 1

            symbol = get_symbol(r)
            if not symbol:
                continue
            series = get_series(r)
            metric_code = (r.get("METRIC_CODE") or "M").strip() or "M"
            key = (symbol, metric_code)
            if key in seen_keys:
                # Avoid PK conflict within this trade_date
                continue
            seen_keys.add(key)

            v1 = r.get("VALUE") or r.get("METRIC_VALUE")
            v2 = r.get("EXTRA") or r.get("VALUE2")

            rows.append(
                (
                    trade_date,
                    symbol,
                    series,
                    metric_code,
                    num_val(v1),
                    num_val(v2),
                )
            )

    # ------------------------------------------------------------------
    # Case 2: header-less numeric format (2025)
    # ------------------------------------------------------------------
    else:
        for row in raw_rows:
            if len(row) < 5:
                continue
            source_rows += 1

            symbol_code = row[0].strip()
            if not symbol_code:
                continue
            series = (row[1] or "").strip() or None
            metric_code = (row[2] or "").strip() or "M"
            key = (symbol_code, metric_code)
            if key in seen_keys:
                # Avoid PK conflict for repeated symbol+metric_code rows
                continue
            seen_keys.add(key)

            metric_period = (row[3] or "").strip() or None
            metric_value = (row[4] or "").strip() or None

            rows.append(
                (
                    trade_date,
                    symbol_code,   # keep raw code in "symbol" for now
                    series,
                    metric_code,
                    num_val(metric_value),
                    num_val(metric_period),
                )
            )

    if not rows:
        debug_no_data("cm_csqr_m_raw", source_rows, first_keys)
        return

    cols = [
        "trade_date",
        "symbol",
        "series",
        "metric_code",
        "metric_value",
        "extra_value",
    ]

    bulk_insert(
        conn,
        "eod.cm_csqr_m_raw",
        cols,
        rows,
        cleanup_sql="DELETE FROM eod.cm_csqr_m_raw WHERE trade_date = %s",
        cleanup_params=(trade_date,),
    )


def load_var_margin(conn, folder: Path, trade_date: date):
    """
    C_VAR1_28112025_1.DAT ... C_VAR1_28112025_6.DAT -> eod.cm_var_margin_rates

    Real structure (C_VAR1 files) is CSV with record type:
    - '10', ... header/metadata
    - '20',SYMBOL,SERIES,ISIN,SECURITY_VAR,INDEX_VAR,ELM,EXP_MARGIN,...

    We only care about type '20' rows.
    """
    rows = []

    for snap in range(1, 7):
        pattern = f"C_VAR1_{trade_date.strftime('%d%m%Y')}_{snap}.DAT"
        path = folder / pattern
        if not path.exists():
            print(f"[cm_var_margin_rates] File not found: {pattern}, skipping snapshot {snap}.")
            continue

        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)  # comma-separated
            for line in reader:
                if not line:
                    continue

                rec_type = line[0].strip()
                if rec_type != "20":
                    # Skip header/summary rows (10/30 etc.)
                    continue

                if len(line) < 5:
                    continue

                symbol = line[1].strip()
                if not symbol:
                    continue
                series = line[2].strip() if len(line) > 2 else ""

                def num_idx(idx: int):
                    if idx >= len(line):
                        return None
                    v = line[idx]
                    if v is None:
                        return None
                    v = str(v).strip()
                    if v in ("", "-", "NA"):
                        return None
                    try:
                        return float(v)
                    except ValueError:
                        return None

                security_var = num_idx(4)
                index_var = num_idx(5)
                elm = num_idx(6)
                exp_margin = num_idx(7)

                rows.append(
                    (
                        trade_date,
                        snap,
                        symbol,
                        series,
                        security_var,
                        index_var,
                        elm,
                        exp_margin,
                    )
                )

    if not rows:
        print("[cm_var_margin_rates] No rows (no C_VAR1 files found or no type 20 rows); skipping.")
        return

    cols = [
        "trade_date",
        "snapshot_no",
        "symbol",
        "series",
        "security_var",
        "index_var",
        "extreme_loss_margin",
        "exposure_margin",
    ]

    bulk_insert(
        conn,
        "eod.cm_var_margin_rates",
        cols,
        rows,
        cleanup_sql="DELETE FROM eod.cm_var_margin_rates WHERE trade_date = %s",
        cleanup_params=(trade_date,),
    )


def load_index_eod_from_pr_zip(conn, folder: Path, trade_date: date):
    """
    PR281125.zip -> pr28112025.csv -> eod.index_eod_prices

    Inside PR zip we look for:
      prDDMMYYYY.csv  (e.g. pr28112025.csv)

    Expected columns (normalized uppercase):
      MKT, SECURITY, PREV_CL_PR, OPEN_PRICE, HIGH_PRICE, LOW_PRICE,
      CLOSE_PRICE, NET_TRDVAL, NET_TRDQTY, TRADES, HI_52_WK, LO_52_WK

    NOTE:
    - Table eod.index_eod_prices has PK (trade_date, index_name).
    - Some PR files can contain the same SECURITY multiple times (different MKT).
      To avoid PK violations, we keep the FIRST row per SECURITY and
      skip subsequent duplicates for that trade_date.
    """
    zip_name = f"PR{trade_date.strftime('%d%m%y')}.zip"
    zip_path = folder / zip_name
    if not zip_path.exists():
        print(f"[index_eod_prices] PR zip not found: {zip_name}, skipping index EOD load.")
        return

    csv_name_expected = f"pr{trade_date.strftime('%d%m%Y')}.csv"
    rows = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        target_name = None

        # Prefer exact expected name; else pick first file starting with 'pr' and ending with '.csv'
        if csv_name_expected in members:
            target_name = csv_name_expected
        else:
            for m in members:
                lower_m = m.lower()
                if lower_m.startswith("pr") and lower_m.endswith(".csv"):
                    target_name = m
                    break

        if not target_name:
            print(f"[index_eod_prices] No pr*.csv found in {zip_name}, skipping.")
            return

        seen_names: set[str] = set()  # to deduplicate SECURITY/index_name

        with zf.open(target_name, "r") as f:
            text = io.TextIOWrapper(f, encoding="utf-8", newline="")
            reader = csv.DictReader(text)
            for row in reader:
                r = _normalize_row(row)

                index_name = (r.get("SECURITY") or "").strip()
                if not index_name:
                    continue

                # Deduplicate on (trade_date, index_name) – trade_date is fixed in this function.
                if index_name in seen_names:
                    continue
                seen_names.add(index_name)

                def num(field: str):
                    v = r.get(field)
                    if v is None:
                        return None
                    v = str(v).strip()
                    if v in ("", "-", "NA"):
                        return None
                    try:
                        return float(v)
                    except ValueError:
                        # If some weird value comes (e.g. non-numeric), treat as missing
                        return None

                def intl(field: str):
                    v = r.get(field)
                    if v is None:
                        return None
                    v = str(v).strip()
                    if v in ("", "-", "NA"):
                        return None
                    try:
                        return int(float(v))
                    except ValueError:
                        return None

                rows.append(
                    (
                        trade_date,
                        index_name,
                        num("PREV_CL_PR"),
                        num("OPEN_PRICE"),
                        num("HIGH_PRICE"),
                        num("LOW_PRICE"),
                        num("CLOSE_PRICE"),
                        num("NET_TRDVAL"),
                        num("NET_TRDQTY"),
                        intl("TRADES"),
                        num("HI_52_WK"),
                        num("LO_52_WK"),
                    )
                )

    if not rows:
        print(f"[index_eod_prices] No index rows parsed from {zip_name}, skipping.")
        return

    cols = [
        "trade_date",
        "index_name",
        "prev_close",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "total_traded_val",
        "total_traded_qty",
        "trades",
        "high_52w",
        "low_52w",
    ]

    bulk_insert(
        conn,
        "eod.index_eod_prices",
        cols,
        rows,
        cleanup_sql="DELETE FROM eod.index_eod_prices WHERE trade_date = %s",
        cleanup_params=(trade_date,),
    )


def load_pr_family_raw(conn, folder: Path, trade_date: date):
    """
    Phase-2: capture optional PR-family CSVs into a single raw table.

    We look inside PR{ddmmyy}.zip for:
        mcapDDMMYYYY.csv      -> file_tag 'MCAP'
        etfDDMMYYYY.csv       -> file_tag 'ETF'
        glDDMMYYYY.csv        -> file_tag 'GL'
        hlDDMMYYYY.csv        -> file_tag 'HL'
        ttDDMMYYYY.csv        -> file_tag 'TT'
        corpbondDDMMYYYY.csv  -> file_tag 'CORPBOND'

    Each row is stored as:
        (trade_date, file_tag, row_no, payload_json)

    Table: eod.cm_pr_family_raw
        trade_date date
        file_tag   text
        row_no     int
        payload    jsonb
    """
    zip_name = f"PR{trade_date.strftime('%d%m%y')}.zip"
    zip_path = folder / zip_name
    if not zip_path.exists():
        print(f"[cm_pr_family_raw] PR zip not found: {zip_name}, skipping PR-family capture.")
        return

    file_specs = [
        ("MCAP", "mcap"),
        ("ETF", "etf"),
        ("GL", "gl"),
        ("HL", "hl"),
        ("TT", "tt"),
        ("CORPBOND", "corpbond"),
    ]

    rows = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        members_map = {m.lower(): m for m in members}

        for tag, prefix in file_specs:
            target_name = None
            for m_lower, m_real in members_map.items():
                if m_lower.startswith(prefix) and m_lower.endswith(".csv"):
                    target_name = m_real
                    break

            if not target_name:
                print(f"[cm_pr_family_raw] {prefix}*.csv not found in {zip_name}; skipping {tag}.")
                continue

            row_no = 0
            with zf.open(target_name, "r") as f:
                text = io.TextIOWrapper(f, encoding="utf-8", newline="")
                reader = csv.DictReader(text)
                for row in reader:
                    norm = _normalize_row(row)
                    row_no += 1
                    rows.append(
                        (
                            trade_date,
                            tag,
                            row_no,
                            Json(norm),
                        )
                    )

    cols = ["trade_date", "file_tag", "row_no", "payload"]

    bulk_insert(
        conn,
        "eod.cm_pr_family_raw",
        cols,
        rows,
        cleanup_sql="DELETE FROM eod.cm_pr_family_raw WHERE trade_date = %s",
        cleanup_params=(trade_date,),
    )


# TODO loaders (second wave, still pending): Margintrdg_*, C_CATG_*.T01, MA*, REG*, REG1*, FCM_INTRM_BC*,
# BhavCopy_NSE_CM_*.zip – we'll add them once core + PR family are verified.


# ---------- Orchestration ----------

def load_all_for_date(trade_date: date):
    folder = day_folder(trade_date)
    if not folder.exists():
        raise SystemExit(f"Folder not found for {trade_date}: {folder}")

    print(f"📂 Loading all CM reports for {trade_date} from {folder}")

    conn = get_conn()
    try:
        # Core cash + delivery
        load_sec_bhavdata_full(conn, folder, trade_date)
        load_mto_delivery(conn, folder, trade_date)

        # Short selling + volatility + 52wk
        load_short_selling(conn, folder, trade_date)
        load_cmvold(conn, folder, trade_date)
        load_52wk_stats(conn, folder, trade_date)

        # SME + security master
        load_sme_bhav(conn, folder, trade_date)
        load_cm_security_master(conn, folder, trade_date)

        # PE + CSQR + VAR
        load_pe_ratio(conn, folder, trade_date)
        load_csqr_m(conn, folder, trade_date)
        load_var_margin(conn, folder, trade_date)

        # Index EOD prices from PR zip
        load_index_eod_from_pr_zip(conn, folder, trade_date)

        # Phase-2: Optional PR-family CSVs (MCAP / ETF / GL / HL / TT / CORPBOND)
        load_pr_family_raw(conn, folder, trade_date)

        print("✅ Core + PR-family loaders finished for", trade_date)
        print("   (Remaining files: Margintrdg, C_CATG, MA, REG/REG1, FCM_INTRM_BC, BhavCopy_NSE_CM_*.zip – next wave.)")
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--date",
        required=True,
        help="Trade date in YYYY-MM-DD (e.g. 2025-11-28)",
    )
    args = ap.parse_args()
    d = parse_trade_date(args.date)
    load_all_for_date(d)


if __name__ == "__main__":
    main()
