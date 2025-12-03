# -*- coding: utf-8 -*-
"""
Standalone loader for NSE CM 52-week high/low report.

Source file (per trade_date):
    CM_52_wk_High_low_DDMMYYYY.csv

Example (26-Nov-2025 file header):
    "SYMBOL","SERIES","Adjusted_52_Week_High","52_Week_High_Date",
    "Adjusted_52_Week_Low","52_Week_Low_DT"

Target table: eod.cm_52wk_stats

Columns expected in DB:
    trade_date      date
    symbol          text
    series          text
    high_52w_price  numeric
    high_52w_date   date
    low_52w_price   numeric
    low_52w_date    date

Usage (from repo root):

    python -m reports.load_cm_52wk_stats --date 2025-11-26
"""

import argparse
import csv
import io
from pathlib import Path
from datetime import datetime, date

import psycopg2

# ---------------------------------------------------------------------
# DB CONNECTION HELPER (same style as load_cm_reports)
# ---------------------------------------------------------------------
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


# ---------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
CM_ROOT = REPO_ROOT / "data" / "nse" / "cm_all_reports"


def parse_trade_date(s: str) -> date:
    """Accept 'YYYY-MM-DD' and return date object."""
    return datetime.strptime(s, "%Y-%m-%d").date()


def day_folder(trade_date: date) -> Path:
    return CM_ROOT / f"{trade_date.year:04d}" / f"{trade_date.month:02d}" / f"{trade_date.day:02d}"


# ---------------------------------------------------------------------
# NORMALIZATION HELPERS (copied from load_cm_reports)
# ---------------------------------------------------------------------

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


def get_symbol(r: dict) -> str:
    """
    Try multiple possible header names for SYMBOL/SECURITY, after _normalize_row.
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


# ---------------------------------------------------------------------
# BULK INSERT HELPER (same pattern as in load_cm_reports)
# ---------------------------------------------------------------------

from psycopg2.extras import execute_values  # noqa: E402


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


# ---------------------------------------------------------------------
# 52-WEEK STATS LOADER (NEW, FIXED IMPLEMENTATION)
# ---------------------------------------------------------------------

def _num_from_row(r: dict, field: str):
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


def _parse_dt_from_row(r: dict, field: str):
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


def load_52wk_stats(conn, folder: Path, trade_date: date):
    """
    CM_52_wk_High_low_DDMMYYYY.csv -> eod.cm_52wk_stats

    Actual 2025 header (26, 27, 28 Nov 2025):
        "SYMBOL","SERIES","Adjusted_52_Week_High","52_Week_High_Date",
        "Adjusted_52_Week_Low","52_Week_Low_DT"

    Key FIX compared to the earlier version:
        - We let csv.DictReader parse the header and quotes properly by
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

    # Feed everything from header onwards to DictReader
    data_text = "".join(all_lines[header_idx:])
    reader = csv.DictReader(io.StringIO(data_text))

    rows = []
    source_rows = 0
    first_keys = None

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
            _num_from_row(r, "HIGH_52_WK")
            or _num_from_row(r, "ADJUSTED_52_WEEK_HIGH")
        )
        high_date = (
            _parse_dt_from_row(r, "HIGH_52_WK_DATE")
            or _parse_dt_from_row(r, "52_WEEK_HIGH_DATE")
        )
        low_price = (
            _num_from_row(r, "LOW_52_WK")
            or _num_from_row(r, "ADJUSTED_52_WEEK_LOW")
        )
        low_date = (
            _parse_dt_from_row(r, "LOW_52_WK_DATE")
            or _parse_dt_from_row(r, "52_WEEK_LOW_DT")
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


# ---------------------------------------------------------------------
# ORCHESTRATION
# ---------------------------------------------------------------------

def load_for_date(trade_date: date):
    folder = day_folder(trade_date)
    if not folder.exists():
        raise SystemExit(f"Folder not found for {trade_date}: {folder}")

    print(f"📂 Loading CM 52-week stats for {trade_date} from {folder}")

    conn = get_conn()
    try:
        load_52wk_stats(conn, folder, trade_date)
        print("✅ 52-week stats loader finished for", trade_date)
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--date",
        required=True,
        help="Trade date in YYYY-MM-DD (e.g. 2025-11-26)",
    )
    args = ap.parse_args()
    d = parse_trade_date(args.date)
    load_for_date(d)


if __name__ == "__main__":
    main()
