# -*- coding: utf-8 -*-
"""
Load NSE Capital Market (cash) reports for a single date
from data/nse/cm_all_reports/YYYY/MM/DD into eod.* tables.

First target date for us: 2025-11-28.

This is a *v1 skeleton*:
- Handles the core, most important reports end-to-end.
- Has stubs/TODOs for some of the rarer ones (we’ll fill later).

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
from psycopg2.extras import execute_values

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
    for raw in read_csv(path):
        r = raw  # already normalized keys

        symbol = (r.get("SYMBOL") or "").strip()
        series = (r.get("SERIES") or "").strip()
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
    File is pipe- or comma-delimited text.
    """
    pattern = f"MTO_{trade_date.strftime('%d%m%Y')}.DAT"
    path = folder / pattern
    if not path.exists():
        print(f"[nse_delivery_eod] File not found: {pattern}, skipping.")
        return

    rows = []
    with path.open("r", encoding="utf-8", newline="") as f:
        first_line = f.readline()
        if not first_line:
            print("[nse_delivery_eod] Empty MTO file, skipping.")
            return

        delimiter = "|" if "|" in first_line else ","
        header = [h.strip().upper() for h in first_line.strip().split(delimiter)]
        reader = csv.DictReader(f, fieldnames=header, delimiter=delimiter)

        for r in reader:
            # Normalize keys
            r = _normalize_row(r)

            # Skip header repetition if present
            first_key = header[0]
            if r.get(first_key, "").strip().upper() == first_key:
                continue

            symbol = (r.get("SYMBOL") or "").strip()
            if not symbol or symbol == "SYMBOL":
                continue
            series = (r.get("SERIES") or "").strip()

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

            rows.append(
                (
                    trade_date,
                    symbol,
                    series,
                    intl("TOTTRDQTY") or intl("TRD_QTY"),
                    intl("DELIV_QTY"),
                    num("DELIV_PER"),
                    num("TRD_VAL") or num("TOTTRDVAL"),
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
    pattern = f"shortselling_{trade_date.strftime('%d%m%Y')}.csv"
    path = folder / pattern
    if not path.exists():
        print(f"[cm_short_selling] File not found: {pattern}, skipping.")
        return

    rows = []
    for r in read_csv(path):
        symbol = (r.get("SYMBOL") or "").strip()
        if not symbol:
            continue
        series = (r.get("SERIES") or "").strip()

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
            (
                trade_date,
                symbol,
                series,
                num("SHORT_QTY") or num("QTY"),
                num("SHORT_VAL") or num("VAL"),
            )
        )

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
    for r in read_csv(path):
        symbol = (r.get("SYMBOL") or "").strip()
        if not symbol:
            continue
        series = (r.get("SERIES") or "").strip()
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
    pattern = f"CM_52_wk_High_low_{trade_date.strftime('%d%m%Y')}.csv"
    path = folder / pattern
    if not path.exists():
        print(f"[cm_52wk_stats] File not found: {pattern}, skipping.")
        return

    rows = []
    for r in read_csv(path):
        symbol = (r.get("SYMBOL") or "").strip()
        if not symbol:
            continue
        series = (r.get("SERIES") or "").strip()

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

        def parse_dt(field: str):
            v = r.get(field)
            if v in (None, "", "-", "NA", " "):
                return None
            return datetime.strptime(str(v).strip(), "%d-%b-%Y").date()

        rows.append(
            (
                trade_date,
                symbol,
                series,
                num("HIGH_52_WK"),
                parse_dt("HIGH_52_WK_DATE"),
                num("LOW_52_WK"),
                parse_dt("LOW_52_WK_DATE"),
            )
        )

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
    for r in read_csv(path):
        symbol = (r.get("SYMBOL") or "").strip()
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
    pattern = f"NSE_CM_security_{trade_date.strftime('%d%m%Y')}.csv.gz"
    path = folder / pattern
    if not path.exists():
        print(f"[cm_security_master] File not found: {pattern}, skipping.")
        return

    rows = []
    for r in read_csv_from_gzip(path):
        symbol = (r.get("SYMBOL") or "").strip()
        series = (r.get("SERIES") or "").strip()

        # IMPORTANT: avoid blank symbol/series which caused PK duplicate ('', '', date)
        if not symbol or not series:
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
            listing_date = datetime.strptime(str(ld).strip(), "%d-%b-%Y").date()

        rows.append(
            (
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
        )

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
    # For master we usually don't delete; we just insert a new 'as_on_date' snapshot.
    bulk_insert(conn, "eod.cm_security_master", cols, rows)


def load_pe_ratio(conn, folder: Path, trade_date: date):
    # For 28-11-2025 the actual name is PE_281125.csv
    pattern = f"PE_{trade_date.strftime('%d%m%y')}.csv"
    path = folder / pattern
    if not path.exists():
        print(f"[cm_pe_ratio] File not found: {pattern}, skipping.")
        return

    rows = []
    for r in read_csv(path):
        symbol = (r.get("SYMBOL") or "").strip()
        if not symbol:
            continue
        series = (r.get("SERIES") or "").strip()

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
    pattern = f"CSQR_M_{trade_date.strftime('%d%m%Y')}.csv"
    path = folder / pattern
    if not path.exists():
        print(f"[cm_csqr_m_raw] File not found: {pattern}, skipping.")
        return

    rows = []
    for r in read_csv(path):
        symbol = (r.get("SYMBOL") or "").strip()
        if not symbol:
            continue
        series = (r.get("SERIES") or "").strip()
        metric_code = (r.get("METRIC_CODE") or "M").strip() or "M"
        v1 = r.get("VALUE") or r.get("METRIC_VALUE")
        v2 = r.get("EXTRA") or r.get("VALUE2")

        def num_val(v):
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
            (trade_date, symbol, series, metric_code, num_val(v1), num_val(v2))
        )

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
                    # Optional: debug log if you ever want to see which ones are duplicated
                    # print(f"[index_eod_prices] Duplicate SECURITY for {trade_date}: {index_name} – skipping extra row.")
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


# TODO loaders (second wave): Margintrdg_*, C_CATG_*.T01, MA*, REG*, REG1*, FCM_INTRM_BC*,
# additional PR* files (mcap, etf, gl, hl, tt, corpbond, etc.), BhavCopy_NSE_CM_*.zip
# – we'll add them once core is verified.


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

        # NEW: Index EOD prices from PR zip
        load_index_eod_from_pr_zip(conn, folder, trade_date)

        print("✅ Core loaders finished for", trade_date)
        print("   (Remaining files: Margintrdg, C_CATG, MA, REG/REG1, FCM_INTRM_BC,")
        print("    and remaining PR CSVs like mcap/etf/gl/hl/tt/corpbond – we'll wire those next.)")
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
