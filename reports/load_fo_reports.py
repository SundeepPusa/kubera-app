# -*- coding: utf-8 -*-
"""
Kubera EOD — NSE F&O Daily Loader (LOCAL-ONLY VERSION)

Phase-1 features:
    - For a given trade_date:
        * Expect that ALL relevant NSE F&O reports for that date are already
          downloaded into:

              data/nse/fo_all_reports/YYYY/MM/DD/

          (This includes Daily zipem bundle + Archives ZIPs/GZs, etc.)

        * Extract all nested ZIPs and GZs in that folder.
        * Load core derivatives tables in osa_db:

          1) eod.fo_bhavcopy
          2) eod.fo_contract_master
          3) eod.fo_oi_mwpl_limit           (combineoi MWPL + limit file)
          4) eod.fo_contract_delta_factor   (Contract_Delta per-contract factor)
          5) eod.fo_ncl_oi
          6) eod.fo_participant_oi
          7) eod.fo_participant_vol
          8) eod.fo_top10_turnover / eod.fo_top10_derivatives_summary
          9) eod.fo_secban
         10) eod.fo_client_oi_limit
         11) eod.fo_elm_margin
         12) eod.fo_settlement_price
         13) eod.fo_volatility_surface   (core columns only + raw_fields when present)

    - Market activity (fo0412…zip) and other exotic reports are downloaded
      by separate scripts and are not yet wired to DB (TODO markers in code).

Usage examples (LOCAL FILES ONLY):

    # Single date: use already-downloaded reports under data/nse/fo_all_reports
    python -m reports.load_fo_reports --date 2025-12-04

Notes:
    * This script DOES NOT call NSE URLs.
      It only works with files present on disk.
    * Make sure your separate download scripts store files into:
         data/nse/fo_all_reports/YYYY/MM/DD/
    * DB connection uses the same helper pattern as load_cm_reports:
      - Prefer reports.etl_utils.get_pg_conn(), but wrapped with a
        DNS-fallback to localhost.
      - Fallback to psycopg2 with password from env/.env

    IMPORTANT:
      You still need a reachable PostgreSQL instance:
        - Either your AWS RDS host, or
        - A local PostgreSQL server on localhost:5432.

      If RDS DNS fails and local DB is not running, the script will now
      raise a clear RuntimeError instead of a confusing chain of errors.
"""
import uuid

import argparse
import csv
import datetime as dt
import gzip
import json
import logging
import os
from pathlib import Path
from typing import Optional, List, Tuple, Dict

import zipfile

import psycopg2
import psycopg2.extras


# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

LOG = logging.getLogger("load_fo_reports")

# Resolve repo root so paths are stable regardless of CWD
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = REPO_ROOT / "data" / "nse" / "fo_all_reports"

# Default RDS host (used only if no env host is provided)
DEFAULT_DB_HOST = "osa-db.c12ccwc88y0j.eu-north-1.rds.amazonaws.com"


# -------------------------------------------------------------------
# DB HELPER (aligned with load_cm_reports, with robust DNS fallback)
# -------------------------------------------------------------------

try:
    # Preferred: shared ETL utility (same as cash loader)
    from reports import etl_utils  # type: ignore
except Exception:  # pragma: no cover
    etl_utils = None  # type: ignore


def _get_conn_from_env(host_override: Optional[str] = None):
    """
    Internal helper: connect using env/.env configuration.

    Priority for password:
    - PGPASSWORD
    - DB_PASSWORD
    - OSA_DB_PASSWORD

    Host resolution (unless overridden):
    - PGHOST
    - DB_HOST
    - OSA_DB_HOST
    - DEFAULT_DB_HOST (RDS)

    If the resolved host is DEFAULT_DB_HOST and we hit a DNS error
    ("could not translate host name", "Name or service not known",
     "Temporary failure in name resolution"), we automatically
    fall back to 'localhost' to support local DB usage when
    RDS is not reachable (e.g. office network without AWS access).

    If BOTH RDS DNS and localhost fail, we raise a RuntimeError with
    a clear explanation so the caller can fix network / DB status.
    """
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv()
    except Exception:
        # .env is optional; ignore if missing or dotenv not installed
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

    dbname = os.getenv("PGDATABASE", "osa_db")
    user = os.getenv("PGUSER", "osa_admin")
    host = (
        host_override
        or os.getenv("PGHOST")
        or os.getenv("DB_HOST")
        or os.getenv("OSA_DB_HOST")
        or DEFAULT_DB_HOST
    )
    port = int(os.getenv("PGPORT", "5432"))

    # First attempt: resolved host (RDS or whatever env says)
    try:
        return psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )
    except psycopg2.OperationalError as exc:
        msg = str(exc)

        # DNS / name resolution problems → try localhost as a fallback
        if (
            host_override is None
            and host == DEFAULT_DB_HOST
            and (
                "could not translate host name" in msg
                or "Name or service not known" in msg
                or "Temporary failure in name resolution" in msg
            )
        ):
            LOG.warning(
                "Primary DB host '%s' not resolvable (%s). "
                "Falling back to 'localhost'.",
                host,
                msg,
            )
            try:
                return psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host="localhost",
                    port=port,
                )
            except psycopg2.OperationalError as exc2:
                # Both RDS DNS and localhost have failed → raise a clear error
                LOG.error(
                    "Tried fallback to localhost:5432 but connection also failed: %s",
                    exc2,
                )
                raise RuntimeError(
                    "Database not reachable.\n"
                    f"- Primary host '{host}' DNS failed with: {msg}\n"
                    "- Fallback to 'localhost:5432' also failed. Either start "
                    "your local PostgreSQL server or ensure your network/DNS "
                    "allows access to the RDS endpoint."
                ) from exc2

        # Any other error: re-raise the original psycopg2 error
        raise


def get_db_conn():
    """
    Public DB helper.

    - If reports.etl_utils.get_pg_conn() exists, use it, but wrap
      with DNS-error handling: on "could not translate host name" /
      "Name or service not known" etc., we fall back to the env/localhost
      connection used by other loaders.

    - If etl_utils is not available, or if it fails with a non-DNS error,
      fall back to _get_conn_from_env() so this script is usable in
      isolation as well.
    """
    # Try shared ETL helper first (if available)
    if etl_utils is not None:
        try:
            return etl_utils.get_pg_conn()
        except psycopg2.OperationalError as exc:
            msg = str(exc)
            if (
                "could not translate host name" in msg
                or "Name or service not known" in msg
                or "Temporary failure in name resolution" in msg
            ):
                LOG.warning(
                    "etl_utils.get_pg_conn() failed with DNS error (%s). "
                    "Falling back to env/localhost connection.",
                    msg,
                )
                return _get_conn_from_env()
            # Non-DNS operational error → surface to caller
            raise
        except Exception as exc:  # pragma: no cover
            LOG.warning(
                "etl_utils.get_pg_conn() raised %s; falling back to env/localhost.",
                exc,
            )
            return _get_conn_from_env()

    # No etl_utils available → use local env-based configuration
    return _get_conn_from_env()


# -------------------------------------------------------------------
# GENERIC HELPERS
# -------------------------------------------------------------------

def parse_iso_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def ensure_folder_for_date(trade_date: dt.date) -> Path:
    """
    Ensure folder: data/nse/fo_all_reports/YYYY/MM/DD
    and return it.
    """
    folder = (
        DATA_ROOT
        / str(trade_date.year)
        / f"{trade_date.month:02d}"
        / f"{trade_date.day:02d}"
    )
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def extract_nested_zips(root_folder: Path) -> None:
    """
    After external download, decompress any nested ZIPs
    (e.g., BhavCopy_NSE_FO_*.csv.zip, ncloi_*.zip, fo0412*.zip).

    NOTE:
        We intentionally SKIP the top-level "fo_all_YYYY-MM-DD*.zip"
        to avoid re-extracting the master bundle if present.
    """
    for z_path in sorted(root_folder.rglob("*.zip")):
        # Skip the top-level "fo_all_YYYY-MM-DD.zip" if present
        if z_path.name.startswith("fo_all_"):
            continue
        LOG.info("Nested ZIP found → extracting: %s", z_path)
        try:
            with zipfile.ZipFile(z_path, "r") as zf:
                zf.extractall(z_path.parent)
        except Exception as exc:
            LOG.warning("Failed to extract nested ZIP %s: %s", z_path, exc)


def decompress_gz_files(root_folder: Path) -> None:
    """
    Decompress any .gz files (e.g., NSE_FO_contract_*.csv.gz) into .csv
    in the same folder (skip if .csv already exists).
    """
    for gz_path in sorted(root_folder.rglob("*.gz")):
        out_name = gz_path.with_suffix("")  # strip .gz
        if out_name.exists():
            LOG.info("CSV from gz already exists; skipping %s", out_name)
            continue

        LOG.info("Decompressing GZ → %s", gz_path)
        try:
            with gzip.open(gz_path, "rb") as gzf, out_name.open("wb") as f:
                f.write(gzf.read())
        except Exception as exc:
            LOG.warning("Failed to decompress %s: %s", gz_path, exc)


def find_first(root_folder: Path, patterns: List[str]) -> Optional[Path]:
    """
    Recursively find the first matching file for any of the glob patterns.

    NOTE:
        Earlier we used root_folder.glob(), which only checked the top level.
        Now we use rglob() so files in archives/, zipem/, fo/, etc. are found.
    """
    for pat in patterns:
        matches = sorted(root_folder.rglob(pat))
        if matches:
            LOG.info("Found file for pattern %r → %s", pat, matches[0])
            return matches[0]
    return None


def to_int(val: str) -> Optional[int]:
    val = (val or "").strip()
    if not val:
        return None
    try:
        return int(val.replace(",", ""))
    except ValueError:
        return None


def to_numeric(val: str) -> Optional[float]:
    val = (val or "").strip()
    if not val:
        return None
    try:
        return float(val.replace(",", ""))
    except ValueError:
        return None


def to_date(val: str, formats: List[str]) -> Optional[dt.date]:
    """
    Try multiple date formats; return None if all fail.

    Extra robustness:
      - Strips whitespace
      - If the string contains letters (month names like DEC / Dec / december),
        we try a few normalized variants (title/upper/lower).
      - If value contains time (space or 'T'), we also try the first 10 chars
        as a pure date (YYYY-MM-DD / DD-MMM-YYYY etc.).
    """
    val = (val or "").strip()
    if not val:
        return None

    base_candidates = [val]

    # If there are alphabetic chars (month names), add normalized variants
    if any(c.isalpha() for c in val):
        base_candidates.extend([val.title(), val.upper(), val.lower()])

    seen = set()
    for base in base_candidates:
        if base in seen:
            continue
        seen.add(base)

        # If string has time part, also try just the date portion
        sub_candidates = [base]
        if len(base) > 10 and (" " in base or "T" in base):
            sub_candidates.append(base[:10])

        for cand in sub_candidates:
            for fmt in formats:
                try:
                    return dt.datetime.strptime(cand, fmt).date()
                except ValueError:
                    continue

    return None


def get_first(row: Dict[str, str], *keys: str) -> str:
    """
    Return the first non-empty value for the given keys from a row dict.
    Used where NSE may change column names (e.g. XPRYDT / XPRY_DT / EXPIRY_DT)
    or minor spacings (FUTIDX_LONG / FUTIDX LONG / FUT IDX LONG).

    Keys should already be in the same normalization (UPPERCASE) used by the loader.
    """
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip():
            return str(v)
    return ""


def sniff_header_index(
    path: Path,
    candidates: List[List[str]],
    *,
    encoding: str = "utf-8",
    max_lines: int = 40,
) -> int:
    """
    Inspect the first `max_lines` of a CSV file and return the 0-based
    line index that looks like the real header row.

    `candidates` is a list of keyword groups:
        [
            ["SYMBOL", "INSTRUMENT"],
            ["CLIENT TYPE", "FUTURE INDEX LONG"],
            ...
        ]

    A line is considered a match if ALL keywords in any group are found
    (case-insensitive) in that line.

    Fallback: if nothing matches, return 0 (first line is header).
    """
    try:
        with path.open("r", encoding=encoding, newline="") as f:
            for idx, line in enumerate(f):
                if idx >= max_lines:
                    break
                text = line.strip().upper()
                if not text:
                    continue
                for group in candidates:
                    if all(g.upper() in text for g in group):
                        LOG.info("Detected header at line %d in %s", idx, path)
                        return idx
    except Exception as exc:
        LOG.warning("sniff_header_index failed for %s: %s", path, exc)

    LOG.info("Header autodetect fallback → line 0 for %s", path)
    return 0


def load_csv_as_dicts(
    path: Path,
    *,
    skip_initial_lines: int = 0,
    encoding: str = "utf-8",
    uppercase_headers: bool = True,
) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Generic CSV helper:

    - Uses csv.reader (handles quoted commas correctly)
    - Skips `skip_initial_lines` lines (for title rows)
    - Normalizes headers: strip + optional upper-case
    - Strips BOM from the first header, if present
    - Returns (headers, rows_as_dicts)

    All subsequent loaders can safely refer to UPPERCASE header names.
    """
    rows: List[Dict[str, str]] = []
    with path.open("r", newline="", encoding=encoding) as f:
        # Skip title / notes lines, if any
        for _ in range(skip_initial_lines):
            f.readline()

        try:
            header_row = next(csv.reader(f))
        except StopIteration:
            return [], []

        raw_headers = [h.strip() for h in header_row]

        # Handle BOM (e.g. \ufeffSYMBOL → SYMBOL)
        if raw_headers and raw_headers[0].startswith("\ufeff"):
            raw_headers[0] = raw_headers[0].lstrip("\ufeff")

        if uppercase_headers:
            headers = [h.upper() for h in raw_headers]
        else:
            headers = raw_headers

        reader = csv.reader(f)
        for cols in reader:
            # Skip completely empty lines
            if not any(str(c).strip() for c in cols):
                continue

            row: Dict[str, str] = {}
            for i, h in enumerate(headers):
                row[h] = cols[i].strip() if i < len(cols) else ""
            rows.append(row)

    return headers, rows


def table_has_columns(conn, schema: str, table: str, columns: List[str]) -> bool:
    """
    Utility: check if all given columns exist in (schema.table).
    Used to make loaders tolerant to older DB schemas.
    """
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name   = %s
          AND column_name IN ({placeholders})
    """
    with conn.cursor() as cur:
        cur.execute(sql, [schema, table] + columns)
        found = {row[0] for row in cur.fetchall()}
    missing = [c for c in columns if c not in found]
    if missing:
        LOG.warning(
            "Table %s.%s is missing columns %s (found: %s)",
            schema,
            table,
            ", ".join(missing),
            ", ".join(sorted(found)) if found else "NONE",
        )
        return False
    return True


def table_has_column(conn, schema: str, table: str, column: str) -> bool:
    """
    Lightweight check for a single column; used where we want
    to branch logic (e.g. presence of raw_fields) without logging
    a noisy warning if the column is not there.
    """
    sql = """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name   = %s
          AND column_name  = %s
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, [schema, table, column])
        return cur.fetchone() is not None


# -------------------------------------------------------------------
# LOADERS — BHAVCOPY
# -------------------------------------------------------------------

def load_fo_bhavcopy(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Load eod.fo_bhavcopy from BhavCopy_NSE_FO_*.csv (MII format).
    Source usually comes from:
        BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv

    NOTE:
        This loader is ALREADY WORKING (as seen in logs), so we keep
        it as-is to avoid breaking a known-good ingestion.
    """
    csv_path = find_first(
        folder,
        ["BhavCopy_NSE_FO_*.csv", "BhavCopy_NSE_FO_0_0_0_*_F_0000.csv"],
    )
    if not csv_path:
        LOG.warning(
            "[%s] fo_bhavcopy CSV not found under %s (recursive)",
            trade_date,
            folder,
        )
        return

    LOG.info("[%s] Loading fo_bhavcopy from %s", trade_date, csv_path)

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            # Expected NSE headers (2025 MII):
            # TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctyGrp,
            # ExpryDt,StrkPric,OptnTp,OpnPric,HghPric,LwPric,ClsPric,SttlmPric,
            # PrvsClsgPric,TtlNbOfCtrctsTrdd,TtlTrfVal,TtlNbOfOprnIntrst,
            # ChngInNbOfOprnIntrst,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,Rmks,
            # Rsvd1,Rsvd2,Rsvd3,Rsvd4

            trade_dt = to_date(row.get("TradDt"), ["%Y-%m-%d", "%d-%b-%Y"])
            if trade_dt != trade_date:
                # We only keep the requested trade_date.
                continue

            rows.append(
                (
                    trade_dt,
                    to_date(row.get("BizDt"), ["%Y-%m-%d", "%d-%b-%Y"]),
                    row.get("Sgmt") or "",
                    row.get("Src") or "",
                    row.get("FinInstrmTp") or "",
                    to_int(row.get("FinInstrmId")),
                    (row.get("ISIN") or "").strip() or None,
                    (row.get("TckrSymb") or "").strip(),
                    (row.get("SctyGrp") or "").strip() or None,
                    to_date(
                        row.get("ExpryDt"),
                        ["%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y"],
                    ),
                    to_numeric(row.get("StrkPric")),
                    (row.get("OptnTp") or "").strip() or None,
                    to_numeric(row.get("OpnPric")),
                    to_numeric(row.get("HghPric")),
                    to_numeric(row.get("LwPric")),
                    to_numeric(row.get("ClsPric")),
                    to_numeric(row.get("SttlmPric")),
                    to_numeric(row.get("PrvsClsgPric")),
                    to_int(row.get("TtlNbOfCtrctsTrdd")),
                    to_numeric(row.get("TtlTrfVal")),
                    to_int(row.get("TtlNbOfOprnIntrst")),
                    to_int(row.get("ChngInNbOfOprnIntrst")),
                    to_int(row.get("TtlNbOfTxsExctd")),
                    (row.get("SsnId") or "").strip() or None,
                    to_int(row.get("NewBrdLotQty")),
                    (row.get("Rmks") or "").strip() or None,
                    (row.get("Rsvd1") or "").strip() or None,
                    (row.get("Rsvd2") or "").strip() or None,
                    (row.get("Rsvd3") or "").strip() or None,
                    (row.get("Rsvd4") or "").strip() or None,
                )
            )

    if not rows:
        LOG.warning("[%s] No valid rows for fo_bhavcopy", trade_date)
        return

    sql = """
        INSERT INTO eod.fo_bhavcopy (
            trade_date, business_date, segment, src, fin_instrument_type,
            fin_instrm_id, isin, symbol, security_group, expiry_date,
            strike_price, option_type, open_price, high_price, low_price,
            close_price, settle_price, prev_close_price, contracts_traded,
            traded_value, open_interest, change_in_oi, total_trades,
            session_id, lot_size, remarks, rsvd1, rsvd2, rsvd3, rsvd4
        )
        VALUES (
            %(trade_date)s, %(business_date)s, %(segment)s, %(src)s,
            %(fin_instrument_type)s, %(fin_instrm_id)s, %(isin)s,
            %(symbol)s, %(security_group)s, %(expiry_date)s,
            %(strike_price)s, %(option_type)s, %(open_price)s,
            %(high_price)s, %(low_price)s, %(close_price)s,
            %(settle_price)s, %(prev_close_price)s, %(contracts_traded)s,
            %(traded_value)s, %(open_interest)s, %(change_in_oi)s,
            %(total_trades)s, %(session_id)s, %(lot_size)s, %(remarks)s,
            %(rsvd1)s, %(rsvd2)s, %(rsvd3)s, %(rsvd4)s
        )
        ON CONFLICT (trade_date, fin_instrm_id) DO UPDATE
        SET
            business_date       = EXCLUDED.business_date,
            segment             = EXCLUDED.segment,
            src                 = EXCLUDED.src,
            fin_instrument_type = EXCLUDED.fin_instrument_type,
            isin                = EXCLUDED.isin,
            symbol              = EXCLUDED.symbol,
            security_group      = EXCLUDED.security_group,
            expiry_date         = EXCLUDED.expiry_date,
            strike_price        = EXCLUDED.strike_price,
            option_type         = EXCLUDED.option_type,
            open_price          = EXCLUDED.open_price,
            high_price          = EXCLUDED.high_price,
            low_price           = EXCLUDED.low_price,
            close_price         = EXCLUDED.close_price,
            settle_price        = EXCLUDED.settle_price,
            prev_close_price    = EXCLUDED.prev_close_price,
            contracts_traded    = EXCLUDED.contracts_traded,
            traded_value        = EXCLUDED.traded_value,
            open_interest       = EXCLUDED.open_interest,
            change_in_oi        = EXCLUDED.change_in_oi,
            total_trades        = EXCLUDED.total_trades,
            session_id          = EXCLUDED.session_id,
            lot_size            = EXCLUDED.lot_size,
            remarks             = EXCLUDED.remarks,
            rsvd1               = EXCLUDED.rsvd1,
            rsvd2               = EXCLUDED.rsvd2,
            rsvd3               = EXCLUDED.rsvd3,
            rsvd4               = EXCLUDED.rsvd4
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            sql,
            [
                {
                    "trade_date": r[0],
                    "business_date": r[1],
                    "segment": r[2],
                    "src": r[3],
                    "fin_instrument_type": r[4],
                    "fin_instrm_id": r[5],
                    "isin": r[6],
                    "symbol": r[7],
                    "security_group": r[8],
                    "expiry_date": r[9],
                    "strike_price": r[10],
                    "option_type": r[11],
                    "open_price": r[12],
                    "high_price": r[13],
                    "low_price": r[14],
                    "close_price": r[15],
                    "settle_price": r[16],
                    "prev_close_price": r[17],
                    "contracts_traded": r[18],
                    "traded_value": r[19],
                    "open_interest": r[20],
                    "change_in_oi": r[21],
                    "total_trades": r[22],
                    "session_id": r[23],
                    "lot_size": r[24],
                    "remarks": r[25],
                    "rsvd1": r[26],
                    "rsvd2": r[27],
                    "rsvd3": r[28],
                    "rsvd4": r[29],
                }
                for r in rows
            ],
            page_size=1000,
        )
    LOG.info("[%s] fo_bhavcopy upserted: %d rows", trade_date, len(rows))


# -------------------------------------------------------------------
# CONTRACT MASTER (NSE_FO_contract + fallback from fo_bhavcopy)
# -------------------------------------------------------------------

from psycopg2.extras import DictCursor


def _insert_contract_master_rows(
    conn,
    rows: List[Dict[str, object]],
    has_raw_fields: bool,
) -> int:
    """
    Common upsert helper for eod.fo_contract_master.

    Each row dict must contain:
        trade_date, fin_instrm_id, underlying_fin_instrm_id,
        instrument_name, symbol, expiry_date, strike_price,
        option_type, series_id, tick_size, lot_size,
        contract_status, raw_fields (optional if has_raw_fields=False).
    """
    if not rows:
        return 0

    if has_raw_fields:
        sql = """
            INSERT INTO eod.fo_contract_master (
                trade_date,
                fin_instrm_id,
                underlying_fin_instrm_id,
                instrument_name,
                symbol,
                expiry_date,
                strike_price,
                option_type,
                series_id,
                tick_size,
                lot_size,
                contract_status,
                raw_fields
            )
            VALUES (
                %(trade_date)s,
                %(fin_instrm_id)s,
                %(underlying_fin_instrm_id)s,
                %(instrument_name)s,
                %(symbol)s,
                %(expiry_date)s,
                %(strike_price)s,
                %(option_type)s,
                %(series_id)s,
                %(tick_size)s,
                %(lot_size)s,
                %(contract_status)s,
                %(raw_fields)s
            )
            ON CONFLICT (fin_instrm_id) DO UPDATE
            SET
                trade_date               = EXCLUDED.trade_date,
                underlying_fin_instrm_id = EXCLUDED.underlying_fin_instrm_id,
                instrument_name          = EXCLUDED.instrument_name,
                symbol                   = EXCLUDED.symbol,
                expiry_date              = EXCLUDED.expiry_date,
                strike_price             = EXCLUDED.strike_price,
                option_type              = EXCLUDED.option_type,
                series_id                = EXCLUDED.series_id,
                tick_size                = EXCLUDED.tick_size,
                lot_size                 = EXCLUDED.lot_size,
                contract_status          = EXCLUDED.contract_status,
                raw_fields               = EXCLUDED.raw_fields
        """
        params = [
            {
                **r,
                "raw_fields": json.dumps(r.get("raw_fields") or {}),
            }
            for r in rows
        ]
    else:
        sql = """
            INSERT INTO eod.fo_contract_master (
                trade_date,
                fin_instrm_id,
                underlying_fin_instrm_id,
                instrument_name,
                symbol,
                expiry_date,
                strike_price,
                option_type,
                series_id,
                tick_size,
                lot_size,
                contract_status
            )
            VALUES (
                %(trade_date)s,
                %(fin_instrm_id)s,
                %(underlying_fin_instrm_id)s,
                %(instrument_name)s,
                %(symbol)s,
                %(expiry_date)s,
                %(strike_price)s,
                %(option_type)s,
                %(series_id)s,
                %(tick_size)s,
                %(lot_size)s,
                %(contract_status)s
            )
            ON CONFLICT (fin_instrm_id) DO UPDATE
            SET
                trade_date               = EXCLUDED.trade_date,
                underlying_fin_instrm_id = EXCLUDED.underlying_fin_instrm_id,
                instrument_name          = EXCLUDED.instrument_name,
                symbol                   = EXCLUDED.symbol,
                expiry_date              = EXCLUDED.expiry_date,
                strike_price             = EXCLUDED.strike_price,
                option_type              = EXCLUDED.option_type,
                series_id                = EXCLUDED.series_id,
                tick_size                = EXCLUDED.tick_size,
                lot_size                 = EXCLUDED.lot_size,
                contract_status          = EXCLUDED.contract_status
        """
        params = rows

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, params, page_size=1000)

    return len(rows)


def derive_contract_master_from_bhavcopy(
    conn,
    trade_date: dt.date,
    has_raw_fields: bool,
) -> int:
    """
    Fallback path when NSE_FO_contract_*.csv cannot be parsed or absent.

    We derive a minimal but VALID contract master directly from
    eod.fo_bhavcopy, which is already loaded and has:

        - trade_date
        - fin_instrm_id
        - symbol
        - expiry_date
        - strike_price
        - option_type
        - lot_size

    This is enough for:
        - instrument resolution
        - mapping fin_instrm_id → (symbol, expiry, strike, option_type, lot)

    Notes:
        - expiry_date is still expected to be NOT NULL in your DB, so we skip
          any rows where expiry_date IS NULL.
        - instrument_name is now allowed to be NULL at DB level, but for the
          fallback we still default it to symbol as a safe, descriptive value
          wherever symbol is available.
    """
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT
                trade_date,
                fin_instrm_id,
                symbol,
                expiry_date,
                strike_price,
                option_type,
                lot_size
            FROM eod.fo_bhavcopy
            WHERE trade_date = %s
              AND fin_instrm_id IS NOT NULL
              AND expiry_date IS NOT NULL
            """,
            (trade_date,),
        )
        rows_bhav = cur.fetchall()

    if not rows_bhav:
        LOG.warning(
            "[%s] derive_contract_master_from_bhavcopy: "
            "no fo_bhavcopy rows found with non-null expiry_date; "
            "cannot build fallback contract master.",
            trade_date,
        )
        return 0

    rows: List[Dict[str, object]] = []
    for r in rows_bhav:
        # Symbol is mandatory for us – without it we can't build a meaningful contract.
        symbol = (r["symbol"] or "").strip()
        if not symbol:
            LOG.debug(
                "[%s] derive_contract_master_from_bhavcopy: "
                "skipping fin_instrm_id=%s due to empty symbol",
                trade_date,
                r["fin_instrm_id"],
            )
            continue

        # Expiry must be non-null to satisfy fo_contract_master NOT NULL constraint
        expiry = r["expiry_date"]
        if expiry is None:
            LOG.debug(
                "[%s] derive_contract_master_from_bhavcopy: "
                "skipping fin_instrm_id=%s symbol=%s due to NULL expiry_date",
                trade_date,
                r["fin_instrm_id"],
                symbol,
            )
            continue

        # instrument_name is now nullable in DB, but for fallback
        # it is still helpful to store something descriptive.
        instrument_name = symbol or None

        rows.append(
            {
                "trade_date": r["trade_date"],
                "fin_instrm_id": r["fin_instrm_id"],
                "underlying_fin_instrm_id": None,
                "instrument_name": instrument_name,
                "symbol": symbol,
                "expiry_date": expiry,
                "strike_price": r["strike_price"],
                "option_type": (r["option_type"] or "").strip() or None,
                "series_id": None,
                "tick_size": None,
                "lot_size": r["lot_size"],
                "contract_status": None,
                "raw_fields": {},
            }
        )

    if not rows:
        LOG.warning(
            "[%s] derive_contract_master_from_bhavcopy: "
            "no usable distinct contracts built after symbol/expiry checks.",
            trade_date,
        )
        return 0

    # Optional: clean old rows for that trade_date to keep things neat
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM eod.fo_contract_master WHERE trade_date = %s",
            (trade_date,),
        )

    inserted = _insert_contract_master_rows(conn, rows, has_raw_fields)
    LOG.info(
        "[%s] fo_contract_master fallback (from fo_bhavcopy) upserted: %d rows",
        trade_date,
        inserted,
    )
    return inserted

def _parse_contract_date_from_contract_file(raw: str) -> Optional[dt.date]:
    """
    NSE_FO_contract files (MII) often store dates as *epoch seconds*,
    e.g. XpryDt = 1451572200.

    This helper first tries to interpret purely numeric values as
    epoch seconds (UTC), and then falls back to normal date formats.
    """
    raw = (raw or "").strip()
    if not raw:
        return None

    # Epoch seconds (typical length 9–11 digits)
    if raw.isdigit() and 9 <= len(raw) <= 11:
        try:
            return dt.datetime.utcfromtimestamp(int(raw)).date()
        except (ValueError, OSError):
            # Fall through to string formats
            pass

    # Normal date formats as backup
    return to_date(
        raw,
        ["%Y-%m-%d", "%d-%b-%Y", "%d-%b-%y", "%d-%m-%Y", "%d/%m/%Y"],
    )

def load_fo_contract_master(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Primary contract master loader.

    Preference:
        1) Use NSE_FO_contract_*.csv if available (MII contract file),
           whether it came from Archives or Zipem (we search recursively
           under the date folder).
        2) If file missing or yields no usable rows → fallback from fo_bhavcopy.

    Key points for MII-style files:
        - FinInstrmId            → FININSTRMID
        - UndrlygFinInstrmId     → UNDRLYGFININSTRMID  (note the 'G')
        - FinInstrmNm            → FININSTRMNM
        - TckrSymb               → TCKRSYMB
        - XpryDt                 → XPRYDT (epoch seconds)
        - StrkPric               → STRKPRIC
        - OptnTp                 → OPTNTP
        - NewBrdLotQty           → NEWBRDLOTQTY
        - SrsId                  → SRSID
    """
    has_raw_fields = table_has_column(conn, "eod", "fo_contract_master", "raw_fields")

    csv_path = find_first(
        folder,
        ["NSE_FO_contract_*.csv", "nse_fo_contract_*.csv"],
    )
    if not csv_path:
        LOG.warning(
            "[%s] NSE_FO_contract CSV not found under %s (recursive); "
            "falling back to fo_bhavcopy.",
            trade_date,
            folder,
        )
        derive_contract_master_from_bhavcopy(conn, trade_date, has_raw_fields)
        return

    LOG.info("[%s] Loading fo_contract_master from %s", trade_date, csv_path)

    headers, raw_rows = load_csv_as_dicts(
        csv_path,
        skip_initial_lines=0,
        uppercase_headers=True,
    )

    rows: List[Dict[str, object]] = []

    for row in raw_rows:
        # --------------------------------------------------------------
        # 1) fin_instrm_id (MUST HAVE)
        # --------------------------------------------------------------
        fin_instrm_id = to_int(
            get_first(
                row,
                "FININSTRMID",
                "FIN_INSTRM_ID",
                "FIN INSTRM ID",
                "FININSTRM_ID",
                "INSTRUMENTID",
                "INSTRM_ID",
            )
        )
        if fin_instrm_id is None:
            continue

        # --------------------------------------------------------------
        # 2) Symbol / ticker (MUST HAVE)
        # --------------------------------------------------------------
        symbol = (
            get_first(row, "TCKRSYMB", "SYMBOL", "UNDERLYING", "SCRIP")
            or ""
        ).strip()
        if not symbol:
            continue

        # --------------------------------------------------------------
        # 3) Expiry date (MUST HAVE; NOT NULL in DB)
        #     - MII file uses XPRYDT with epoch seconds.
        #     - Older formats may have EXPRYDT / EXPIRYDATE etc.
        # --------------------------------------------------------------
        expiry_raw = get_first(
            row,
            "XPRYDT",         # MII epoch seconds
            "EXPRYDT",
            "XPRY_DT",
            "EXPRY_DT",
            "EXPIRYDATE",
            "EXPIRY DATE",
            "EXPIRY_DT",
        )
        expiry = _parse_contract_date_from_contract_file(expiry_raw)
        if expiry is None:
            # Can't satisfy NOT NULL constraint on expiry_date
            continue

        # --------------------------------------------------------------
        # 4) Strike, option type, lot size
        # --------------------------------------------------------------
        strike_price = to_numeric(
            get_first(row, "STRKPRIC", "STRIKEPRICE", "STRIKE PRICE")
        )

        option_type = (
            get_first(row, "OPTNTP", "OPTIONTYPE", "OPTION TYPE", "OPT_TYPE")
            or ""
        ).strip().upper() or None

        lot_size = to_int(
            get_first(
                row,
                "NEWBRDLOTQTY",
                "LOT_SIZE",
                "LOT SIZE",
                "CONTRACTSIZ",
                "CONTRACT SIZE",
                "MINLOT",
            )
        )

        # --------------------------------------------------------------
        # 5) Underlying fin_instrm_id (optional, but useful)
        #     File header: UNDRLYGFININSTRMID (note the 'G')
        # --------------------------------------------------------------
        underlying_fin_instrm_id = to_int(
            get_first(
                row,
                "UNDRLYGFININSTRMID",  # MII spelling
                "UNDERLYFININSTRMID",
                "UNDRLYFININSTRMID",
                "UNDERLYINGFININSTRMID",
                "UNDERLYING_FININSTRMID",
            )
        )

        # --------------------------------------------------------------
        # 6) Instrument name, series, tick size, contract status
        # --------------------------------------------------------------
        instrument_name = (
            get_first(
                row,
                "FININSTRMNM",
                "INSTRUMENT",
                "INSTRUMENTNAME",
                "SECURITY_NAME",
                "SCTYNM",
            )
            or symbol
        ).strip() or None

        series_id = (
            get_first(row, "SRSID", "SERIES_ID", "SERIES") or ""
        ).strip() or None

        tick_size = to_numeric(
            get_first(
                row,
                "MINPRC",        # some old files
                "TICKSIZE",
                "TICK SIZE",
                "TICK_SIZE",
                "BIDINTRVL",     # MII Bid Interval ~ tick
            )
        )

        contract_status = (
            get_first(
                row,
                "CONTRACTSTATUS",
                "CONTRACT STATUS",
                "STATUS",
                "PRTDTOTRAD",   # sometimes "Permitted to Trade" style flag
            )
            or ""
        ).strip() or None

        # --------------------------------------------------------------
        # 7) Build row dict for DB insert/upsert
        # --------------------------------------------------------------
        rows.append(
            {
                "trade_date": trade_date,
                "fin_instrm_id": fin_instrm_id,
                "underlying_fin_instrm_id": underlying_fin_instrm_id,
                "instrument_name": instrument_name,
                "symbol": symbol,
                "expiry_date": expiry,
                "strike_price": strike_price,
                "option_type": option_type,
                "series_id": series_id,
                "tick_size": tick_size,
                "lot_size": lot_size,
                "contract_status": contract_status,
                "raw_fields": row,  # full row (UPPERCASE keys) for debugging
            }
        )

    # --------------------------------------------------------------
    # If the file gave us nothing usable, fall back to bhavcopy
    # --------------------------------------------------------------
    if not rows:
        LOG.warning(
            "[%s] NSE_FO_contract CSV yielded no usable rows; "
            "falling back to fo_bhavcopy.",
            trade_date,
        )
        derive_contract_master_from_bhavcopy(conn, trade_date, has_raw_fields)
        return

    # Clean existing rows for this trade_date to keep the snapshot neat
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM eod.fo_contract_master WHERE trade_date = %s",
            (trade_date,),
        )

    inserted = _insert_contract_master_rows(conn, rows, has_raw_fields)
    LOG.info(
        "[%s] fo_contract_master upserted from NSE_FO_contract file: %d rows",
        trade_date,
        inserted,
    )


# -------------------------------------------------------------------
# LOADERS — COMBINE OI (MWPL LIMIT) / DELTA FACTOR / NCL OI
# -------------------------------------------------------------------

def load_fo_combine_oi(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Load MWPL + limit file from combineoi_*.csv into eod.fo_oi_mwpl_limit.

    File format (2025):
        Date, ISIN, Scrip Name, NSE Symbol, MWPL, Open Interest,
        Future Equivalent Open Interest, Limit for Next Day
    """
    csv_path = find_first(folder, ["combineoi_*.csv"])
    if not csv_path:
        LOG.warning(
            "[%s] combineoi CSV (MWPL) not found under %s (recursive)",
            trade_date,
            folder,
        )
        return

    LOG.info("[%s] Loading fo_oi_mwpl_limit from %s", trade_date, csv_path)

    headers, raw_rows = load_csv_as_dicts(
        csv_path,
        skip_initial_lines=0,
        uppercase_headers=True,
    )

    rows = []
    for row in raw_rows:
        row_date = to_date(
            row.get("DATE"),
            ["%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"],
        )
        if row_date != trade_date:
            continue

        isin = (row.get("ISIN") or "").strip() or None
        scrip_name = (row.get("SCRIP NAME") or "").strip() or None
        symbol = (
            get_first(row, "NSE SYMBOL", "SYMBOL")
            or ""
        ).strip()
        if not symbol:
            continue

        mwpl = to_numeric(row.get("MWPL"))
        open_interest = to_numeric(
            get_first(row, "OPEN INTEREST", "OPENINTEREST")
        )
        fut_eq_oi = to_numeric(
            get_first(
                row,
                "FUTURE EQUIVALENT OPEN INTEREST",
                "FUTURE EQUIVALENT OI",
            )
        )

        limit_raw = (
            get_first(row, "LIMIT FOR NEXT DAY", "LIMIT FOR NEXTDAY") or ""
        ).strip()
        limit_next_day = to_numeric(limit_raw)  # "No Fresh Positions" → None

        rows.append(
            {
                "trade_date": trade_date,
                "isin": isin,
                "symbol": symbol,
                "scrip_name": scrip_name,
                "mwpl": mwpl,
                "open_interest": open_interest,
                "fut_eq_oi": fut_eq_oi,
                "limit_next_day": limit_next_day,
                "raw_fields": row,
            }
        )

    if not rows:
        LOG.warning("[%s] No MWPL rows after parsing combineoi file", trade_date)
        return

    sql = """
        INSERT INTO eod.fo_oi_mwpl_limit (
            trade_date, isin, symbol, scrip_name,
            mwpl, open_interest, fut_eq_oi, limit_next_day, raw_fields
        )
        VALUES (
            %(trade_date)s, %(isin)s, %(symbol)s, %(scrip_name)s,
            %(mwpl)s, %(open_interest)s, %(fut_eq_oi)s,
            %(limit_next_day)s, %(raw_fields)s
        )
        ON CONFLICT (trade_date, symbol) DO UPDATE
        SET
            isin          = EXCLUDED.isin,
            scrip_name    = EXCLUDED.scrip_name,
            mwpl          = EXCLUDED.mwpl,
            open_interest = EXCLUDED.open_interest,
            fut_eq_oi     = EXCLUDED.fut_eq_oi,
            limit_next_day= EXCLUDED.limit_next_day,
            raw_fields    = EXCLUDED.raw_fields
    """

    params_list = []
    for r in rows:
        params_list.append(
            {
                "trade_date": r["trade_date"],
                "isin": r["isin"],
                "symbol": r["symbol"],
                "scrip_name": r["scrip_name"],
                "mwpl": r["mwpl"],
                "open_interest": r["open_interest"],
                "fut_eq_oi": r["fut_eq_oi"],
                "limit_next_day": r["limit_next_day"],
                "raw_fields": json.dumps(r["raw_fields"]),
            }
        )

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, params_list, page_size=500)
    LOG.info("[%s] fo_oi_mwpl_limit upserted: %d rows", trade_date, len(rows))


def load_fo_contract_delta(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Load eod.fo_contract_delta_factor from Contract_Delta_*.csv

    File format (2025):
        Date,Symbol,Expiry day,Strike Price,Option Type,Delta Factor

    We map into fo_contract_delta_factor with generic:
        segment          = 'FNO'
        underlying       = Symbol
        instrument       = 'FUT' if Option Type='FF' else 'OPT'
        expiryyymm       = YYYYMM (from Expiry day)
        price_range      = 'ALL'
        moneyness_factor = 'GENERIC'
        base_delta_factor= Delta Factor
    """
    csv_path = find_first(folder, ["Contract_Delta_*.csv"])
    if not csv_path:
        LOG.warning(
            "[%s] Contract_Delta CSV not found under %s (recursive)",
            trade_date,
            folder,
        )
        return

    LOG.info(
        "[%s] Loading fo_contract_delta_factor from %s",
        trade_date,
        csv_path,
    )

    headers, raw_rows = load_csv_as_dicts(
        csv_path,
        skip_initial_lines=0,
        uppercase_headers=True,
    )

    rows = []
    for row in raw_rows:
        row_date = to_date(
            row.get("DATE"),
            ["%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"],
        )
        if row_date != trade_date:
            continue

        symbol = (row.get("SYMBOL") or "").strip()
        if not symbol:
            continue

        expiry = to_date(
            get_first(row, "EXPIRY DAY", "EXPIRY DATE"),
            ["%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"],
        )
        expiryyymm = expiry.strftime("%Y%m") if expiry else None

        opt_type = (row.get("OPTION TYPE") or "").strip().upper()
        if opt_type == "FF":
            instrument = "FUT"
        else:
            instrument = "OPT"

        delta_factor = to_numeric(row.get("DELTA FACTOR"))

        if not expiryyymm:
            # Without expiry bucket we can't satisfy NOT NULL, so skip
            LOG.debug(
                "[%s] Skipping delta row for %s due to missing expiry",
                trade_date,
                symbol,
            )
            continue

        rows.append(
            {
                "trade_date": trade_date,
                "segment": "FNO",
                "underlying": symbol,
                "instrument": instrument,
                "expiryyymm": expiryyymm,
                "price_range": "ALL",
                "moneyness_factor": "GENERIC",
                "base_delta_factor": delta_factor,
                "lower_lim_base_delta_factor": None,
                "higher_lim_base_delta_factor": None,
                "higher_delta_factor": None,
                "lower_threshold_val": None,
                "higher_threshold_val": None,
                "raw_fields": row,
            }
        )

    if not rows:
        LOG.warning("[%s] No contract delta rows after parsing", trade_date)
        return

    sql = """
        INSERT INTO eod.fo_contract_delta_factor (
            trade_date, segment, underlying, instrument, expiryyymm,
            expiry_group_code, price_range, moneyness_factor,
            base_delta_factor,
            lower_lim_base_delta_factor,
            higher_lim_base_delta_factor,
            higher_delta_factor,
            lower_threshold_val,
            higher_threshold_val,
            raw_fields
        )
        VALUES (
            %(trade_date)s, %(segment)s, %(underlying)s, %(instrument)s,
            %(expiryyymm)s, %(expiry_group_code)s, %(price_range)s,
            %(moneyness_factor)s, %(base_delta_factor)s,
            %(lower_lim_base_delta_factor)s,
            %(higher_lim_base_delta_factor)s,
            %(higher_delta_factor)s,
            %(lower_threshold_val)s,
            %(higher_threshold_val)s,
            %(raw_fields)s
        )
        ON CONFLICT (trade_date, segment, underlying, instrument, expiryyymm, price_range, moneyness_factor)
        DO UPDATE SET
            base_delta_factor           = EXCLUDED.base_delta_factor,
            lower_lim_base_delta_factor = EXCLUDED.lower_lim_base_delta_factor,
            higher_lim_base_delta_factor= EXCLUDED.higher_lim_base_delta_factor,
            higher_delta_factor         = EXCLUDED.higher_delta_factor,
            lower_threshold_val         = EXCLUDED.lower_threshold_val,
            higher_threshold_val        = EXCLUDED.higher_threshold_val,
            raw_fields                  = EXCLUDED.raw_fields
    """

    params_list = []
    for r in rows:
        params_list.append(
            {
                "trade_date": r["trade_date"],
                "segment": r["segment"],
                "underlying": r["underlying"],
                "instrument": r["instrument"],
                "expiryyymm": r["expiryyymm"],
                "expiry_group_code": None,
                "price_range": r["price_range"],
                "moneyness_factor": r["moneyness_factor"],
                "base_delta_factor": r["base_delta_factor"],
                "lower_lim_base_delta_factor": r["lower_lim_base_delta_factor"],
                "higher_lim_base_delta_factor": r["higher_lim_base_delta_factor"],
                "higher_delta_factor": r["higher_delta_factor"],
                "lower_threshold_val": r["lower_threshold_val"],
                "higher_threshold_val": r["higher_threshold_val"],
                "raw_fields": json.dumps(r["raw_fields"]),
            }
        )

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, params_list, page_size=500)
    LOG.info(
        "[%s] fo_contract_delta_factor upserted: %d rows",
        trade_date,
        len(rows),
    )


def load_fo_ncl_oi(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Load eod.fo_ncl_oi from NCL OI csv (ncloi_*.csv).

    New format (2025):
        Date,ISIN,Scrip Name,NSE Symbol,MWPL,
        NCL Open Interest,NCL Futeq OI,NCL OI as % of MWPL

    We aggregate at (trade_date, symbol, instrument) level in DB.
    For this report, instrument is a logical bucket and we use 'ALL'
    to represent the stock-level aggregate.
    """
    csv_path = find_first(folder, ["ncloi_*.csv"])
    if not csv_path:
        LOG.warning(
            "[%s] ncloi CSV not found under %s (recursive)",
            trade_date,
            folder,
        )
        return

    LOG.info("[%s] Loading fo_ncl_oi from %s", trade_date, csv_path)

    headers, raw_rows = load_csv_as_dicts(
        csv_path,
        skip_initial_lines=0,
        uppercase_headers=True,
    )

    rows = []
    for row in raw_rows:
        row_date = to_date(
            row.get("DATE"),
            ["%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"],
        )
        if row_date != trade_date:
            continue

        isin = (row.get("ISIN") or "").strip() or None
        scrip_name = (row.get("SCRIP NAME") or "").strip() or None
        symbol = (get_first(row, "NSE SYMBOL", "SYMBOL") or "").strip()
        if not symbol or symbol.upper() == "SYMBOL":
            continue

        mwpl = to_numeric(row.get("MWPL"))

        open_interest = to_numeric(
            get_first(
                row,
                "NCL OPEN INTEREST",
                "OPEN INTEREST",
                "NCL OPENINTEREST",
                "OI",
            )
        )

        ncl_fut_eq_oi = to_numeric(
            get_first(row, "NCL FUTEQ OI", "NCL FUTEQ OI ")
        )

        oi_pct = to_numeric(
            get_first(
                row,
                "NCL OI AS % OF MWPL",
                "% OI",
                "%OI",
                "OI%",
                "OI %",
            )
        )

        rows.append(
            {
                "trade_date": trade_date,
                "symbol": symbol,
                "instrument": "ALL",      # logical aggregate bucket
                "mwpl": mwpl,
                "open_interest": open_interest,
                "oi_pct": oi_pct,
                "isin": isin,
                "scrip_name": scrip_name,
                "ncl_fut_eq_oi": ncl_fut_eq_oi,
                "raw_fields": row,
            }
        )

    if not rows:
        LOG.warning("[%s] No ncloi rows after parsing", trade_date)
        return

    sql = """
        INSERT INTO eod.fo_ncl_oi (
            trade_date,
            symbol,
            instrument,
            mwpl,
            open_interest,
            oi_pct,
            isin,
            scrip_name,
            ncl_fut_eq_oi,
            raw_fields
        )
        VALUES (
            %(trade_date)s,
            %(symbol)s,
            %(instrument)s,
            %(mwpl)s,
            %(open_interest)s,
            %(oi_pct)s,
            %(isin)s,
            %(scrip_name)s,
            %(ncl_fut_eq_oi)s,
            %(raw_fields)s
        )
        ON CONFLICT (trade_date, symbol, instrument) DO UPDATE
        SET
            mwpl          = EXCLUDED.mwpl,
            open_interest = EXCLUDED.open_interest,
            oi_pct        = EXCLUDED.oi_pct,
            isin          = EXCLUDED.isin,
            scrip_name    = EXCLUDED.scrip_name,
            ncl_fut_eq_oi = EXCLUDED.ncl_fut_eq_oi,
            raw_fields    = EXCLUDED.raw_fields
    """

    params_list = []
    for r in rows:
        params_list.append(
            {
                "trade_date": r["trade_date"],
                "symbol": r["symbol"],
                "instrument": r["instrument"],
                "mwpl": r["mwpl"],
                "open_interest": r["open_interest"],
                "oi_pct": r["oi_pct"],
                "isin": r["isin"],
                "scrip_name": r["scrip_name"],
                "ncl_fut_eq_oi": r["ncl_fut_eq_oi"],
                "raw_fields": json.dumps(r["raw_fields"]),
            }
        )

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, params_list, page_size=500)
    LOG.info("[%s] fo_ncl_oi upserted: %d rows", trade_date, len(rows))


# -------------------------------------------------------------------
# LOADERS — PARTICIPANT OI / VOLUME / TOP10
# -------------------------------------------------------------------

def load_fo_participant_oi(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Load eod.fo_participant_oi from fao_participant_oi_*.csv

    File format usually has 1–2 title lines before the header.
    We now auto-detect the header row using 'CLIENT TYPE' and
    'FUTURE INDEX LONG' markers.
    """
    csv_path = find_first(folder, ["fao_participant_oi_*.csv"])
    if not csv_path:
        LOG.warning(
            "[%s] fao_participant_oi CSV not found under %s (recursive)",
            trade_date,
            folder,
        )
        return

    LOG.info("[%s] Loading fo_participant_oi from %s", trade_date, csv_path)

    header_idx = sniff_header_index(
        csv_path,
        candidates=[["CLIENT TYPE", "FUTURE INDEX LONG"]],
    )
    _, raw_rows = load_csv_as_dicts(
        csv_path,
        skip_initial_lines=header_idx,
        uppercase_headers=True,
    )

    rows = []
    for row in raw_rows:
        client_type = (row.get("CLIENT TYPE") or "").strip()
        if not client_type:
            continue
        if client_type.upper() == "TOTAL":
            # skip grand total row
            continue

        rows.append(
            (
                trade_date,
                client_type,
                to_int(row.get("FUTURE INDEX LONG")),
                to_int(row.get("FUTURE INDEX SHORT")),
                to_int(row.get("FUTURE STOCK LONG")),
                to_int(row.get("FUTURE STOCK SHORT")),
                to_int(row.get("OPTION INDEX CALL LONG")),
                to_int(row.get("OPTION INDEX CALL SHORT")),
                to_int(row.get("OPTION INDEX PUT LONG")),
                to_int(row.get("OPTION INDEX PUT SHORT")),
                to_int(row.get("OPTION STOCK CALL LONG")),
                to_int(row.get("OPTION STOCK CALL SHORT")),
                to_int(row.get("OPTION STOCK PUT LONG")),
                to_int(row.get("OPTION STOCK PUT SHORT")),
                to_int(row.get("TOTAL LONG CONTRACTS")),
                to_int(row.get("TOTAL SHORT CONTRACTS")),
            )
        )

    if not rows:
        LOG.warning("[%s] No participant OI rows after parsing", trade_date)
        return

    sql = """
        INSERT INTO eod.fo_participant_oi (
            trade_date, client_type,
            fut_index_long, fut_index_short,
            fut_stock_long, fut_stock_short,
            opt_index_call_long, opt_index_call_short,
            opt_index_put_long, opt_index_put_short,
            opt_stock_call_long, opt_stock_call_short,
            opt_stock_put_long, opt_stock_put_short,
            total_long_contracts, total_short_contracts
        )
        VALUES (
            %(trade_date)s, %(client_type)s,
            %(fut_index_long)s, %(fut_index_short)s,
            %(fut_stock_long)s, %(fut_stock_short)s,
            %(opt_index_call_long)s, %(opt_index_call_short)s,
            %(opt_index_put_long)s, %(opt_index_put_short)s,
            %(opt_stock_call_long)s, %(opt_stock_call_short)s,
            %(opt_stock_put_long)s, %(opt_stock_put_short)s,
            %(total_long_contracts)s, %(total_short_contracts)s
        )
        ON CONFLICT (trade_date, client_type) DO UPDATE
        SET
            fut_index_long        = EXCLUDED.fut_index_long,
            fut_index_short       = EXCLUDED.fut_index_short,
            fut_stock_long        = EXCLUDED.fut_stock_long,
            fut_stock_short       = EXCLUDED.fut_stock_short,
            opt_index_call_long   = EXCLUDED.opt_index_call_long,
            opt_index_call_short  = EXCLUDED.opt_index_call_short,
            opt_index_put_long    = EXCLUDED.opt_index_put_long,
            opt_index_put_short   = EXCLUDED.opt_index_put_short,
            opt_stock_call_long   = EXCLUDED.opt_stock_call_long,
            opt_stock_call_short  = EXCLUDED.opt_stock_call_short,
            opt_stock_put_long    = EXCLUDED.opt_stock_put_long,
            opt_stock_put_short   = EXCLUDED.opt_stock_put_short,
            total_long_contracts  = EXCLUDED.total_long_contracts,
            total_short_contracts = EXCLUDED.total_short_contracts
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            sql,
            [
                {
                    "trade_date": r[0],
                    "client_type": r[1],
                    "fut_index_long": r[2],
                    "fut_index_short": r[3],
                    "fut_stock_long": r[4],
                    "fut_stock_short": r[5],
                    "opt_index_call_long": r[6],
                    "opt_index_call_short": r[7],
                    "opt_index_put_long": r[8],
                    "opt_index_put_short": r[9],
                    "opt_stock_call_long": r[10],
                    "opt_stock_call_short": r[11],
                    "opt_stock_put_long": r[12],
                    "opt_stock_put_short": r[13],
                    "total_long_contracts": r[14],
                    "total_short_contracts": r[15],
                }
                for r in rows
            ],
            page_size=100,
        )
    LOG.info("[%s] fo_participant_oi upserted: %d rows", trade_date, len(rows))


def load_fo_participant_volume(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Load eod.fo_participant_vol from fao_participant_vol_*.csv

    NOTE:
        - DB uses column name `participant_type` as the key (NOT `client_type`).
        - To avoid depending on a specific PK/unique constraint definition,
          we do a full-refresh per trade_date:

              DELETE FROM eod.fo_participant_vol WHERE trade_date = <date>;
              INSERT ... (no ON CONFLICT)

        This keeps the loader robust even if the table's primary key changes.
    """
    csv_path = find_first(folder, ["fao_participant_vol_*.csv"])
    if not csv_path:
        LOG.warning(
            "[%s] fao_participant_vol CSV not found under %s (recursive)",
            trade_date,
            folder,
        )
        return

    LOG.info("[%s] Loading fo_participant_vol from %s", trade_date, csv_path)

    header_idx = sniff_header_index(
        csv_path,
        candidates=[["CLIENT TYPE", "FUTURE INDEX"]],
    )
    _, raw_rows = load_csv_as_dicts(
        csv_path,
        skip_initial_lines=header_idx,
        uppercase_headers=True,
    )

    rows = []
    for row in raw_rows:
        ctype = (row.get("CLIENT TYPE") or "").strip()
        if not ctype or ctype.upper() == "TOTAL":
            continue

        rows.append(
            {
                "trade_date": trade_date,
                "participant_type": ctype,
                "fut_index_turnover": to_numeric(row.get("FUTURE INDEX")),
                "fut_stock_turnover": to_numeric(row.get("FUTURE STOCK")),
                "opt_index_call_turnover": to_numeric(
                    row.get("OPTION INDEX CALL")
                ),
                "opt_index_put_turnover": to_numeric(
                    row.get("OPTION INDEX PUT")
                ),
                "opt_stock_call_turnover": to_numeric(
                    row.get("OPTION STOCK CALL")
                ),
                "opt_stock_put_turnover": to_numeric(
                    row.get("OPTION STOCK PUT")
                ),
                "total_turnover": to_numeric(row.get("TOTAL")),
            }
        )

    if not rows:
        LOG.warning("[%s] No participant volume rows after parsing", trade_date)
        return

    sql_insert = """
        INSERT INTO eod.fo_participant_vol (
            trade_date, participant_type,
            fut_index_turnover, fut_stock_turnover,
            opt_index_call_turnover, opt_index_put_turnover,
            opt_stock_call_turnover, opt_stock_put_turnover,
            total_turnover
        )
        VALUES (
            %(trade_date)s, %(participant_type)s,
            %(fut_index_turnover)s, %(fut_stock_turnover)s,
            %(opt_index_call_turnover)s, %(opt_index_put_turnover)s,
            %(opt_stock_call_turnover)s, %(opt_stock_put_turnover)s,
            %(total_turnover)s
        )
    """

    with conn.cursor() as cur:
        # Full refresh per day keeps things idempotent and avoids ON CONFLICT
        cur.execute(
            "DELETE FROM eod.fo_participant_vol WHERE trade_date = %s",
            (trade_date,),
        )
        psycopg2.extras.execute_batch(
            cur,
            sql_insert,
            rows,
            page_size=100,
        )

    LOG.info(
        "[%s] fo_participant_vol upserted (via delete+insert): %d rows",
        trade_date,
        len(rows),
    )

import pandas as pd
from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)


def detect_header_line(csv_path: str, marker: str = "Serial Number") -> int:
    """
    Scan the file and return the 0-based line number containing the marker.
    Used to skip explanatory lines above the real header.
    """
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            if marker in line:
                return i
    # Fallback: assume header is first line
    return 0

def load_fo_top10_turnover(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Load 'fo_top10_turnover_*.csv' (zipem Top-10 turnover report)
    into eod.fo_top10_turnover.

    We try to detect the header row and map a few canonical columns
    (symbol, contracts, turnover), but we also store the entire raw
    CSV row in `raw_fields` for future-proofing.
    """
    csv_path = find_first(folder, ["fo_top10_turnover_*.csv", "fao_top10_turnover_*.csv"])
    if not csv_path:
        LOG.info(
            "[%s] fo_top10_turnover CSV not found under %s (recursive)",
            trade_date,
            folder,
        )
        return

    LOG.info("[%s] Loading fo_top10_turnover from %s", trade_date, csv_path)

    # Try to find the header row using typical markers
    header_idx = sniff_header_index(
        csv_path,
        candidates=[
            ["SYMBOL", "TURNOVER"],
            ["SYMBOL", "TRDVAL"],
            ["SYMBOL", "VALUE"],
        ],
    )

    headers, raw_rows = load_csv_as_dicts(
        csv_path,
        skip_initial_lines=header_idx,
        uppercase_headers=True,
    )

    def pick(*candidates: str) -> Optional[str]:
        for cand in candidates:
            if cand in headers:
                return cand
        return None

    col_rank = pick("SR NO", "SRNO", "SNO", "RANK", "SERIAL NO", "SERIAL_NO")
    col_symbol = pick("SYMBOL", "UNDERLYING")
    col_instr = pick("INSTRUMENT", "INSTRUMENT TYPE")
    col_series = pick("SERIES", "MKT TYPE", "MARKET TYPE")
    col_contracts = pick(
        "NO OF CONTRACTS",
        "NO_OF_CONTRACTS",
        "NO. OF CONTRACTS",
        "CONTRACTS",
    )
    col_turnover_cr = pick(
        "TURNOVER (CR)",
        "TURNOVER(CR)",
        "TRDVAL_IN_CR",
        "TRD VAL (CR)",
        "TRADED VALUE(CR)",
        "VALUE_IN_CR",
    )
    col_turnover_lacs = pick(
        "TURNOVER (LACS)",
        "TURNOVER(LACS)",
        "TRDVAL_IN_LACS",
        "VALUE_IN_LACS",
    )

    rows = []
    for idx, row in enumerate(raw_rows, start=1):
        # SYMBOL is mandatory — skip rows without symbol
        symbol = (row.get(col_symbol) or "").strip() if col_symbol else ""
        if not symbol:
            continue

        # Rank: use explicit rank if present, else fallback to row index
        rank_val = to_int(row.get(col_rank)) if col_rank else idx
        if rank_val is None:
            rank_val = idx

        instrument = None
        if col_instr:
            instrument = (row.get(col_instr) or "").strip() or None

        series = None
        if col_series:
            series = (row.get(col_series) or "").strip() or None

        contracts = to_int(row.get(col_contracts)) if col_contracts else None
        turnover_cr = to_numeric(row.get(col_turnover_cr)) if col_turnover_cr else None
        turnover_lacs = (
            to_numeric(row.get(col_turnover_lacs)) if col_turnover_lacs else None
        )

        rows.append(
            (
                trade_date,
                rank_val,
                symbol,
                instrument,
                series,
                contracts,
                turnover_cr,
                turnover_lacs,
                json.dumps(row or {}, ensure_ascii=False),
            )
        )

    with conn.cursor() as cur:
        # Remove existing rows for that date (idempotent re-runs)
        cur.execute(
            "DELETE FROM eod.fo_top10_turnover WHERE trade_date = %s",
            (trade_date,),
        )

        if rows:
            execute_values(
                cur,
                """
                INSERT INTO eod.fo_top10_turnover (
                    trade_date,
                    rank,
                    symbol,
                    instrument,
                    series,
                    contracts,
                    turnover_cr,
                    turnover_lacs,
                    raw_fields
                ) VALUES %s
                """,
                rows,
            )

    LOG.info(
        "[%s] fo_top10_turnover upserted: %d rows",
        trade_date,
        len(rows),
    )


def load_fo_top10_derivatives_summary(conn, trade_date, csv_path: str) -> None:
    """
    Load 'fao_top10cm_to_*.csv' into eod.fo_top10_derivatives_summary.

    CSV columns (in order):
        Serial Number
        Indx Futures Vol(Contracts)
        Index Futures Trnvr(Crores)
        Stock Futures Vol(Contracts)
        Stock Futures Trnvr(Crores)
        Index Options Vol(Contracts)
        Index Options Trnvr(Crores)
        Indx Opt Trnvr(prm)(Crores)
        Stock Options Vol(Contracts)
        Stock Options Trnvr(Crores)
        Stock Opt Trnvr(prm)(Crores)
    """
    logger.info("[%s] Loading fo_top10_derivatives_summary from %s",
                trade_date, csv_path)

    header_line = detect_header_line(csv_path, "Serial Number")
    logger.info("Detected header at line %s in %s", header_line, csv_path)

    # Read CSV starting from header line
    df = pd.read_csv(
        csv_path,
        skiprows=header_line,
        header=0
    )

    # Map by position to our canonical column names
    df = df.rename(columns={
        df.columns[0]: "serial_number",
        df.columns[1]: "idx_fut_vol",
        df.columns[2]: "idx_fut_turnover_cr",
        df.columns[3]: "stk_fut_vol",
        df.columns[4]: "stk_fut_turnover_cr",
        df.columns[5]: "idx_opt_vol",
        df.columns[6]: "idx_opt_turnover_cr",
        df.columns[7]: "idx_opt_prem_turnover",
        df.columns[8]: "stk_opt_vol",
        df.columns[9]: "stk_opt_turnover_cr",
        df.columns[10]: "stk_opt_prem_turnover",
    })

    cols = [
        "serial_number",
        "idx_fut_vol",
        "idx_fut_turnover_cr",
        "stk_fut_vol",
        "stk_fut_turnover_cr",
        "idx_opt_vol",
        "idx_opt_turnover_cr",
        "idx_opt_prem_turnover",
        "stk_opt_vol",
        "stk_opt_turnover_cr",
        "stk_opt_prem_turnover",
    ]
    df = df[cols]

    # Drop TOTAL row, if present
    df = df[~df["serial_number"].astype(str)
                            .str.contains("TOTAL", case=False, na=False)]

    # Attach trade_date
    df["trade_date"] = trade_date

    # Convert numeric columns (everything except serial_number)
    num_cols = [c for c in cols if c != "serial_number"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Clean serial_number to int
    df["serial_number"] = pd.to_numeric(df["serial_number"], errors="coerce").astype("Int64")

    # Build rows for insert
    rows = [
        (
            row.trade_date,
            int(row.serial_number),
            row.idx_fut_vol,
            row.idx_fut_turnover_cr,
            row.stk_fut_vol,
            row.stk_fut_turnover_cr,
            row.idx_opt_vol,
            row.idx_opt_turnover_cr,
            row.idx_opt_prem_turnover,
            row.stk_opt_vol,
            row.stk_opt_turnover_cr,
            row.stk_opt_prem_turnover,
        )
        for row in df.itertuples(index=False)
        if pd.notna(row.serial_number)
    ]

    with conn.cursor() as cur:
        # Remove existing rows for that date
        cur.execute(
            "DELETE FROM eod.fo_top10_derivatives_summary WHERE trade_date = %s",
            (trade_date,),
        )

        if rows:
            execute_values(cur, """
                INSERT INTO eod.fo_top10_derivatives_summary (
                    trade_date,
                    serial_number,
                    idx_fut_vol,
                    idx_fut_turnover_cr,
                    stk_fut_vol,
                    stk_fut_turnover_cr,
                    idx_opt_vol,
                    idx_opt_turnover_cr,
                    idx_opt_prem_turnover,
                    stk_opt_vol,
                    stk_opt_turnover_cr,
                    stk_opt_prem_turnover
                ) VALUES %s
            """, rows)

    
    logger.info("[%s] fo_top10_derivatives_summary upserted: %d rows",
                trade_date, len(rows))

# -------------------------------------------------------------------
# LOADERS — SECBAN / OI LIMIT / ELM / SETTLEMENT / VOL SURFACE
# -------------------------------------------------------------------

def load_fo_secban(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Load eod.fo_secban from:
      - Very simple text CSV:
          line 1 : "Securities in Ban For Trade Date 04-DEC-2025:"
          line 2+: "<SYMBOL>"  or  "<no>,<SYMBOL>"

      - Generic CSV with a header row containing SYMBOL
        (optionally COMPANY NAME / SECURITY NAME etc.)

    Schema-tolerant behaviour:
        - If table has (trade_date, serial_no, symbol, company_name) → use all
        - If table has (trade_date, sr_no,     symbol, company_name) → use sr_no
        - If table has (trade_date, serial_no, symbol)               → ignore company_name
        - If table has (trade_date, sr_no,     symbol)               → ignore company_name
        - If table has only (trade_date, symbol, company_name)       → ignore serial
        - If table has only (trade_date, symbol)                     → ignore both

    IMPORTANT:
        We do a full-refresh for that trade_date:

            DELETE FROM eod.fo_secban WHERE trade_date = <date>;

        and then INSERT, with NO ON CONFLICT clause. This keeps the
        loader idempotent regardless of the PK/unique definition.
    """
    csv_path = find_first(
        folder,
        ["fo_secban_*.csv", "Securities in Ban*.csv"],
    )
    if not csv_path:
        LOG.warning(
            "[%s] Securities in Ban file not found under %s (recursive)",
            trade_date,
            folder,
        )
        return

    LOG.info("[%s] Loading fo_secban from %s", trade_date, csv_path)

    # Read first line to detect the simple "title + plain symbols" format
    with csv_path.open("r", encoding="utf-8") as f:
        first_line = (f.readline() or "").strip()

    upper_first = first_line.upper()
    rows_raw: List[Tuple[dt.date, Optional[int], str, Optional[str]]] = []

    # ------------------------------------------------------------------
    # Case 1: Very simple format — title line, then one symbol per line
    #         (optionally prefixed with a number / comma separated).
    # ------------------------------------------------------------------
    if "SECURITIES IN BAN" in upper_first and "SYMBOL" not in upper_first:
        with csv_path.open("r", encoding="utf-8") as f:
            # Skip the title line
            next(f, None)
            for line in f:
                line = line.strip()
                if not line:
                    continue

                # Split on comma if present e.g. "1,SAMMAANCAP"
                parts = [p.strip() for p in line.split(",") if p.strip()]

                if not parts:
                    continue

                # First token might be a number (serial), or might be the symbol itself
                sr = to_int(parts[0])
                # Symbol: last non-empty comma-separated part
                symbol = parts[-1]

                if not symbol:
                    continue

                rows_raw.append((trade_date, sr, symbol, None))

    else:
        # ------------------------------------------------------------------
        # Case 2: Generic CSV with a header row containing SYMBOL.
        # ------------------------------------------------------------------
        header_idx = sniff_header_index(
            csv_path,
            candidates=[["SYMBOL"], ["SECURITY", "SYMBOL"]],
        )
        headers, raw_rows = load_csv_as_dicts(
            csv_path,
            skip_initial_lines=header_idx,
            uppercase_headers=True,
        )
        header_set = set(headers)

        if "SYMBOL" not in header_set and "SECURITY" not in header_set:
            LOG.warning(
                "[%s] fo_secban headers not recognized (no SYMBOL/SECURITY); skipping",
                trade_date,
            )
            return

        for row in raw_rows:
            # Sr No might be present under different names, or absent
            sr = (
                to_int(row.get("SR NO"))
                or to_int(row.get("S NO"))
                or to_int(row.get("S. NO"))
                or to_int(row.get("SERIAL NO"))
                or to_int(row.get("SERIAL NUMBER"))
            )

            symbol = (
                get_first(
                    row,
                    "SYMBOL",
                    "SECURITY",
                    "SYMBOL CODE",
                    "SYMBL",
                )
                or ""
            ).strip()

            company = (
                get_first(
                    row,
                    "COMPANY NAME",
                    "COMPANY",
                    "SECURITY NAME",
                    "NAME",
                )
                or ""
            ).strip()

            if not symbol:
                continue

            rows_raw.append((trade_date, sr, symbol, company or None))

    if not rows_raw:
        LOG.warning("[%s] No secban rows after parsing", trade_date)
        return

    # ------------------------------------------------------------------
    # Ensure serial is NEVER NULL: if missing, assign sequential 1..N
    # ------------------------------------------------------------------
    rows: List[Tuple[dt.date, int, str, Optional[str]]] = []
    for idx, (td, sr, sym, comp) in enumerate(rows_raw, start=1):
        final_sr = sr if sr is not None else idx
        rows.append((td, final_sr, sym, comp))

    # ------------------------------------------------------------------
    # Schema discovery: which columns exist in eod.fo_secban?
    # ------------------------------------------------------------------
    has_serial_no = table_has_column(conn, "eod", "fo_secban", "serial_no")
    has_sr_no = table_has_column(conn, "eod", "fo_secban", "sr_no")
    has_company_name = table_has_column(conn, "eod", "fo_secban", "company_name")

    # Build INSERT + params based on actual schema shape
    if has_serial_no and has_company_name:
        # trade_date, serial_no, symbol, company_name
        sql = """
            INSERT INTO eod.fo_secban (
                trade_date, serial_no, symbol, company_name
            )
            VALUES (
                %(trade_date)s, %(serial_no)s, %(symbol)s, %(company_name)s
            )
        """
        params_list = [
            {
                "trade_date": r[0],
                "serial_no": r[1],
                "symbol": r[2],
                "company_name": r[3],
            }
            for r in rows
        ]

    elif has_serial_no and not has_company_name:
        # trade_date, serial_no, symbol
        sql = """
            INSERT INTO eod.fo_secban (
                trade_date, serial_no, symbol
            )
            VALUES (
                %(trade_date)s, %(serial_no)s, %(symbol)s
            )
        """
        params_list = [
            {
                "trade_date": r[0],
                "serial_no": r[1],
                "symbol": r[2],
            }
            for r in rows
        ]

    elif has_sr_no and has_company_name:
        # trade_date, sr_no, symbol, company_name
        sql = """
            INSERT INTO eod.fo_secban (
                trade_date, sr_no, symbol, company_name
            )
            VALUES (
                %(trade_date)s, %(sr_no)s, %(symbol)s, %(company_name)s
            )
        """
        params_list = [
            {
                "trade_date": r[0],
                "sr_no": r[1],
                "symbol": r[2],
                "company_name": r[3],
            }
            for r in rows
        ]

    elif has_sr_no and not has_company_name:
        # trade_date, sr_no, symbol
        sql = """
            INSERT INTO eod.fo_secban (
                trade_date, sr_no, symbol
            )
            VALUES (
                %(trade_date)s, %(sr_no)s, %(symbol)s
            )
        """
        params_list = [
            {
                "trade_date": r[0],
                "sr_no": r[1],
                "symbol": r[2],
            }
            for r in rows
        ]

    elif has_company_name and not (has_serial_no or has_sr_no):
        # Only trade_date, symbol, company_name
        sql = """
            INSERT INTO eod.fo_secban (
                trade_date, symbol, company_name
            )
            VALUES (
                %(trade_date)s, %(symbol)s, %(company_name)s
            )
        """
        params_list = [
            {
                "trade_date": r[0],
                "symbol": r[2],
                "company_name": r[3],
            }
            for r in rows
        ]

    else:
        # Minimal: only (trade_date, symbol)
        sql = """
            INSERT INTO eod.fo_secban (
                trade_date, symbol
            )
            VALUES (
                %(trade_date)s, %(symbol)s
            )
        """
        params_list = [
            {
                "trade_date": r[0],
                "symbol": r[2],
            }
            for r in rows
        ]

    with conn.cursor() as cur:
        # Full refresh for that day keeps loader idempotent
        cur.execute(
            "DELETE FROM eod.fo_secban WHERE trade_date = %s",
            (trade_date,),
        )
        psycopg2.extras.execute_batch(cur, sql, params_list, page_size=50)

    LOG.info("[%s] fo_secban upserted (via delete+insert): %d rows", trade_date, len(rows))


def load_fo_client_oi_limit(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Load eod.fo_client_oi_limit from oi_cli_limit_*.lst

    Supports:
      - New simple format: SYMBOL,LIMIT_VALUE,SEGMENT
            e.g.  360ONE,4466000,NSE

      - Old format: SYMBOL,SEGMENT,LIMIT_TYPE,LIMIT_VALUE,[REMARKS]
            e.g.  RELIANCE,NSE,CLIENT_LIMIT,123456,'Some remark'
    """
    lst_path = find_first(folder, ["oi_cli_limit_*.lst"])
    if not lst_path:
        LOG.warning(
            "[%s] oi_cli_limit lst not found under %s (recursive)",
            trade_date,
            folder,
        )
        return

    LOG.info("[%s] Loading fo_client_oi_limit from %s", trade_date, lst_path)

    rows = []
    with lst_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = [p.strip() for p in line.split(",")]
            # We expect at least 3 columns in *any* supported format
            if len(parts) < 3:
                continue

            # ------------------------------------------------------------------
            # Case 1: New simple format → SYMBOL,LIMIT_VALUE,SEGMENT
            # ------------------------------------------------------------------
            if len(parts) == 3:
                symbol = parts[0]
                limit_val_str = parts[1].strip().strip("'")
                segment = parts[2]
                limit_type = "CLIENT_LIMIT"
                remarks = None

            # ------------------------------------------------------------------
            # Case 2: Old format → SYMBOL,SEGMENT,LIMIT_TYPE,LIMIT_VALUE,[REMARKS]
            # ------------------------------------------------------------------
            else:
                symbol = parts[0]
                segment = parts[1]
                limit_type = parts[2]
                limit_val_str = parts[3].strip().strip("'")
                remarks = parts[4] if len(parts) > 4 else None

            limit_val = to_numeric(limit_val_str)
            rows.append(
                (
                    trade_date,
                    symbol,
                    segment,
                    limit_type,
                    limit_val,
                    remarks,
                )
            )

    if not rows:
        LOG.warning("[%s] No client OI limit rows", trade_date)
        return

    sql = """
        INSERT INTO eod.fo_client_oi_limit (
            trade_date, symbol, segment,
            limit_type, limit_value, remarks
        )
        VALUES (
            %(trade_date)s, %(symbol)s, %(segment)s,
            %(limit_type)s, %(limit_value)s, %(remarks)s
        )
        ON CONFLICT (trade_date, symbol, segment, limit_type) DO UPDATE
        SET
            limit_value = EXCLUDED.limit_value,
            remarks     = EXCLUDED.remarks
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            sql,
            [
                {
                    "trade_date": r[0],
                    "symbol": r[1],
                    "segment": r[2],
                    "limit_type": r[3],
                    "limit_value": r[4],
                    "remarks": r[5],
                }
                for r in rows
            ],
            page_size=200,
        )

    LOG.info("[%s] fo_client_oi_limit upserted: %d rows", trade_date, len(rows))


def load_fo_elm_margin(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Load eod.fo_elm_margin from ael_*.csv (Exposure Limit/Margin file).

    Uses header normalization to avoid mixed-case / spacing issues.

    Behaviour:
        - We load margins at (trade_date, symbol, instrument_type) level.
        - For idempotence, we do a full-refresh per trade_date:

              DELETE FROM eod.fo_elm_margin WHERE trade_date = <date>;
              INSERT ... (no ON CONFLICT required)

    Schema tolerance:
        - If columns (elm_pct, mm_pct, addl_margin_pct) are missing,
          we SKIP the loader with a warning instead of crashing the pipeline.
        - If column instrument_type is missing, we fall back to a
          synthetic value 'ALL', but this is logged as a warning.
    """
    csv_path = find_first(folder, ["ael_*.csv"])
    if not csv_path:
        LOG.warning(
            "[%s] ael CSV not found under %s (recursive)",
            trade_date,
            folder,
        )
        return

    LOG.info("[%s] Loading fo_elm_margin from %s", trade_date, csv_path)

    # Autodetect header (should contain SYMBOL + ELM / MARGIN)
    header_idx = sniff_header_index(
        csv_path,
        candidates=[["SYMBOL", "ELM"], ["SYMBOL", "MARGIN"]],
    )
    headers, raw_rows = load_csv_as_dicts(
        csv_path,
        skip_initial_lines=header_idx,
        uppercase_headers=True,
    )
    header_set = set(headers)

    # Try to locate an "instrument type" style column
    instr_col = None
    for cand in [
        "INSTRUMENT TYPE",
        "INSTRUMENT",
        "INST TYPE",
        "INSTR TYPE",
        "PRODUCT TYPE",
    ]:
        if cand in header_set:
            instr_col = cand
            break

    if instr_col is None:
        # We can still proceed using a synthetic type,
        # but log a warning so you know schema is weaker
        LOG.warning(
            "[%s] ael CSV has no explicit INSTRUMENT TYPE column; "
            "using synthetic instrument_type='ALL' for all rows.",
            trade_date,
        )

    rows = []
    for row in raw_rows:
        symbol = (row.get("SYMBOL") or "").strip()
        if not symbol:
            continue

        # Instrument type handling
        if instr_col is not None:
            instrument_type = (row.get(instr_col) or "").strip()
            if not instrument_type:
                # Avoid NULL in NOT NULL column; fall back but log once
                instrument_type = "ALL"
        else:
            instrument_type = "ALL"

        # Normalize to upper for consistency
        instrument_type = instrument_type.upper()

        elm_pct = (
            to_numeric(row.get("ELM%"))
            or to_numeric(row.get("ELM %"))
            or to_numeric(row.get("ELM PERCENT"))
        )
        mm_pct = (
            to_numeric(row.get("MM%"))
            or to_numeric(row.get("MM %"))
            or to_numeric(row.get("MM PERCENT"))
        )
        addl_margin_pct = (
            to_numeric(row.get("ADDITIONAL_MARGIN%"))
            or to_numeric(row.get("ADDITIONAL MARGIN%"))
            or to_numeric(row.get("ADDITIONAL MARGIN %"))
        )

        rows.append(
            (
                trade_date,
                symbol,
                instrument_type,
                elm_pct,
                mm_pct,
                addl_margin_pct,
            )
        )

    if not rows:
        LOG.warning("[%s] No elm margin rows", trade_date)
        return

    # Ensure required numeric columns exist in DB; if not, skip gracefully
    needed_cols = ["elm_pct", "mm_pct", "addl_margin_pct"]
    if not table_has_columns(conn, "eod", "fo_elm_margin", needed_cols):
        LOG.warning(
            "[%s] fo_elm_margin loader skipped because required columns "
            "(elm_pct, mm_pct, addl_margin_pct) are not present in DB. "
            "Add columns or adjust schema when ready.",
            trade_date,
        )
        return

    # Check if instrument_type column exists; if not, we can't insert into it
    has_instr_col = table_has_column(conn, "eod", "fo_elm_margin", "instrument_type")

    if has_instr_col:
        sql_insert = """
            INSERT INTO eod.fo_elm_margin (
                trade_date,
                symbol,
                instrument_type,
                elm_pct,
                mm_pct,
                addl_margin_pct
            )
            VALUES (
                %(trade_date)s,
                %(symbol)s,
                %(instrument_type)s,
                %(elm_pct)s,
                %(mm_pct)s,
                %(addl_margin_pct)s
            )
        """
        params_list = [
            {
                "trade_date": r[0],
                "symbol": r[1],
                "instrument_type": r[2],
                "elm_pct": r[3],
                "mm_pct": r[4],
                "addl_margin_pct": r[5],
            }
            for r in rows
        ]
    else:
        # Fallback for older schema without instrument_type column
        LOG.warning(
            "[%s] eod.fo_elm_margin has no instrument_type column; "
            "inserting only (trade_date, symbol, elm_pct, mm_pct, addl_margin_pct).",
            trade_date,
        )
        sql_insert = """
            INSERT INTO eod.fo_elm_margin (
                trade_date,
                symbol,
                elm_pct,
                mm_pct,
                addl_margin_pct
            )
            VALUES (
                %(trade_date)s,
                %(symbol)s,
                %(elm_pct)s,
                %(mm_pct)s,
                %(addl_margin_pct)s
            )
        """
        params_list = [
            {
                "trade_date": r[0],
                "symbol": r[1],
                "elm_pct": r[3],
                "mm_pct": r[4],
                "addl_margin_pct": r[5],
            }
            for r in rows
        ]

    with conn.cursor() as cur:
        # Full refresh for that day
        cur.execute(
            "DELETE FROM eod.fo_elm_margin WHERE trade_date = %s",
            (trade_date,),
        )
        psycopg2.extras.execute_batch(
            cur,
            sql_insert,
            params_list,
            page_size=200,
        )

    LOG.info(
        "[%s] fo_elm_margin upserted (via delete+insert): %d rows",
        trade_date,
        len(rows),
    )


def load_fo_settlement_price(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Load eod.fo_settlement_price from FOSett_prce_*.csv

    Expected 2025 format (NSE):
        DATE,INSTRUMENT,UNDERLYING,EXPIRY DATE,MTM SETTLEMENT PRICE

    DB schema (as per current RDS):
        eod.fo_settlement_price(
            trade_date       date          NOT NULL,
            instrument       text          NOT NULL,
            underlying       text          NOT NULL,
            expiry_date      date          NOT NULL,
            mtm_settle_price numeric(18,4) NOT NULL,
            symbol           text          NOT NULL,
            PRIMARY KEY (trade_date, instrument, symbol, expiry_date)
        )

    Notes:
        - We now populate BOTH `underlying` and `symbol`.
          For now, `symbol` defaults to the UNDERLYING code from the file
          (you can refine this later if NSE adds a separate symbol column).
        - We dynamically detect the settlement price column name in DB
          (mtm_settle_price / settlement_price / settle_price / mtm_settle_price).
        - Rows with missing expiry_date or price are skipped to respect NOT NULL.
    """
    csv_path = find_first(folder, ["FOSett_prce_*.csv"])
    if not csv_path:
        LOG.warning(
            "[%s] FOSett_prce CSV not found under %s (recursive)",
            trade_date,
            folder,
        )
        return

    LOG.info("[%s] Loading fo_settlement_price from %s", trade_date, csv_path)

    headers, raw_rows = load_csv_as_dicts(
        csv_path,
        skip_initial_lines=0,
        uppercase_headers=True,
    )

    rows: List[Dict[str, object]] = []
    for row in raw_rows:
        row_date = to_date(
            row.get("DATE"),
            ["%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"],
        )
        if row_date != trade_date:
            continue

        instrument = (row.get("INSTRUMENT") or "").strip()
        underlying = (row.get("UNDERLYING") or "").strip()

        # Basic sanity: we must have instrument + underlying
        if not instrument or not underlying:
            continue

        # Expiry is NOT NULL in DB → skip if we can't parse
        expiry = to_date(
            get_first(row, "EXPIRY DATE", "EXPIRY_DT"),
            ["%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%b-%y", "%d/%m/%y"],
        )
        if expiry is None:
            continue

        # Settlement price: use any known header variant
        settle_price_val = to_numeric(
            get_first(
                row,
                "MTM SETTLEMENT PRICE",
                "MTM SETTLE PRICE",
                "SETTLEMENT PRICE",
                "SETTLE_PR",
            )
        )
        # mtm_settle_price is NOT NULL → skip missing
        if settle_price_val is None:
            continue

        # For now, use underlying as symbol as well (they are usually same code)
        symbol = underlying

        rows.append(
            {
                "trade_date": trade_date,
                "instrument": instrument,
                "underlying": underlying,
                "symbol": symbol,
                "expiry_date": expiry,
                "settle_price": settle_price_val,
            }
        )

    if not rows:
        LOG.warning("[%s] No settlement price rows after parsing", trade_date)
        return

    # ------------------------------------------------------------------
    # Detect which numeric column actually exists in eod.fo_settlement_price
    # ------------------------------------------------------------------
    candidate_cols = [
        "mtm_settle_price",
        "mtm_settlement_price",
        "settlement_price",
        "settle_price",
    ]
    target_col = None
    for col in candidate_cols:
        if table_has_column(conn, "eod", "fo_settlement_price", col):
            target_col = col
            break

    if not target_col:
        LOG.error(
            "[%s] fo_settlement_price loader: none of the expected price columns "
            "exist in eod.fo_settlement_price (%s). Skipping this loader.",
            trade_date,
            ", ".join(candidate_cols),
        )
        return

    LOG.info(
        "[%s] fo_settlement_price: using DB column '%s' for settlement price",
        trade_date,
        target_col,
    )

    # Build SQL dynamically with the detected column name.
    # We now insert both `underlying` and `symbol`.
    sql = f"""
        INSERT INTO eod.fo_settlement_price (
            trade_date,
            instrument,
            underlying,
            expiry_date,
            symbol,
            {target_col}
        )
        VALUES (
            %(trade_date)s,
            %(instrument)s,
            %(underlying)s,
            %(expiry_date)s,
            %(symbol)s,
            %(settle_price)s
        )
        ON CONFLICT (trade_date, instrument, symbol, expiry_date) DO UPDATE
        SET
            underlying = EXCLUDED.underlying,
            {target_col} = EXCLUDED.{target_col}
    """

    params = [
        {
            "trade_date": r["trade_date"],
            "instrument": r["instrument"],
            "underlying": r["underlying"],
            "expiry_date": r["expiry_date"],
            "symbol": r["symbol"],
            "settle_price": r["settle_price"],
        }
        for r in rows
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, params, page_size=500)

    LOG.info("[%s] fo_settlement_price upserted: %d rows", trade_date, len(rows))


def load_fo_volatility_surface(conn, folder: Path, trade_date: dt.date) -> None:
    """
    Load eod.fo_volatility_surface from FOVOLT_*.csv

    We map:
        - trade_date
        - symbol
        - underlying_close_price
        - underlying_prev_close_price
        - applicable_ann_vol

    Additionally, if the DB table has a JSONB `raw_fields` column
    (NOT NULL in your current schema), we store the full raw CSV row
    for future debugging / analytics.

    Uses normalized header names and substring matching so that minor
    wording changes in NSE files don't break ingestion.
    """
    csv_path = find_first(folder, ["FOVOLT_*.csv"])
    if not csv_path:
        LOG.warning(
            "[%s] FOVOLT CSV not found under %s (recursive)",
            trade_date,
            folder,
        )
        return

    LOG.info("[%s] Loading fo_volatility_surface from %s", trade_date, csv_path)

    headers, raw_rows = load_csv_as_dicts(
        csv_path, skip_initial_lines=0, uppercase_headers=True
    )

    def find_col(*fragments: str) -> Optional[str]:
        """
        Find first header containing ALL given fragments (case-insensitive),
        using the already-UPPERCASE headers list.
        """
        frags = [f.upper() for f in fragments]
        for h in headers:
            if all(f in h for f in frags):
                return h
        return None

    col_date = find_col("DATE")
    col_symbol = find_col("SYMBOL")
    col_close = find_col("UNDERLYING", "CLOSE", "PRICE")
    col_prev_close = find_col("PREVIOUS", "DAY", "CLOSE")
    col_app_vol = find_col("APPLICABLE", "ANNUALISED", "VOL")

    if not col_date or not col_symbol:
        LOG.warning(
            "[%s] FOVOLT headers missing DATE/SYMBOL columns; skipping",
            trade_date,
        )
        return

    rows: List[Dict[str, object]] = []
    for row in raw_rows:
        row_date = to_date(
            row.get(col_date),
            ["%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%b-%y"],
        )
        if row_date != trade_date:
            continue

        symbol = (row.get(col_symbol) or "").strip()
        if not symbol:
            continue

        underlying_close = (
            to_numeric(row.get(col_close)) if col_close else None
        )
        underlying_prev_close = (
            to_numeric(row.get(col_prev_close)) if col_prev_close else None
        )
        app_vol = to_numeric(row.get(col_app_vol)) if col_app_vol else None

        rows.append(
            {
                "trade_date": trade_date,
                "symbol": symbol,
                "underlying_close_price": underlying_close,
                "underlying_prev_close_price": underlying_prev_close,
                "applicable_ann_vol": app_vol,
                "raw_fields": row,  # full CSV row (UPPERCASE keys)
            }
        )

    if not rows:
        LOG.warning("[%s] No volatility surface rows", trade_date)
        return

    # Detect whether table has raw_fields column
    has_raw_fields = table_has_column(conn, "eod", "fo_volatility_surface", "raw_fields")

    if has_raw_fields:
        sql = """
            INSERT INTO eod.fo_volatility_surface (
                trade_date, symbol,
                underlying_close_price,
                underlying_prev_close_price,
                applicable_ann_vol,
                raw_fields
            )
            VALUES (
                %(trade_date)s, %(symbol)s,
                %(underlying_close_price)s,
                %(underlying_prev_close_price)s,
                %(applicable_ann_vol)s,
                %(raw_fields)s
            )
            ON CONFLICT (trade_date, symbol) DO UPDATE
            SET
                underlying_close_price      = EXCLUDED.underlying_close_price,
                underlying_prev_close_price = EXCLUDED.underlying_prev_close_price,
                applicable_ann_vol          = EXCLUDED.applicable_ann_vol,
                raw_fields                  = EXCLUDED.raw_fields
        """
    else:
        sql = """
            INSERT INTO eod.fo_volatility_surface (
                trade_date, symbol,
                underlying_close_price,
                underlying_prev_close_price,
                applicable_ann_vol
            )
            VALUES (
                %(trade_date)s, %(symbol)s,
                %(underlying_close_price)s,
                %(underlying_prev_close_price)s,
                %(applicable_ann_vol)s
            )
            ON CONFLICT (trade_date, symbol) DO UPDATE
            SET
                underlying_close_price      = EXCLUDED.underlying_close_price,
                underlying_prev_close_price = EXCLUDED.underlying_prev_close_price,
                applicable_ann_vol          = EXCLUDED.applicable_ann_vol
        """

    params_list = []
    for r in rows:
        params = {
            "trade_date": r["trade_date"],
            "symbol": r["symbol"],
            "underlying_close_price": r["underlying_close_price"],
            "underlying_prev_close_price": r["underlying_prev_close_price"],
            "applicable_ann_vol": r["applicable_ann_vol"],
        }
        if has_raw_fields:
            params["raw_fields"] = json.dumps(r["raw_fields"])
        params_list.append(params)

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            sql,
            params_list,
            page_size=500,
        )
    LOG.info("[%s] fo_volatility_surface upserted: %d rows", trade_date, len(rows))

# -------------------------------------------------------------------
# MASTER PIPELINE FOR A SINGLE DATE (LOCAL-ONLY)
# -------------------------------------------------------------------
def _record_fo_load_summary(
    conn,
    run_id,
    trade_date: dt.date,
    table_label: str,
    status: str,
    rows_count: Optional[int],
    file_path: Optional[str] = None,
    note: Optional[str] = None,
) -> None:
    """
    Insert one summary row into eod.fo_load_summary.

    - run_id:      UUID grouping all tables for this FO pipeline run.
                   Can be a uuid.UUID or a plain string.
    - trade_date:  report date (NSE trade date).
    - table_label: logical name like 'fo_bhavcopy' (we store as table_name).
    - status:      'OK', 'MISS', 'ERR' etc.
    - rows_count:  number of rows for this trade_date in the table
                   (None → stored as 0 for summary purposes).
    - file_path / note: optional metadata.
    """
    # Normalise rows_count to a simple int (NULL → 0)
    rows_int = 0 if rows_count is None else int(rows_count)

    # Normalise run_id so psycopg2 can adapt it regardless of DB type (uuid/text)
    if isinstance(run_id, uuid.UUID):
        run_id_db = str(run_id)
    else:
        run_id_db = str(run_id)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO eod.fo_load_summary (
                run_id,
                trade_date,
                table_name,
                rows_affected,
                status,
                file_path,
                note
            )
            VALUES (
                %(run_id)s,
                %(trade_date)s,
                %(table_name)s,
                %(rows_affected)s,
                %(status)s,
                %(file_path)s,
                %(note)s
            )
            """,
            {
                "run_id": run_id_db,
                "trade_date": trade_date,
                "table_name": table_label,
                "rows_affected": rows_int,
                "status": status,
                "file_path": file_path,
                "note": note,
            },
        )


def _log_daily_summary(conn, trade_date: dt.date, run_id) -> None:
    """
    After a successful commit, print a compact tabular summary showing
    how many rows exist for this trade_date in each core FO table AND
    persist the same summary into eod.fo_load_summary.

    SAFE VERSION:
        - Does NOT break the pipeline if eod.fo_load_summary is missing.
        - Does NOT break the pipeline if a single insert into
          fo_load_summary fails (UUID / type / constraint issues etc.).
        - If a table does not exist, marks status='MISS'.
        - If a count fails for any other reason, marks status='ERR'
          but rolls back so subsequent tables can still be counted.
    """
    # (label, (schema, table_name))
    tables = [
        ("fo_bhavcopy",              ("eod", "fo_bhavcopy")),
        ("fo_contract_master",       ("eod", "fo_contract_master")),
        ("fo_oi_mwpl_limit",         ("eod", "fo_oi_mwpl_limit")),
        ("fo_contract_delta_factor", ("eod", "fo_contract_delta_factor")),
        ("fo_ncl_oi",                ("eod", "fo_ncl_oi")),
        ("fo_participant_oi",        ("eod", "fo_participant_oi")),
        ("fo_participant_vol",       ("eod", "fo_participant_vol")),
        ("fo_top10_turnover",        ("eod", "fo_top10_turnover")),
        ("fo_top10_deriv_summary",   ("eod", "fo_top10_derivatives_summary")),
        ("fo_secban",                ("eod", "fo_secban")),
        ("fo_client_oi_limit",       ("eod", "fo_client_oi_limit")),
        ("fo_elm_margin",            ("eod", "fo_elm_margin")),
        ("fo_settlement_price",      ("eod", "fo_settlement_price")),
        ("fo_volatility_surface",    ("eod", "fo_volatility_surface")),
    ]

    LOG.info(
        "[%s] ----- FO DAILY SUMMARY (rows per table for this date) -----",
        trade_date,
    )
    LOG.info("Table                        | Status   | Rows")

    # Check once if fo_load_summary exists; if not, we only log to console.
    has_summary_table = table_exists(conn, "eod", "fo_load_summary")
    if not has_summary_table:
        LOG.warning(
            "[%s] eod.fo_load_summary does not exist; "
            "will skip DB persistence of summary and only log to console.",
            trade_date,
        )

    # Detect which FO tables actually exist (so we don't ever hit 'relation does not exist').
    existing_fq = set()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = 'eod'
            """
        )
        for schema, name in cur.fetchall():
            existing_fq.add(f"{schema}.{name}")

    summary_rows = []

    for label, (schema, table) in tables:
        fq_table = f"{schema}.{table}"

        status = "OK"
        cnt: Optional[int] = None

        # 1) Table missing completely → MISS
        if fq_table not in existing_fq:
            LOG.warning(
                "[%s] Summary: table %s does not exist; marking as MISS",
                trade_date,
                fq_table,
            )
            status = "MISS"
            cnt = None

        else:
            # 2) Table exists → try COUNT(*)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {fq_table} WHERE trade_date = %s",
                        (trade_date,),
                    )
                    cnt = cur.fetchone()[0]
                status = "OK"
            except Exception as exc:
                LOG.warning(
                    "[%s] Summary: failed to count %s: %s",
                    trade_date,
                    fq_table,
                    exc,
                )
                # Clean up aborted transaction so later ops work
                conn.rollback()
                status = "ERR"
                cnt = None

        # Collect for console summary
        summary_rows.append((label, status, cnt))

        # Persist into fo_load_summary if table exists and no prior insert failure
        if has_summary_table:
            try:
                _record_fo_load_summary(
                    conn=conn,
                    run_id=run_id,
                    trade_date=trade_date,
                    table_label=label,   # keep label like 'fo_bhavcopy'
                    status=status,
                    rows_count=cnt,
                    file_path=None,
                    note="FO loader",
                )
            except Exception as exc:
                # Any error here should NOT break the main pipeline.
                LOG.warning(
                    "[%s] Failed to insert into eod.fo_load_summary for table %s: %s. "
                    "Disabling DB persistence of summary for remaining tables.",
                    trade_date,
                    label,
                    exc,
                )
                conn.rollback()
                has_summary_table = False  # don't try further inserts

    # Pretty ASCII summary to console logs
    header = f"{'Table':28s} | {'Status':8s} | {'Rows':10s}"
    sep = f"{'-'*28}-+-{'-'*8}-+-{'-'*10}"
    LOG.info(header)
    LOG.info(sep)
    for label, status, cnt in summary_rows:
        rows_str = "" if cnt is None else str(cnt)
        LOG.info("%-28s | %-8s | %10s", label, status, rows_str)

    # Commit only if we actually wrote to fo_load_summary successfully
    if has_summary_table:
        conn.commit()
        LOG.info(
            "[%s] FO load summary persisted in eod.fo_load_summary (run_id=%s)",
            trade_date,
            run_id,
        )
    else:
        LOG.info(
            "[%s] FO load summary NOT persisted to eod.fo_load_summary "
            "(summary table missing or insert error).",
            trade_date,
        )


def process_single_date(
    conn,
    trade_date: dt.date,
) -> None:
    """
    For a single trade_date:
      - Ensure folder for that date
      - Extract nested ZIPs and GZs
      - Load all Phase-1 FO tables into DB using LOCAL files only
      - On success, print + persist a compact tabular summary
        of per-table row counts into eod.fo_load_summary.
    """
    # One UUID for this entire FO run (all tables for this date)
    run_id = uuid.uuid4()
    LOG.info(
        "[%s] ===== START FO PIPELINE (LOCAL FILES) ===== (run_id=%s)",
        trade_date,
        run_id,
    )

    folder = ensure_folder_for_date(trade_date)
    LOG.info("[%s] Using local folder: %s", trade_date, folder)

    # 1) Nested ZIPs + .gz
    extract_nested_zips(folder)
    decompress_gz_files(folder)

    # 2) DB load
    try:
        # Core FO loaders (all inside a single transaction)
        load_fo_bhavcopy(conn, folder, trade_date)
        load_fo_contract_master(conn, folder, trade_date)
        load_fo_combine_oi(conn, folder, trade_date)          # MWPL + limit file
        load_fo_contract_delta(conn, folder, trade_date)      # delta factors
        load_fo_ncl_oi(conn, folder, trade_date)
        load_fo_participant_oi(conn, folder, trade_date)
        load_fo_participant_volume(conn, folder, trade_date)

        # Top-10 Turnover (new table) + Top-10 derivatives summary (if files present)
        load_fo_top10_turnover(conn, folder, trade_date)
        csv_path = find_first(folder, ["fao_top10cm_to_*.csv"])
        if csv_path:
            load_fo_top10_derivatives_summary(conn, trade_date, str(csv_path))

        load_fo_secban(conn, folder, trade_date)
        load_fo_client_oi_limit(conn, folder, trade_date)
        load_fo_elm_margin(conn, folder, trade_date)
        load_fo_settlement_price(conn, folder, trade_date)
        load_fo_volatility_surface(conn, folder, trade_date)

        # TODO (Phase-2):
        #   - load Market Activity summary (fo0412*.zip content)
        #   - other niche files from archives (latency stats, etc.)

        # Commit all FO tables in one atomic transaction
        conn.commit()
        LOG.info("[%s] ✅ FO pipeline committed. (run_id=%s)", trade_date, run_id)

        # After a clean commit, log + persist per-table summary
        _log_daily_summary(conn, trade_date, run_id)

    except Exception as exc:
        conn.rollback()
        LOG.exception(
            "[%s] ❌ Error in FO DB load, rolled back: %s", trade_date, exc
        )


# -------------------------------------------------------------------
# DAILY SUMMARY (SAFE, TABLE-AWARE)
# -------------------------------------------------------------------

def table_exists(conn, schema: str, table: str) -> bool:
    """
    Check if a given (schema, table) exists in the DB.
    Used to avoid 'relation does not exist' errors in the summary.
    """
    sql = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name   = %s
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, [schema, table])
        return cur.fetchone() is not None


def safe_count_for_date(
    conn,
    schema: str,
    table: str,
    trade_date: dt.date,
    date_col: str = "trade_date",
) -> Tuple[str, Optional[int]]:
    """
    Safely count rows in schema.table for a given trade_date.

    Returns:
        (status, count)
        status ∈ {"OK", "SKIP", "ERR"}
    """
    if not table_exists(conn, schema, table):
        LOG.warning(
            "[%s] Summary: table %s.%s does not exist; marking as SKIP",
            trade_date,
            schema,
            table,
        )
        # Ensure connection is in clean state
        conn.rollback()
        return "SKIP", None

    sql = f"SELECT COUNT(*) FROM {schema}.{table} WHERE {date_col} = %s"

    with conn.cursor() as cur:
        try:
            cur.execute(sql, (trade_date,))
            n = cur.fetchone()[0]
            return "OK", n
        except Exception as exc:
            # Extremely important: clear the aborted transaction
            conn.rollback()
            LOG.warning(
                "[%s] Summary: failed to count %s.%s: %s",
                trade_date,
                schema,
                table,
                exc,
            )
            return "ERR", None


def print_daily_summary(conn, trade_date: dt.date) -> None:
    """
    Print a robust per-table summary for the given trade_date.
    Each failed/absent table is isolated so one error doesn't
    poison the whole summary.
    """
    LOG.info("[%s] ----- FO DAILY SUMMARY (rows per table for this date) -----", trade_date)
    LOG.info("Table                        | Status   | Rows")

    summary_specs = [
        ("fo_bhavcopy",               "eod", "fo_bhavcopy"),
        ("fo_contract_master",        "eod", "fo_contract_master"),
        ("fo_oi_mwpl_limit",          "eod", "fo_oi_mwpl_limit"),
        ("fo_contract_delta_factor",  "eod", "fo_contract_delta_factor"),
        ("fo_ncl_oi",                 "eod", "fo_ncl_oi"),
        ("fo_participant_oi",         "eod", "fo_participant_oi"),
        ("fo_participant_vol",        "eod", "fo_participant_vol"),

        ("fo_top10_turnover",         "eod", "fo_top10_turnover"),

        # Actual Top-10 derivatives summary table (already loaded above)
        ("fo_top10_deriv_summary",    "eod", "fo_top10_derivatives_summary"),

        ("fo_secban",                 "eod", "fo_secban"),
        ("fo_client_oi_limit",        "eod", "fo_client_oi_limit"),
        ("fo_elm_margin",             "eod", "fo_elm_margin"),
        ("fo_settlement_price",       "eod", "fo_settlement_price"),
        ("fo_volatility_surface",     "eod", "fo_volatility_surface"),
    ]

    for label, schema, table in summary_specs:
        status, count = safe_count_for_date(conn, schema, table, trade_date)
        count_str = "" if count is None else f"{count:11d}"
        LOG.info(
            "%-28s | %-8s | %s",
            label,
            status,
            count_str,
        )


def _log_fo_summary_for_run(conn, run_id, trade_date: dt.date) -> None:
    """
    Print a nice per-table summary for a given (run_id, trade_date) to the log.

    This is what drives the final "SUMMARY" section you were seeing earlier.
    It is tolerant of run_id being stored as TEXT or UUID in the DB.
    """
    # Normalise run_id to string; in SQL we cast run_id::text for safety
    if isinstance(run_id, uuid.UUID):
        run_id_str = str(run_id)
    else:
        run_id_str = str(run_id)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                table_name,
                status,
                rows_affected
            FROM eod.fo_load_summary
            WHERE trade_date = %s
              AND run_id::text = %s
            ORDER BY table_name
            """,
            (trade_date, run_id_str),
        )
        rows = cur.fetchall()

    if not rows:
        LOG.warning(
            "[%s] No rows found in eod.fo_load_summary for run_id=%s",
            trade_date,
            run_id_str,
        )
        return

    LOG.info(
        "[%s] ===== FO LOAD SUMMARY (run_id=%s) =====",
        trade_date,
        run_id_str,
    )
    for table_name, status, rows_affected in rows:
        LOG.info(
            "[%s] %-26s | %-4s | %6s rows",
            trade_date,
            table_name,
            status,
            rows_affected,
        )

# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Load NSE F&O Daily + Archives reports into osa_db "
            "USING LOCAL FILES ONLY (no NSE download)."
        )
    )

    parser.add_argument(
        "--date",
        required=True,
        help="Trade date (YYYY-MM-DD)",
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    trade_date = parse_iso_date(args.date)

    conn = get_db_conn()
    try:
        process_single_date(
            conn=conn,
            trade_date=trade_date,
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
