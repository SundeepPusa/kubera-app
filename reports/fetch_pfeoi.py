# -*- coding: utf-8 -*-
"""
PFEOI / Combined OI – Delta Equivalent
======================================

Source (NSE All Reports):
  Market Data → All Reports → Derivatives → Equity Derivatives → Archives tab
  Report name:
    "F&O – Combine Delta Equivalent Open Interest across exchanges (csv)"

Typical archives JSON URL shape (from browser DevTools):

  https://www.nseindia.com/api/reports?archives=%5B%7B%22name%22%3A%22F%26O%20-%20Combine%20Delta%20Equivalent%20Open%20Interest%20across%20exchanges%20(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%5D&date=31-Oct-2025&type=equity&mode=single

This module:

  • Downloads the PFEOI CSV for a given trade_date (using either:
      - api/reports archives JSON URL, or
      - direct CSV URL from DevTools, or
      - guessed archive URLs as fallback).
  • Parses the CSV to canonical columns.
  • Upserts into AWS Postgres (RDS) table eod.nse_combined_oi using DB_* vars.

IMPORTANT: from your laptop the ingestion goes **directly to AWS RDS** because
DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD in .env point to RDS.

Usage examples
--------------

1) Single-day probe (REAL URL, NO '...'):

  python -m reports.fetch_pfeoi 2025-10-31 --retries 3 --timeout 25 \
    --archives-json "https://www.nseindia.com/api/reports?archives=ENCODED_STUFF&date=31-Oct-2025&type=equity&mode=single" \
    --check-only

2) Using a direct CSV URL from DevTools:

  python -m reports.fetch_pfeoi 2025-10-31 \
    --direct-url "https://nsearchives.nseindia.com/content/fo/combineoi_deleq_31102025.csv" \
    --check-only

3) ETL from already-downloaded file (e.g. manual download to Downloads):

  python -m reports.fetch_pfeoi 2025-10-31 --etl-only \
    --file "C:/Users/YourUser/Downloads/combineoi_deleq_31102025.csv"
"""

from __future__ import annotations

import os
import csv
import gzip
import time
import argparse
import pathlib
import datetime as dt
from io import BytesIO, TextIOWrapper
from typing import List, Optional, Tuple

import requests
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# -------------------- ENV / PATHS --------------------

# repo root: .../osa-final
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "nse"

# load .env / .ENV (project root) so DB_* env vars point to AWS RDS
for p in (BASE_DIR / ".env", BASE_DIR / ".ENV"):
    if p.exists():
        load_dotenv(p.as_posix())
        break

EOD_SCHEMA = (os.getenv("EOD_SCHEMA") or "eod").strip() or "eod"


def log(msg: str) -> None:
    print(f"[pfeoi] {dt.datetime.now():%Y-%m-%d %H:%M:%S} | {msg}")


def ensure_outdir(d: dt.date, outdir: Optional[pathlib.Path] = None) -> pathlib.Path:
    """
    Ensure directory data/nse/YYYY/MM/DD (or custom outdir/YYYY/MM/DD) exists.
    """
    base = pathlib.Path(outdir) if outdir else DATA_DIR
    final = base / f"{d.year}" / f"{d:%m}" / f"{d:%d}"
    final.mkdir(parents=True, exist_ok=True)
    return final


def pg_conn():
    """
    Open PostgreSQL connection using DB_* env vars.

    On your laptop, DB_HOST/DB_NAME/DB_USER/DB_PASSWORD in .env are set to
    the AWS RDS instance (osa-db...). So every ETL here writes directly to RDS.
    """
    host = os.getenv("DB_HOST")
    log(f"Connecting to Postgres host={host!r} (RDS from laptop)")
    return psycopg2.connect(
        host=host,
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        connect_timeout=10,
    )


# -------------------- HTTP helpers --------------------


def http_session() -> requests.Session:
    """
    Create a session with NSE-friendly headers + optional cookies.
    """
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/119 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nseindia.com/all-reports",
            "Origin": "https://www.nseindia.com",
        }
    )
    ck = (os.getenv("NSE_COOKIES") or "").strip()
    if ck:
        s.headers["Cookie"] = ck

    # Best-effort warm-up (ignore failures)
    for u in (
        "https://www.nseindia.com/",
        "https://www.nseindia.com/all-reports",
        "https://nsearchives.nseindia.com/",
    ):
        try:
            s.get(u, timeout=10)
        except Exception:
            pass
    return s


def ddmmyyyy(d: dt.date) -> str:
    return d.strftime("%d%m%Y")


def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def candidate_urls(d: dt.date) -> List[str]:
    """
    Fallback direct archive URLs if api/reports JSON fails.

    For PFEOI we *guess* the filename pattern:
      combineoi_deleq_DDMMYYYY.csv
    """
    bases = [
        "https://nsearchives.nseindia.com/content/fo",
        "https://archives.nseindia.com/content/fo",
        "https://www.nseindia.com/content/fo",
    ]
    names = [
        f"combineoi_deleq_{ddmmyyyy(d)}.csv",
        f"combineoi_deleq_{yyyymmdd(d)}.csv",  # extra fallback
    ]
    out: List[str] = []
    for b in bases:
        out.extend([f"{b}/{n}" for n in names])
    return out


def _abs(u: str) -> str:
    """
    Make a relative path absolute against appropriate NSE host.
    """
    if u.startswith(("http://", "https://")):
        return u
    host = (
        "https://nsearchives.nseindia.com"
        if "/content/" in u
        else "https://www.nseindia.com"
    )
    return host + (u if u.startswith("/") else "/" + u)


def fetch_from_archives_json(
    url: str,
    timeout: int,
    retries: int,
    outdir: Optional[pathlib.Path],
    d: dt.date,
) -> Tuple[Optional[pathlib.Path], Optional[str], List[str]]:
    """
    Try NSE /api/reports?... JSON.

    Returns:
        (saved_path, kind, extra_urls)
        kind is 'csv' or None; extra_urls are any downloadLink/path values.
    """
    # Guard against placeholder like "archives=..."
    if "archives=..." in url:
        log("Given --archives-json still contains '...'. Please paste full URL from DevTools.")
        return None, None, []

    s = http_session()
    for i in range(1, retries + 1):
        try:
            log(f"GET archives JSON (try {i}/{retries}) → {url}")
            r = s.get(
                url,
                timeout=timeout,
                stream=True,
                headers={"X-Requested-With": "XMLHttpRequest"},
            )
            ct = (r.headers.get("Content-Type") or "").lower()

            if r.status_code != 200:
                log(f"archives JSON HTTP {r.status_code}, ct={ct or 'n/a'}")

            # Some days the API returns CSV directly (often as vnd.ms-excel)
            if (
                r.status_code == 200
                and r.content
                and any(
                    x in ct
                    for x in (
                        "text/csv",
                        "application/octet-stream",
                        "text/plain",
                        "application/vnd.ms-excel",
                    )
                )
            ):
                dest = (
                    ensure_outdir(d, outdir)
                    / f"combineoi_deleq_{ddmmyyyy(d)}.csv"
                )
                dest.write_bytes(r.content)
                log(
                    f"archives delivered PFEOI CSV directly → {dest} "
                    f"({len(r.content)} bytes)"
                )
                return dest, "csv", []

            urls: List[str] = []
            if "application/json" in ct and r.text:
                try:
                    j = r.json()
                except Exception:
                    j = None

                if isinstance(j, dict):
                    data = j.get("data") or j.get("rows")
                    if isinstance(data, dict):
                        if data.get("downloadLink"):
                            urls.append(_abs(data["downloadLink"]))
                        if data.get("path"):
                            urls.append(_abs(data["path"]))
                    if isinstance(data, list):
                        for it in data:
                            if not isinstance(it, dict):
                                continue
                            if it.get("downloadLink"):
                                urls.append(_abs(it["downloadLink"]))
                            if it.get("path"):
                                urls.append(_abs(it["path"]))
                    if j.get("downloadLink"):
                        urls.append(_abs(j["downloadLink"]))

            if urls:
                seen = set()
                urls = [u for u in urls if not (u in seen or seen.add(u))]
                log(f"archives JSON resolved {len(urls)} PFEOI file url(s)")
                return None, None, urls

        except Exception as e:
            log(f"archives JSON fetch failed (try {i}/{retries}): {e}")
            time.sleep(min(2 * i, 8))

    return None, None, []


def sniff_kind(data: bytes, url: str, content_type: Optional[str]) -> Optional[str]:
    """
    Simplified: we only care about CSV-like content.
    """
    if content_type and (
        "text/html" in content_type.lower()
        or "javascript" in content_type.lower()
    ):
        return None
    low = url.lower()
    if low.endswith(".csv"):
        return "csv"
    if content_type and any(
        x in content_type.lower()
        for x in (
            "text/csv",
            "application/octet-stream",
            "text/plain",
            "application/vnd.ms-excel",
        )
    ):
        return "csv"
    # Default: assume CSV if file isn't obviously HTML/JS and size is OK
    return "csv"


def http_get_any(urls: List[str], timeout: int, retries: int):
    """
    Try multiple candidate CSV URLs, return first good one.
    """
    if not urls:
        return None

    s = http_session()
    total = len(urls)
    for idx, u in enumerate(urls, start=1):
        log(f"Trying PFEOI candidate {idx}/{total} → {u}")
        for i in range(1, retries + 1):
            try:
                r = s.get(u, timeout=timeout, allow_redirects=True)
                if r.status_code == 200 and r.content:
                    kind = sniff_kind(
                        r.content,
                        u,
                        r.headers.get("Content-Type", ""),
                    )
                    if kind and len(r.content) >= 200:
                        log(f"OK 200 PFEOI {u} (bytes={len(r.content)})")
                        return r.content, kind, u
                    else:
                        log(
                            f"Discard PFEOI candidate {u} "
                            f"(ct={r.headers.get('Content-Type','n/a')}, "
                            f"bytes={len(r.content)})"
                        )
                else:
                    log(f"HTTP {r.status_code} {u} (try {i}/{retries})")
            except Exception as e:
                log(f"GET failed {u} (try {i}/{retries}): {e}")
            time.sleep(min(2 * i, 8))
    return None


def open_text_any(raw: bytes) -> TextIOWrapper:
    """
    Robust text decoder for CSV bytes.
    """
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return TextIOWrapper(BytesIO(raw), encoding=enc, errors="replace")
        except Exception:
            continue
    return TextIOWrapper(BytesIO(raw), encoding="utf-8", errors="replace")


def iter_zip_csv(path: pathlib.Path) -> bytes:
    """
    Read first CSV member from a ZIP file (kept for safety).
    """
    import zipfile

    with zipfile.ZipFile(path, "r") as zf:
        csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            raise RuntimeError("ZIP has no CSV member")
        return zf.read(sorted(csvs)[0])


# -------------------- CSV helpers --------------------


def norm(s: Optional[str]) -> str:
    return (s or "").strip()


def nint(x) -> Optional[int]:
    if x is None:
        return None
    t = str(x).replace(",", "").strip().strip('"')
    if t in ("", "-", "NA", "NIL"):
        return None
    try:
        return int(float(t))
    except Exception:
        return None


def nnum(x) -> Optional[float]:
    if x is None:
        return None
    t = str(x).replace(",", "").strip().strip('"')
    if t in ("", "-", "NA", "NIL"):
        return None
    try:
        return float(t)
    except Exception:
        return None


def norm_header(h: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in (h or "")).strip("_")


def parse_pfeoi_csv(raw_bytes: bytes) -> List[dict]:
    """
    Returns list of rows with canonical keys:
      date, isin, scrip_name, symbol, notional_oi_sh, pfe_oi_sh
    """
    txt = open_text_any(raw_bytes).read()
    lines = [ln for ln in txt.splitlines() if ln.strip() != ""]
    if not lines:
        return []

    reader = csv.DictReader(lines)
    out: List[dict] = []
    for r in reader:
        m = {norm_header(k): norm(v) for k, v in r.items()}

        # basic sanity
        if not (m.get("symbol") and m.get("isin")):
            continue

        out.append(
            {
                "date": m.get("date") or m.get("trade_date") or "",
                "isin": m.get("isin"),
                "scrip_name": m.get("scrip_name") or m.get("scripname") or "",
                "symbol": m.get("symbol"),
                "notional_oi_sh": nint(
                    m.get("notional_open_interest") or m.get("notional_oi")
                ),
                "pfe_oi_sh": nnum(
                    m.get("portfolio_wise_futures_equivalent_open_interest")
                    or m.get("portfolio_wise_futures_equivalent_oi")
                    or m.get("pfe_oi")
                ),
            }
        )
    return out


# -------------------- DDL / ETL --------------------


DDL_TABLE = f"""
CREATE SCHEMA IF NOT EXISTS {EOD_SCHEMA};

CREATE TABLE IF NOT EXISTS {EOD_SCHEMA}.nse_combined_oi (
  trade_date      date           NOT NULL,
  isin            text           NOT NULL,
  scrip_name      text           NOT NULL,
  symbol          text           NOT NULL,
  symbol_ci       citext         GENERATED ALWAYS AS (symbol) STORED,
  notional_oi_sh  numeric(20,0)  NOT NULL,
  pfe_oi_sh       numeric(20,4)  NOT NULL,
  created_at      timestamptz    NOT NULL DEFAULT now(),
  CONSTRAINT nse_combined_oi_pk PRIMARY KEY (trade_date, symbol_ci)
);
"""


def ensure_indexes(cur) -> None:
    """
    Ensure basic indexes for nse_combined_oi.
    """
    cur.execute(
        f"""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = '{EOD_SCHEMA}' AND indexname = 'ix_nse_combined_oi_symbol_date'
          ) THEN
            CREATE INDEX ix_nse_combined_oi_symbol_date
              ON {EOD_SCHEMA}.nse_combined_oi (symbol_ci, trade_date DESC);
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = '{EOD_SCHEMA}' AND indexname = 'ix_nse_combined_oi_pfe_oi'
          ) THEN
            CREATE INDEX ix_nse_combined_oi_pfe_oi
              ON {EOD_SCHEMA}.nse_combined_oi (trade_date, pfe_oi_sh DESC);
          END IF;
        END
        $$ LANGUAGE plpgsql;
        """
    )


def etl(
    trade_date: dt.date,
    rows: List[dict],
    skip_ddl: bool = False,
) -> int:
    """
    Upsert PFEOI rows into eod.nse_combined_oi for given trade_date.
    """
    if not rows:
        log("No PFEOI rows to ETL.")
        return 0

    with pg_conn() as conn:
        cur = conn.cursor()
        if not skip_ddl:
            cur.execute(DDL_TABLE)
            ensure_indexes(cur)

        q = sql.SQL(
            f"""
            INSERT INTO {EOD_SCHEMA}.nse_combined_oi AS t(
              trade_date, isin, scrip_name, symbol,
              notional_oi_sh, pfe_oi_sh
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date, symbol_ci) DO UPDATE SET
              isin           = EXCLUDED.isin,
              scrip_name     = EXCLUDED.scrip_name,
              notional_oi_sh = EXCLUDED.notional_oi_sh,
              pfe_oi_sh      = EXCLUDED.pfe_oi_sh,
              created_at     = now();
            """
        )

        n = 0
        for r in rows:
            if r.get("symbol") is None:
                continue
            not_oi = r.get("notional_oi_sh")
            pfe = r.get("pfe_oi_sh")
            if not_oi is None and pfe is None:
                continue
            cur.execute(
                q,
                (
                    trade_date,
                    r.get("isin"),
                    r.get("scrip_name") or "",
                    r.get("symbol"),
                    not_oi,
                    pfe,
                ),
            )
            n += 1

        conn.commit()
        log(f"Upserted PFEOI rows: {n}")
        return n


# -------------------- Orchestration --------------------


def save_bytes(
    d: dt.date, data: bytes, outdir: Optional[pathlib.Path]
) -> pathlib.Path:
    """
    Save bytes as combineoi_deleq_DDMMYYYY.csv.
    """
    out = ensure_outdir(d, outdir)
    name = f"combineoi_deleq_{ddmmyyyy(d)}.csv"
    p = out / name
    p.write_bytes(data)
    log(f"Saved PFEOI → {p} (bytes={len(data)})")
    return p


def resolve_etl_file(
    arg_path: Optional[str], d: dt.date, outdir: Optional[str]
) -> pathlib.Path:
    """
    Decide which local CSV file to ETL when using --etl-only.
    Respects explicit --file path; otherwise searches data/nse/YYYY/MM/DD.
    """
    if arg_path:
        p = pathlib.Path(os.path.expanduser(arg_path)).resolve()
        if p.exists():
            return p

    daydir = ensure_outdir(d, pathlib.Path(outdir) if outdir else None)
    pats = [
        f"combineoi_deleq_{ddmmyyyy(d)}.csv",
        f"combineoi_deleq_{yyyymmdd(d)}.csv",
    ]
    matches: List[pathlib.Path] = []
    for pat in pats:
        matches.extend(sorted(daydir.glob(pat)))
    if not matches:
        matches.extend(sorted(daydir.glob("*combine*deleq*.csv")))
        matches.extend(sorted(daydir.glob("*combined*oi*.csv")))
    if not matches:
        raise FileNotFoundError(f"PFEOI --file missing; scanned {daydir}")

    # If multiple, pick the largest CSV.
    def score(p: pathlib.Path) -> tuple:
        try:
            size = p.stat().st_size
        except Exception:
            size = 0
        return (-size, p.name)

    best = sorted(matches, key=score)[0]
    return best


def run_one_day(d: dt.date, args) -> None:
    log(f"PFEOI trade_date = {d}")
    outp = pathlib.Path(args.outdir) if args.outdir else None

    saved: Optional[pathlib.Path] = None
    kind: Optional[str] = None
    urls: List[str] = []

    # ---------- FETCH ----------
    if not args.etl_only:
        # 1) Direct URL, highest priority if provided
        if args.direct_url:
            log(f"Using direct CSV URL: {args.direct_url}")
            got = http_get_any([args.direct_url], args.timeout, args.retries)
            if not got:
                raise RuntimeError(
                    "Direct URL fetch failed for PFEOI. "
                    "Check --direct-url (copy exact CSV URL from DevTools)."
                )
            data, kind, _ = got
            saved = save_bytes(d, data, outp)

        # 2) archives-json → downloadLink/path
        elif args.archives_json:
            saved_api, kind_api, urls = fetch_from_archives_json(
                args.archives_json, args.timeout, args.retries, outp, d
            )
            if saved_api:
                saved, kind = saved_api, kind_api

            # if JSON returned urls, use them
            if (not saved) and urls:
                got = http_get_any(urls, args.timeout, args.retries)
                if got:
                    data, kind, _ = got
                    saved = save_bytes(d, data, outp)

        # 3) Fallback guessed archive URLs
        if not saved:
            urls += candidate_urls(d)
            got = http_get_any(urls, args.timeout, args.retries)
            if not got:
                first = urls[0] if urls else "<none>"
                raise RuntimeError(
                    "All PFEOI candidate URLs failed or returned non-CSV content.\n"
                    f"Example candidate: {first}\n"
                    "Most likely causes:\n"
                    "  - --archives-json was a placeholder (contains '...'), or\n"
                    "  - NSE changed the archive path.\n\n"
                    "Fix: use a REAL --archives-json URL from DevTools Network tab, "
                    "or pass the exact CSV link via --direct-url, or ETL a manual "
                    "download via --etl-only --file path/to/combineoi_deleq_*.csv"
                )
            data, kind_guess, _ = got
            saved = save_bytes(d, data, outp)
            kind = kind_guess or "csv"

        if args.check_only:
            # fetch + parse only
            raw = saved.read_bytes()
            rows = parse_pfeoi_csv(raw)
            log(f"[CHECK-ONLY] Parsed PFEOI rows: {len(rows)}")
            return

        if args.fetch_only:
            # Just download and save, no parse/ETL
            return

    else:
        # ETL from local file
        saved = resolve_etl_file(args.file, d, args.outdir)
        low = saved.name.lower()
        if low.endswith(".csv"):
            kind = "csv"
        elif low.endswith(".csv.gz"):
            kind = "csv.gz"
        elif low.endswith(".zip"):
            kind = "zip"
        else:
            raise ValueError(f"Unknown PFEOI --file type: {saved.name}")

    # ---------- PARSE ----------
    if kind == "zip":
        raw = iter_zip_csv(saved)
    elif kind == "csv":
        raw = saved.read_bytes()
    elif kind == "csv.gz":
        with saved.open("rb") as fh:
            raw = gzip.GzipFile(fileobj=fh).read()
    else:
        raw = b""

    rows = parse_pfeoi_csv(raw)
    log(f"Parsed PFEOI rows: {len(rows)}")

    if not rows:
        log("PFEOI CSV appears empty or headers not recognised.")
        return

    # ---------- ETL ----------
    if not (args.check_only or args.fetch_only):
        up = etl(d, rows, skip_ddl=args.skip_ddl)
        log(f"ETL complete, rows upserted: {up}")


# -------------------- CLI --------------------


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="F&O – Combine Delta Equivalent Open Interest across exchanges (PFEOI)"
    )
    ap.add_argument("trade_date", help="YYYY-MM-DD")
    ap.add_argument(
        "--archives-json",
        default=None,
        help="NSE api/reports?archives=... URL (full encoded one from DevTools Network tab)",
    )
    ap.add_argument(
        "--direct-url",
        default=None,
        help=(
            "Direct CSV URL (e.g. "
            "https://nsearchives.nseindia.com/content/fo/combineoi_deleq_31102025.csv). "
            "If provided, bypasses archives-json and guessed URLs."
        ),
    )
    ap.add_argument(
        "--check-only",
        action="store_true",
        help="Only fetch+parse; do not ETL",
    )
    ap.add_argument(
        "--fetch-only",
        action="store_true",
        help="Download only; skip parse+ETL after saving",
    )
    ap.add_argument(
        "--etl-only",
        action="store_true",
        help="Only ETL from --file (auto-resolves from data/nse/YYYY/MM/DD if omitted)",
    )
    ap.add_argument(
        "--skip-ddl",
        action="store_true",
        help="Skip CREATE SCHEMA/TABLE/INDEX (if lacking perms)",
    )
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=25)
    ap.add_argument("--outdir", default=None)
    ap.add_argument(
        "--file",
        default=None,
        help=(
            "Local CSV path when using --etl-only "
            "(e.g., C:/Users/You/Downloads/combineoi_deleq_31102025.csv)"
        ),
    )
    ap.add_argument(
        "--refresh",
        action="store_true",
        help="(Reserved) optional place to hook MV refresh later",
    )
    args = ap.parse_args()

    d = dt.datetime.strptime(args.trade_date, "%Y-%m-%d").date()
    run_one_day(d, args)
