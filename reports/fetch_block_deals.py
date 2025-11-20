# -*- coding: utf-8 -*-
"""
FILE: reports/fetch_block_deals.py
Fetch + ETL → {EOD_SCHEMA}.nse_block_deals  (NSE → CM → Equities → “Block Deals”)

Quick usage:
  # Latest (IST)
  python -m reports.fetch_block_deals latest

  # Yesterday (IST)
  python -m reports.fetch_block_deals latest --latest-offset 1

  # Explicit date
  python -m reports.fetch_block_deals 2025-10-31

Optional:
  # Only fetch (no DB writes)
  python -m reports.fetch_block_deals 2025-10-31 --fetch-only

  # ETL-only from saved file
  python -m reports.fetch_block_deals 2025-10-31 --etl-only \
    --file data/nse/2025/10/31/block_deals_31102025.csv
"""

from __future__ import annotations
import argparse, csv, io, json, re, time, gzip, os, pathlib, datetime as dt
from typing import Dict, Any, Iterable, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# -------------------- ENV / PATHS --------------------
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "nse"

for p in (BASE_DIR / ".env", BASE_DIR / ".ENV"):
    if p.exists():
        load_dotenv(p.as_posix())
        break

def env(k: str, dflt: Optional[str] = None) -> Optional[str]:
    v = os.getenv(k)
    return v if v is not None else dflt

EOD_SCHEMA = (env("EOD_SCHEMA", "eod") or "eod").strip()

def log(msg: str) -> None:
    print(f"[block] {dt.datetime.now():%Y-%m-%d %H:%M:%S} | {msg}")

def ensure_outdir(d: dt.date, outdir: Optional[pathlib.Path]) -> pathlib.Path:
    base = pathlib.Path(outdir) if outdir else DATA_DIR
    final = base / f"{d.year}" / f"{d:%m}" / f"{d:%d}"
    final.mkdir(parents=True, exist_ok=True)
    return final

# -------------------- DATE HELPERS --------------------
def today_ist() -> dt.date:
    # IST = UTC+5:30
    return (dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)).date()

def ist_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)

def ddmmyyyy(d: dt.date) -> str: return d.strftime("%d%m%Y")
def yyyymmdd(d: dt.date) -> str: return d.strftime("%Y%m%d")
def ddMonYYYY(d: dt.date) -> str: return d.strftime("%d-%b-%Y")

# -------------------- DB --------------------
def pg_conn():
    return psycopg2.connect(
        host=env("DB_HOST", "localhost"),
        port=env("DB_PORT", "5432"),
        dbname=env("DB_NAME", "osa_db"),
        user=env("DB_USER", "postgres"),
        password=env("DB_PASSWORD", ""),
        connect_timeout=10,
    )

DDL = f"""
CREATE SCHEMA IF NOT EXISTS {EOD_SCHEMA};

-- Table matches the live schema you shared
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS {EOD_SCHEMA}.nse_block_deals (
  trade_date      date                     NOT NULL,
  symbol          text                     NOT NULL,
  security_name   text,
  client_name     text                     NOT NULL,
  side            text                     NOT NULL,           -- BUY / SELL
  quantity_traded bigint                   NOT NULL,
  remarks         text,
  trade_price     numeric(14,2)            NOT NULL,
  wght_avg_price  numeric(14,2),
  source_file     text,
  load_ts         timestamptz              NOT NULL DEFAULT now(),
  symbol_ci       citext GENERATED ALWAYS AS (upper(symbol)) STORED,
  CONSTRAINT pk_nse_block_deals PRIMARY KEY
   (trade_date, symbol, client_name, side, quantity_traded, trade_price)
);

CREATE INDEX IF NOT EXISTS ix_block_symbol_ci_date
  ON {EOD_SCHEMA}.nse_block_deals (symbol_ci, trade_date DESC);

CREATE INDEX IF NOT EXISTS ix_block_trade_date
  ON {EOD_SCHEMA}.nse_block_deals (trade_date DESC);

CREATE INDEX IF NOT EXISTS brin_block_trade_date
  ON {EOD_SCHEMA}.nse_block_deals USING brin (trade_date) WITH (pages_per_range=32);
"""

def ensure_ddl(skip: bool = False) -> None:
    if skip: return
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        conn.commit()

# -------------------- HTTP HELPERS --------------------
def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/119 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nseindia.com/all-reports",
        "Origin": "https://www.nseindia.com",
        "Accept-Encoding": "gzip, deflate",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    })
    ck = (env("NSE_COOKIES", "") or "").strip()
    if ck:
        s.headers["Cookie"] = ck
    # warm session quietly
    for u in ("https://www.nseindia.com/", "https://www.nseindia.com/all-reports"):
        try:
            s.get(u, timeout=12)
        except Exception:
            pass
    return s

def sniff_is_csv(data: bytes, content_type: Optional[str]) -> bool:
    if not data:
        return False
    if content_type and ("html" in content_type.lower() or "javascript" in content_type.lower()):
        return False
    # gzip?
    if data[:2] == b"\x1f\x8b":
        try:
            data = gzip.decompress(data)
        except Exception:
            return False
    head = data[:1024].decode("utf-8", "ignore").lower()
    return ("symbol" in head and ("buy" in head or "sell" in head)) and ("," in head or ";" in head)

def http_get(url: str, timeout: int, retries: int) -> Optional[bytes]:
    s = http_session()
    last: Optional[Exception] = None
    for i in range(1, retries + 1):
        try:
            r = s.get(url, timeout=timeout, allow_redirects=True, stream=True)
            if r.status_code == 200 and r.content:
                data = r.content
                if r.headers.get("Content-Encoding", "").lower() == "gzip":
                    try: data = gzip.decompress(data)
                    except Exception: pass
                return data
            else:
                log(f"HTTP {r.status_code} {url} (try {i}/{retries})")
        except Exception as e:
            last = e
            log(f"GET failed {url} (try {i}/{retries}): {e}")
        time.sleep(min(2 * i, 8))
    if last:
        log(f"All retries failed for {url}: {last}")
    return None

# -------------------- URL BUILDERS --------------------
def archives_json_url(trade_date: dt.date) -> str:
    d = ddMonYYYY(trade_date)
    return (
        "https://www.nseindia.com/api/reports?"
        "archives=%5B%7B%22name%22%3A%22CM%20-%20Block%20Deals%22%2C%22type%22%3A%22archives%22%2C"
        "%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%5D"
        f"&date={d}&type=equities&mode=single"
    )

def candidate_csv_urls(trade_date: dt.date) -> List[str]:
    ymd = yyyymmdd(trade_date)
    return [
        f"https://archives.nseindia.com/content/equities/cm_blockdeals_{ymd}.csv",
        f"https://nsearchives.nseindia.com/content/equities/cm_blockdeals_{ymd}.csv",
    ]

def _abs(u: str) -> str:
    if u.startswith(("http://", "https://")): return u
    host = "https://archives.nseindia.com" if "/content/" in u else "https://www.nseindia.com"
    return host + (u if u.startswith("/") else "/" + u)

# -------------------- ARCHIVES-JSON FLOW --------------------
def fetch_from_archives_json(url: str, timeout: int, retries: int,
                             outdir: Optional[pathlib.Path],
                             d: dt.date) -> Tuple[Optional[pathlib.Path], Optional[List[str]]]:
    """
    Try the NSE 'api/reports?archives=...' endpoint.
    Returns (saved_csv_path if inline CSV, list_of_urls if JSON points to files)
    """
    s = http_session()
    for i in range(1, retries + 1):
        try:
            r = s.get(url, timeout=timeout, headers={"X-Requested-With": "XMLHttpRequest"})
            ct = (r.headers.get("Content-Type") or "").lower()

            # Case 1: inline CSV
            if any(x in ct for x in ("text/csv", "application/octet-stream", "text/plain")) and r.content:
                if len(r.content) < 20:
                    raise RuntimeError(f"archives delivered tiny payload ({len(r.content)} bytes)")
                dest = ensure_outdir(d, outdir) / f"block_deals_{ddmmyyyy(d)}.csv"
                dest.write_bytes(r.content)
                log(f"archives delivered CSV directly → {dest} ({len(r.content)} bytes)")
                return dest, None

            # Case 2: JSON with download links
            if "application/json" in ct:
                try:
                    j = r.json()
                except Exception:
                    j = None
                urls: List[str] = []
                if isinstance(j, dict):
                    data_block = j.get("data")
                    if isinstance(data_block, dict):
                        for key in ("downloadLink", "path", "url", "uri"):
                            if data_block.get(key):
                                urls.append(_abs(data_block[key]))
                    if isinstance(data_block, list):
                        for it in data_block:
                            if isinstance(it, dict):
                                for key in ("downloadLink", "path", "url", "uri"):
                                    if it.get(key):
                                        urls.append(_abs(it[key]))
                    for it in j.get("rows") or []:
                        if isinstance(it, dict):
                            for key in ("downloadLink", "path", "url", "uri"):
                                if it.get(key):
                                    urls.append(_abs(it[key]))
                    for key in ("downloadLink", "path", "url", "uri"):
                        if j.get(key):
                            urls.append(_abs(j[key]))
                if urls:
                    urls = list(dict.fromkeys(urls))
                    log(f"archives resolved {len(urls)} file url(s)")
                    return None, urls
        except Exception as e:
            log(f"archives fetch failed (try {i}/{retries}): {e}")
        time.sleep(min(2 * i, 8))
    return None, None

# -------------------- PARSING --------------------
_int_re = re.compile(r"-?\d+")

def safe_int(s: str | None) -> Optional[int]:
    if s is None: return None
    s = s.strip().replace(",", "")
    return int(s) if s and _int_re.fullmatch(s) else None

def safe_decimal(s: str | None) -> Optional[float]:
    if s is None: return None
    s = s.strip().replace(",", "")
    try: return float(s) if s else None
    except Exception: return None

def parse_price_combo(raw: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Accepts:
      "7474" → (7474, 7474)
      "1332/1332" or "1332 / 1332" → (1332, 1332)
      "-" or "" → (None, None)
    Returns (trade_price, wght_avg_price). If only one number is present, mirror it.
    """
    if not raw: return (None, None)
    s = raw.replace(",", "").strip()
    if s in ("", "-"): return (None, None)
    parts = [p for p in re.split(r"\s*/\s*", s) if p]
    if len(parts) == 1:
        p = safe_decimal(parts[0]); return (p, p)
    p1 = safe_decimal(parts[0]); p2 = safe_decimal(parts[1])
    if p1 is None and p2 is not None: p1 = p2
    if p2 is None and p1 is not None: p2 = p1
    return (p1, p2)

def _norm_header(h: str) -> str:
    h = (h or "").strip().replace("\u00A0", " ")
    h = re.sub(r"\s+", " ", h)
    h = h.replace(" / ", "/").replace(" /", "/").replace("/ ", "/")
    h = h.replace(".", "")
    return h.lower()

HEADER_ALIASES = {
    "date": "date",
    "symbol": "symbol",
    "security name": "security_name",
    "security name ltd": "security_name",
    "client name": "client_name",
    "buy/sell": "side",
    "buy / sell": "side",
    "quantity traded": "quantity_traded",
    "qty traded": "quantity_traded",
    "trade price / wght avg price": "price_combo",
    "trade price/wght avg price": "price_combo",
    "trade price": "price_combo",
    "remarks": "remarks",
}

def parse_rows(csv_bytes: bytes) -> List[Dict[str, Any]]:
    text = csv_bytes.decode("utf-8-sig", errors="replace")
    rdr = csv.DictReader(io.StringIO(text))
    if not rdr.fieldnames or len(rdr.fieldnames) < 2:
        return []
    colmap: Dict[str, str] = {}
    for k in (rdr.fieldnames or []):
        nk = HEADER_ALIASES.get(_norm_header(k), k.strip())
        colmap[k] = nk

    out: List[Dict[str, Any]] = []
    for row in rdr:
        r = {colmap.get(k, k): (row.get(k) or "").strip() for k in row.keys()}
        if not r.get("date") or not r.get("symbol"):
            continue

        # Date parse (NSE usually %d-%b-%y or %d-%b-%Y)
        dt_obj = None
        for fmt in ("%d-%b-%y", "%d-%b-%Y"):
            try:
                dt_obj = dt.datetime.strptime(r["date"], fmt).date()
                break
            except Exception:
                pass
        if dt_obj is None:
            continue

        side = (r.get("side") or "").upper()
        if side not in ("BUY", "SELL"):
            continue

        qty = safe_int(r.get("quantity_traded")) or 0
        p1, p2 = parse_price_combo(r.get("price_combo") or r.get("trade_price") or "")
        trade_price = p1 if p1 is not None else (p2 if p2 is not None else 0.0)
        wght_price  = p2 if p2 is not None else (p1 if p1 is not None else None)

        out.append({
            "trade_date": dt_obj,
            "symbol": r.get("symbol") or "",
            "security_name": r.get("security_name") or "",
            "client_name": r.get("client_name") or "",
            "side": side,
            "quantity_traded": qty,
            "trade_price": float(trade_price),
            "wght_avg_price": None if wght_price is None else float(wght_price),
            "remarks": (r.get("remarks") or None),
        })
    return out

# -------------------- FETCH ORCHESTRATION --------------------
def save_bytes(d: dt.date, data: bytes, outdir: Optional[pathlib.Path]) -> pathlib.Path:
    dest_dir = ensure_outdir(d, outdir)
    p = dest_dir / f"block_deals_{ddmmyyyy(d)}.csv"
    p.write_bytes(data)
    log(f"Saved → {p} (bytes={len(data)})")
    return p

def fetch_csv_bytes(d: dt.date, timeout: int, retries: int,
                    archives_json_override: Optional[str],
                    outdir: Optional[pathlib.Path]) -> Tuple[pathlib.Path, bytes]:
    arch_url = archives_json_override or archives_json_url(d)
    saved, urls = fetch_from_archives_json(arch_url, timeout, retries, outdir, d)

    if saved:
        data = saved.read_bytes()
        return saved, data

    # Try resolved URLs, else static mirrors
    candidates = urls if urls else candidate_csv_urls(d)
    for u in candidates:
        data = http_get(u, timeout, max(1, min(2, retries)))
        if data and sniff_is_csv(data, None) and len(data) >= 20:
            path = ensure_outdir(d, outdir) / f"block_deals_{ddmmyyyy(d)}.csv"
            path.write_bytes(data)
            log(f"Saved → {path} (bytes={len(data)})")
            return path, data

    raise RuntimeError("Failed to fetch a valid Block Deals CSV from both archives-json and fallback URLs.")

# -------------------- ETL (Replace-per-day, no duplicates ever) --------------------
COLUMNS_ORDER = [
    "trade_date",
    "symbol",
    "security_name",
    "client_name",
    "side",
    "quantity_traded",
    "remarks",
    "trade_price",
    "wght_avg_price",
    "source_file",
    # load_ts has DEFAULT now(), symbol_ci is generated
]

def replace_day_insert(schema: str, trade_date: dt.date, rows: List[Dict[str, Any]], source_file: str,
                       skip_ddl: bool = False, analyze: bool = True) -> int:
    """
    Atomically REPLACE all rows for `trade_date` in {schema}.nse_block_deals with `rows`.
    Guarantees: re-run for the same day will never duplicate; removed rows disappear.
    """
    ensure_ddl(skip=skip_ddl)
    table_fqn = f"{schema}.nse_block_deals"

    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Advisory lock per table+date to avoid races
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s) # hashtext(%s));",
                        (table_fqn, str(trade_date)))

            # Delete existing for the day
            cur.execute(f"DELETE FROM {table_fqn} WHERE trade_date = %s;", (trade_date,))

            # Temp table for fast COPY
            cur.execute("""
                CREATE TEMP TABLE _tmp_block_deals(
                  trade_date date,
                  symbol text,
                  security_name text,
                  client_name text,
                  side text,
                  quantity_traded bigint,
                  remarks text,
                  trade_price numeric(14,2),
                  wght_avg_price numeric(14,2),
                  source_file text
                ) ON COMMIT DROP;
            """)

            # Stage rows
            stage_rows = []
            for r in rows:
                stage_rows.append({
                    "trade_date": r["trade_date"],
                    "symbol": r["symbol"],
                    "security_name": r.get("security_name"),
                    "client_name": r["client_name"],
                    "side": r["side"],
                    "quantity_traded": r["quantity_traded"],
                    "remarks": r.get("remarks"),
                    "trade_price": r["trade_price"],
                    "wght_avg_price": r.get("wght_avg_price"),
                    "source_file": source_file,
                })

            # COPY via execute_batch (adequate for small/medium); switch to COPY FROM STDIN if needed
            execute_batch(cur, """
                INSERT INTO _tmp_block_deals(
                  trade_date, symbol, security_name, client_name, side,
                  quantity_traded, remarks, trade_price, wght_avg_price, source_file
                ) VALUES (
                  %(trade_date)s, %(symbol)s, %(security_name)s, %(client_name)s, %(side)s,
                  %(quantity_traded)s, %(remarks)s, %(trade_price)s, %(wght_avg_price)s, %(source_file)s
                )
            """, stage_rows, page_size=1000)

            # Insert into target
            cur.execute(f"""
                INSERT INTO {table_fqn}(
                  trade_date, symbol, security_name, client_name, side,
                  quantity_traded, remarks, trade_price, wght_avg_price, source_file
                )
                SELECT
                  trade_date, symbol, security_name, client_name, side,
                  quantity_traded, remarks, trade_price, wght_avg_price, source_file
                FROM _tmp_block_deals;
            """)

            affected = cur.rowcount or 0

            if analyze:
                cur.execute(f"ANALYZE {table_fqn};")

        conn.commit()
        return affected

# -------------------- MAIN --------------------
def run_one_day(d: dt.date, args) -> None:
    log(f"Trade date = {d}")
    outp = pathlib.Path(args.outdir) if args.outdir else None

    # FETCH
    if not args.etl_only:
        if args.check_only:
            log(f"archives url → {args.archives_json or archives_json_url(d)}")
            dst = ensure_outdir(d, outp) / f"block_deals_{ddmmyyyy(d)}.csv"
            log(f"would save → {dst}")
            return
        saved_path, csv_data = fetch_csv_bytes(d, args.timeout, args.retries, args.archives_json, outp)
    else:
        # ETL-only path
        if not args.file:
            raise FileNotFoundError("--etl-only requires --file=<path-to-csv>")
        saved_path = pathlib.Path(args.file)
        if not saved_path.exists():
            raise FileNotFoundError(f"--file not found: {saved_path}")
        csv_data = saved_path.read_bytes()

    # Parse
    parsed = parse_rows(csv_data)
    log(f"Parsed rows: {len(parsed)}")

    if args.fetch_only:
        return

    # REPLACE-DAY INSERT (no duplicates on rerun)
    if parsed:
        n = replace_day_insert(EOD_SCHEMA, parsed[0]["trade_date"], parsed, saved_path.as_posix(), skip_ddl=args.skip_ddl)
        log(f"Day replace complete. inserted rows = {n}")
    else:
        # still write empty day replacement? Usually skip; leaving as no-op
        log("No rows parsed; nothing to write.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Fetch + ETL NSE Block Deals into Postgres (replace-per-day, no duplicates).")
    ap.add_argument("trade_date", help="YYYY-MM-DD (IST) or 'latest'")
    ap.add_argument("--archives-json", default=None, help="NSE api/reports?archives=... URL")
    ap.add_argument("--check-only", action="store_true", help="Only show planned fetch + save path")
    ap.add_argument("--fetch-only", action="store_true", help="Fetch + save locally; skip DB writes")
    ap.add_argument("--etl-only", action="store_true", help="Only ETL from --file (no network fetch)")
    ap.add_argument("--skip-ddl", action="store_true", help="Skip CREATE SCHEMA/TABLE/INDEX")
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=12)
    ap.add_argument("--outdir", default="data/nse")
    ap.add_argument("--file", default=None, help="Local CSV when using --etl-only")
    ap.add_argument("--latest-offset", type=int, default=0, help="When using 'latest', pick (IST today - offset days)")
    args = ap.parse_args()

    # Resolve date
    if args.trade_date.lower() == "latest":
        d = today_ist() - dt.timedelta(days=max(0, args.latest_offset))
    else:
        d = ist_date(args.trade_date)

    run_one_day(d, args)
