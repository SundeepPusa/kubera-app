# -*- coding: utf-8 -*-
"""
FILE: reports/fetch_bulk_deals.py
Fetch + ETL → {EOD_SCHEMA}.nse_bulk_deals  (NSE → CM → Equities → “Bulk Deals”)

Quick usage (IST-aware):
  # Latest trading day (auto-skip Sat/Sun; optional --holidays)
  python -m reports.fetch_bulk_deals latest

  # Yesterday (IST)
  python -m reports.fetch_bulk_deals latest --latest-offset 1

  # Latest with data within 10 prior business days (skips weekends/holidays)
  python -m reports.fetch_bulk_deals latest --probe-back 10 --holidays "C:\\Users\\Limak\\Documents\\osa-final\\holidays.txt"

  # Explicit date
  python -m reports.fetch_bulk_deals 2025-10-31

Optional:
  # Only fetch (no DB writes)
  python -m reports.fetch_bulk_deals 2025-10-31 --fetch-only

  # ETL-only from saved file
  python -m reports.fetch_bulk_deals 2025-10-31 --etl-only \
    --file data/nse/2025/10/31/bulk_deals_31102025.csv

  # Ensure views / materialized view
  python -m reports.fetch_bulk_deals latest --ensure-views
"""

from __future__ import annotations
import argparse, csv, io, json, gzip, os, pathlib, datetime as dt, time, re
from typing import Dict, Any, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2 import sql
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
    print(f"[bulk] {dt.datetime.now():%Y-%m-%d %H:%M:%S} | {msg}")

def ensure_outdir(d: dt.date, outdir: Optional[pathlib.Path]) -> pathlib.Path:
    base = pathlib.Path(outdir) if outdir else DATA_DIR
    final = base / f"{d.year}" / f"{d:%m}" / f"{d:%d}"
    final.mkdir(parents=True, exist_ok=True)
    return final

# -------------------- DATE HELPERS (IST + weekend/holiday skip) --------------------
def today_ist() -> dt.date:
    return (dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)).date()

def ist_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)

def ddmmyyyy(d: dt.date) -> str: return d.strftime("%d%m%Y")
def yyyymmdd(d: dt.date) -> str: return d.strftime("%Y%m%d")
def ddMonYYYY(d: dt.date) -> str: return d.strftime("%d-%b-%Y")

def load_holidays(path: Optional[str]) -> set[dt.date]:
    hol: set[dt.date] = set()
    if not path:
        return hol
    p = pathlib.Path(path)
    if not p.exists():
        return hol
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        try:
            hol.add(dt.date.fromisoformat(s))
        except Exception:
            pass
    return hol

def is_bday(d: dt.date, holidays: set[dt.date]) -> bool:
    return d.weekday() < 5 and d not in holidays  # 0=Mon..4=Fri

def prev_bday(d: dt.date, holidays: set[dt.date]) -> dt.date:
    cur = d
    while not is_bday(cur, holidays):
        cur -= dt.timedelta(days=1)
    return cur

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

CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS {EOD_SCHEMA}.nse_bulk_deals (
  trade_date     date        NOT NULL,
  symbol         text        NOT NULL,
  security_name  text,
  client_name    text        NOT NULL,
  buy_sell       text        NOT NULL,              -- BUY / SELL
  quantity       bigint,
  trade_price    numeric(14,2),
  market_type    text,
  exchange       text,
  remarks        text,
  raw            jsonb       NOT NULL,
  load_ts        timestamptz NOT NULL DEFAULT now(),
  symbol_ci      citext GENERATED ALWAYS AS (upper(symbol)) STORED,
  CONSTRAINT pk_nse_bulk_deals
    PRIMARY KEY (trade_date, symbol, client_name, buy_sell, COALESCE(quantity,0), COALESCE(trade_price,0))
);

CREATE INDEX IF NOT EXISTS ix_bulk_symbol_ci_date
  ON {EOD_SCHEMA}.nse_bulk_deals (symbol_ci, trade_date DESC);

CREATE INDEX IF NOT EXISTS ix_bulk_trade_date
  ON {EOD_SCHEMA}.nse_bulk_deals (trade_date DESC);

CREATE INDEX IF NOT EXISTS brin_bulk_trade_date
  ON {EOD_SCHEMA}.nse_bulk_deals USING brin (trade_date) WITH (pages_per_range=32);
"""

def ensure_ddl(skip: bool = False) -> None:
    if skip:
        return
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
    # gzip magic
    if data[:2] == b"\x1f\x8b":
        try:
            data = gzip.decompress(data)
        except Exception:
            return False
    head = data[:1024].decode("utf-8", "ignore").lower()
    # bulk files always have "symbol" and buy/sell column variants
    return ("symbol" in head) and (("buy" in head) or ("sell" in head)) and ("," in head or ";" in head)

def http_get(url: str, timeout: int, retries: int) -> Optional[bytes]:
    s = http_session()
    last: Optional[Exception] = None
    for i in range(1, retries + 1):
        try:
            r = s.get(url, timeout=timeout, allow_redirects=True, stream=True)
            if r.status_code == 200 and r.content:
                data = r.content
                if r.headers.get("Content-Encoding", "").lower() == "gzip":
                    try:
                        data = gzip.decompress(data)
                    except Exception:
                        pass
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
        "archives=%5B%7B%22name%22%3A%22CM%20-%20Bulk%20Deals%22%2C%22type%22%3A%22archives%22%2C"
        "%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%5D"
        f"&date={d}&type=equities&mode=single"
    )

def _abs(u: str) -> str:
    if u.startswith(("http://", "https://")):
        return u
    host = "https://archives.nseindia.com" if "/content/" in u else "https://www.nseindia.com"
    return host + (u if u.startswith("/") else "/" + u)

def candidate_csv_urls(trade_date: dt.date) -> List[str]:
    # observed mirrors / name variants
    ymd = yyyymmdd(trade_date)
    dmy = ddmmyyyy(trade_date)
    bases = [
        "https://archives.nseindia.com/content/equities",
        "https://archives.nseindia.com/content/cm",
        "https://nsearchives.nseindia.com/content/equities",
        "https://nsearchives.nseindia.com/content/cm",
    ]
    names = [
        f"cm_bulk_deals_{ymd}.csv",
        f"cm_bulk_deals_{dmy}.csv",
        f"bulk_deals_{ymd}.csv",
        f"bulk_deals_{dmy}.csv",
        f"cm_bulk_deals_{ymd}.csv.gz",
        f"bulk_deals_{ymd}.csv.gz",
        f"bulk_deals_{ymd}.zip",
    ]
    return [f"{b}/{n}" for b in bases for n in names]

# -------------------- ARCHIVES-JSON FLOW --------------------
def fetch_from_archives_json(url: str, timeout: int, retries: int,
                             outdir: Optional[pathlib.Path],
                             d: dt.date) -> Tuple[Optional[pathlib.Path], Optional[List[str]]]:
    s = http_session()
    for i in range(1, retries + 1):
        try:
            r = s.get(url, timeout=timeout, headers={"X-Requested-With": "XMLHttpRequest"})
            ct = (r.headers.get("Content-Type") or "").lower()

            # Inline CSV directly
            if any(x in ct for x in ("text/csv", "application/octet-stream", "text/plain")) and r.content:
                if len(r.content) < 20:
                    raise RuntimeError(f"archives delivered tiny payload ({len(r.content)} bytes)")
                dest = ensure_outdir(d, outdir) / f"bulk_deals_{ddmmyyyy(d)}.csv"
                dest.write_bytes(r.content)
                log(f"archives delivered CSV directly → {dest} ({len(r.content)} bytes)")
                return dest, None

            # JSON with file links
            if "application/json" in ct:
                urls: List[str] = []
                try:
                    j = r.json()
                except Exception:
                    j = None
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
    try:
        return float(s) if s else None
    except Exception:
        return None

def norm_header(h: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in (h or "")).strip("_")

HEADER_ALIASES = {
    "date": {"date", "trade_date"},
    "symbol": {"symbol", "security_code", "ticker"},
    "security_name": {"security_name", "company_name", "name_of_security"},
    "client_name": {"client_name", "client", "party_name"},
    "buy_sell": {"buy/sell", "buy_sell", "buy__sell", "side"},
    "qty": {"quantity_traded", "qty", "quantity"},
    "price": {"trade_price", "price", "avg_price", "trade_price___wght__avg__price", "trade_price_wght_avg_price"},
    "exchange": {"exchange", "exch"},
    "market_type": {"market_type", "series"},
    "remarks": {"remarks", "remark"},
}

def get_any(m: Dict[str, str], keys: set[str]) -> Optional[str]:
    for k in keys:
        k2 = norm_header(k)
        if k2 in m and m[k2] != "":
            return m[k2]
    return None

def parse_rows(csv_bytes: bytes) -> List[Dict[str, Any]]:
    text = csv_bytes.decode("utf-8-sig", errors="replace")
    rdr = csv.DictReader(io.StringIO(text))
    if not rdr.fieldnames:
        return []

    # normalize header map per row (keeps raw)
    out: List[Dict[str, Any]] = []
    for row in rdr:
        m = {norm_header(k): (row.get(k) or "").strip() for k in rdr.fieldnames}
        dstr = get_any(m, HEADER_ALIASES["date"])
        sym  = get_any(m, HEADER_ALIASES["symbol"])
        cli  = get_any(m, HEADER_ALIASES["client_name"])
        side = (get_any(m, HEADER_ALIASES["buy_sell"]) or "").upper().replace(" ", "")
        if not (dstr and sym and cli and side in {"BUY", "SELL"}):
            continue

        # date parse (accept %d-%b-%y / %d-%b-%Y / %Y-%m-%d)
        d: Optional[dt.date] = None
        for fmt in ("%d-%b-%y", "%d-%b-%Y", "%Y-%m-%d"):
            try:
                d = dt.datetime.strptime(dstr, fmt).date()
                break
            except Exception:
                pass
        if d is None:
            continue

        qty   = safe_int(get_any(m, HEADER_ALIASES["qty"]))
        price = get_any(m, HEADER_ALIASES["price"])
        tp    = safe_decimal(price) if price is not None else None

        out.append({
            "trade_date": d,
            "symbol": sym,
            "security_name": get_any(m, HEADER_ALIASES["security_name"]),
            "client_name": cli,
            "buy_sell": side,
            "quantity": qty,
            "trade_price": tp,
            "market_type": get_any(m, HEADER_ALIASES["market_type"]),
            "exchange": get_any(m, HEADER_ALIASES["exchange"]),
            "remarks": get_any(m, HEADER_ALIASES["remarks"]),
            "raw": m
        })
    return out

# -------------------- FETCH ORCHESTRATION --------------------
def save_bytes(d: dt.date, data: bytes, outdir: Optional[pathlib.Path]) -> pathlib.Path:
    dest_dir = ensure_outdir(d, outdir)
    p = dest_dir / f"bulk_deals_{ddmmyyyy(d)}.csv"
    p.write_bytes(data)
    log(f"Saved → {p} (bytes={len(data)})")
    return p

def fetch_csv_bytes(d: dt.date, timeout: int, retries: int,
                    archives_json_override: Optional[str],
                    outdir: Optional[pathlib.Path]) -> Tuple[pathlib.Path, bytes]:
    # 1) Try archives JSON (can inline CSV or return URLs)
    arch_url = archives_json_override or archives_json_url(d)
    saved, urls = fetch_from_archives_json(arch_url, timeout, retries, outdir, d)
    if saved:
        return saved, saved.read_bytes()

    # 2) Try resolved URLs (if any), else static mirrors
    candidates = urls if urls else candidate_csv_urls(d)
    for u in candidates:
        data = http_get(u, timeout, max(1, min(2, retries)))
        if data and sniff_is_csv(data, None) and len(data) >= 20:
            path = ensure_outdir(d, outdir) / f"bulk_deals_{ddmmyyyy(d)}.csv"
            path.write_bytes(data)
            log(f"Saved → {path} (bytes={len(data)})")
            return path, data

    raise RuntimeError("Failed to fetch a valid Bulk Deals CSV from archives-json and fallback URLs.")

# -------------------- ETL (Replace-per-day) --------------------
def replace_day_insert(schema: str, trade_date: dt.date, rows: List[Dict[str, Any]],
                       source_file: str, skip_ddl: bool = False, analyze: bool = True) -> int:
    ensure_ddl(skip=skip_ddl)
    table_fqn = f"{schema}.nse_bulk_deals"

    with pg_conn() as conn:
        with conn.cursor() as cur:
            # per-day advisory lock to avoid races in cron
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s) # hashtext(%s));",
                        (table_fqn, str(trade_date)))

            # replace-day
            cur.execute(f"DELETE FROM {table_fqn} WHERE trade_date = %s;", (trade_date,))

            cur.execute("""
                CREATE TEMP TABLE _tmp_bulk(
                  trade_date date,
                  symbol text,
                  security_name text,
                  client_name text,
                  buy_sell text,
                  quantity bigint,
                  trade_price numeric(14,2),
                  market_type text,
                  exchange text,
                  remarks text,
                  raw jsonb
                ) ON COMMIT DROP;
            """)

            stage_rows = []
            for r in rows:
                stage_rows.append({
                    "trade_date": r["trade_date"],
                    "symbol": (r["symbol"] or "").strip(),
                    "security_name": r.get("security_name"),
                    "client_name": (r["client_name"] or "").strip(),
                    "buy_sell": (r["buy_sell"] or "").strip().upper(),
                    "quantity": r.get("quantity"),
                    "trade_price": r.get("trade_price"),
                    "market_type": r.get("market_type"),
                    "exchange": r.get("exchange"),
                    "remarks": r.get("remarks"),
                    "raw": json.dumps(r.get("raw", r), ensure_ascii=False)
                })

            execute_batch(cur, """
                INSERT INTO _tmp_bulk(
                  trade_date, symbol, security_name, client_name, buy_sell,
                  quantity, trade_price, market_type, exchange, remarks, raw
                ) VALUES (
                  %(trade_date)s, %(symbol)s, %(security_name)s, %(client_name)s, %(buy_sell)s,
                  %(quantity)s, %(trade_price)s, %(market_type)s, %(exchange)s, %(remarks)s, %(raw)s
                )
            """, stage_rows, page_size=1000)

            cur.execute(f"""
                INSERT INTO {table_fqn}(
                  trade_date, symbol, security_name, client_name, buy_sell,
                  quantity, trade_price, market_type, exchange, remarks, raw
                )
                SELECT trade_date, symbol, security_name, client_name, buy_sell,
                       quantity, trade_price, market_type, exchange, remarks, raw
                FROM _tmp_bulk;
            """)
            affected = cur.rowcount or 0

            if analyze:
                cur.execute(f"ANALYZE {table_fqn};")

        conn.commit()
        return affected

# -------------------- Views / MV --------------------
def create_views() -> None:
    with pg_conn() as conn:
        conn.autocommit = True
        cur = conn.cursor()
        ensure_ddl(False)

        # Daily counts
        cur.execute(sql.SQL("""
          CREATE OR REPLACE VIEW {}.vw_bulk_deals_daily AS
          SELECT trade_date, COUNT(*) AS rows
          FROM {}.nse_bulk_deals
          GROUP BY trade_date;
        """).format(sql.Identifier(EOD_SCHEMA), sql.Identifier(EOD_SCHEMA)))

        # Banner for latest day
        cur.execute(sql.SQL("""
          CREATE OR REPLACE VIEW {}.vw_bulk_deals_banner_latest AS
          WITH latest AS (SELECT MAX(trade_date) AS dt FROM {}.nse_bulk_deals)
          SELECT b.trade_date, b.symbol,
                 SUM(CASE WHEN b.buy_sell='BUY'  THEN 1 ELSE 0 END) AS buy_cnt,
                 SUM(CASE WHEN b.buy_sell='SELL' THEN 1 ELSE 0 END) AS sell_cnt,
                 SUM(COALESCE(b.quantity,0)) AS qty_sum
          FROM {}.nse_bulk_deals b
          JOIN latest l ON b.trade_date = l.dt
          GROUP BY b.trade_date, b.symbol
          ORDER BY qty_sum DESC, symbol
          LIMIT 25;
        """).format(sql.Identifier(EOD_SCHEMA), sql.Identifier(EOD_SCHEMA), sql.Identifier(EOD_SCHEMA)))

        # Last 90d MV (distinct by key)
        cur.execute(sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {}.mv_bulk_last90").format(sql.Identifier(EOD_SCHEMA)))
        cur.execute(sql.SQL("""
          CREATE MATERIALIZED VIEW {}.mv_bulk_last90 AS
          WITH src AS (
            SELECT DISTINCT ON (trade_date, symbol, client_name, buy_sell, COALESCE(quantity,0), COALESCE(trade_price,0))
                   trade_date, symbol, security_name, client_name, buy_sell,
                   quantity, trade_price, market_type, exchange, remarks, raw
            FROM {}.nse_bulk_deals
            WHERE trade_date >= CURRENT_DATE - INTERVAL '100 days'
            ORDER BY trade_date, symbol, client_name, buy_sell,
                     COALESCE(quantity,0) DESC, COALESCE(trade_price,0) DESC
          )
          SELECT * FROM src;
        """).format(sql.Identifier(EOD_SCHEMA), sql.Identifier(EOD_SCHEMA)))

        cur.execute(sql.SQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_bulk_last90 "
            "ON {}.mv_bulk_last90(trade_date, symbol, client_name, buy_sell, COALESCE(quantity,0), COALESCE(trade_price,0))"
        ).format(sql.Identifier(EOD_SCHEMA)))

        cur.execute(sql.SQL("""
          CREATE OR REPLACE FUNCTION {}.refresh_bulk() RETURNS void
          LANGUAGE plpgsql AS $$
          BEGIN
            BEGIN
              REFRESH MATERIALIZED VIEW CONCURRENTLY {}.mv_bulk_last90;
            EXCEPTION WHEN feature_not_supported THEN
              REFRESH MATERIALIZED VIEW {}.mv_bulk_last90;
            END;
          END$$;
        """).format(sql.Identifier(EOD_SCHEMA), sql.Identifier(EOD_SCHEMA), sql.Identifier(EOD_SCHEMA)))
        log("Views/MV ensured.")

# -------------------- MAIN ORCHESTRATION --------------------
def try_run_for_date(d: dt.date, args) -> bool:
    """Attempt fetch+parse (+ETL unless --fetch-only). Return True on success with data."""
    outp = pathlib.Path(args.outdir) if args.outdir else None

    # FETCH
    if not args.etl_only:
        if args.check_only:
            log(f"archives url → {args.archives_json or archives_json_url(d)}")
            dst = ensure_outdir(d, outp) / f"bulk_deals_{ddmmyyyy(d)}.csv"
            log(f"would save → {dst}")
            return True
        try:
            saved_path, csv_data = fetch_csv_bytes(d, args.timeout, args.retries, args.archives_json, outp)
        except Exception as e:
            log(f"No file for {d}: {e}")
            return False
    else:
        if not args.file:
            raise FileNotFoundError("--etl-only requires --file=<path-to-csv>")
        saved_path = pathlib.Path(args.file)
        if not saved_path.exists():
            raise FileNotFoundError(f"--file not found: {saved_path}")
        csv_data = saved_path.read_bytes()

    # Parse
    parsed = parse_rows(csv_data)
    log(f"Parsed rows: {len(parsed)} for {d}")

    if args.fetch_only:
        return len(parsed) > 0

    if parsed:
        n = replace_day_insert(EOD_SCHEMA, parsed[0]["trade_date"], parsed, saved_path.as_posix(), skip_ddl=args.skip_ddl)
        log(f"Day replace complete for {d}. inserted rows = {n}")
        return True

    log(f"No rows parsed for {d}; nothing to write.")
    return False

def run_one_day_or_probe(d: dt.date, args) -> None:
    """Run for date d; if fail and --probe-back>0, walk backwards over business days until success or limit."""
    # First attempt
    hols = load_holidays(args.holidays)
    target = prev_bday(d, hols) if args.trade_date.lower() == "latest" else d

    if try_run_for_date(target, args):
        return

    # Probe back if asked
    remaining = max(0, args.probe_back)
    probe_d = target
    while remaining > 0:
        # walk back to previous business day
        probe_d -= dt.timedelta(days=1)
        if not is_bday(probe_d, hols):
            continue
        log(f"Probing previous bday {probe_d} (remaining {remaining}) …")
        if try_run_for_date(probe_d, args):
            return
        remaining -= 1

    log("Exhausted probe-back window without finding a valid Bulk Deals file.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Fetch + ETL NSE Bulk Deals into Postgres (replace-per-day, no duplicates).")
    ap.add_argument("trade_date", help="'latest' or YYYY-MM-DD (IST)")
    ap.add_argument("--latest-offset", type=int, default=0, help="When using 'latest', pick (IST today - offset days)")
    ap.add_argument("--probe-back", type=int, default=0, help="If latest day has no file yet, probe this many prior business days")
    ap.add_argument("--holidays", default=None, help="Optional path to newline-separated YYYY-MM-DD holiday list")
    ap.add_argument("--archives-json", default=None, help="NSE api/reports?archives=... URL override")
    ap.add_argument("--check-only", action="store_true", help="Only show planned fetch + save path")
    ap.add_argument("--fetch-only", action="store_true", help="Fetch + save locally; skip DB writes")
    ap.add_argument("--etl-only", action="store_true", help="Only ETL from --file (no network fetch)")
    ap.add_argument("--skip-ddl", action="store_true", help="Skip CREATE SCHEMA/TABLE/INDEX")
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=15)
    ap.add_argument("--outdir", default="data/nse")
    ap.add_argument("--file", default=None, help="Local CSV when using --etl-only")
    ap.add_argument("--ensure-views", action="store_true")
    args = ap.parse_args()

    if args.ensure_views:
        create_views()

    # Resolve target date
    if args.trade_date.lower() == "latest":
        base = today_ist() - dt.timedelta(days=max(0, args.latest_offset))
        d = base
    else:
        d = ist_date(args.trade_date)

    run_one_day_or_probe(d, args)
