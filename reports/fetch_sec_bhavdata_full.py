# -*- coding: utf-8 -*-
"""
Fetch + ETL: NSE Security-wise Bhavcopy (Full) → {EOD_SCHEMA}.nse_sec_bhav_full

- Uses --outdir exactly; overwrites same-day files by default (atomic write).
- Flattens ZIP/GZ to clean CSV by default (Excel-friendly).
- Robust header normalization (TTL_TRD_QNTY → ttl_trd_qty, etc.).
- Parses DATE1 → trade_day (dd-Mon-YYYY).
- Supports --archives-json URL.
- Optional --truncate-day to hard-replace DB rows for that date.
"""

import os, csv, sys, json, gzip, argparse, pathlib, datetime as dt
from io import TextIOWrapper, StringIO, BytesIO
from typing import List, Tuple, Optional

import psycopg2
from dotenv import load_dotenv
import requests

# ----------------------- bootstrap -----------------------
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "nse"
for p in (BASE_DIR / ".env", BASE_DIR / ".ENV"):
    if p.exists():
        load_dotenv(p.as_posix()); break

EOD_SCHEMA = os.getenv("EOD_SCHEMA", "eod")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/",
}

def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[sec_bhav_full] {ts} | {msg}")

# ----------------------- http & urls -----------------------
def http_get(url: str, timeout: int = 30, retries: int = 4) -> Optional[requests.Response]:
    sess = requests.Session(); sess.headers.update(HEADERS)
    import time as _t
    for i in range(1, retries + 1):
        try:
            r = sess.get(url, timeout=timeout)
            if r.status_code == 200 and r.content:
                return r
            log(f"HTTP {r.status_code} for {url} (try {i}/{retries})")
        except Exception as e:
            log(f"GET failed {url} (try {i}/{retries}): {e}")
        _t.sleep(min(2 * i, 8))
    return None

def ddmmyyyy(d: dt.date) -> str: return d.strftime("%d%m%Y")
def ddMONyyyy_upper(d: dt.date) -> str: return d.strftime("%d%b%Y").upper()

def candidate_urls(trade_date: dt.date) -> List[str]:
    d1, d2 = ddmmyyyy(trade_date), ddMONyyyy_upper(trade_date)
    return [
        f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{d1}.csv",
        f"https://www.nseindia.com/products/content/sec_bhavdata_full_{d1}.csv",
        f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{d2}.csv",
        f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{d1}.csv.zip",
    ]

def resolve_archives_json(url: str, timeout: int, retries: int) -> Optional[bytes]:
    r = http_get(url, timeout=timeout, retries=retries)
    if not r: return None
    ctype = (r.headers.get("Content-Type") or "").lower()
    if any(k in ctype for k in ("text/csv","application/zip","application/x-zip","application/octet-stream")):
        return r.content
    if "text/plain" in ctype and r.content[:6].upper().startswith(b"SYMBOL"):
        return r.content
    if "json" in ctype:
        try: payload = r.json()
        except Exception: payload = None
        if isinstance(payload, dict):
            path = None
            if isinstance(payload.get("data"), list) and payload["data"]:
                path = payload["data"][0].get("path") or payload["data"][0].get("downloadUrl")
            if not path and payload.get("path"): path = payload["path"]
            if not path and isinstance(payload.get("files"), list) and payload["files"]:
                path = payload["files"][0].get("path") or payload["files"][0].get("downloadUrl")
            if path:
                if path.startswith("/"): path = "https://www.nseindia.com" + path
                r2 = http_get(path, timeout=timeout, retries=retries)
                if r2 and r2.status_code == 200: return r2.content
    return None

# ----------------------- kind detect & fs -----------------------
def is_mostly_binary(data: bytes) -> bool:
    if not data: return False
    sample = data[:2048]
    nul = sample.count(b"\x00")
    nontext = sum(1 for b in sample if b < 9 or (13 < b < 32) or b > 126)
    return nul > 0 or (nontext / max(1, len(sample)) > 0.30)

def detect_kind(data: bytes) -> str:
    if len(data) >= 2 and data[:2] == b"\x1f\x8b": return "csv.gz"
    if len(data) >= 4 and data[:4] == b"PK\x03\x04": return "zip"
    if is_mostly_binary(data): return "zip"
    return "csv"

def ensure_outdir(outdir: Optional[pathlib.Path]) -> pathlib.Path:
    if outdir:
        outdir.mkdir(parents=True, exist_ok=True)
        return outdir
    today = dt.date.today()
    d = DATA_DIR / str(today.year) / f"{today:%m}" / f"{today:%d}"
    d.mkdir(parents=True, exist_ok=True)
    return d

def atomic_write_bytes(dest: pathlib.Path, data: bytes) -> pathlib.Path:
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    tmp.write_bytes(data)
    try:
        tmp.replace(dest)  # atomic on same FS (Windows-friendly)
        return dest
    except Exception:
        alt = dest.with_name(dest.stem + "_new" + dest.suffix)
        tmp.replace(alt)
        log(f"File in use; wrote side-by-side: {alt}")
        return alt

def day_glob(outdir: pathlib.Path, trade_date: dt.date) -> List[pathlib.Path]:
    stem = f"sec_bhavdata_full_{trade_date:%Y-%m-%d}"
    return list(outdir.glob(stem + ".*"))

def remove_existing_for_day(outdir: pathlib.Path, trade_date: dt.date) -> None:
    for p in day_glob(outdir, trade_date):
        try: p.unlink()
        except Exception: pass

def save_and_optionally_flatten(data: bytes, outdir: pathlib.Path, trade_date: dt.date,
                                kind: str, flatten: bool) -> Tuple[pathlib.Path, str]:
    base = f"sec_bhavdata_full_{trade_date:%Y-%m-%d}"
    if kind == "csv":
        dest = outdir / f"{base}.csv"
        return atomic_write_bytes(dest, data), "csv"
    if not flatten:
        dest = outdir / (f"{base}.zip" if kind == "zip" else f"{base}.csv.gz")
        return atomic_write_bytes(dest, data), kind
    if kind == "csv.gz":
        csv_bytes = gzip.GzipFile(fileobj=BytesIO(data)).read()
        dest = outdir / f"{base}.csv"
        return atomic_write_bytes(dest, csv_bytes), "csv"
    if kind == "zip":
        import zipfile, io
        zf = zipfile.ZipFile(io.BytesIO(data), "r")
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not members: raise RuntimeError("ZIP has no CSV members")
        prefer = [m for m in members if "sec_bhavdata_full" in m.lower()]
        name = prefer[0] if prefer else members[0]
        csv_bytes = zf.read(name)
        dest = outdir / f"{base}.csv"
        return atomic_write_bytes(dest, csv_bytes), "csv"
    dest = outdir / f"{base}.csv"
    return atomic_write_bytes(dest, data), "csv"

def fetch_file(trade_date: dt.date, timeout: int, retries: int,
               min_bytes: int, allow_tiny: bool, outdir: pathlib.Path,
               archives_json_url: Optional[str], overwrite: bool,
               flatten_archive: bool) -> Tuple[pathlib.Path, str]:
    if overwrite:
        remove_existing_for_day(outdir, trade_date)
    if archives_json_url:
        log("Trying archives-json endpoint…")
        data = resolve_archives_json(archives_json_url, timeout=timeout, retries=retries)
        if data and (allow_tiny or len(data) >= max(1, min_bytes)):
            kind = detect_kind(data)
            dest, final_kind = save_and_optionally_flatten(data, outdir, trade_date, kind, flatten_archive)
            log(f"Saved → {dest} ({len(data)} bytes)")
            return dest, final_kind
    for url in candidate_urls(trade_date):
        r = http_get(url, timeout=timeout, retries=retries)
        if not r: continue
        content = r.content
        if not allow_tiny and len(content) < max(1, min_bytes):
            log(f"Downloaded but too small ({len(content)} bytes) from {url} → skip"); continue
        kind = detect_kind(content)
        dest, final_kind = save_and_optionally_flatten(content, outdir, trade_date, kind, flatten_archive)
        log(f"Saved (fallback) → {dest} ({len(content)} bytes)")
        return dest, final_kind
    raise RuntimeError("All sources failed. If holiday, try --allow-tiny or pass --file.")

# ----------------------- csv readers -----------------------
def open_text_from_bytes(path: pathlib.Path, kind: str):
    if kind == "csv":
        fh = path.open("rb"); return TextIOWrapper(fh, encoding="utf-8", errors="replace")
    if kind == "csv.gz":
        fh = path.open("rb"); gz = gzip.GzipFile(fileobj=fh)
        return TextIOWrapper(gz, encoding="utf-8", errors="replace")
    if kind == "zip":
        import zipfile
        zf = zipfile.ZipFile(path, "r")
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not members: raise RuntimeError("ZIP has no CSV members")
        prefer = [m for m in members if "sec_bhavdata_full" in m.lower()]
        name = (prefer[0] if prefer else members[0])
        fh = zf.open(name, "r")
        return TextIOWrapper(fh, encoding="utf-8", errors="replace")
    raise ValueError(f"Unknown kind: {kind}")

def normalize_header(h: str) -> str:
    h = h.strip().upper().replace(" ", "").replace("_","")
    mapping = {
        "SYMBOL":"SYMBOL","SERIES":"SERIES","DATE1":"DATE1",
        "OPEN":"OPEN","HIGH":"HIGH","LOW":"LOW","CLOSE":"CLOSE",
        "PREVCLOSE":"PREVCLOSE","LASTPRICE":"LASTPRICE","AVGPRICE":"AVGPRICE",
        "TTLTRDQNTY":"TTLTRDQNTY","TTLTRDQTY":"TTLTRDQNTY","TOTTRDQTY":"TTLTRDQNTY","TTLTRDQNY":"TTLTRDQNTY",
        "TURNOVERLACS":"TURNOVERLACS","NOOFTRADES":"NOOFTRADES",
        "DELIVQTY":"DELIVQTY","DELIVPER":"DELIVPER",
        "ISIN":"ISIN","ISINCODE":"ISIN",
    }
    return mapping.get(h, h)

def iter_csv_rows(path: pathlib.Path, kind: str):
    with open_text_from_bytes(path, kind) as text:
        first = text.read(1)
        if first != "\ufeff": text.seek(0)
        header_line = text.readline()
        if not header_line: return
        raw_headers = [h for h in next(csv.reader([header_line]))]
        norm_headers = [normalize_header(h) for h in raw_headers]
        reader = csv.reader(text)
        for row in reader:
            if not row:
                continue
            if len(row) != len(norm_headers):
                row = (row + [None]*len(norm_headers))[:len(norm_headers)]
            yield dict(zip(norm_headers, row))

# ----------------------- DB layer -----------------------
def pg_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"), connect_timeout=10,
    )

DDL_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {EOD_SCHEMA};

CREATE TABLE IF NOT EXISTS {EOD_SCHEMA}.nse_sec_bhav_full (
  trade_date      date   NOT NULL,
  symbol          text   NOT NULL,
  series          text   NOT NULL,
  open_price      numeric,
  high_price      numeric,
  low_price       numeric,
  close_price     numeric,
  prev_close      numeric,
  ttl_trd_qty     bigint,
  ttl_trd_qnty    bigint,
  no_of_trades    bigint,
  turnover_lacs   numeric,
  last_price      numeric,
  avg_price       numeric,
  deliv_qty       bigint,
  deliv_per       numeric,
  trade_day       date,
  isin            text,
  raw             jsonb,
  CONSTRAINT nse_sec_bhav_full_pk PRIMARY KEY (trade_date, symbol, series)
);

ALTER TABLE {EOD_SCHEMA}.nse_sec_bhav_full
  ADD COLUMN IF NOT EXISTS last_price      numeric,
  ADD COLUMN IF NOT EXISTS avg_price       numeric,
  ADD COLUMN IF NOT EXISTS deliv_qty       bigint,
  ADD COLUMN IF NOT EXISTS deliv_per       numeric,
  ADD COLUMN IF NOT EXISTS trade_day       date,
  ADD COLUMN IF NOT EXISTS ttl_trd_qty     bigint,
  ADD COLUMN IF NOT EXISTS ttl_trd_qnty    bigint,
  ADD COLUMN IF NOT EXISTS turnover_lacs   numeric,
  ADD COLUMN IF NOT EXISTS no_of_trades    bigint;

CREATE INDEX IF NOT EXISTS ix_bhav_trade_date ON {EOD_SCHEMA}.nse_sec_bhav_full (trade_date);
CREATE INDEX IF NOT EXISTS ix_bhav_symbol     ON {EOD_SCHEMA}.nse_sec_bhav_full (symbol);
CREATE INDEX IF NOT EXISTS ix_bhav_sym_day    ON {EOD_SCHEMA}.nse_sec_bhav_full (symbol, trade_date DESC);
"""

NUM_FIELDS = {
    "OPEN"        : "open_price",
    "HIGH"        : "high_price",
    "LOW"         : "low_price",
    "CLOSE"       : "close_price",
    "PREVCLOSE"   : "prev_close",
    "LASTPRICE"   : "last_price",
    "AVGPRICE"    : "avg_price",
    "TTLTRDQNTY"  : "ttl_trd_qty",
    "TURNOVERLACS": "turnover_lacs",
    "NOOFTRADES"  : "no_of_trades",
    "DELIVQTY"    : "deliv_qty",
    "DELIVPER"    : "deliv_per",
}

def to_num(x: Optional[str], as_int=False):
    if x is None: return None
    s = str(x).strip()
    if not s or s.upper()=="NA": return None
    s = s.replace(",","")
    try:
        return int(float(s)) if as_int else float(s)
    except:
        return None

def parse_date1(val: Optional[str]) -> Optional[dt.date]:
    if not val: return None
    s = str(val).strip()
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%d/%m/%Y"):
        try: return dt.datetime.strptime(s, fmt).date()
        except Exception: pass
    return None

def transform_row(row: dict, trade_date: dt.date) -> Optional[dict]:
    # SYMBOL must exist
    sym = (row.get("SYMBOL") or "").strip().upper()
    if not sym:
        return None

    # SERIES is now REQUIRED. Never default to EQ.
    s_raw = row.get("SERIES")
    if not s_raw:
        return None
    series = s_raw.strip().upper()

    out = {
        "trade_date": trade_date,
        "symbol": sym,
        "series": series,

        "open_price": None, "high_price": None, "low_price": None, "close_price": None,
        "prev_close": None, "last_price": None, "avg_price": None,
        "ttl_trd_qty": None, "ttl_trd_qnty": None,
        "turnover_lacs": None, "no_of_trades": None,
        "deliv_qty": None, "deliv_per": None,
        "trade_day": parse_date1(row.get("DATE1")),
        "isin": (row.get("ISIN") or "").strip() or None,
        "raw": json.dumps(row, ensure_ascii=False),
    }
    for src, dst in NUM_FIELDS.items():
        if src in row:
            as_int = dst in ("ttl_trd_qty","no_of_trades","deliv_qty")
            out[dst] = to_num(row[src], as_int=as_int)

    # legacy mirror for compatibility
    out["ttl_trd_qnty"] = out["ttl_trd_qty"]
    return out

def etl_into_db(trade_date: dt.date, rows: List[dict], skip_ddl: bool=False, truncate_day: bool=False) -> int:
    if not rows:
        log("No rows to ETL"); return 0
    with pg_conn() as conn:
        conn.autocommit = False; cur = conn.cursor()
        if not skip_ddl: cur.execute(DDL_SQL)
        if truncate_day:
            cur.execute(f"DELETE FROM {EOD_SCHEMA}.nse_sec_bhav_full WHERE trade_date=%s", (trade_date,))
        cur.execute(f"""
            CREATE TEMP TABLE tmp_bhav (
              trade_date date, symbol text, series text,
              open_price numeric, high_price numeric, low_price numeric, close_price numeric,
              prev_close numeric, last_price numeric, avg_price numeric,
              ttl_trd_qty bigint, ttl_trd_qnty bigint, turnover_lacs numeric, no_of_trades bigint,
              deliv_qty bigint, deliv_per numeric, trade_day date,
              isin text, raw jsonb
            ) ON COMMIT DROP;
        """)
        buf = StringIO(); w = csv.writer(buf, lineterminator="\n")
        for r in rows:
            w.writerow([
                r["trade_date"], r["symbol"], r["series"],
                r["open_price"], r["high_price"], r["low_price"], r["close_price"],
                r["prev_close"], r["last_price"], r["avg_price"],
                r["ttl_trd_qty"], r["ttl_trd_qnty"], r["turnover_lacs"], r["no_of_trades"],
                r["deliv_qty"], r["deliv_per"], r["trade_day"],
                r["isin"], r["raw"]
            ])
        buf.seek(0)
        cur.copy_expert(
          "COPY tmp_bhav(trade_date,symbol,series,open_price,high_price,low_price,close_price,prev_close,last_price,avg_price,ttl_trd_qty,ttl_trd_qnty,turnover_lacs,no_of_trades,deliv_qty,deliv_per,trade_day,isin,raw) FROM STDIN WITH CSV",
          buf,
        )
        cur.execute(f"""
            WITH d AS (
              SELECT DISTINCT ON (trade_date, UPPER(TRIM(symbol)), UPPER(TRIM(series)))
                     trade_date, UPPER(TRIM(symbol)) AS symbol, UPPER(TRIM(series)) AS series,
                     open_price, high_price, low_price, close_price, prev_close,
                     last_price, avg_price, ttl_trd_qty, ttl_trd_qnty,
                     turnover_lacs, no_of_trades, deliv_qty, deliv_per, trade_day, isin, raw
              FROM tmp_bhav
              WHERE symbol IS NOT NULL AND symbol <> ''
              ORDER BY trade_date, UPPER(TRIM(symbol)), UPPER(TRIM(series)),
                       COALESCE(no_of_trades,0) DESC, COALESCE(ttl_trd_qty,0) DESC
            )
            INSERT INTO {EOD_SCHEMA}.nse_sec_bhav_full AS t (
              trade_date, symbol, series,
              open_price, high_price, low_price, close_price, prev_close,
              last_price, avg_price, ttl_trd_qty, ttl_trd_qnty,
              turnover_lacs, no_of_trades, deliv_qty, deliv_per, trade_day, isin, raw
            )
            SELECT
              trade_date, symbol, series,
              open_price, high_price, low_price, close_price, prev_close,
              last_price, avg_price, ttl_trd_qty, ttl_trd_qnty,
              turnover_lacs, no_of_trades, deliv_qty, deliv_per, trade_day, isin, raw
            FROM d
            ON CONFLICT (trade_date, symbol, series) DO UPDATE SET
              open_price     = EXCLUDED.open_price,
              high_price     = EXCLUDED.high_price,
              low_price      = EXCLUDED.low_price,
              close_price    = EXCLUDED.close_price,
              prev_close     = EXCLUDED.prev_close,
              last_price     = EXCLUDED.last_price,
              avg_price      = EXCLUDED.avg_price,
              ttl_trd_qty    = EXCLUDED.ttl_trd_qty,
              ttl_trd_qnty   = EXCLUDED.ttl_trd_qnty,
              turnover_lacs  = EXCLUDED.turnover_lacs,
              no_of_trades   = EXCLUDED.no_of_trades,
              deliv_qty      = EXCLUDED.deliv_qty,
              deliv_per      = EXCLUDED.deliv_per,
              trade_day      = COALESCE(EXCLUDED.trade_day, t.trade_day),
              isin           = COALESCE(EXCLUDED.isin, t.isin),
              raw            = EXCLUDED.raw;
        """)
        affected = cur.rowcount
        conn.commit()
        return affected

# ----------------------- runner -----------------------
def run(trade_date: dt.date, check_only: bool, fetch_only: bool, etl_only: bool,
        skip_ddl: bool, allow_tiny: bool, min_bytes: int, retries: int, timeout: int,
        outdir: Optional[str], series_allow: Optional[List[str]],
        archives_json_url: Optional[str], explicit_file: Optional[str],
        overwrite: bool, flatten_archive: bool, truncate_day: bool) -> None:
    log(f"Trade date = {trade_date.isoformat()}")
    outdir_final = ensure_outdir(pathlib.Path(outdir) if outdir else None)
    out_path: Optional[pathlib.Path] = None; kind: Optional[str] = None

    if explicit_file:
        out_path = pathlib.Path(explicit_file)
        if not out_path.exists(): raise FileNotFoundError(f"--file not found: {out_path}")
        ext = out_path.suffix.lower()
        kind = "csv" if ext == ".csv" else ("csv.gz" if ext in (".gz",".gzip") else ("zip" if ext == ".zip" else "csv"))
    elif not etl_only or archives_json_url:
        out_path, kind = fetch_file(trade_date, timeout, retries, min_bytes, allow_tiny,
                                    outdir_final, archives_json_url, overwrite, flatten_archive)
        if check_only or fetch_only: return
    else:
        files = day_glob(outdir_final, trade_date)
        if not files: raise FileNotFoundError("No sec_bhavdata_full file for the day in --outdir; use --file")
        csvs = [p for p in files if p.suffix.lower() == ".csv"]
        out_path = csvs[0] if csvs else files[0]
        data = out_path.read_bytes()
        kind = "csv" if out_path.suffix.lower()==".csv" else detect_kind(data)

    keep_all = (series_allow and series_allow == ["*"])
    kept, total, rows = 0, 0, []
    for row in iter_csv_rows(out_path, kind):
        total += 1
        tr = transform_row(row, trade_date)
        if not tr: continue
        if not keep_all and series_allow and tr["series"] not in series_allow:
            continue
        rows.append(tr); kept += 1
    log(f"Parsed rows: {total} (kept: {kept})")

    affected = etl_into_db(trade_date, rows, skip_ddl=skip_ddl, truncate_day=truncate_day)
    log(f"Upserted rows: {affected}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Fetch + ETL: sec_bhavdata_full (Security-wise Bhavcopy Full)")
    ap.add_argument("trade_date", nargs="?", help="YYYY-MM-DD (default: yesterday IST)")
    ap.add_argument("--check-only", action="store_true")
    ap.add_argument("--fetch-only", action="store_true")
    ap.add_argument("--etl-only", action="store_true")
    ap.add_argument("--skip-ddl", action="store_true")
    ap.add_argument("--allow-tiny", action="store_true")
    ap.add_argument("--min-bytes", type=int, default=200)
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--outdir", type=str, default=None)
    ap.add_argument("--series", type=str, default="EQ,SM", help="Comma list or '*' for all.")
    ap.add_argument("--archives-json", type=str, default=None)
    ap.add_argument("--file", type=str, default=None)
    ap.add_argument("--overwrite", action="store_true", default=True)
    ap.add_argument("--no-overwrite", dest="overwrite", action="store_false")
    ap.add_argument("--flatten-archive", action="store_true", default=True)
    ap.add_argument("--no-flatten-archive", dest="flatten_archive", action="store_false")
    ap.add_argument("--truncate-day", action="store_true", help="Delete day in DB before ETL.")
    args = ap.parse_args()

    td = dt.datetime.strptime(args.trade_date, "%Y-%m-%d").date() if args.trade_date else (dt.datetime.now()-dt.timedelta(days=1)).date()
    series_allow = [s.strip().upper() for s in args.series.split(",")] if args.series else ["EQ","SM"]
    try:
        run(
            trade_date=td,
            check_only=args.check_only, fetch_only=args.fetch_only, etl_only=args.etl_only,
            skip_ddl=args.skip_ddl, allow_tiny=args.allow_tiny, min_bytes=args.min_bytes,
            retries=args.retries, timeout=args.timeout, outdir=args.outdir,
            series_allow=series_allow, archives_json_url=args.archives_json, explicit_file=args.file,
            overwrite=args.overwrite, flatten_archive=args.flatten_archive, truncate_day=args.truncate_day,
        )
    except Exception as e:
        log(f"ERROR: {e}"); sys.exit(1)
