# -*- coding: utf-8 -*-
"""
FILE: reports/fetch_participants_oi.py

Fetch + ETL → {EOD_SCHEMA}.nse_participant_oi_daily
Source: NSE All Reports → Derivatives → Equity Derivatives → “F&O – Participant wise Open Interest (csv)”

Usage
-----
python -m reports.fetch_participants_oi 2025-10-31 --retries 2 --timeout 20
python -m reports.fetch_participants_oi 2025-10-31 \
  --archives-json "https://www.nseindia.com/api/reports?archives=%5B%7B%22name%22%3A%22F%26O%20-%20Participant%20wise%20Open%20Interest(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%5D&date=31-Oct-2025&type=equity&mode=single"
python -m reports.fetch_participants_oi 2025-10-31 --etl-only --file data/nse/2025/10/31/fao_participant_oi_31102025.csv
"""

import os
import csv
import gzip
import json
import time
import argparse
import pathlib
import datetime as dt
from io import BytesIO, StringIO, TextIOWrapper
from typing import List, Tuple, Optional

import requests
import psycopg2
from psycopg2.errors import FeatureNotSupported
from dotenv import load_dotenv

# -------------------- ENV / PATHS --------------------
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "nse"

for p in (BASE_DIR / ".env", BASE_DIR / ".ENV"):
    if p.exists():
        load_dotenv(p.as_posix())
        break

EOD_SCHEMA = (os.getenv("EOD_SCHEMA") or "eod").strip() or "eod"

def log(msg: str) -> None:
    print(f"[poi] {dt.datetime.now():%Y-%m-%d %H:%M:%S} | {msg}")

def ensure_outdir(d: dt.date, outdir: Optional[pathlib.Path] = None) -> pathlib.Path:
    base = pathlib.Path(outdir) if outdir else DATA_DIR
    final = base / f"{d.year}" / f"{d:%m}" / f"{d:%d}"
    final.mkdir(parents=True, exist_ok=True)
    return final

def pg_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        connect_timeout=10,
    )

# -------------------- HTTP helpers --------------------
def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nseindia.com/all-reports",
        "Origin": "https://www.nseindia.com",
    })
    ck = (os.getenv("NSE_COOKIES") or "").strip()
    if ck:
        s.headers["Cookie"] = ck
    # warm session (ignore failures)
    for u in ("https://www.nseindia.com/", "https://www.nseindia.com/all-reports"):
        try: s.get(u, timeout=12)
        except Exception: pass
    return s

def ddmmyyyy(d: dt.date) -> str: return d.strftime("%d%m%Y")
def yyyymmdd(d: dt.date) -> str: return d.strftime("%Y%m%d")

def candidate_urls(d: dt.date) -> List[str]:
    bases = [
        "https://archives.nseindia.com/content/fo",
        "https://www.nseindia.com/content/fo",
    ]
    names = [
        f"participant_oi_{ddmmyyyy(d)}.csv",
        f"participant_oi_{yyyymmdd(d)}.csv",
        f"participant_oi_{ddmmyyyy(d)}.csv.gz",
        f"participant_oi_{yyyymmdd(d)}.csv.gz",
        f"participant_oi_{ddmmyyyy(d)}.zip",
        f"participant_oi_{yyyymmdd(d)}.zip",
        f"fao_participant_oi_{ddmmyyyy(d)}.csv",
        f"fao_participant_oi_{yyyymmdd(d)}.csv",
        f"fao_participant_oi_{ddmmyyyy(d)}.zip",
        f"fao_participant_oi_{yyyymmdd(d)}.zip",
    ]
    return [f"{b}/{n}" for b in bases for n in names]

def _abs(u: str) -> str:
    if u.startswith(("http://", "https://")):
        return u
    host = "https://archives.nseindia.com" if "/content/" in u else "https://www.nseindia.com"
    return host + (u if u.startswith("/") else "/" + u)

def fetch_from_archives_json(url: str, timeout: int, retries: int,
                             outdir: Optional[pathlib.Path], d: dt.date) -> Tuple[Optional[pathlib.Path], Optional[str], List[str]]:
    """NSE api/reports?archives=... — sometimes streams CSV directly; else returns JSON with download links."""
    s = http_session()
    for i in range(1, retries + 1):
        try:
            r = s.get(url, timeout=timeout, stream=True, headers={"X-Requested-With": "XMLHttpRequest"})
            ct = (r.headers.get("Content-Type") or "").lower()
            # direct CSV stream case
            if any(x in ct for x in ("text/csv", "application/octet-stream", "text/plain")) and r.content:
                dest = ensure_outdir(d, outdir) / f"fao_participant_oi_{ddmmyyyy(d)}.csv"
                if len(r.content) < 200:
                    raise RuntimeError(f"archives delivered tiny payload ({len(r.content)} bytes)")
                dest.write_bytes(r.content)
                log(f"archives delivered file directly → {dest} ({len(r.content)} bytes)")
                return dest, "csv", []
            urls: List[str] = []
            if "application/json" in ct:
                try:
                    j = r.json()
                except Exception:
                    j = None
                if isinstance(j, dict):
                    d1 = j.get("data")
                    if isinstance(d1, dict) and d1.get("downloadLink"): urls.append(_abs(d1["downloadLink"]))
                    if isinstance(d1, list):
                        for it in d1:
                            if isinstance(it, dict):
                                if it.get("downloadLink"): urls.append(_abs(it["downloadLink"]))
                                if it.get("path"):         urls.append(_abs(it["path"]))
                    rows = j.get("rows") or []
                    for it in rows:
                        if isinstance(it, dict) and it.get("path"): urls.append(_abs(it["path"]))
                    if j.get("downloadLink"): urls.append(_abs(j["downloadLink"]))
            if urls:
                seen = set()
                urls = [u for u in urls if not (u in seen or seen.add(u))]
                log(f"archives resolved {len(urls)} file url(s)")
                return None, None, urls
        except Exception as e:
            log(f"archives fetch failed (try {i}/{retries}): {e}")
            time.sleep(min(2 * i, 8))
    return None, None, []

def sniff_kind(data: bytes, url: str, content_type: Optional[str]) -> Optional[str]:
    if content_type and ("text/html" in content_type.lower() or "javascript" in content_type.lower()):
        return None
    if data[:2] == b"\x1f\x8b": return "csv.gz"
    if data[:4] == b"PK\x03\x04": return "zip"
    low = url.lower()
    if low.endswith(".csv"): return "csv"
    if low.endswith(".gz"):  return "csv.gz"
    if low.endswith(".zip"): return "zip"
    # crude sniff
    head = data[:512].decode("utf-8", "ignore").lower()
    if "participant wise open interest" in head or "client type" in head or "client" in head:
        return "csv"
    return None

def http_get_any(urls: List[str], timeout: int, retries: int):
    s = http_session()
    for u in urls:
        for i in range(1, retries + 1):
            try:
                r = s.get(u, timeout=timeout, allow_redirects=True)
                if r.status_code == 200 and r.content:
                    kind = sniff_kind(r.content, u, r.headers.get("Content-Type", ""))
                    if kind and len(r.content) >= 200:
                        log(f"OK 200 {u} (bytes={len(r.content)})")
                        return r.content, kind, u
                    else:
                        log(f"Discard non-POI {u} (ct={r.headers.get('Content-Type','n/a')}, bytes={len(r.content)})")
                else:
                    log(f"HTTP {r.status_code} {u} (try {i}/{retries})")
            except Exception as e:
                log(f"GET failed {u} (try {i}/{retries}): {e}")
            time.sleep(min(2 * i, 8))
    return None

def open_text_any(raw: bytes) -> TextIOWrapper:
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return TextIOWrapper(BytesIO(raw), encoding=enc, errors="replace")
        except Exception:
            continue
    return TextIOWrapper(BytesIO(raw), encoding="utf-8", errors="replace")

def iter_zip_csv(path: pathlib.Path) -> bytes:
    import zipfile
    with zipfile.ZipFile(path, "r") as zf:
        csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            raise RuntimeError("ZIP has no CSV member")
        return zf.read(sorted(csvs)[0])

# -------------------- CSV mapping --------------------
def norm(s: Optional[str]) -> str: return (s or "").strip()

def nint(x) -> Optional[int]:
    if x is None: return None
    t = str(x).replace(",", "").strip()
    if t in ("", "-", "NA", "NIL"): return None
    try: return int(float(t))
    except Exception: return None

def norm_header(h: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in (h or "")).strip("_")

CANON = {
    "client": "CLIENT", "clients": "CLIENT",
    "pro": "PRO", "proprietary": "PRO",
    "dii": "DII",
    "fii": "FII", "fia": "FII", "fpi": "FII", "fii/fpi": "FII",
}

COLS = {
    "idx_fut_long":       {"future index long", "idx fut long", "index futures long"},
    "idx_fut_short":      {"future index short", "idx fut short", "index futures short"},
    "stk_fut_long":       {"future stock long", "stock futures long"},
    "stk_fut_short":      {"future stock short", "stock futures short"},
    "idx_opt_call_long":  {"option index call long", "index options call long"},
    "idx_opt_put_long":   {"option index put long", "index options put long"},
    "idx_opt_call_short": {"option index call short", "index options call short"},
    "idx_opt_put_short":  {"option index put short", "index options put short"},
    "stk_opt_call_long":  {"option stock call long", "stock options call long"},
    "stk_opt_put_long":   {"option stock put long", "stock options put long"},
    "stk_opt_call_short": {"option stock call short", "stock options call short"},
    "stk_opt_put_short":  {"option stock put short", "stock options put short"},
    "total_long":         {"total long contracts", "total long"},
    "total_short":        {"total short contracts", "total short"},
}

def get_any(m: dict, names: set):
    for k in names:
        k2 = norm_header(k)
        if k2 in m and m[k2] != "":
            return m[k2]
    return None

def iter_csv_flexible(raw_bytes: bytes) -> List[dict]:
    """
    Detect header row (contains 'Client' / 'Client Type') and parse from there.
    Returns a list of dict rows with raw header keys (to be normalized later).
    """
    txt = open_text_any(raw_bytes).read()
    lines = [ln for ln in txt.splitlines() if ln.strip()]
    if not lines:
        return []
    header_idx = 0
    for i, ln in enumerate(lines[:5]):  # header usually within first 2 lines
        low = ln.lower()
        if ("client" in low and "future" in low) or ("client type" in low):
            header_idx = i
            break
    body = "\n".join(lines[header_idx:])
    return list(csv.DictReader(StringIO(body)))

def map_row(r: dict) -> Optional[dict]:
    m = {norm_header(k): norm(v) for k, v in r.items()}
    cat = m.get("client_type") or m.get("participant") or m.get("category") or ""
    if not cat:
        return None
    cat_up = cat.strip().upper()
    if cat_up == "TOTAL":
        return None
    key = CANON.get(cat.lower(), cat_up)
    if key not in {"CLIENT", "DII", "FII", "PRO"}:
        return None
    out = {"category": key}
    for dst, syns in COLS.items():
        out[dst] = nint(get_any(m, syns))
    out["raw"] = m
    return out

# -------------------- DDL / ETL --------------------
DDL = f"""
CREATE SCHEMA IF NOT EXISTS {EOD_SCHEMA};

CREATE TABLE IF NOT EXISTS {EOD_SCHEMA}.nse_participant_oi_daily(
  trade_date date NOT NULL,
  category   text NOT NULL,
  idx_fut_long bigint, idx_fut_short bigint,
  stk_fut_long bigint, stk_fut_short bigint,
  idx_opt_call_long bigint, idx_opt_put_long bigint,
  idx_opt_call_short bigint, idx_opt_put_short bigint,
  stk_opt_call_long bigint, stk_opt_put_long bigint,
  stk_opt_call_short bigint, stk_opt_put_short bigint,
  total_long bigint, total_short bigint,
  raw jsonb,
  PRIMARY KEY (trade_date, category)
);

CREATE INDEX IF NOT EXISTS idx_poi_date ON {EOD_SCHEMA}.nse_participant_oi_daily(trade_date);
"""

def etl(trade_date: dt.date, rows: List[dict], skip_ddl: bool = False, refresh_mv: bool = False) -> int:
    if not rows:
        log("No rows to ETL")
        return 0

    with pg_conn() as conn:
        cur = conn.cursor()
        if not skip_ddl:
            cur.execute(DDL)

        # temp table for fast COPY
        cur.execute("""
          CREATE TEMP TABLE tmp_poi(
            trade_date date, category text,
            idx_fut_long bigint, idx_fut_short bigint,
            stk_fut_long bigint, stk_fut_short bigint,
            idx_opt_call_long bigint, idx_opt_put_long bigint,
            idx_opt_call_short bigint, idx_opt_put_short bigint,
            stk_opt_call_long bigint, stk_opt_put_long bigint,
            stk_opt_call_short bigint, stk_opt_put_short bigint,
            total_long bigint, total_short bigint, raw jsonb
          ) ON COMMIT DROP;
        """)

        buf = StringIO()
        w = csv.writer(buf, lineterminator="\n")
        written = 0
        for r in rows:
            if not r or r.get("category") not in {"CLIENT", "DII", "FII", "PRO"}:
                continue
            w.writerow([
                trade_date, r["category"],
                r.get("idx_fut_long"), r.get("idx_fut_short"),
                r.get("stk_fut_long"), r.get("stk_fut_short"),
                r.get("idx_opt_call_long"), r.get("idx_opt_put_long"),
                r.get("idx_opt_call_short"), r.get("idx_opt_put_short"),
                r.get("stk_opt_call_long"), r.get("stk_opt_put_long"),
                r.get("stk_opt_call_short"), r.get("stk_opt_put_short"),
                r.get("total_long"), r.get("total_short"),
                json.dumps(r.get("raw", r), ensure_ascii=False),
            ])
            written += 1

        if written == 0:
            log("Nothing valid for COPY")
            return 0

        buf.seek(0)
        cur.copy_expert("COPY tmp_poi FROM STDIN WITH CSV", buf)

        # Normalize category to UPPER() on write so PK never clashes by case
        cur.execute(f"""
          INSERT INTO {EOD_SCHEMA}.nse_participant_oi_daily AS t(
            trade_date, category,
            idx_fut_long, idx_fut_short,
            stk_fut_long, stk_fut_short,
            idx_opt_call_long, idx_opt_put_long,
            idx_opt_call_short, idx_opt_put_short,
            stk_opt_call_long, stk_opt_put_long,
            stk_opt_call_short, stk_opt_put_short,
            total_long, total_short, raw
          )
          SELECT
            trade_date, UPPER(TRIM(category)),
            idx_fut_long, idx_fut_short,
            stk_fut_long, stk_fut_short,
            idx_opt_call_long, idx_opt_put_long,
            idx_opt_call_short, idx_opt_put_short,
            stk_opt_call_long, stk_opt_put_long,
            stk_opt_call_short, stk_opt_put_short,
            total_long, total_short, raw
          FROM tmp_poi
          ON CONFLICT (trade_date, category) DO UPDATE SET
            idx_fut_long       = COALESCE(EXCLUDED.idx_fut_long,       t.idx_fut_long),
            idx_fut_short      = COALESCE(EXCLUDED.idx_fut_short,      t.idx_fut_short),
            stk_fut_long       = COALESCE(EXCLUDED.stk_fut_long,       t.stk_fut_long),
            stk_fut_short      = COALESCE(EXCLUDED.stk_fut_short,      t.stk_fut_short),
            idx_opt_call_long  = COALESCE(EXCLUDED.idx_opt_call_long,  t.idx_opt_call_long),
            idx_opt_put_long   = COALESCE(EXCLUDED.idx_opt_put_long,   t.idx_opt_put_long),
            idx_opt_call_short = COALESCE(EXCLUDED.idx_opt_call_short, t.idx_opt_call_short),
            idx_opt_put_short  = COALESCE(EXCLUDED.idx_opt_put_short,  t.idx_opt_put_short),
            stk_opt_call_long  = COALESCE(EXCLUDED.stk_opt_call_long,  t.stk_opt_call_long),
            stk_opt_put_long   = COALESCE(EXCLUDED.stk_opt_put_long,   t.stk_opt_put_long),
            stk_opt_call_short = COALESCE(EXCLUDED.stk_opt_call_short, t.stk_opt_call_short),
            stk_opt_put_short  = COALESCE(EXCLUDED.stk_opt_put_short,  t.stk_opt_put_short),
            total_long         = COALESCE(EXCLUDED.total_long,         t.total_long),
            total_short        = COALESCE(EXCLUDED.total_short,        t.total_short),
            raw                = EXCLUDED.raw;
        """)

        affected = cur.rowcount or 0
        conn.commit()
        log(f"Upserted rows: {affected}")

        if refresh_mv:
            try:
                with conn.cursor() as c2:
                    c2.execute("SELECT eod.refresh_poi();")
                conn.commit()
                log("Refreshed MV/banner views via eod.refresh_poi()")
            except FeatureNotSupported:
                with conn.cursor() as c2:
                    c2.execute("REFRESH MATERIALIZED VIEW eod.mv_poi_last30;")
                conn.commit()
                log("Refreshed eod.mv_poi_last30 (blocking)")

        return affected

# -------------------- Orchestration --------------------
def save_bytes(d: dt.date, data: bytes, kind: str, outdir: Optional[pathlib.Path]) -> pathlib.Path:
    out = ensure_outdir(d, outdir)
    name = f"participants_oi_{d:%Y-%m-%d}." + {"csv": "csv", "csv.gz": "csv.gz", "zip": "zip"}[kind]
    p = out / name
    p.write_bytes(data)
    log(f"Saved → {p} (bytes={len(data)})")
    return p

def run_one_day(d: dt.date, args) -> None:
    log(f"Trade date = {d}")
    outp = pathlib.Path(args.outdir) if args.outdir else None

    saved: Optional[pathlib.Path] = None
    kind: Optional[str] = None
    urls: List[str] = []

    if not args.etl_only:
        if args.archives_json:
            saved_api, kind_api, urls = fetch_from_archives_json(args.archives_json, args.timeout, args.retries, outp, d)
            if saved_api:
                saved, kind = saved_api, kind_api

        if not saved:
            urls += candidate_urls(d)
            got = http_get_any(urls, args.timeout, args.retries)
            if not got:
                raise RuntimeError("All POI candidate URLs failed or returned non-POI content.")
            data, kind, _ = got
            saved = save_bytes(d, data, kind, outp)

        if args.check_only or args.fetch_only:
            return
    else:
        p = pathlib.Path(args.file) if args.file else None
        if not p or not p.exists():
            raise FileNotFoundError("--file missing or not found")
        low = p.name.lower()
        if   low.endswith(".csv"):    kind = "csv"
        elif low.endswith(".csv.gz"): kind = "csv.gz"
        elif low.endswith(".zip"):    kind = "zip"
        else: raise ValueError("Unknown --file type for ETL")
        saved = p

    # parse → map
    if kind == "zip":
        raw = iter_zip_csv(saved)
        rows = iter_csv_flexible(raw)
    elif kind == "csv":
        raw = saved.read_bytes()
        rows = iter_csv_flexible(raw)
    elif kind == "csv.gz":
        with saved.open("rb") as fh:
            raw = gzip.GzipFile(fileobj=fh).read()
        rows = iter_csv_flexible(raw)
    else:
        rows = []

    mapped: List[dict] = []
    for r in rows:
        m = map_row(r)
        if m:
            mapped.append(m)

    log(f"Parsed rows: {len(rows)} (valid after map: {len(mapped)})")

    if not (args.check_only or args.fetch_only):
        etl(d, mapped, skip_ddl=args.skip_ddl, refresh_mv=args.refresh_mv)

# -------------------- CLI --------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="F&O Participant-wise Open Interest (csv)")
    ap.add_argument("trade_date", help="YYYY-MM-DD")
    ap.add_argument("--archives-json", default=None, help="NSE api/reports?archives=... URL")
    ap.add_argument("--check-only", action="store_true", help="Only fetch/probe; do not ETL")
    ap.add_argument("--fetch-only", action="store_true", help="Download only; skip ETL")
    ap.add_argument("--etl-only", action="store_true", help="Only ETL from --file")
    ap.add_argument("--skip-ddl", action="store_true", help="Skip CREATE SCHEMA/TABLE/INDEX if lacking perms")
    ap.add_argument("--refresh-mv", action="store_true", help="Call eod.refresh_poi() after ETL")
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=25)
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--file", default=None, help="Local CSV/CSV.GZ/ZIP when using --etl-only")
    args = ap.parse_args()

    d = dt.datetime.strptime(args.trade_date, "%Y-%m-%d").date()
    run_one_day(d, args)
