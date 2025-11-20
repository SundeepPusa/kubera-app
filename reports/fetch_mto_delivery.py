# -*- coding: utf-8 -*-
"""
FILE: reports/fetch_mto_delivery.py

Fetch + ETL NSE MTO (deliverable data) for a trade date—or a whole date range.

Range
python -m reports.fetch_mto_delivery \
  --date-range 2020-11-05 2025-11-04 \
  --skip-weekends \
  --series ALL \
  --retries 1 \
  --timeout 15

Single Day
python -m reports.fetch_mto_delivery 2025-10-31 --series ALL

Usage (from repo root):

  # A) Probe likely archive URLs (prints, no fetch)
  python -m reports.fetch_mto_delivery 2025-10-31 --list-urls

  # B) Auto-fetch from known archive patterns (tries many names), then ETL
  python -m reports.fetch_mto_delivery 2025-10-31

  # C) Use the exact All-Reports XHR JSON (DevTools request with 'reports?archives=...')
  python -m reports.fetch_mto_delivery 2025-10-31 \
    --archives-json "https://www.nseindia.com/api/reports?archives=%5B%7B%22name%22%3A...%7D%5D&date=31-Oct-2025&type=equities&mode=single"

  # D) ETL-only from a saved file
  python -m reports.fetch_mto_delivery 2025-10-31 --etl-only \
    --file "C:\\Users\\Limak\\Documents\\osa-final\\data\\nse\\2025\\10\\31\\MTO_31102025.DAT" \
    --series EQ,SM

  # E) Backfill a range (inclusive), skip weekends, continue on errors by default
  python -m reports.fetch_mto_delivery --date-range 2020-11-05 2025-11-04 --skip-weekends --series ALL --retries 1 --timeout 15

Notes
-----
- Env: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD; EOD_SCHEMA (default "eod")
- Accepts DAT (| or ,), CSV, CSV.GZ, or ZIP (containing DAT/CSV)
- We PREFER .DAT; if the All-Reports endpoint streams a file directly, we save it and short-circuit.
"""

import os
import sys
import csv
import gzip
import json
import time
import argparse
import pathlib
import datetime as dt
from io import TextIOWrapper, StringIO, BytesIO
from typing import List, Tuple, Optional

import requests
import psycopg2
from dotenv import load_dotenv


# ---------------- Env / paths ----------------
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "nse"
for p in (BASE_DIR / ".env", BASE_DIR / ".ENV"):
    if p.exists():
        load_dotenv(p.as_posix()); break

EOD_SCHEMA = os.getenv("EOD_SCHEMA", "eod")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36",
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/",
}

def log(msg: str) -> None:
    print(f"[mto_delivery] {dt.datetime.now():%Y-%m-%d %H:%M:%S} | {msg}")

def ensure_outdir(trade_date: dt.date, outdir: Optional[pathlib.Path]=None) -> pathlib.Path:
    base = pathlib.Path(outdir) if outdir else DATA_DIR
    final = base / f"{trade_date.year}" / f"{trade_date:%m}" / f"{trade_date:%d}"
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

def http_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://www.nseindia.com/all-reports",
        "Origin": "https://www.nseindia.com",
    })
    cookie_env = os.getenv("NSE_COOKIES", "").strip()
    if cookie_env:
        s.headers["Cookie"] = cookie_env
    try:
        s.get("https://www.nseindia.com/", timeout=12)
        s.get("https://www.nseindia.com/all-reports", timeout=12)
    except Exception:
        pass
    return s


# ---------------- URL candidates & helpers ----------------
def ddmmyyyy(d: dt.date) -> str: return d.strftime("%d%m%Y")
def yyyymmdd(d: dt.date) -> str: return d.strftime("%Y%m%d")

def candidate_urls(trade_date: dt.date) -> List[str]:
    bases = [
        "https://archives.nseindia.com/content/equities/mto",
        "https://archives.nseindia.com/content/mto",
        "https://www.nseindia.com/content/equities/mto",
        "https://www.nseindia.com/content/mto",
    ]
    names = [
        # DAT first
        f"MTO_{ddmmyyyy(trade_date)}.DAT",
        f"MTO_{yyyymmdd(trade_date)}.DAT",
        f"MTO_{ddmmyyyy(trade_date)}.DAT.zip",
        f"MTO_{yyyymmdd(trade_date)}.DAT.zip",
        f"mto_{ddmmyyyy(trade_date)}.dat",
        f"mto_{yyyymmdd(trade_date)}.dat",
        f"mto_{ddmmyyyy(trade_date)}.dat.zip",
        f"mto_{yyyymmdd(trade_date)}.dat.zip",
        # CSV after
        f"mto_{ddmmyyyy(trade_date)}.csv",
        f"mto_{yyyymmdd(trade_date)}.csv",
        f"mto_{ddmmyyyy(trade_date)}.csv.zip",
        f"mto_{yyyymmdd(trade_date)}.csv.zip",
    ]
    out = []
    for b in bases:
        out.extend([f"{b}/{n}" for n in names])
    return out

def build_urls_from_template(base_url: str, name_template: str, trade_date: dt.date) -> List[str]:
    repl = {
        "YYYY": trade_date.strftime("%Y"),
        "YY": trade_date.strftime("%y"),
        "MM": trade_date.strftime("%m"),
        "DD": trade_date.strftime("%d"),
        "DDMMYYYY": ddmmyyyy(trade_date),
        "YYYYMMDD": yyyymmdd(trade_date),
    }
    name = name_template
    for k, v in repl.items():
        name = name.replace(f"{{{k}}}", v)
    return [f"{base_url.rstrip('/')}/{name}"]


# ---------------- Content validation ----------------
def sniff_kind(data: bytes, url: str, content_type: Optional[str]) -> Optional[str]:
    if content_type:
        ct = content_type.lower()
        if "text/html" in ct or "javascript" in ct or "text/javascript" in ct:
            return None
    if data[:2] == b"\x1f\x8b":  # gz
        return "csv.gz"
    if data[:4] == b"PK\x03\x04":  # zip
        return "zip"
    low = url.lower()
    if low.endswith(".dat"): return "dat"
    if low.endswith(".csv"): return "csv"
    head = data[:4096].decode("utf-8", "ignore").lower()
    if "security wise delivery position" in head or "record type" in head:
        return "dat"
    if "name of security" in head or "symbol" in head:
        return "csv"
    return None

def validate_mto_bytes(data: bytes) -> bool:
    if not data or len(data) < 2000:
        return False
    head = data[:4096].decode("utf-8", "ignore").lower()
    return ("security wise delivery position" in head or "name of security" in head
            or "deliverable" in head or "record type" in head)


# ---------------- Fetchers ----------------
def http_get_any(urls: List[str], timeout: int, retries: int) -> Optional[Tuple[bytes, str, str]]:
    s = http_session()
    for url in urls:
        for i in range(1, retries+1):
            try:
                r = s.get(url, timeout=timeout, allow_redirects=True)
                ct = r.headers.get("Content-Type", "")
                if r.status_code == 200 and r.content:
                    kind = sniff_kind(r.content, url, ct)
                    if kind and validate_mto_bytes(r.content):
                        log(f"OK 200 {url} (bytes={len(r.content)})")
                        return r.content, kind, url
                    else:
                        log(f"Discard non-MTO (ct={ct or 'n/a'}) {url} (bytes={len(r.content)})")
                else:
                    log(f"HTTP {r.status_code} {url} (try {i}/{retries})")
            except Exception as e:
                log(f"GET failed {url} (try {i}/{retries}): {e}")
            time.sleep(min(2*i, 8))
    return None


def fetch_from_archives_json(
    archives_json_url: str,
    timeout: int,
    retries: int,
    outdir: Optional[pathlib.Path] = None,
    trade_date: Optional[dt.date] = None,
) -> Tuple[Optional[pathlib.Path], Optional[str], List[str]]:
    """
    Try the 'api/reports?archives=...' endpoint.

    Returns:
      (saved_path, kind, urls)
      - If the endpoint streams the file (octet-stream/text), save it and return (path, kind, []).
      - If the endpoint returns JSON with links, return (None, None, [urls...]).
    """
    s = http_session()
    last_err = None

    def _sniff_kind_from_bytes(b: bytes) -> str:
        head = b[:256].decode("latin-1", errors="ignore").lower()
        if "security wise delivery" in head or "record type" in head:
            return "dat"
        if "symbol" in head and "deliverable" in head and "," in head:
            return "csv"
        return "dat"

    def _pick_filename(r: requests.Response, kind_guess: str) -> str:
        cd = r.headers.get("Content-Disposition") or r.headers.get("content-disposition") or ""
        fname = None
        if "filename=" in cd:
            fname = cd.split("filename=", 1)[1].strip('"; ')
        if not fname:
            base = f"MTO_{ddmmyyyy(trade_date or dt.date.today())}"
            ext = ".DAT" if kind_guess == "dat" else ".csv"
            fname = base + ext
        return fname

    for i in range(1, retries + 1):
        try:
            r = s.get(
                archives_json_url,
                timeout=timeout,
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.nseindia.com/all-reports",
                    "X-Requested-With": "XMLHttpRequest",
                },
                stream=True,
            )
            ctype = (r.headers.get("Content-Type") or "").lower()

            # A) Endpoint streams the FILE directly → save & return
            if "application/octet-stream" in ctype or "text/plain" in ctype:
                data = r.content or b""
                if not data:
                    raise ValueError("empty octet-stream payload")
                kind = _sniff_kind_from_bytes(data)
                out = ensure_outdir(trade_date or dt.date.today(), outdir)
                fname = _pick_filename(r, kind)
                dest = out / fname
                dest.write_bytes(data)              # overwrites if present
                log(f"archives delivered file directly → {dest} ({len(data)} bytes)")
                return dest, ("csv" if fname.lower().endswith(".csv") else "dat"), []

            # B) JSON with one or more links → return URL list
            if "application/json" in ctype:
                data = r.json()
                urls: List[str] = []

                def _abs(p: str) -> str:
                    p = (p or "").strip()
                    if p.startswith("http://") or p.startswith("https://"):
                        return p
                    host = "https://archives.nseindia.com" if "/content/" in p else "https://www.nseindia.com"
                    if not p.startswith("/"):
                        p = "/" + p
                    return host + p

                if isinstance(data, dict):
                    if "data" in data:
                        d = data["data"]
                        if isinstance(d, dict) and d.get("downloadLink"):
                            urls.append(_abs(d["downloadLink"]))
                        elif isinstance(d, list):
                            for it in d:
                                if isinstance(it, dict):
                                    if it.get("downloadLink"):
                                        urls.append(_abs(it["downloadLink"]))
                                    elif it.get("path"):
                                        urls.append(_abs(it["path"]))
                    elif data.get("downloadLink"):
                        urls.append(_abs(data["downloadLink"]))

                if urls:
                    log(f"archives resolved {len(urls)} file url(s)")
                    return None, None, urls

                preview_keys = list(data.keys()) if isinstance(data, dict) else type(data)
                log(f"archives JSON had no links; keys={preview_keys}")
                raise ValueError("archives JSON missing file links")

            # Neither JSON nor octet-stream → show a preview, then retry
            preview = (r.text or "")[:200].replace("\n", " ")
            log(f"archives unknown content-type ctype={ctype} preview={preview!r}")
            raise ValueError(f"unsupported content-type {ctype}")

        except Exception as e:
            last_err = e
            log(f"archives fetch failed (try {i}/{retries}): {e}")
            time.sleep(min(2 * i, 8))

    raise RuntimeError(f"Failed reading archives endpoint: {last_err}")

# ---------------- Save helpers ----------------
def save_bytes(trade_date: dt.date, data: bytes, kind: str, outdir: Optional[pathlib.Path]) -> pathlib.Path:
    out = ensure_outdir(trade_date, outdir)
    ext = {"csv":"csv", "dat":"DAT", "csv.gz":"csv.gz", "zip":"zip"}[kind]
    # deterministic, overwriting on re-run:
    dest = out / f"MTO_{ddmmyyyy(trade_date)}.{ext}"
    dest.write_bytes(data)  # overwrites if present
    log(f"Saved → {dest} (bytes={len(data)})")
    return dest


# ---------------- Parsing ----------------
def normalize_header(h: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in h).strip("_")

def parse_intish(s: Optional[str]) -> Optional[int]:
    if s is None: return None
    t = str(s).strip()
    if not t or t.upper() == "NA": return None
    t = t.replace(",", "")
    try:
        return int(float(t))
    except Exception:
        return None

def parse_percent(s: Optional[str]) -> Optional[float]:
    if s is None: return None
    t = str(s).strip().replace("%", "")
    if not t: return None
    t = t.replace(",", "")
    try:
        return float(t)
    except Exception:
        return None

TRADED_SYNS = {
    "quantity_traded","traded_qty","traded_quantity","total_traded_qty","qty_traded",
    "total_traded_quantity","total_traded_qnty","traded_quantity_lakh",
}
DELIV_SYNS = {
    "deliverable_qty","deliverable_quantity","deliverable_quantity_gross",
    "deliverable_qty_gross","deliverable_qnty",
    "deliverable_quantity_gross_across_client_level",
}
PERC_SYNS = {
    "of_deliverable_quantity_to_traded_quantity",
    "deli_qty_to_traded_qty","deliverable_to_traded_qty","deliv_per",
    "_deli_qty_to_traded_qty","deli_qty_to_traded_qty__","per_del_qty_to_traded_qty",
    "del_per","per_del_to_traded_qty",
}

def first_present(m: dict, keys: set) -> Optional[str]:
    for k in keys:
        if k in m and m[k] not in (None, ""):
            return m[k]
    return None

def map_mto_row(raw: dict) -> dict:
    m = {normalize_header(k): (v.strip() if isinstance(v, str) else v) for k, v in raw.items()}
    symbol = (m.get("symbol") or m.get("name_of_security") or "").upper()
    series = (m.get("series") or "").upper()
    traded = first_present(m, TRADED_SYNS)
    deli   = first_present(m, DELIV_SYNS)
    pct    = first_present(m, PERC_SYNS)
    return {
        "symbol": symbol,
        "series": series,  # default later to EQ
        "total_traded_qty": parse_intish(traded),
        "deliverable_qty": parse_intish(deli),
        "deliverable_pct": parse_percent(pct),
        "raw": m,
    }

def open_text_any(fb, encodings=("utf-8-sig", "cp1252", "latin-1")) -> TextIOWrapper:
    data = fb.read()
    for enc in encodings:
        try:
            return TextIOWrapper(BytesIO(data), encoding=enc, errors="replace")
        except Exception:
            continue
    return TextIOWrapper(BytesIO(data), encoding="utf-8", errors="replace")

def iter_csv(stream_or_path):
    if isinstance(stream_or_path, (str, pathlib.Path)):
        fh = open(stream_or_path, "rb"); close_me = True
    else:
        fh = stream_or_path; close_me = False
    try:
        text = open_text_any(fh)
        rdr = csv.DictReader(text)
        for r in rdr:
            if not r: continue
            sym = (r.get("SYMBOL") or r.get("Symbol") or r.get("Name of Security") or "").strip().upper()
            if sym in {"", "TOTAL"}: continue
            yield map_mto_row(r)
    finally:
        if close_me: fh.close()

def iter_dat(stream_or_path):
    """
    Tolerant DAT reader:
      - skips preamble until header with 'Record Type' and 'Name of Security'
      - detects delimiter (| or ,)
      - if one extra field after 'Name of Security', inject as 'series'
    """
    if isinstance(stream_or_path, (str, pathlib.Path)):
        fh = open(stream_or_path, "rb"); close_me = True
    else:
        fh = stream_or_path; close_me = False
    try:
        text = open_text_any(fh)

        header_line = None
        for line in text:
            probe = (line or "").strip()
            if not probe: continue
            norm = normalize_header(probe)
            if "record_type" in norm and "name_of_security" in norm:
                header_line = probe; break
        if not header_line: return

        delim = "|" if "|" in header_line else ","
        headers = [normalize_header(h) for h in header_line.split(delim)]

        for line in text:
            raw = line.strip()
            if not raw or raw.upper().startswith("TOTAL"): continue
            parts = [p.strip() for p in raw.split(delim)]
            if len(parts) == len(headers)+1 and "name_of_security" in headers:
                idx = headers.index("name_of_security")
                aug = headers[:]
                aug.insert(idx+1, "series")
                if len(parts) != len(aug): continue
                row = {aug[i]: parts[i] for i in range(len(parts))}
            elif len(parts) == len(headers):
                row = {headers[i]: parts[i] for i in range(len(headers))}
            else:
                continue
            yield map_mto_row(row)
    finally:
        if close_me: fh.close()

def iter_rows_from_zip(path: pathlib.Path):
    import zipfile
    with zipfile.ZipFile(path, "r") as zf:
        names = zf.namelist()
        members = sorted([n for n in names if n.lower().endswith(".dat")]) or \
                  sorted([n for n in names if n.lower().endswith(".csv")])
        if not members: raise RuntimeError("ZIP has no DAT/CSV members")
        name = members[0]
        with zf.open(name, "r") as fh:
            data = fh.read()
    if name.lower().endswith(".dat"):
        yield from iter_dat(BytesIO(data))
    else:
        yield from iter_csv(BytesIO(data))

def iter_file(path: pathlib.Path, kind: str):
    if kind == "zip":
        yield from iter_rows_from_zip(path)
    elif kind == "csv":
        with path.open("rb") as fh: yield from iter_csv(fh)
    elif kind == "csv.gz":
        with path.open("rb") as fh:
            gz = gzip.GzipFile(fileobj=fh); yield from iter_csv(gz)
    elif kind == "dat":
        with path.open("rb") as fh: yield from iter_dat(fh)
    else:
        raise ValueError(f"Unknown kind: {kind}")


# ---------------- DDL & ETL ----------------
DDL_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {EOD_SCHEMA};

CREATE TABLE IF NOT EXISTS {EOD_SCHEMA}.nse_delivery_eod (
  trade_date         date   NOT NULL,
  symbol             text   NOT NULL,
  series             text   NOT NULL DEFAULT 'EQ',
  total_traded_qty   bigint,
  deliverable_qty    bigint,
  deliverable_pct    numeric(8,4),
  raw                jsonb,
  CONSTRAINT nse_delivery_eod_pk PRIMARY KEY (trade_date, symbol, series)
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='{EOD_SCHEMA}' AND table_name='nse_delivery_eod' AND column_name='total_traded_qty')
  THEN ALTER TABLE {EOD_SCHEMA}.nse_delivery_eod ADD COLUMN total_traded_qty bigint; END IF;

  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='{EOD_SCHEMA}' AND table_name='nse_delivery_eod' AND column_name='deliverable_qty')
  THEN ALTER TABLE {EOD_SCHEMA}.nse_delivery_eod ADD COLUMN deliverable_qty bigint; END IF;

  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='{EOD_SCHEMA}' AND table_name='nse_delivery_eod' AND column_name='deliverable_pct')
  THEN ALTER TABLE {EOD_SCHEMA}.nse_delivery_eod ADD COLUMN deliverable_pct numeric(8,4); END IF;

  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='{EOD_SCHEMA}' AND table_name='nse_delivery_eod' AND column_name='raw')
  THEN ALTER TABLE {EOD_SCHEMA}.nse_delivery_eod ADD COLUMN raw jsonb; END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_mto_symbol    ON {EOD_SCHEMA}.nse_delivery_eod(symbol);
CREATE INDEX IF NOT EXISTS idx_mto_tradedate ON {EOD_SCHEMA}.nse_delivery_eod(trade_date);
"""

def etl_into_db(trade_date: dt.date, rows: List[dict], skip_ddl: bool=False) -> int:
    if not rows:
        log("No rows to ETL (empty MTO file or series filtered out)")
        return 0
    with pg_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        if not skip_ddl:
            cur.execute(DDL_SQL)

        cur.execute("""
            CREATE TEMP TABLE tmp_mto (
              trade_date         date,
              symbol             text,
              series             text,
              total_traded_qty   bigint,
              deliverable_qty    bigint,
              deliverable_pct    numeric(8,4),
              raw                jsonb
            ) ON COMMIT DROP;
        """)
        buf = StringIO()
        w = csv.writer(buf, lineterminator="\n")
        cnt = 0
        for r in rows:
            sym = (r.get("symbol") or "").strip().upper()
            if not sym: continue
            ser = (r.get("series") or "").strip().upper() or "EQ"
            w.writerow([
                trade_date, sym, ser,
                r.get("total_traded_qty"),
                r.get("deliverable_qty"),
                r.get("deliverable_pct"),
                json.dumps(r.get("raw", r), ensure_ascii=False),
            ])
            cnt += 1
        if cnt == 0:
            log("Nothing to copy into temp table after cleaning")
            return 0

        buf.seek(0)
        cur.copy_expert(
            "COPY tmp_mto(trade_date,symbol,series,total_traded_qty,deliverable_qty,deliverable_pct,raw) FROM STDIN WITH CSV",
            buf,
        )

        cur.execute(f"""
            WITH d AS (
              SELECT DISTINCT ON (trade_date, symbol, series)
                     trade_date,
                     UPPER(TRIM(symbol)) AS symbol,
                     TRIM(UPPER(COALESCE(series,'EQ'))) AS series,
                     total_traded_qty, deliverable_qty, deliverable_pct, raw
              FROM tmp_mto
              WHERE symbol IS NOT NULL AND symbol <> ''
              ORDER BY trade_date, symbol, series,
                       (deliverable_qty IS NOT NULL) DESC, COALESCE(deliverable_qty,0) DESC,
                       COALESCE(total_traded_qty,0) DESC
            )
            INSERT INTO {EOD_SCHEMA}.nse_delivery_eod AS t (
              trade_date, symbol, series, total_traded_qty, deliverable_qty, deliverable_pct, raw
            )
            SELECT trade_date, symbol, series, total_traded_qty, deliverable_qty, deliverable_pct, raw
            FROM d
            ON CONFLICT (trade_date, symbol, series) DO UPDATE SET
                total_traded_qty = COALESCE(EXCLUDED.total_traded_qty, t.total_traded_qty),
                deliverable_qty  = COALESCE(EXCLUDED.deliverable_qty,  t.deliverable_qty),
                deliverable_pct  = COALESCE(EXCLUDED.deliverable_pct,  t.deliverable_pct),
                raw              = EXCLUDED.raw;
        """)
        affected = cur.rowcount or 0
        conn.commit()
        return affected


# ---------------- Orchestration ----------------
def pick_saved_file(trade_date: dt.date, outdir: Optional[pathlib.Path]) -> Tuple[pathlib.Path, str]:
    out = ensure_outdir(trade_date, outdir)
    dd = ddmmyyyy(trade_date); ymd = yyyymmdd(trade_date)
    candidates = [
        # Prefer DAT first
        f"MTO_{dd}.DAT", f"MTO_{ymd}.DAT",
        f"mto_{dd}.dat", f"mto_{ymd}.dat",
        f"MTO_{dd}.DAT.zip", f"MTO_{ymd}.DAT.zip",
        # Then CSV
        f"mto_{trade_date:%Y-%m-%d}.csv",
        f"mto_{trade_date:%Y-%m-%d}.csv.gz",
        f"mto_{dd}.csv", f"mto_{ymd}.csv",
        f"mto_{trade_date:%Y-%m-%d}.zip",
        f"mto_{dd}.csv.zip", f"mto_{ymd}.csv.zip",
    ]
    for name in candidates:
        p = out / name
        if p.exists():
            low = p.name.lower()
            if   low.endswith(".dat"):    return p, "dat"
            elif low.endswith(".zip"):    return p, "zip"
            elif low.endswith(".csv.gz"): return p, "csv.gz"
            elif low.endswith(".csv"):    return p, "csv"
    # fallback: any file in dir (DAT preferred)
    for p in sorted(out.iterdir(), key=lambda x: x.name.lower()):
        if not p.is_file(): continue
        low = p.name.lower()
        if   low.endswith(".dat"):    return p, "dat"
        elif low.endswith(".zip"):    return p, "zip"
        elif low.endswith(".csv.gz"): return p, "csv.gz"
        elif low.endswith(".csv"):    return p, "csv"
    raise FileNotFoundError(f"No saved MTO file in {out} for ETL-only.")

def run(trade_date: dt.date,
        check_only: bool,
        fetch_only: bool,
        etl_only: bool,
        skip_ddl: bool,
        allow_tiny: bool,
        min_bytes: int,
        retries: int,
        timeout: int,
        outdir: Optional[str],
        series_allow: Optional[List[str]],
        file_path: Optional[str],
        list_urls: bool,
        base_url: Optional[str],
        name_template: Optional[str],
        urls_override: Optional[List[str]],
        archives_json: Optional[str]) -> None:

    log(f"Trade date = {trade_date.isoformat()}")

    if list_urls:
        urls = []
        if base_url and name_template:
            urls += build_urls_from_template(base_url, name_template, trade_date)
        urls += candidate_urls(trade_date)
        log("Candidate URL list (DAT first):")
        for u in urls:
            print(f"  - {u}")
        return

    saved: Optional[pathlib.Path] = None
    kind:  Optional[str] = None
    outp = pathlib.Path(outdir) if outdir else None

    # FETCH
    if not etl_only:
        # 1) archives_json may directly stream the file → if so, SHORT-CIRCUIT (no further fetching)
        if archives_json:
            saved_api, kind_api, urls_api = fetch_from_archives_json(
                archives_json,
                timeout,
                retries,
                outdir=outp,
                trade_date=trade_date
            )
            if saved_api:
                saved, kind = saved_api, kind_api
            else:
                # Not streamed; we’ll try these resolved links next
                urls = urls_api or []
                if base_url and name_template:
                    urls += build_urls_from_template(base_url, name_template, trade_date)
                if urls_override:
                    urls += urls_override
                urls += candidate_urls(trade_date)
                got = http_get_any(urls, timeout=timeout, retries=retries)
                if not got:
                    raise RuntimeError("All candidate MTO URLs failed or returned non-MTO content.")
                data, kind, used_url = got
                if not allow_tiny and len(data) < max(1, min_bytes):
                    raise RuntimeError(f"Downloaded file is too small ({len(data)} bytes); use --allow-tiny to bypass")
                saved = save_bytes(trade_date, data, kind, outp)
        else:
            # No archives_json → try direct URL list (template/override + candidates)
            urls: List[str] = []
            if base_url and name_template:
                urls += build_urls_from_template(base_url, name_template, trade_date)
            if urls_override:
                urls += urls_override
            urls += candidate_urls(trade_date)
            got = http_get_any(urls, timeout=timeout, retries=retries)
            if not got:
                raise RuntimeError("All candidate MTO URLs failed or returned non-MTO content.")
            data, kind, used_url = got
            if not allow_tiny and len(data) < max(1, min_bytes):
                raise RuntimeError(f"Downloaded file is too small ({len(data)} bytes); use --allow-tiny to bypass")
            saved = save_bytes(trade_date, data, kind, outp)

        if check_only or fetch_only:
            return
    else:
        # ETL-only path: choose an existing file or use --file
        if file_path:
            p = pathlib.Path(file_path)
            if not p.exists():
                raise FileNotFoundError(f"--file not found: {p}")
            low = p.name.lower()
            if   low.endswith(".dat"):    kind = "dat"
            elif low.endswith(".zip"):    kind = "zip"
            elif low.endswith(".csv.gz"): kind = "csv.gz"
            elif low.endswith(".csv"):    kind = "csv"
            else: raise ValueError(f"Unknown file type: {p.name}")
            saved = p
        else:
            saved, kind = pick_saved_file(trade_date, outp)

    # parse → series filter → ETL
    rows, total = [], 0
    for r in iter_file(saved, kind):
        total += 1
        sym = (r.get("symbol") or "").strip().upper()
        if not sym: continue
        ser = (r.get("series") or "").strip().upper() or "EQ"
        if series_allow and ser not in series_allow: continue
        rows.append(r)

    log(f"Parsed rows: {total} (valid after filter: {len(rows)})")
    if not (check_only or fetch_only):
        up = etl_into_db(trade_date, rows, skip_ddl=skip_ddl)
        log(f"Upserted rows: {up}")


# ---------------- CLI ----------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Fetch + ETL: NSE MTO (deliverables) — single day or a date range")
    ap.add_argument("trade_date", nargs="?", help="YYYY-MM-DD (single-day mode; default: yesterday local)")
    ap.add_argument("--date-range", nargs=2, metavar=("START", "END"),
                    help="Backfill range inclusive, YYYY-MM-DD YYYY-MM-DD")
    ap.add_argument("--skip-weekends", action="store_true",
                    help="When used with --date-range, skip Saturdays/Sundays")
    ap.add_argument("--fail-fast", action="store_true",
                    help="Stop on first error during --date-range (default: continue and log)")

    ap.add_argument("--check-only", action="store_true")
    ap.add_argument("--fetch-only", action="store_true")
    ap.add_argument("--etl-only", action="store_true")
    ap.add_argument("--skip-ddl", action="store_true")
    ap.add_argument("--allow-tiny", action="store_true")
    ap.add_argument("--min-bytes", type=int, default=200)
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--outdir", type=str, default=None)

    ap.add_argument("--series", type=str, default="EQ,SM",
                    help="Comma-separated series allow-list (use 'ALL' to disable filtering)")

    # URL helpers
    ap.add_argument("--list-urls", action="store_true", help="Print candidate URLs and exit")
    ap.add_argument("--base-url", type=str, default=None,
                    help="Override base URL, e.g. https://archives.nseindia.com/content/equities/mto")
    ap.add_argument("--name-template", type=str, default=None,
                    help="e.g. MTO_{DDMMYYYY}.DAT or mto_{YYYYMMDD}.csv")
    ap.add_argument("--urls", type=str, default=None,
                    help="Comma/space-separated explicit URLs to try (first valid wins)")
    ap.add_argument("--archives-json", type=str, default=None,
                    help="Full All-Reports XHR JSON URL (DevTools 'reports?archives=...' request) to resolve the real file link")

    # File override
    ap.add_argument("--file", type=str, default=None,
                    help="Explicit path to an MTO file (.DAT/.csv/.zip). Skips fetch.")

    args = ap.parse_args()

    # Range mode?
    if args.date_range:
        start = dt.datetime.strptime(args.date_range[0], "%Y-%m-%d").date()
        end   = dt.datetime.strptime(args.date_range[1], "%Y-%m-%d").date()
        if end < start:
            print("END must be >= START", file=sys.stderr)
            sys.exit(2)

        # Series parsing (ALL = no filter)
        if args.series and args.series.strip().upper() == "ALL":
            series_allow = None
        else:
            series_allow = [s.strip().upper() for s in args.series.split(",")] if args.series else None

        # optional urls override parsing
        urls_override = None
        if args.urls:
            sep = "," if "," in args.urls else " "
            urls_override = [u.strip() for u in args.urls.split(sep) if u.strip()]

        cur = start
        while cur <= end:
            if args.skip_weekends and cur.weekday() >= 5:
                log(f"Skip weekend {cur.isoformat()}")
                cur += dt.timedelta(days=1)
                continue
            try:
                run(trade_date=cur,
                    check_only=args.check_only,
                    fetch_only=args.fetch_only,
                    etl_only=args.etl_only,
                    skip_ddl=args.skip_ddl,
                    allow_tiny=args.allow_tiny,
                    min_bytes=args.min_bytes,
                    retries=args.retries,
                    timeout=args.timeout,
                    outdir=args.outdir,
                    series_allow=series_allow,
                    file_path=args.file,
                    list_urls=args.list_urls,
                    base_url=args.base_url,
                    name_template=args.name_template,
                    urls_override=urls_override,
                    archives_json=args.archives_json)
            except Exception as e:
                # "Holidays let error return": surface the error line, but continue unless fail-fast.
                log(f"ERROR {cur.isoformat()}: {e}")
                if args.fail_fast:
                    sys.exit(1)
            cur += dt.timedelta(days=1)
        sys.exit(0)

    # Single-day mode (default ≈ yesterday)
    if args.trade_date:
        td = dt.datetime.strptime(args.trade_date, "%Y-%m-%d").date()
    else:
        td = (dt.datetime.now() - dt.timedelta(days=1)).date()

    # Series parsing (ALL = no filter)
    if args.series and args.series.strip().upper() == "ALL":
        series_allow = None
    else:
        series_allow = [s.strip().upper() for s in args.series.split(",")] if args.series else None

    urls_override = None
    if args.urls:
        sep = "," if "," in args.urls else " "
        urls_override = [u.strip() for u in args.urls.split(sep) if u.strip()]

    try:
        run(trade_date=td,
            check_only=args.check_only,
            fetch_only=args.fetch_only,
            etl_only=args.etl_only,
            skip_ddl=args.skip_ddl,
            allow_tiny=args.allow_tiny,
            min_bytes=args.min_bytes,
            retries=args.retries,
            timeout=args.timeout,
            outdir=args.outdir,
            series_allow=series_allow,
            file_path=args.file,
            list_urls=args.list_urls,
            base_url=args.base_url,
            name_template=args.name_template,
            urls_override=urls_override,
            archives_json=args.archives_json)
    except Exception as e:
        log(f"ERROR: {e}")
        sys.exit(1)
