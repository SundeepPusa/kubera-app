# -*- coding: utf-8 -*-
"""
START=$(date -d "5 years ago" +%F)
END=$(date -d "yesterday" +%F)

d="$START"
while [ "$(date -d "$d" +%F)" != "$(date -d "$END + 1 day" +%F)" ]; do
  wd=$(date -d "$d" +%u); if [ "$wd" -ge 6 ]; then d=$(date -d "$d + 1 day" +%F); continue; fi
  DDMONYYYY=$(date -d "$d" +%d-%b-%Y)
  echo ">>> Backfilling $d"

  python -m reports.fetch_participants_vol "$d" --retries 1 --timeout 12 \
    --archives-json "https://www.nseindia.com/api/reports?archives=%5B%7B%22name%22%3A%22F%26O%20-%20Participant%20wise%20Trading%20Volumes(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%5D&date=${DDMONYYYY}&type=derivatives&mode=single" \
  || true

  d=$(date -d "$d + 1 day" +%F)
done

psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY eod.mv_pvol_last30;"

FILE: reports/fetch_participants_vol.py

Fetch + ETL → {EOD_SCHEMA}.nse_participant_trading_vol_daily
Source: NSE All Reports → Derivatives → Equity Derivatives →
        “F&O – Participant wise Trading Volumes (csv)”

        
        # ---- set a 5-year window (adjust START if you want) ----
START=$(date -d "5 years ago" +%F)   # e.g., 2020-11-06 if today is 2025-11-06
END=$(date -d "yesterday" +%F)

# Optional: cookie header if NSE rate-limits you (paste once from your browser)
# export NSE_COOKIES='ak_bmsc=...; bm_sv=...; ...'

d="$START"
while [ "$(date -d "$d" +%F)" != "$(date -d "$END + 1 day" +%F)" ]; do
  # skip weekends
  wd=$(date -d "$d" +%u); if [ "$wd" -ge 6 ]; then d=$(date -d "$d + 1 day" +%F); continue; fi
  DDMONYYYY=$(date -d "$d" +%d-%b-%Y)

  echo ">>> Backfilling $d"

  # Try archives API (some days return direct CSV, others via a link the code can follow)
  python -m reports.fetch_participants_vol "$d" --retries 1 --timeout 12 \
    --archives-json "https://www.nseindia.com/api/reports?archives=%5B%7B%22name%22%3A%22F%26O%20-%20Participant%20wise%20Trading%20Volumes(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%5D&date=${DDMONYYYY}&type=derivatives&mode=single" \
  || python -m reports.fetch_participants_vol "$d" --retries 1 --timeout 12 \
    --archives-json "https://www.nseindia.com/api/reports?archives=%5B%7B%22name%22%3A%22F%26O%20-%20Participant%20wise%20Trading%20Volumes(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%5D&date=${DDMONYYYY}&type=equity&mode=single" \
  || true

  # If you already have local files for some days, you can force ETL-only:
  # python -m reports.fetch_participants_vol "$d" --etl-only || true

  d=$(date -d "$d + 1 day" +%F)
done

# Refresh MV once at the end
psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY eod.mv_pvol_last30;"

Usage
-----
python -m reports.fetch_participants_vol 2025-10-31 --retries 3 --timeout 25
python -m reports.fetch_participants_vol 2025-10-31 \
  --archives-json "https://www.nseindia.com/api/reports?archives=%5B%7B%22name%22%3A%22F%26O%20-%20Participant%20wise%20Trading%20Volumes(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%5D&date=31-Oct-2025&type=equity&mode=single"
python -m reports.fetch_participants_vol 2025-10-31 --etl-only \
  --file data/nse/2025/10/31/fao_participant_vol_31102025.csv
"""
from __future__ import annotations

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
from psycopg2 import sql
from psycopg2.errors import InsufficientPrivilege, DuplicateObject
from dotenv import load_dotenv

# -------------------- ENV / PATHS --------------------
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "nse"

# load .env or .ENV if present (project-root)
for p in (BASE_DIR / ".env", BASE_DIR / ".ENV"):
    if p.exists():
        load_dotenv(p.as_posix())
        break

EOD_SCHEMA = (os.getenv("EOD_SCHEMA") or "eod").strip() or "eod"

def log(msg: str) -> None:
    print(f"[pvol] {dt.datetime.now():%Y-%m-%d %H:%M:%S} | {msg}")

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
    # Warm-up (best-effort)
    for u in ("https://www.nseindia.com/", "https://www.nseindia.com/all-reports"):
        try:
            s.get(u, timeout=12)
        except Exception:
            pass
    return s

def ddmmyyyy(d: dt.date) -> str: return d.strftime("%d%m%Y")
def yyyymmdd(d: dt.date) -> str: return d.strftime("%Y%m%d")

def candidate_urls(d: dt.date) -> List[str]:
    bases = [
        "https://archives.nseindia.com/content/fo",
        "https://www.nseindia.com/content/fo",
    ]
    names = [
        f"participant_vol_{ddmmyyyy(d)}.csv",
        f"participant_vol_{yyyymmdd(d)}.csv",
        f"participant_vol_{ddmmyyyy(d)}.csv.gz",
        f"participant_vol_{yyyymmdd(d)}.csv.gz",
        f"participant_vol_{ddmmyyyy(d)}.zip",
        f"participant_vol_{yyyymmdd(d)}.zip",
        f"fao_participant_vol_{ddmmyyyy(d)}.csv",
        f"fao_participant_vol_{yyyymmdd(d)}.csv",
        f"fao_participant_vol_{ddmmyyyy(d)}.zip",
        f"fao_participant_vol_{yyyymmdd(d)}.zip",
        f"participant_trading_{ddmmyyyy(d)}.csv",
        f"participant_trading_{yyyymmdd(d)}.csv",
    ]
    out: List[str] = []
    for b in bases:
        out.extend([f"{b}/{n}" for n in names])
    return out

def _abs(u: str) -> str:
    if u.startswith(("http://", "https://")):
        return u
    host = "https://archives.nseindia.com" if "/content/" in u else "https://www.nseindia.com"
    return host + (u if u.startswith("/") else "/" + u)

def fetch_from_archives_json(url: str, timeout: int, retries: int,
                             outdir: Optional[pathlib.Path], d: dt.date) -> Tuple[Optional[pathlib.Path], Optional[str], List[str]]:
    s = http_session()
    for i in range(1, retries + 1):
        try:
            r = s.get(url, timeout=timeout, stream=True, headers={"X-Requested-With": "XMLHttpRequest"})
            ct = (r.headers.get("Content-Type") or "").lower()
            if any(x in ct for x in ("text/csv", "application/octet-stream", "text/plain")) and r.content:
                dest = ensure_outdir(d, outdir) / f"fao_participant_vol_{ddmmyyyy(d)}.csv"
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
                    if isinstance(d1, dict) and d1.get("downloadLink"):
                        urls.append(_abs(d1["downloadLink"]))
                    if isinstance(d1, list):
                        for it in d1:
                            if isinstance(it, dict):
                                if it.get("downloadLink"): urls.append(_abs(it["downloadLink"]))
                                if it.get("path"):         urls.append(_abs(it["path"]))
                    rows = j.get("rows") or []
                    for it in rows:
                        if isinstance(it, dict) and it.get("path"):
                            urls.append(_abs(it["path"]))
                    if j.get("downloadLink"):
                        urls.append(_abs(j["downloadLink"]))
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
    head = data[:512].decode("utf-8", "ignore").lower()
    if "client type" in head or "participant wise trading" in head or "future index long" in head:
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
                        log(f"Discard non-volume {u} (ct={r.headers.get('Content-Type','n/a')}, bytes={len(r.content)})")
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

CANON = {
    "client": "CLIENT", "clients": "CLIENT",
    "pro": "PRO", "proprietary": "PRO",
    "dii": "DII",
    "fii": "FII", "fia": "FII", "fpi": "FII", "fii/fpi": "FII",
}

COLS_CONTRACTS_SPLIT = {
    "idx_fut_long":       {"future index long", "index futures long"},
    "idx_fut_short":      {"future index short", "index futures short"},
    "stk_fut_long":       {"future stock long", "stock futures long"},
    "stk_fut_short":      {"future stock short", "stock futures short"},
    "idx_opt_call_long":  {"option index call long", "index options call long"},
    "idx_opt_put_long":   {"option index put long",  "index options put long"},
    "idx_opt_call_short": {"option index call short","index options call short"},
    "idx_opt_put_short":  {"option index put short", "index options put short"},
    "stk_opt_call_long":  {"option stock call long", "stock options call long"},
    "stk_opt_put_long":   {"option stock put long",  "stock options put long"},
    "stk_opt_call_short": {"option stock call short","stock options call short"},
    "stk_opt_put_short":  {"option stock put short", "stock options put short"},
    "total_long_contracts":  {"total long contracts","total_long_contracts"},
    "total_short_contracts": {"total short contracts","total_short_contracts"},
    "total_contracts":       {"total contracts","total_contracts"},
}

COLS_TURNOVER = {
    "idx_fut_turnover_lacs": {"index futures (rs. lacs)", "future index turnover", "index futures turnover"},
    "stk_fut_turnover_lacs": {"stock futures (rs. lacs)", "future stock turnover", "stock futures turnover"},
    "idx_opt_turnover_lacs": {"index options (rs. lacs)","option index turnover",  "index options turnover"},
    "stk_opt_turnover_lacs": {"stock options (rs. lacs)","option stock turnover",  "stock options turnover"},
    "total_turnover_lacs":   {"total (rs. lacs)", "total turnover", "turnover (rs. lacs)"},
}

def get_any(m: dict, names: set):
    for k in names:
        k2 = norm_header(k)
        if k2 in m and m[k2] != "":
            return m[k2]
    return None

def _find_header_row(lines: List[str]) -> int:
    targets = {"client type", "client", "participant"}
    split_any = {"future index long", "future index short", "option index call long",
                 "option index put long", "future stock long", "future stock short"}
    for i in range(min(15, len(lines))):
        low = lines[i].lower()
        if any(t in low for t in targets) and any(s in low for s in split_any):
            return i
    return 0

def iter_csv_flexible(raw_bytes: bytes) -> List[dict]:
    txt = open_text_any(raw_bytes).read()
    lines = [ln for ln in txt.splitlines() if ln.strip() != ""]
    if not lines:
        return []
    header_idx = _find_header_row(lines)
    body = "\n".join(lines[header_idx:])
    try:
        sniff = csv.Sniffer().sniff(
            body.splitlines()[0] + "\n" + (body.splitlines()[1] if len(body.splitlines()) > 1 else "")
        )
        delim = sniff.delimiter
    except Exception:
        delim = ","
    reader = csv.DictReader(StringIO(body), delimiter=delim)
    return list(reader)

def map_row(r: dict) -> Optional[dict]:
    m = {norm_header(k): norm(v) for k, v in r.items()}

    cat = m.get("client_type") or m.get("participant")
    if not cat:
        maybe = (m.get("category") or "").strip().upper()
        if maybe in {"CLIENT", "DII", "FII", "PRO"}:
            cat = maybe
    if not cat:
        return None

    cat_key = CANON.get(cat.lower(), cat.strip().upper())
    if cat_key in {"TOTAL", "ALL", "SUMMARY"}:
        return None
    if cat_key not in {"CLIENT", "DII", "FII", "PRO"}:
        return None

    out = {
        "category": cat_key,
        "client_type": cat.strip(),  # original label (e.g., 'FII/FPI')
    }

    for dst, syns in COLS_CONTRACTS_SPLIT.items():
        out[dst] = nint(get_any(m, syns))

    tl = out.get("total_long_contracts")
    ts = out.get("total_short_contracts")
    if tl is None:
        parts = [out.get("idx_opt_call_long"), out.get("idx_opt_put_long"),
                 out.get("stk_opt_call_long"), out.get("stk_opt_put_long"),
                 out.get("idx_fut_long"), out.get("stk_fut_long")]
        if any(p is not None for p in parts):
            tl = sum(p or 0 for p in parts)
            out["total_long_contracts"] = tl
    if ts is None:
        parts = [out.get("idx_opt_call_short"), out.get("idx_opt_put_short"),
                 out.get("stk_opt_call_short"), out.get("stk_opt_put_short"),
                 out.get("idx_fut_short"), out.get("stk_fut_short")]
        if any(p is not None for p in parts):
            ts = sum(p or 0 for p in parts)
            out["total_short_contracts"] = ts

    # SAFER DEFAULT: prefer LONGS (matches NSE headline column), else shorts
    if out.get("total_contracts") is None:
        if tl is not None:
            out["total_contracts"] = tl
        elif ts is not None:
            out["total_contracts"] = ts

    for dst, syns in COLS_TURNOVER.items():
        out[dst] = nnum(get_any(m, syns))

    out["raw"] = m
    return out

# -------------------- DDL / ETL --------------------
DDL_TABLE = f"""
CREATE SCHEMA IF NOT EXISTS {EOD_SCHEMA};

CREATE TABLE IF NOT EXISTS {EOD_SCHEMA}.nse_participant_trading_vol_daily(
  trade_date date NOT NULL,
  category   text NOT NULL,
  client_type text NOT NULL,

  idx_fut_long bigint,  idx_fut_short bigint,
  stk_fut_long bigint,  stk_fut_short bigint,
  idx_opt_call_long bigint, idx_opt_put_long bigint,
  idx_opt_call_short bigint, idx_opt_put_short bigint,
  stk_opt_call_long bigint, stk_opt_put_long bigint,
  stk_opt_call_short bigint, stk_opt_put_short bigint,

  total_long_contracts  bigint,
  total_short_contracts bigint,
  total_contracts       bigint,

  idx_fut_turnover_lacs numeric,
  stk_fut_turnover_lacs numeric,
  idx_opt_turnover_lacs numeric,
  stk_opt_turnover_lacs numeric,
  total_turnover_lacs   numeric,

  raw jsonb NOT NULL,

  CONSTRAINT nse_participant_trading_vol_daily_pk
    PRIMARY KEY (trade_date, category, client_type)
);
"""

def etl(trade_date: dt.date, rows: List[dict], skip_ddl: bool = False) -> int:
    if not rows:
        log("No rows to ETL")
        return 0

    with pg_conn() as conn:
        cur = conn.cursor()
        if not skip_ddl:
            # table + schema
            cur.execute(DDL_TABLE)
            # category check constraint (idempotent)
            cur.execute(f"""
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'nse_pvol_category_ck'
                      AND conrelid = '{EOD_SCHEMA}.nse_participant_trading_vol_daily'::regclass
                  ) THEN
                    ALTER TABLE {EOD_SCHEMA}.nse_participant_trading_vol_daily
                      ADD CONSTRAINT nse_pvol_category_ck
                      CHECK (upper(trim(both from category)) = ANY (ARRAY['CLIENT','DII','FII','PRO'])) NOT VALID;
                    ALTER TABLE {EOD_SCHEMA}.nse_participant_trading_vol_daily
                      VALIDATE CONSTRAINT nse_pvol_category_ck;
                  END IF;
                END
                $$ LANGUAGE plpgsql;
            """)
            # helper unique index (idempotent)
            cur.execute(f"""
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes
                    WHERE schemaname = '{EOD_SCHEMA}' AND indexname = 'ux_part_vol_day_client'
                  ) THEN
                    CREATE UNIQUE INDEX ux_part_vol_day_client
                      ON {EOD_SCHEMA}.nse_participant_trading_vol_daily(trade_date, client_type);
                  END IF;
                END
                $$ LANGUAGE plpgsql;
            """)

        # temp copy table
        cur.execute("""
          CREATE TEMP TABLE tmp_pvol(
            trade_date date,
            category text,
            client_type text,
            idx_fut_long bigint,  idx_fut_short bigint,
            stk_fut_long bigint,  stk_fut_short bigint,
            idx_opt_call_long bigint, idx_opt_put_long bigint,
            idx_opt_call_short bigint, idx_opt_put_short bigint,
            stk_opt_call_long bigint,  stk_opt_put_long bigint,
            stk_opt_call_short bigint, stk_opt_put_short bigint,
            total_long_contracts  bigint,
            total_short_contracts bigint,
            total_contracts       bigint,
            idx_fut_turnover_lacs numeric,
            stk_fut_turnover_lacs numeric,
            idx_opt_turnover_lacs numeric,
            stk_opt_turnover_lacs numeric,
            total_turnover_lacs   numeric,
            raw jsonb
          ) ON COMMIT DROP;
        """)

        buf = StringIO()
        w = csv.writer(buf, lineterminator="\n")
        written = 0
        for r in rows:
            if not r or r.get("category") not in {"CLIENT", "DII", "FII", "PRO"}:
                continue
            client_type = r.get("client_type") or r["category"]
            w.writerow([
                trade_date, r["category"], client_type,
                r.get("idx_fut_long"),  r.get("idx_fut_short"),
                r.get("stk_fut_long"),  r.get("stk_fut_short"),
                r.get("idx_opt_call_long"), r.get("idx_opt_put_long"),
                r.get("idx_opt_call_short"), r.get("idx_opt_put_short"),
                r.get("stk_opt_call_long"), r.get("stk_opt_put_long"),
                r.get("stk_opt_call_short"), r.get("stk_opt_put_short"),
                r.get("total_long_contracts"),
                r.get("total_short_contracts"),
                r.get("total_contracts"),
                r.get("idx_fut_turnover_lacs"),
                r.get("stk_fut_turnover_lacs"),
                r.get("idx_opt_turnover_lacs"),
                r.get("stk_opt_turnover_lacs"),
                r.get("total_turnover_lacs"),
                json.dumps(r.get("raw", r), ensure_ascii=False),
            ])
            written += 1

        if written == 0:
            log("Nothing valid for COPY")
            return 0

        buf.seek(0)
        cur.copy_expert("COPY tmp_pvol FROM STDIN WITH CSV", buf)

        cur.execute(sql.SQL("""
          INSERT INTO {}.nse_participant_trading_vol_daily AS t(
            trade_date, category, client_type,
            idx_fut_long,  idx_fut_short,
            stk_fut_long,  stk_fut_short,
            idx_opt_call_long, idx_opt_put_long,
            idx_opt_call_short, idx_opt_put_short,
            stk_opt_call_long, stk_opt_put_long,
            stk_opt_call_short, stk_opt_put_short,
            total_long_contracts, total_short_contracts, total_contracts,
            idx_fut_turnover_lacs, stk_fut_turnover_lacs,
            idx_opt_turnover_lacs, stk_opt_turnover_lacs, total_turnover_lacs,
            raw
          )
          SELECT
            trade_date,
            UPPER(TRIM(category)),
            UPPER(TRIM(client_type)),
            idx_fut_long,  idx_fut_short,
            stk_fut_long,  stk_fut_short,
            idx_opt_call_long, idx_opt_put_long,
            idx_opt_call_short, idx_opt_put_short,
            stk_opt_call_long, stk_opt_put_long,
            stk_opt_call_short, stk_opt_put_short,
            total_long_contracts, total_short_contracts, total_contracts,
            idx_fut_turnover_lacs, stk_fut_turnover_lacs,
            idx_opt_turnover_lacs, stk_opt_turnover_lacs, total_turnover_lacs,
            raw
          FROM tmp_pvol
          ON CONFLICT (trade_date, category, client_type) DO UPDATE SET
            idx_fut_long         = COALESCE(EXCLUDED.idx_fut_long,         t.idx_fut_long),
            idx_fut_short        = COALESCE(EXCLUDED.idx_fut_short,        t.idx_fut_short),
            stk_fut_long         = COALESCE(EXCLUDED.stk_fut_long,         t.stk_fut_long),
            stk_fut_short        = COALESCE(EXCLUDED.stk_fut_short,        t.stk_fut_short),
            idx_opt_call_long    = COALESCE(EXCLUDED.idx_opt_call_long,    t.idx_opt_call_long),
            idx_opt_put_long     = COALESCE(EXCLUDED.idx_opt_put_long,     t.idx_opt_put_long),
            idx_opt_call_short   = COALESCE(EXCLUDED.idx_opt_call_short,   t.idx_opt_call_short),
            idx_opt_put_short    = COALESCE(EXCLUDED.idx_opt_put_short,    t.idx_opt_put_short),
            stk_opt_call_long    = COALESCE(EXCLUDED.stk_opt_call_long,    t.stk_opt_call_long),
            stk_opt_put_long     = COALESCE(EXCLUDED.stk_opt_put_long,     t.stk_opt_put_long),
            stk_opt_call_short   = COALESCE(EXCLUDED.stk_opt_call_short,   t.stk_opt_call_short),
            stk_opt_put_short    = COALESCE(EXCLUDED.stk_opt_put_short,    t.stk_opt_put_short),
            total_long_contracts = COALESCE(EXCLUDED.total_long_contracts, t.total_long_contracts),
            total_short_contracts= COALESCE(EXCLUDED.total_short_contracts,t.total_short_contracts),
            total_contracts      = COALESCE(EXCLUDED.total_contracts,      t.total_contracts),
            idx_fut_turnover_lacs= COALESCE(EXCLUDED.idx_fut_turnover_lacs,t.idx_fut_turnover_lacs),
            stk_fut_turnover_lacs= COALESCE(EXCLUDED.stk_fut_turnover_lacs,t.stk_fut_turnover_lacs),
            idx_opt_turnover_lacs= COALESCE(EXCLUDED.idx_opt_turnover_lacs,t.idx_opt_turnover_lacs),
            stk_opt_turnover_lacs= COALESCE(EXCLUDED.stk_opt_turnover_lacs,t.stk_fut_turnover_lacs),
            total_turnover_lacs  = COALESCE(EXCLUDED.total_turnover_lacs,  t.total_turnover_lacs),
            raw                  = EXCLUDED.raw;
        """).format(sql.Identifier(EOD_SCHEMA)))

        conn.commit()
        return cur.rowcount or 0

# -------------------- Views / MV helpers --------------------
def create_views() -> None:
    with pg_conn() as conn:
        cur = conn.cursor()

        # 1) vw_pvol_daily (prefer LONGS to match NSE headline)
        cur.execute(f"""
            CREATE OR REPLACE VIEW {EOD_SCHEMA}.vw_pvol_daily AS
            WITH base AS (
              SELECT trade_date, category,
                     COALESCE(total_long_contracts, total_contracts, total_short_contracts) AS total_contracts,
                     total_long_contracts, total_short_contracts, total_turnover_lacs
              FROM {EOD_SCHEMA}.nse_participant_trading_vol_daily
            ),
            dod AS (
              SELECT b.*,
                     b.total_contracts
                       - LAG(b.total_contracts) OVER (PARTITION BY category ORDER BY trade_date) AS total_contracts_dod
              FROM base b
            )
            SELECT * FROM dod;
        """)

        # 2) vw_pvol_daily_pivot
        cur.execute(f"""
            CREATE OR REPLACE VIEW {EOD_SCHEMA}.vw_pvol_daily_pivot AS
            SELECT
              trade_date,
              MAX(CASE WHEN category='CLIENT' THEN total_contracts END)     AS client_contracts,
              MAX(CASE WHEN category='DII'    THEN total_contracts END)     AS dii_contracts,
              MAX(CASE WHEN category='FII'    THEN total_contracts END)     AS fii_contracts,
              MAX(CASE WHEN category='PRO'    THEN total_contracts END)     AS pro_contracts,
              MAX(CASE WHEN category='CLIENT' THEN total_contracts_dod END) AS client_dod,
              MAX(CASE WHEN category='DII'    THEN total_contracts_dod END) AS dii_dod,
              MAX(CASE WHEN category='FII'    THEN total_contracts_dod END) AS fii_dod,
              MAX(CASE WHEN category='PRO'    THEN total_contracts_dod END) AS pro_dod
            FROM {EOD_SCHEMA}.vw_pvol_daily
            GROUP BY trade_date;
        """)

        # 3) mv_pvol_last30 — create if missing
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_matviews
                    WHERE schemaname = '{EOD_SCHEMA}' AND matviewname = 'mv_pvol_last30'
                ) THEN
                    CREATE MATERIALIZED VIEW {EOD_SCHEMA}.mv_pvol_last30 AS
                    SELECT
                      p.trade_date,
                      p.category,
                      p.total_contracts,
                      p.total_turnover_lacs,
                      p.total_contracts
                        - LAG(p.total_contracts) OVER (PARTITION BY p.category ORDER BY p.trade_date) AS total_contracts_dod,
                      AVG(p.total_contracts) OVER (
                        PARTITION BY p.category ORDER BY p.trade_date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                      ) AS total_contracts_sma7
                    FROM {EOD_SCHEMA}.vw_pvol_daily p
                    WHERE p.trade_date >= CURRENT_DATE - INTERVAL '40 days';
                END IF;
            END $$ LANGUAGE plpgsql;
        """)

        # 3a) unique index for CONCURRENT refresh
        cur.execute("""
            SELECT 1
            FROM pg_indexes
            WHERE schemaname = %s AND indexname = %s
        """, (EOD_SCHEMA, "idx_mv_pvol_last30_uniq"))
        if cur.fetchone() is None:
            try:
                cur.execute(f"""
                    CREATE UNIQUE INDEX idx_mv_pvol_last30_uniq
                      ON {EOD_SCHEMA}.mv_pvol_last30 (trade_date, category);
                """)
            except DuplicateObject:
                pass

        # 4) banner view
        cur.execute(f"""
            CREATE OR REPLACE VIEW {EOD_SCHEMA}.vw_pvol_banner_latest AS
            SELECT *
            FROM {EOD_SCHEMA}.vw_pvol_daily_pivot
            WHERE trade_date = (SELECT MAX(trade_date) FROM {EOD_SCHEMA}.vw_pvol_daily_pivot);
        """)

        # 5) refresh helper function (best-effort)
        try:
            cur.execute(f"""
                CREATE OR REPLACE FUNCTION {EOD_SCHEMA}.refresh_pvol() RETURNS void
                LANGUAGE plpgsql AS $$
                BEGIN
                  BEGIN
                    REFRESH MATERIALIZED VIEW CONCURRENTLY {EOD_SCHEMA}.mv_pvol_last30;
                  EXCEPTION
                    WHEN feature_not_supported THEN
                      REFRESH MATERIALIZED VIEW {EOD_SCHEMA}.mv_pvol_last30;
                  END;
                END;
                $$;
            """)
        except InsufficientPrivilege:
            log("WARN: Not owner of function refresh_pvol(); skipping function creation.")

        conn.commit()
        log("Views/MV ensured.")

# -------------------- Orchestration --------------------
def save_bytes(d: dt.date, data: bytes, kind: str, outdir: Optional[pathlib.Path]) -> pathlib.Path:
    out = ensure_outdir(d, outdir)
    name = f"participants_vol_{d:%Y-%m-%d}." + {"csv": "csv", "csv.gz": "csv.gz", "zip": "zip"}[kind]
    p = out / name
    p.write_bytes(data)
    log(f"Saved → {p} (bytes={len(data)})")
    return p

def resolve_etl_file(arg_path: Optional[str], d: dt.date, outdir: Optional[str]) -> pathlib.Path:
    if arg_path:
        p = pathlib.Path(os.path.expanduser(arg_path)).resolve()
        if p.exists():
            return p
    daydir = ensure_outdir(d, pathlib.Path(outdir) if outdir else None)
    pats = [
        f"fao_participant_vol_{ddmmyyyy(d)}.csv",
        f"fao_participant_vol_{yyyymmdd(d)}.csv",
        f"fao_participant_vol_{ddmmyyyy(d)}.zip",
        f"fao_participant_vol_{yyyymmdd(d)}.zip",
        f"fao_participant_vol_{ddmmyyyy(d)}.csv.gz",
        f"fao_participant_vol_{yyyymmdd(d)}.csv.gz",
        f"participant_vol_{ddmmyyyy(d)}.csv",
        f"participant_vol_{yyyymmdd(d)}.csv",
        f"participant_vol_{ddmmyyyy(d)}.zip",
        f"participant_vol_{yyyymmdd(d)}.zip",
        f"participant_vol_{ddmmyyyy(d)}.csv.gz",
        f"participant_vol_{yyyymmdd(d)}.csv.gz",
        f"participants_vol_{d:%Y-%m-%d}.csv",
        f"participant_trading_{ddmmyyyy(d)}.csv",
        f"participant_trading_{yyyymmdd(d)}.csv",
    ]
    matches: list[pathlib.Path] = []
    for pat in pats:
        matches.extend(sorted(daydir.glob(pat)))
    if not matches:
        matches.extend(sorted(daydir.glob("*participant*vol*.csv")))
        matches.extend(sorted(daydir.glob("*participant*vol*.csv.gz")))
        matches.extend(sorted(daydir.glob("*participant*vol*.zip")))
        matches.extend(sorted(daydir.glob("*participant*trading*.csv")))
        matches.extend(sorted(daydir.glob("*participant*trading*.zip")))
        matches.extend(sorted(daydir.glob("*participant*trading*.csv.gz")))
    if not matches:
        raise FileNotFoundError(f"--file missing or not found; scanned {daydir}")

    def score(p: pathlib.Path) -> tuple:
        ext = p.suffix.lower()
        ext_rank = 0 if ext == ".csv" else (1 if ext in (".gz",) or p.name.lower().endswith(".csv.gz") else 2)
        try:
            size = p.stat().st_size
        except Exception:
            size = 0
        return (ext_rank, -size)

    best = sorted(matches, key=score)[0]
    return best

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
                raise RuntimeError("All Participant Volume candidate URLs failed or returned non-volume content.")
            data, kind, _ = got
            saved = save_bytes(d, data, kind, outp)
        if args.check_only or args.fetch_only:
            return
    else:
        saved = resolve_etl_file(args.file, d, args.outdir)
        low = saved.name.lower()
        if   low.endswith(".csv"):    kind = "csv"
        elif low.endswith(".csv.gz"): kind = "csv.gz"
        elif low.endswith(".zip"):    kind = "zip"
        else: raise ValueError(f"Unknown --file type for ETL: {saved.name}")

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
    if not mapped:
        if rows:
            log(f"Headers seen: {list(rows[0].keys())}")
        else:
            log("CSV appears empty after header detection.")
        return

    if not (args.check_only or args.fetch_only):
        up = etl(d, mapped, skip_ddl=args.skip_ddl)
        log(f"Upserted rows: {up}")

        if args.refresh:
            # best-effort MV refresh via function; fall back to direct refresh if function absent
            try:
                with pg_conn() as conn2:
                    cur2 = conn2.cursor()
                    try:
                        cur2.execute(f"SELECT {EOD_SCHEMA}.refresh_pvol();")
                    except Exception:
                        cur2.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {EOD_SCHEMA}.mv_pvol_last30;")
                    conn2.commit()
                log("Refreshed mv_pvol_last30.")
            except Exception as e:
                log(f"WARN: refresh failed ({e})")

# -------------------- CLI --------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="F&O Participant-wise Trading Volumes (csv)")
    ap.add_argument("trade_date", help="YYYY-MM-DD")
    ap.add_argument("--archives-json", default=None, help="NSE api/reports?archives=... URL")
    ap.add_argument("--check-only", action="store_true", help="Only fetch/probe; do not ETL")
    ap.add_argument("--fetch-only", action="store_true", help="Download only; skip ETL")
    ap.add_argument("--etl-only", action="store_true", help="Only ETL from --file (auto-resolves common files if not found)")
    ap.add_argument("--skip-ddl", action="store_true", help="Skip CREATE SCHEMA/TABLE/INDEX if lacking perms")
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=25)
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--file", default=None, help="Local CSV/CSV.GZ/ZIP when using --etl-only (path or leave blank to auto-resolve)")
    ap.add_argument("--ensure-views", action="store_true", help="Create/refresh views & MV definitions")
    ap.add_argument("--refresh", action="store_true", help="After ETL, refresh the MV (best-effort)")
    args = ap.parse_args()

    if args.ensure_views:
        create_views()

    d = dt.datetime.strptime(args.trade_date, "%Y-%m-%d").date()
    run_one_day(d, args)
