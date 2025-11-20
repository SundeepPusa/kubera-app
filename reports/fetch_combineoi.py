# -*- coding: utf-8 -*-
"""
Fetch + ETL: F&O - Combine Open Interest across exchanges (ZIP -> CSV)

Usage:
  python -m reports.fetch_combineoi 2025-10-31 --retries 2 --timeout 20
  python -m reports.fetch_combineoi 2025-10-31 --file data/nse/2025/10/31/combineoi_31102025.zip
"""
import os, csv, zipfile, argparse, pathlib, datetime as dt, io
from typing import Optional, List, Tuple
import requests, psycopg2
from dotenv import load_dotenv

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "nse"
for p in (BASE_DIR/".env", BASE_DIR/".ENV"):
    if p.exists(): load_dotenv(p.as_posix()); break
EOD_SCHEMA = os.getenv("EOD_SCHEMA", "eod")

def log(m): print(f"[combineoi] {dt.datetime.now():%Y-%m-%d %H:%M:%S} | {m}")
def ddmmyyyy(d: dt.date) -> str: return d.strftime("%d%m%Y")

def ensure_outdir(d: dt.date, outdir: Optional[pathlib.Path]=None)->pathlib.Path:
    base = pathlib.Path(outdir) if outdir else DATA_DIR
    p = base / f"{d.year}" / f"{d:%m}" / f"{d:%d}"; p.mkdir(parents=True, exist_ok=True); return p

def pg(): return psycopg2.connect(
    host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"), connect_timeout=10)

def http() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent":"Mozilla/5.0", "Accept":"*/*",
        "Referer":"https://www.nseindia.com/all-reports"})
    return s

def candidate_urls(d: dt.date) -> List[str]:
    name = f"combineoi_{ddmmyyyy(d)}.zip"
    bases = ["https://archives.nseindia.com/content/fo", "https://www.nseindia.com/content/fo"]
    return [f"{b}/{name}" for b in bases]

def fetch_zip(d: dt.date, retries:int, timeout:int) -> Optional[bytes]:
    s=http()
    for url in candidate_urls(d):
        for i in range(retries):
            r=s.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code==200 and r.content and r.content[:4]==b'PK\x03\x04':
                log(f"OK 200 {url} bytes={len(r.content)}"); return r.content
            log(f"HTTP {r.status_code} {url} (try {i+1}/{retries})")
    return None

def open_zip_bytes(b: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(b), "r") as z:
        members=[n for n in z.namelist() if n.lower().endswith(".csv")]
        if not members: raise RuntimeError("zip has no csv")
        return z.read(members[0])

def parse_rows(csv_bytes: bytes) -> List[dict]:
    rdr = csv.DictReader(io.TextIOWrapper(io.BytesIO(csv_bytes), encoding="utf-8", errors="replace"))
    rows=[]
    for r in rdr:
        if not r: continue
        rows.append({
            "date": r.get("Date") or r.get("date"),
            "isin": r.get("ISIN") or r.get("isin"),
            "scrip_name": r.get("Scrip Name") or r.get("ScripName"),
            "symbol": r.get("NSE Symbol") or r.get("Symbol"),
            "mwpl": r.get("MWPL"),
            "open_interest": r.get("Open Interest"),
            "future_equivalent_oi": r.get("Future Equivalent Open Interest"),
            "limit_next_day": r.get("Limit for Next Day"),
        })
    return rows

DDL=f"""
CREATE SCHEMA IF NOT EXISTS {EOD_SCHEMA};
CREATE TABLE IF NOT EXISTS {EOD_SCHEMA}.nse_combined_oi_daily(
  trade_date date NOT NULL,
  isin text NOT NULL,
  scrip_name text, symbol text,
  mwpl numeric, open_interest numeric,
  future_equivalent_oi numeric, limit_next_day numeric,
  PRIMARY KEY(trade_date, isin)
);
"""

def num(x):
    if x is None: return None
    t=str(x).replace(",","").strip()
    if t in ("","-","NA"): return None
    try: return float(t)
    except: return None

def etl(d: dt.date, rows: List[dict]) -> int:
    if not rows: return 0
    with pg() as conn:
        cur=conn.cursor(); cur.execute(DDL)
        cur.execute("CREATE TEMP TABLE tmp AS SELECT NULL::date trade_date, NULL::text isin, NULL::text scrip_name, NULL::text symbol, NULL::numeric mwpl, NULL::numeric open_interest, NULL::numeric future_equivalent_oi, NULL::numeric limit_next_day WHERE FALSE;")
        buf=io.StringIO(); w=csv.writer(buf, lineterminator="\n")
        for r in rows:
            w.writerow([
                d, (r["isin"] or "").strip(),
                (r["scrip_name"] or "").strip(),
                (r["symbol"] or "").strip(),
                num(r["mwpl"]), num(r["open_interest"]),
                num(r["future_equivalent_oi"]), num(r["limit_next_day"]),
            ])
        buf.seek(0); cur.copy_expert("COPY tmp FROM STDIN WITH CSV", buf)
        cur.execute(f"""
        INSERT INTO {EOD_SCHEMA}.nse_combined_oi_daily AS t
          (trade_date, isin, scrip_name, symbol, mwpl, open_interest, future_equivalent_oi, limit_next_day)
        SELECT DISTINCT ON (trade_date, isin) *
        FROM (
          SELECT * FROM tmp
        ) s
        ORDER BY trade_date, isin
        ON CONFLICT (trade_date, isin) DO UPDATE SET
          scrip_name = EXCLUDED.scrip_name,
          symbol     = EXCLUDED.symbol,
          mwpl       = COALESCE(EXCLUDED.mwpl, t.mwpl),
          open_interest = COALESCE(EXCLUDED.open_interest, t.open_interest),
          future_equivalent_oi = COALESCE(EXCLUDED.future_equivalent_oi, t.future_equivalent_oi),
          limit_next_day = COALESCE(EXCLUDED.limit_next_day, t.limit_next_day);
        """)
        n=cur.rowcount or 0; conn.commit(); return n

def main(trade_date: dt.date, retries:int, timeout:int, outdir: Optional[str], file_override: Optional[str]):
    log(f"Trade date = {trade_date.isoformat()}")
    if file_override:
        p=pathlib.Path(file_override)
        csv_bytes = open_zip_bytes(p.read_bytes())
    else:
        raw = fetch_zip(trade_date, retries, timeout)
        if not raw: raise RuntimeError("combineoi zip not found")
        out = ensure_outdir(trade_date, pathlib.Path(outdir) if outdir else None)
        (out / f"combineoi_{ddmmyyyy(trade_date)}.zip").write_bytes(raw)
        csv_bytes = open_zip_bytes(raw)
    rows = parse_rows(csv_bytes)
    up = etl(trade_date, rows); log(f"Upserted rows: {up}")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("trade_date"); ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--timeout", type=int, default=20); ap.add_argument("--outdir", type=str, default=None)
    ap.add_argument("--file", type=str, default=None, help="Path to combineoi_*.zip")
    a=ap.parse_args()
    d=dt.datetime.strptime(a.trade_date, "%Y-%m-%d").date()
    main(d,a.retries,a.timeout,a.outdir,a.file)
