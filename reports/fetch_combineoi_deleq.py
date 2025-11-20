# -*- coding: utf-8 -*-
"""
FILE: reports/fetch_combineoi_deleq.py

Fetch + ETL → {EOD_SCHEMA}.nse_combined_deltaeq_oi_daily
Report: “F&O – Combine Delta Equivalent Open Interest across exchanges (csv)”
Your sample columns:
  Date, ISIN, Scrip Name, Symbol, Notional Open Interest,
  Portfolio-wise Futures Equivalent Open Interest
"""

# Top of file (imports)
import os, csv, argparse, pathlib, datetime as dt, io
from datetime import date
from typing import List, Optional
import requests, psycopg2
from dotenv import load_dotenv


BASE_DIR=pathlib.Path(__file__).resolve().parents[1]
DATA_DIR=BASE_DIR/"data"/"nse"
for p in (BASE_DIR/".env", BASE_DIR/".ENV"):
    if p.exists(): load_dotenv(p.as_posix()); break
EOD_SCHEMA=os.getenv("EOD_SCHEMA","eod")

def log(m): print(f"[deleq] {dt.datetime.now():%Y-%m-%d %H:%M:%S} | {m}")
def ensure_outdir(d,out=None):
    b=pathlib.Path(out) if out else DATA_DIR
    p=b/f"{d.year}"/f"{d:%m}"/f"{d:%d}"; p.mkdir(parents=True,exist_ok=True); return p
def pg(): return psycopg2.connect(host=os.getenv("DB_HOST"),port=os.getenv("DB_PORT"),
                                  dbname=os.getenv("DB_NAME"),user=os.getenv("DB_USER"),
                                  password=os.getenv("DB_PASSWORD"),connect_timeout=10)
def http():
    s=requests.Session()
    s.headers.update({"User-Agent":"Mozilla/5.0","Accept":"application/json,text/plain,*/*",
                      "Referer":"https://www.nseindia.com/all-reports","Origin":"https://www.nseindia.com"})
    ck=os.getenv("NSE_COOKIES","").strip()
    if ck: s.headers["Cookie"]=ck
    return s

def ddmmyyyy(d): return d.strftime("%d%m%Y")

def archives_fetch(archives_json_url: str, d: dt.date, timeout:int, retries:int) -> Optional[bytes]:
    s=http()
    for i in range(1,retries+1):
        try:
            r=s.get(archives_json_url, timeout=timeout, stream=True)
            ct=(r.headers.get("Content-Type") or "").lower()
            if any(x in ct for x in ("text/csv","application/octet-stream","text/plain")) and r.content:
                log("archives returned CSV directly"); return r.content
            if "application/json" in ct:
                try: j=r.json()
                except Exception: j=None
                url=None
                if isinstance(j,dict):
                    if j.get("downloadLink"): url=j["downloadLink"]
                    elif isinstance(j.get("data"),dict) and j["data"].get("downloadLink"): url=j["data"]["downloadLink"]
                if url:
                    if not url.startswith("http"): url="https://www.nseindia.com"+url
                    rr=s.get(url,timeout=timeout)
                    if rr.status_code==200 and rr.content: return rr.content
        except Exception as e:
            log(f"archives fetch failed (try {i}/{retries}): {e}")
            time.sleep(min(2*i,8))
    return None

def rows_from_csv(raw: bytes) -> List[dict]:
    txt=raw.decode("utf-8","replace")
    return list(csv.DictReader(StringIO(txt)))

def fnum(x):
    if x is None: return None
    t=str(x).replace(",","").strip()
    if t in ("","-","NA","NIL"): return None
    try: return float(t)
    except Exception: return None

DDL=f"""
CREATE SCHEMA IF NOT EXISTS {EOD_SCHEMA};
CREATE TABLE IF NOT EXISTS {EOD_SCHEMA}.nse_combined_deltaeq_oi_daily(
  trade_date date NOT NULL,
  isin text NOT NULL,
  scrip_name text,
  symbol text,
  notional_oi numeric,
  futures_eq_oi numeric,
  raw jsonb,
  PRIMARY KEY (trade_date, isin)
);"""

# Replace your etl() with this
def etl(trade_date: date, rows: List[dict]) -> int:
    if not rows: return 0
    with pg_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {EOD_SCHEMA};
        CREATE TABLE IF NOT EXISTS {EOD_SCHEMA}.nse_combined_deltaeq_oi_daily(
          trade_date date NOT NULL,
          isin       text NOT NULL,
          scrip_name text,
          symbol     text,
          notional_oi numeric,
          deleq_oi    numeric,
          PRIMARY KEY (trade_date, isin)
        );
        """)
        cur.execute("""
        CREATE TEMP TABLE tmp_deleq(
          trade_date date,
          isin       text,
          scrip_name text,
          symbol     text,
          notional_oi numeric,
          deleq_oi    numeric
        ) ON COMMIT DROP;
        """)
        buf = io.StringIO(); w = csv.writer(buf, lineterminator="\n")
        for r in rows:
            w.writerow([
                trade_date,
                (r.get("isin") or "").strip(),
                (r.get("scrip_name") or r.get("Scrip Name") or r.get("scripname") or "").strip(),
                (r.get("symbol") or r.get("NSE Symbol") or "").strip(),
                to_num(r.get("notional_oi") or r.get("Notional Open Interest")),
                to_num(r.get("deleq_oi")    or r.get("Portfolio-wise Futures Equivalent Open Interest")),
            ])
        buf.seek(0); cur.copy_expert("COPY tmp_deleq FROM STDIN WITH CSV", buf)

        cur.execute(f"""
        INSERT INTO {EOD_SCHEMA}.nse_combined_deltaeq_oi_daily AS t
          (trade_date, isin, scrip_name, symbol, notional_oi, deleq_oi)
        SELECT DISTINCT ON (trade_date, isin)
               trade_date, isin, scrip_name, symbol, notional_oi, deleq_oi
        FROM (
          SELECT *,
                 COALESCE(notional_oi, 0) AS notl
          FROM tmp_deleq
          WHERE isin IS NOT NULL AND isin <> ''
        ) s
        ORDER BY trade_date, isin, notl DESC
        ON CONFLICT (trade_date, isin) DO UPDATE SET
          scrip_name = EXCLUDED.scrip_name,
          symbol     = EXCLUDED.symbol,
          notional_oi= COALESCE(EXCLUDED.notional_oi, t.notional_oi),
          deleq_oi   = COALESCE(EXCLUDED.deleq_oi,    t.deleq_oi);
        """)
        n=cur.rowcount or 0; conn.commit(); return n


def main(d, archives_json, timeout, retries, outdir):
    log(f"Trade date = {d}")
    raw = archives_fetch(archives_json, d, timeout, retries)
    if not raw: raise RuntimeError("DeltaEq CSV not obtained via archives JSON")
    out=ensure_outdir(d,outdir); (out/f"combineoi_deleq_{ddmmyyyy(d)}.csv").write_bytes(raw)
    rows=rows_from_csv(raw); log(f"Parsed rows: {len(rows)}")
    up=etl(d, rows); log(f"Upserted rows: {up}")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("trade_date"); ap.add_argument("--archives-json", required=True)
    ap.add_argument("--timeout", type=int, default=20); ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--outdir", default=None)
    a=ap.parse_args(); d=dt.datetime.strptime(a.trade_date,"%Y-%m-%d").date()
    main(d, a.archives_json, a.timeout, a.retries, a.outdir)
