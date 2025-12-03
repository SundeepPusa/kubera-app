# reports/download_cash_zip.py
# Download "ALL Capital Market → Equities & SME" reports for a date range
# using NSE's /api/reports endpoint (Select All Reports → Multiple file download).

import argparse
import datetime as dt
import logging
import time
from pathlib import Path

import requests


# -------------------------------------------------------------------
# 1) CONSTANTS – EDIT ONLY THESE IF NEEDED
# -------------------------------------------------------------------

BASE_URL = "https://www.nseindia.com/api/reports"

# 🔴 IMPORTANT:
# Paste your full `archives=...` query string from DevTools here EXACTLY as-is,
# WITHOUT the leading '?'.
#
# Even if you accidentally keep trailing "&date=...&type=...&mode=...",
# build_url() below will strip them and inject the correct date from CLI.
#
ARCHIVES_PARAM_ENCODED = (
    "archives=%5B%7B%22name%22%3A%22NSE%20Market%20Pulse%20(.pdf)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20VaR%20Margin%20Rates%20(1st%20intra-day)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20VaR%20Margin%20Rates%20(2nd%20intra-day)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20VaR%20Margin%20Rates%20(3rd%20intra-day)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20VaR%20Margin%20Rates%20(4th%20intra-day)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20VaR%20Margin%20Rates%20(End%20of%20day)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20VaR%20Margin%20Rates%20(Begin%20of%20day)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20Daily%20Volatility%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20Category-wise%20Turnover%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20Bhavcopy(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20Common%20Bhavcopy%20(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20Bhavcopy%20(PR.zip)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20Short%20Selling%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20Market%20Activity%20Report%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20Security-wise%20Delivery%20Positions%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20Margin%20Trading%20Disclosure%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22Client%20Funding%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22VaR%20Multiplier%20files%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22ALBM%20Yield%20Statistics%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22Extreme%20Loss%20Margin%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20Security%20Catergory%20Impact%20Cost%20(.T01)%22%2C%22type%22%3A%22monthly-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22SME%20-%20BHAVCOPY%20(.csv)%22%2C%22type%22%3A%22monthly-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22Bhavcopy%20File%20(DAT)%22%2C%22type%22%3A%22monthly-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22Full%20Bhavcopy%20and%20Security%20Deliverable%20data%22%2C%22type%22%3A%22daily-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20Mode%20of%20Trading%22%2C%22type%22%3A%22daily-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM-UDiFF%20Common%20Bhavcopy%20Final%20(zip)%22%2C%22type%22%3A%22daily-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22Surveillance%20Indicator%22%2C%22type%22%3A%22daily-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22Surveillance%20Indicator%20New%22%2C%22type%22%3A%22daily-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20MII%20-%20Security%20File%20(.gz)%20(NSE%20Listed%20securities)%22%2C%22type%22%3A%22daily-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%2252%20Week%20High%20Low%20Report%22%2C%22type%22%3A%22daily-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20Close%20out%20prices%20(.csv)%22%2C%22type%22%3A%22daily-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22PE%20Ratio%22%2C%22type%22%3A%22daily-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%2C%7B%22name%22%3A%22CM%20-%20MII%20-%20Security%20File%20(.gz)%20(NSE%20Listed%20and%20BSE%20Exclusive%20securities)%22%2C%22type%22%3A%22daily-reports%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equities%22%7D%5D&date=28-Nov-2025&type=Archives"
    # ⬆⬆⬆ Even though this captured string ends with &date=...&type=...,
    # build_url() below will strip them and inject the correct date.
)

# Default root folder where ZIPs will be stored (relative to repo root)
DEFAULT_OUT_ROOT = Path("data/nse/cm_all_reports")

# NSE needs decent headers; reuse same style as your other NSE scripts
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}


# -------------------------------------------------------------------
# 2) HELPERS
# -------------------------------------------------------------------

def build_url(trade_date: dt.date) -> str:
    """
    Build the full NSE /api/reports URL for a given trade_date.

    We *only* want the 'archives=...' part from ARCHIVES_PARAM_ENCODED.
    If the captured string accidentally contains '&date=...&type=...&mode=...',
    we strip those pieces and append the correct date from the CLI prompt.
    """
    base = ARCHIVES_PARAM_ENCODED

    # Strip any accidentally captured &date=..., &type=..., or &mode=...
    for marker in ("&date=", "&type=", "&mode="):
        idx = base.find(marker)
        if idx != -1:
            base = base[:idx]

    date_str = trade_date.strftime("%d-%b-%Y")  # e.g. 28-Nov-2025
    return f"{BASE_URL}?{base}&date={date_str}&type=Archives"


def daterange(start: dt.date, end: dt.date):
    """Yield dates from start to end inclusive."""
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def download_zip_for_date(
    trade_date: dt.date,
    out_root: Path,
    session: requests.Session,
    max_retries: int = 3,
    backoff_sec: float = 5.0,
) -> Path | None:
    """
    Download the all-reports ZIP for a given trade_date.

    Returns the path of the saved file on success, or None if failed after retries.
    """
    url = build_url(trade_date)
    out_dir = out_root / str(trade_date.year) / f"{trade_date.month:02d}" / f"{trade_date.day:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"cash_reports_{trade_date.isoformat()}.zip"

    # Idempotent: if file already exists and non-empty, skip
    if out_path.exists() and out_path.stat().st_size > 0:
        logging.info(f"[{trade_date}] ZIP already exists, skipping: {out_path}")
        return out_path

    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"[{trade_date}] Fetching ALL cash reports (attempt {attempt})")
            resp = session.get(url, headers=DEFAULT_HEADERS, timeout=120)
            if resp.status_code != 200:
                logging.warning(
                    f"[{trade_date}] HTTP {resp.status_code} from NSE. "
                    f"Text (truncated): {resp.text[:200]!r}"
                )
                # Raise to trigger retry logic
                resp.raise_for_status()

            content = resp.content
            if not content:
                logging.warning(f"[{trade_date}] Empty response body from NSE.")
                raise RuntimeError("Empty response body")

            out_path.write_bytes(content)
            size_mb = len(content) / 1_000_000
            logging.info(f"[{trade_date}] Saved ZIP: {out_path} ({size_mb:.2f} MB)")
            return out_path

        except Exception as exc:
            logging.error(f"[{trade_date}] Error fetching ZIP: {exc}")
            if attempt < max_retries:
                logging.info(f"[{trade_date}] Retrying in {backoff_sec} seconds...")
                time.sleep(backoff_sec)
            else:
                logging.error(f"[{trade_date}] FAILED after {max_retries} attempts.")

    return None


# -------------------------------------------------------------------
# 3) MAIN CLI
# -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Download NSE Capital Market → Equities & SME ALL-REPORTS ZIP for a date range."
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        help="End date (YYYY-MM-DD). If omitted, only the start date is fetched.",
    )
    parser.add_argument(
        "--out-root",
        help=f"Output root folder (default: {DEFAULT_OUT_ROOT})",
    )

    args = parser.parse_args()

    start_date = dt.date.fromisoformat(args.start)
    end_date = dt.date.fromisoformat(args.end) if args.end else start_date

    out_root = Path(args.out_root) if args.out_root else DEFAULT_OUT_ROOT

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    logging.info(f"Downloading NSE cash ALL-REPORTS ZIP from {start_date} to {end_date}")
    logging.info(f"Output root: {out_root.resolve()}")

    session = requests.Session()

    for d in daterange(start_date, end_date):
        # Skip weekends – NSE cash is closed
        if d.weekday() >= 5:  # 5 = Sat, 6 = Sun
            logging.info(f"[{d}] Weekend – skipping")
            continue

        download_zip_for_date(d, out_root, session=session)


if __name__ == "__main__":
    main()
