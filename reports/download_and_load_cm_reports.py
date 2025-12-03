# -*- coding: utf-8 -*-
"""
Unified NSE CM loader — Download + Extract + Upsert (DB)

This script combines:
  - download_cash_zip.download_zip_for_date  (ALL Capital Market reports ZIP)
  - load_cm_reports.load_all_for_date        (DB upsert into eod.* tables)

Flow for each date in [start, end]:
  1) Skip weekends (Sat/Sun).
  2) Try downloading ALL-REPORTS ZIP from NSE.
     - If download fails -> treat as holiday / no-trading day; skip DB load.
     - If ZIP is very small -> also treat as holiday / no-trading day.
  3) Extract ZIP content into: data/nse/cm_all_reports/YYYY/MM/DD/
  4) Run load_all_for_date(date) to upsert into eod.* tables.

Usage (from repo root):

    python -m reports.download_and_load_cm_reports --start 2024-11-18 --end 2025-12-02

If --end is omitted, only --start is processed.

Notes:
- Holiday handling: any NON-weekend date where the ZIP can't be
  downloaded (HTTP error / empty body / repeated failure) is logged
  as "likely holiday or data not published yet" and skipped.
"""

import argparse
import datetime as dt
import logging
from pathlib import Path
import zipfile

import requests

from reports import download_cash_zip
from reports import load_cm_reports


# Minimum ZIP size in bytes to consider it a "valid" all-reports ZIP.
# If file is smaller than this, we treat it as a failed/invalid download
# (often what happens on holidays or when NSE returns an error page).
MIN_ZIP_BYTES = 50_000  # ~50 KB; tune if needed


def extract_all_reports(zip_path: Path) -> Path:
    """
    Extract the ALL-REPORTS ZIP into its day folder.

    Example:
        zip_path = data/nse/cm_all_reports/2025/11/28/cash_reports_2025-11-28.zip

    We extract into:
        data/nse/cm_all_reports/2025/11/28/

    This matches the folder structure expected by load_cm_reports.load_all_for_date().
    """
    out_dir = zip_path.parent
    logging.info(f"[{zip_path.name}] Extracting into: {out_dir}")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(out_dir)
    return out_dir


def is_weekend(d: dt.date) -> bool:
    """True if Saturday or Sunday."""
    return d.weekday() >= 5  # 5 = Sat, 6 = Sun


def process_single_date(
    trade_date: dt.date,
    session: requests.Session,
    out_root: Path | None = None,
) -> None:
    """
    Full pipeline for a single trade_date:

        1) Skip weekend.
        2) Download all-reports ZIP (if trading day).
        3) Check size; if tiny -> treat as holiday / invalid.
        4) Extract inside the same YYYY/MM/DD folder.
        5) Run load_all_for_date(trade_date) to upsert DB.

    We treat "could not download" and "too-small ZIP" as *likely*
    exchange holidays or days where data isn't published, and skip DB load.
    """
    if is_weekend(trade_date):
        logging.info(f"[{trade_date}] Weekend (Sat/Sun) – skipping.")
        return

    # Decide out_root: either passed in, or use the default from download_cash_zip
    if out_root is None:
        out_root = download_cash_zip.DEFAULT_OUT_ROOT

    # Download the ALL-REPORTS ZIP using existing helper
    logging.info(f"[{trade_date}] Starting download + load pipeline.")
    zip_path = download_cash_zip.download_zip_for_date(
        trade_date,
        out_root=out_root,
        session=session,
    )

    if zip_path is None:
        # After max_retries, download_zip_for_date returns None
        logging.warning(
            f"[{trade_date}] Unable to download ALL-REPORTS ZIP. "
            f"Treating as holiday / no-trading-day or data-not-yet-available."
        )
        return

    # Quick sanity check on file size
    size_bytes = zip_path.stat().st_size
    if size_bytes < MIN_ZIP_BYTES:
        logging.warning(
            f"[{trade_date}] ZIP is very small ({size_bytes} bytes). "
            f"Treating as invalid/holiday; skipping DB load."
        )
        return

    # Extract into the same YYYY/MM/DD folder
    extract_all_reports(zip_path)

    # Now run the loaders (sec_bhav, MTO, volatility, PR, etc.)
    try:
        # load_cm_reports.load_all_for_date expects a datetime.date
        logging.info(f"[{trade_date}] Running load_all_for_date()...")
        load_cm_reports.load_all_for_date(trade_date)
        logging.info(f"[{trade_date}] ✅ Completed download + extract + DB upsert.")
    except SystemExit as exc:
        # load_all_for_date may call SystemExit if folder missing
        logging.error(
            f"[{trade_date}] load_all_for_date() aborted with SystemExit: {exc}"
        )
    except Exception as exc:
        logging.exception(
            f"[{trade_date}] ERROR while running load_all_for_date(): {exc}"
        )


def daterange(start: dt.date, end: dt.date):
    """Yield calendar dates from start to end inclusive."""
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Download + Extract + Upsert NSE CM ALL-REPORTS ZIP into eod.* tables "
            "for a date range."
        )
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date in YYYY-MM-DD (e.g. 2024-11-18)",
    )
    parser.add_argument(
        "--end",
        help=(
            "End date in YYYY-MM-DD (inclusive). "
            "If omitted, only the start date is processed."
        ),
    )
    parser.add_argument(
        "--out-root",
        help=(
            "Root folder for day-wise cache of ZIPs and extracted files "
            f"(default: {download_cash_zip.DEFAULT_OUT_ROOT})"
        ),
    )

    args = parser.parse_args()

    start_date = dt.date.fromisoformat(args.start)
    end_date = dt.date.fromisoformat(args.end) if args.end else start_date

    if end_date < start_date:
        raise SystemExit("End date cannot be earlier than start date.")

    out_root = Path(args.out_root) if args.out_root else download_cash_zip.DEFAULT_OUT_ROOT

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    logging.info(
        f"Running unified NSE CM pipeline from {start_date} to {end_date}. "
        f"Output root: {out_root.resolve()}"
    )

    session = requests.Session()

    for d in daterange(start_date, end_date):
        process_single_date(d, session=session, out_root=out_root)


if __name__ == "__main__":
    main()
