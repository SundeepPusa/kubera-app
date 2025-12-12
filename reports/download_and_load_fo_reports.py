# -*- coding: utf-8 -*-
"""
Unified NSE F&O loader — Zipem Download + Local Extract + DB Upsert

This script combines:
  - reports.download_fo_zipem.process_single_date
      → Uses NSE "zipem" + "archives" API to download all required
        F&O reports for a given trade_date into:

            data/nse/fo_all_reports/YYYY/MM/DD/

  - reports.load_fo_reports.process_single_date
      → Extracts nested ZIPs (Bhavcopy, combine OI, participant OI/Vol,
        MWPL, NCL, etc.) and upserts rows into eod.fo_* tables in Postgres.

Flow for each date in [start, end]:
  1) Skip weekends (Saturday / Sunday).
  2) Run download_fo_zipem.process_single_date(...) to fetch all archives.
  3) Run load_fo_reports.process_single_date(conn, trade_date) to load DB.
  4) Commit per-date; on DB error, rollback that date but continue others.

ASSUMPTIONS / NOTES
-------------------
* This is a LOCAL-ONLY script — it expects your code checkout and
  data folder structure to look like:

      <repo_root>/
          reports/
              download_fo_zipem.py
              load_fo_reports.py
              ...
          data/
              nse/
                  fo_all_reports/
                      YYYY/
                          MM/
                              DD/
                                  archives/...
                                  zipem/...

* The Zipem + Archives URL templates are embedded inside
  reports.download_fo_zipem (captured once for 2025-12-04 and then
  dynamically rewritten for any trade_date). You NO LONGER need to
  pass zipem / archives URLs on the CLI. This script just delegates
  to download_fo_zipem.process_single_date().

USAGE EXAMPLES
--------------
Single trading day:

    python -m reports.download_and_load_fo_reports \
        --date 2025-12-04

Date range (1 month), skipping weekends:

    python -m reports.download_and_load_fo_reports \
        --start 2025-11-01 \
        --end   2025-11-30

"""

import argparse
import datetime as dt
import logging
from typing import Iterable

import requests

from reports import download_fo_zipem
from reports import load_fo_reports


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logger = logging.getLogger("download_and_load_fo_reports")


def _setup_logging(level: int = logging.INFO) -> None:
    """Configure a simple root logger if not already configured."""
    if logging.getLogger().handlers:
        # Assume already configured by caller/other module.
        return

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _daterange(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    """Yield each date from start to end inclusive."""
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def _is_weekend(d: dt.date) -> bool:
    """Return True if date is Saturday (5) or Sunday (6)."""
    return d.weekday() >= 5


# ---------------------------------------------------------------------------
# Core per-date pipeline
# ---------------------------------------------------------------------------

def process_single_date(
    sess: requests.Session,
    conn,
    trade_date: dt.date,
) -> None:
    """
    Run the FULL F&O pipeline for a single trade_date:

        1) Skip weekends.
        2) Download all F&O archives via download_fo_zipem.
        3) Upsert into DB via load_fo_reports.process_single_date.

    Any download failure is treated as "holiday / no data" and the DB
    step is skipped. Any DB error rolls back that date but does not
    stop the entire range.
    """
    date_str = trade_date.isoformat()

    # 1) Weekend guard
    if _is_weekend(trade_date):
        logger.info("[%s] Weekend (Sat/Sun) → skipping.", date_str)
        return

    logger.info("[%s] ===== NSE F&O PIPELINE (DOWNLOAD + LOAD) START =====", date_str)

    # 2) Download F&O bundles using zipem + archives API
    logger.info("[%s] Step 1/2: Download F&O bundles via zipem...", date_str)
    try:
        # New download_fo_zipem.process_single_date signature:
        #   process_single_date(sess: Session, trade_date: date) -> None
        download_fo_zipem.process_single_date(
            sess=sess,
            trade_date=trade_date,
        )
    except Exception as exc:
        logger.exception(
            "[%s] Download step failed (treat as holiday / no data). "
            "Skipping DB load. Error: %s",
            date_str,
            exc,
        )
        logger.info("[%s] ===== NSE F&O PIPELINE (DOWNLOAD ONLY) END (FAILED) =====", date_str)
        return

    logger.info("[%s] Step 1/2: Download complete.", date_str)

    # 3) Load into Postgres using load_fo_reports
    logger.info("[%s] Step 2/2: Upserting F&O reports into Postgres...", date_str)
    try:
        load_fo_reports.process_single_date(conn, trade_date)
        conn.commit()
    except Exception as exc:
        logger.exception(
            "[%s] DB upsert failed. Rolling back this date. Error: %s",
            date_str,
            exc,
        )
        conn.rollback()
        logger.info("[%s] ===== NSE F&O PIPELINE (LOAD FAILED) END =====", date_str)
        return

    logger.info("[%s] ===== NSE F&O PIPELINE (DOWNLOAD + LOAD) DONE =====", date_str)


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Kubera EOD — NSE F&O combined loader (zipem download + DB load).\n\n"
            "You can run for a single date (--date) or a date range "
            "(--start ... --end ...). Weekends are automatically skipped."
        )
    )

    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--date",
        help="Single trade date (YYYY-MM-DD).",
    )
    date_group.add_argument(
        "--start",
        help="Start trade_date (YYYY-MM-DD) for a range (inclusive).",
    )
    parser.add_argument(
        "--end",
        help="End trade_date (YYYY-MM-DD) for a range (inclusive). "
             "Required if --start is used.",
    )

    # No more --zipem-url / --archives-url; URLs are generated inside download_fo_zipem
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    _setup_logging()
    args = parse_args()

    # Resolve date / date range
    if args.date:
        start_date = end_date = dt.datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        if not args.end:
            raise SystemExit("--end is required when --start is provided.")
        start_date = dt.datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = dt.datetime.strptime(args.end, "%Y-%m-%d").date()

    if end_date < start_date:
        raise SystemExit("End date cannot be before start date.")

    logger.info(
        "F&O combined loader starting for range [%s → %s]",
        start_date.isoformat(),
        end_date.isoformat(),
    )

    # Prepare a requests.Session for all downloads in this run
    # Use the same helper as download_fo_zipem main so headers/retries match.
    sess = download_fo_zipem.make_session()

    # Prepare DB connection (same as load_fo_reports.main)
    conn = load_fo_reports.get_db_conn()
    try:
        for trade_date in _daterange(start_date, end_date):
            process_single_date(
                sess=sess,
                conn=conn,
                trade_date=trade_date,
            )
    finally:
        conn.close()

    logger.info(
        "F&O combined loader finished for range [%s → %s]",
        start_date.isoformat(),
        end_date.isoformat(),
    )


if __name__ == "__main__":
    main()
