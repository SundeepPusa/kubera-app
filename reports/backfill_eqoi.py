# -*- coding: utf-8 -*-
"""
Backfill PFEOI (Combined Delta Equivalent OI)
from 18-Nov-2024 up to **today** (inclusive).

We only skip:
  - Weekends (Saturday/Sunday).

For **every weekday** in this range, we attempt to fetch the PFEOI
("F&O – Combine Delta Equivalent Open Interest across exchanges (csv)")
report from NSE using the same api/reports archives URL pattern that
worked for 31-Oct-2025.

If NSE does not have the PFEOI file for a given weekday (holiday, older
period with a different report name, etc.), the fetch layer will raise
an error (404 / non-CSV). We log that day as failed and continue.

You can safely re-run this script; ETL is idempotent because of
ON CONFLICT (trade_date, symbol_ci) upsert in fetch_pfeoi.etl().

Run from repo root on your laptop (newenv active, .env pointing to AWS):

    python -m reports.backfill_eqoi
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import List, Tuple

from .fetch_pfeoi import run_one_day, log

# Same pattern you used for 31-Oct-2025, but with {DDMONYYYY} placeholder.
ARCHIVES_TEMPLATE = (
    "https://www.nseindia.com/api/reports?"
    "archives=%5B%7B%22name%22%3A%22F%26O%20-%20Combine%20Delta%20Equivalent%20Open%20Interest%20across%20exchanges%20(csv)%22"
    "%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%5D"
    "&date={DDMONYYYY}&type=equity&mode=single"
)

# Backfill window: 18-Nov-2024 → today (inclusive)
START_DATE = dt.date(2024, 11, 18)
END_DATE = dt.date.today()


# ---------- Trading-day helper (weekend-only filter) ----------


def is_weekend(d: dt.date) -> bool:
    """
    Return True if d is Saturday or Sunday, else False.

    We intentionally do NOT depend on bhavcopy here; every weekday
    between START_DATE and END_DATE is attempted.
    """
    # Monday = 0 ... Sunday = 6
    return d.weekday() >= 5  # 5=Sat, 6=Sun


def make_args_for_date(d: dt.date) -> SimpleNamespace:
    """
    Build a lightweight args object compatible with fetch_pfeoi.run_one_day(...).

    For backfill we keep retries/timeouts modest so that 404 / missing-report
    days don't waste too much time.
    """
    ddmonyyyy = d.strftime("%d-%b-%Y")  # e.g. 20-Nov-2024
    url = ARCHIVES_TEMPLATE.format(DDMONYYYY=ddmonyyyy)

    return SimpleNamespace(
        archives_json=url,
        direct_url=None,
        check_only=False,
        fetch_only=False,
        etl_only=False,
        skip_ddl=False,
        retries=1,   # backfill: single attempt per URL set
        timeout=15,  # backfill: shorter timeout, faster over older days
        outdir=None,
        file=None,
        refresh=False,
    )


# ---------- Main loop ----------


def main() -> None:
    d = START_DATE

    total_days: int = 0
    skipped_weekend: int = 0
    attempted_days: int = 0
    success_days: int = 0
    failed_days: int = 0
    failed_dates: List[Tuple[dt.date, str]] = []

    while d <= END_DATE:
        total_days += 1

        if is_weekend(d):
            skipped_weekend += 1
            log(f"[BACKFILL] Skip {d} (weekend)")
            d += dt.timedelta(days=1)
            continue

        # Weekday → attempt PFEOI fetch + ETL
        attempted_days += 1
        log(f"[BACKFILL] Running PFEOI for {d}")
        args = make_args_for_date(d)

        try:
            run_one_day(d, args)
            success_days += 1
        except Exception as e:
            # Days with missing PFEOI archive or transient HTTP issues land here.
            failed_days += 1
            failed_dates.append((d, str(e)))
            log(f"[BACKFILL] ERROR on {d}: {e}")

        d += dt.timedelta(days=1)

    # Summary
    log(
        f"[BACKFILL] PFEOI {START_DATE} → {END_DATE} completed (loop finished). "
        f"total_days={total_days}, "
        f"skipped_weekend={skipped_weekend}, "
        f"attempted={attempted_days}, "
        f"success={success_days}, "
        f"failed={failed_days}"
    )

    if failed_dates:
        log("[BACKFILL] Sample failed dates (first 10):")
        for d_fail, err in failed_dates[:10]:
            log(f"  - {d_fail}: {err}")


if __name__ == "__main__":
    main()
