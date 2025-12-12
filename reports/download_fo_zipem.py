# reports/download_fo_zipem.py
"""
Download NSE Derivatives "ALL F&O reports" bundles via:
    - /api/zipem   (Daily cluster → fileURLs=...)
    - /api/reports (Archives cluster → archives=...)

Features:
    - Take a date range: --start YYYY-MM-DD [--end YYYY-MM-DD]
    - Skip weekends (Sat/Sun)
    - For each trade_date, create:

          data/nse/fo_all_reports/YYYY/MM/DD/
              zipem/      # extracted Daily cluster (FO all reports)
              archives/   # extracted Archives cluster (derivatives)

    Optionally keep or delete master ZIP bundles after extraction
    (controlled by KEEP_MASTER_ZIPS flag).
"""

import argparse
import datetime as dt
import logging
from pathlib import Path
from typing import Iterable

import requests
from requests.adapters import HTTPAdapter, Retry
import zipfile
import shutil


# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

LOG = logging.getLogger("download_fo_zipem")

DATA_ROOT = Path("data") / "nse" / "fo_all_reports"

BASE_ZIPEM_URL = "https://www.nseindia.com/api/zipem"
BASE_FO_ARCHIVES_URL = "https://www.nseindia.com/api/reports"

# If False, we delete fo_zipem_YYYY-MM-DD.zip and fo_archives_YYYY-MM-DD.zip
# after successful extraction to save disk space.
KEEP_MASTER_ZIPS = False

# This template is taken from your **FO all-reports zipem URL** for
# trade_date = 2025-12-04. It is exactly the value of the `fileURLs=`
# query parameter (WITHOUT the leading "fileURLs=" and WITHOUT the
# trailing "&type=Daily").
#
# Original you ran:
#   https://www.nseindia.com/api/zipem?fileURLs=...&type=Daily
#
ZIPEM_FILEURL_PARAM_TEMPLATE = (
    "%5B%22https%3A%2F%2Fnsearchives.nseindia.com%2Farchives%2Fnsccl%2Fspan%2Fnsccl.20251204.i1.zip%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Farchives%2Fnsccl%2Fspan%2Fnsccl.20251204.i2.zip%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Farchives%2Fnsccl%2Fspan%2Fnsccl.20251204.i3.zip%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Farchives%2Fnsccl%2Fspan%2Fnsccl.20251204.i4.zip%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Farchives%2Fnsccl%2Fspan%2Fnsccl.20251204.i5.zip%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Farchives%2Fnsccl%2Fspan%2Fnsccl.20251204.s.zip%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2Fequities%2FAPPSEC_COLLVAL_04122025.csv%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Farchives%2Ffo%2Fmf_haircut%2FMF_VAR_04122025.csv%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2Ffo%2Ffo.zip%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Farchives%2Fnsccl%2Fmwpl%2Fncloi_04122025.zip%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2Fnsccl%2Fmwpl_cli_03122025.xls%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2Fnsccl%2Ffao_participant_vol_04122025.csv%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2Fnsccl%2Ffao_participant_oi_04122025.csv%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2Fnsccl%2Ffao_top10cm_to_04122025.csv%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Farchives%2Ffo%2Fmkt%2Ffo04122025.zip%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Farchives%2Fnsccl%2Fsett%2FFOSett_prce_04122025.csv%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Farchives%2Fnsccl%2Fvolt%2FFOVOLT_04122025.csv%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2Fnsccl%2Foi_cli_limit_04-DEC-2025.lst%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2Ffo%2Ffii_stats_04-Dec-2025.xls%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Farchives%2Fexp_lim%2Fael_04122025.csv%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2FFO_Latency_stats03122025.csv%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2Ftrdops%2FFNO_BC04122025.DAT%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2Ffo%2FBhavCopy_NSE_FO_0_0_0_20251204_F_0000.csv.zip%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2Ffo%2FNSE_FO_contract_04122025.csv.gz%22%2C%22https%3A%2F%2Fnsearchives.nseindia.com%2Fcontent%2Ffo%2FNSE_FO_spdcontract_04122025.csv.gz%22%5D"
)

# Template reference date
ZIPEM_TEMPLATE_DATE = dt.date(2025, 12, 4)

# Derivatives Archives template: query-string part from your original
# /api/reports URL, starting from "archives=".
FO_ARCHIVES_PARAM_ENCODED = (
    "archives=%5B%7B%22name%22%3A%22F%26O%20-%20Span%20Risk%20Parameter%20File%20(1st%20intra-day)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Span%20Risk%20Parameter%20File%20(2nd%20intra-day)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Span%20Risk%20Parameter%20File%20(3rd%20intra-day)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Span%20Risk%20Parameter%20File%20(4th%20intra-day)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Span%20Risk%20Parameter%20File%20(End%20of%20day)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Span%20Risk%20Parameter%20File%20(Begin%20of%20day)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Clients%20Position%20%25%20greater%20than%20equal%20to%203%25%20of%20Stock%20MWPL(xls)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Category-wise%20Turnover%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Market%20Activity%20Report%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Daily%20Volatility%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Daily%20Settlement%20Prices%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Bhavcopy(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Bhavcopy%20(fo.zip)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Combined%20Report%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20NCL%20Open%20Interest%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Security%20in%20ban%20period%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Combine%20Open%20Interest%20across%20exchanges%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Combine%20Delta%20Equivalent%20Open%20Interest%20across%20exchanges%20(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Participant%20wise%20Open%20Interest(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Participant%20wise%20Trading%20Volumes(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Top%2010%20Clearing%20Member%20Volumes(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Base%20Prices%20for%20illliquid%20Contracts%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Clientwise%20Position%20Limits%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20FII%20Derivatives%20Statistics%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Exposure%20Limit%20file%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Derivatives%20Update%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Exercise%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Bhavcopy%20File%20(DAT)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Common%20Bhavcopy(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20Mode%20of%20Trading%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O%20-%20UDiFF%20Common%20Bhavcopy%20Final%20(zip)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O-MII%20-%20Contract%20File%20(.gz)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O-MII%20-%20Spd%20Contract%20File%20(.gz)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%2C%7B%22name%22%3A%22F%26O-NCL%20Contract%20Delta%20(csv)%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%5D&date=04-Dec-2025&type=Archives"
)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}


# -------------------------------------------------------------------
# SESSION & DATE HELPERS
# -------------------------------------------------------------------

def make_session() -> requests.Session:
    """Create a requests Session with retries and NSE-friendly headers."""
    sess = requests.Session()
    sess.headers.update(DEFAULT_HEADERS)

    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


def daterange(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    """Yield dates from start to end inclusive."""
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def ensure_date_folder(trade_date: dt.date) -> Path:
    """
    Ensure folder: data/nse/fo_all_reports/YYYY/MM/DD and return it.
    """
    folder = (
        DATA_ROOT
        / str(trade_date.year)
        / f"{trade_date.month:02d}"
        / f"{trade_date.day:02d}"
    )
    folder.mkdir(parents=True, exist_ok=True)
    return folder


# -------------------------------------------------------------------
# URL BUILDERS
# -------------------------------------------------------------------

def build_zipem_url(trade_date: dt.date) -> str:
    """
    Build the /api/zipem URL for a given trade_date by transforming the
    captured template (for ZIPEM_TEMPLATE_DATE) to the requested date.

    Handles:
        20251204      (YYYYMMDD)
        04122025      (DDMMYYYY)
        03122025      (prev-business-day DDMMYYYY for mwpl_cli, latency)
        04-DEC-2025   (DD-MMM-YYYY uppercase)
        04-Dec-2025   (DD-Mmm-YYYY mixed case)
    """
    base = ZIPEM_FILEURL_PARAM_TEMPLATE

    ref = ZIPEM_TEMPLATE_DATE
    ref_prev = ref - dt.timedelta(days=1)

    tgt = trade_date

    # Previous business day for mwpl_cli / latency:
    if tgt.weekday() == 0:  # Monday → use Friday
        tgt_prev = tgt - dt.timedelta(days=3)
    else:
        tgt_prev = tgt - dt.timedelta(days=1)

    # 1) Replace previous-day DDMMYYYY first
    ref_prev_ddmmyyyy = ref_prev.strftime("%d%m%Y")
    tgt_prev_ddmmyyyy = tgt_prev.strftime("%d%m%Y")
    base = base.replace(ref_prev_ddmmyyyy, tgt_prev_ddmmyyyy)

    # 2) Then replace same-day tokens
    ref_yyyymmdd = ref.strftime("%Y%m%d")
    tgt_yyyymmdd = tgt.strftime("%Y%m%d")
    base = base.replace(ref_yyyymmdd, tgt_yyyymmdd)

    ref_ddmmyyyy = ref.strftime("%d%m%Y")
    tgt_ddmmyyyy = tgt.strftime("%d%m%Y")
    base = base.replace(ref_ddmmyyyy, tgt_ddmmyyyy)

    # 3) DD-MMM-YYYY variants
    ref_dd_mmm_yyyy_upper = ref.strftime("%d-%b-%Y").upper()
    tgt_dd_mmm_yyyy_upper = tgt.strftime("%d-%b-%Y").upper()
    base = base.replace(ref_dd_mmm_yyyy_upper, tgt_dd_mmm_yyyy_upper)

    ref_dd_mmm_yyyy_mixed = ref.strftime("%d-%b-%Y")
    tgt_dd_mmm_yyyy_mixed = tgt.strftime("%d-%b-%Y")
    base = base.replace(ref_dd_mmm_yyyy_mixed, tgt_dd_mmm_yyyy_mixed)

    url = f"{BASE_ZIPEM_URL}?fileURLs={base}&type=Daily"
    LOG.info("[URL DEBUG] %s | ZIPEM URL built: %s", trade_date, url)
    return url


def build_archives_url(trade_date: dt.date) -> str:
    """
    Build the /api/reports (derivatives Archives cluster) URL for a date.

    Uses FO_ARCHIVES_PARAM_ENCODED (captured once) and injects
    the correct &date=DD-MMM-YYYY&type=Archives.
    """
    base = FO_ARCHIVES_PARAM_ENCODED

    # If captured string already had &date=&type=&mode=, strip them.
    for marker in ("&date=", "&type=", "&mode="):
        idx = base.find(marker)
        if idx != -1:
            base = base[:idx]

    date_str = trade_date.strftime("%d-%b-%Y")  # e.g. 04-Dec-2025
    url = f"{BASE_FO_ARCHIVES_URL}?{base}&date={date_str}&type=Archives"
    LOG.info("[URL DEBUG] %s | ARCHIVES URL built: %s", trade_date, url)
    return url


# -------------------------------------------------------------------
# FILE / ZIP HELPERS
# -------------------------------------------------------------------

def download_to_file(
    sess: requests.Session,
    url: str,
    dest: Path,
    label: str,
) -> None:
    """
    Download a response from NSE to dest.

    Always overwrites dest (we want fresh bundles each run).
    """
    LOG.info("[%s] Downloading from NSE → %s", label, dest)
    with sess.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    size_mb = dest.stat().st_size / (1024 * 1024)
    LOG.info("[%s] Saved file: %s (%.2f MB)", label, dest, size_mb)


def clean_folder(folder: Path, label: str) -> None:
    """
    Hard-clean a folder before extraction:
        - If it exists, remove it completely (shutil.rmtree).
        - Then recreate it empty.

    This guarantees that only fresh files for that trade_date are present.
    """
    if folder.exists():
        LOG.info("[%s] Cleaning folder before extract: %s", label, folder)
        try:
            shutil.rmtree(folder)
        except PermissionError as exc:
            LOG.error(
                "[%s] PermissionError while deleting %s. "
                "Make sure no file from this folder is open in Excel/Explorer. Error: %s",
                label,
                folder,
                exc,
            )
            # Re-raise so pipeline clearly fails instead of silently mixing dates
            raise
    folder.mkdir(parents=True, exist_ok=True)


def extract_zip(zip_path: Path, target_folder: Path, label: str) -> None:
    """
    Extract a ZIP into target_folder.

    We first hard-clean the folder (delete + recreate) to avoid
    mixing files from old runs.
    """
    LOG.info("[%s] Extracting %s → %s", label, zip_path, target_folder)
    clean_folder(target_folder, label)

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(target_folder)
    except PermissionError as exc:
        LOG.error(
            "[%s] PermissionError during extraction. "
            "A file under %s might be open. Error: %s",
            label,
            target_folder,
            exc,
        )
        raise

    LOG.info("[%s] Extraction complete.", label)


def maybe_delete(zip_path: Path, label: str) -> None:
    """
    Delete the given zip_path if KEEP_MASTER_ZIPS is False.
    """
    if KEEP_MASTER_ZIPS:
        return

    if zip_path.exists():
        try:
            size_mb = zip_path.stat().st_size / (1024 * 1024)
            LOG.info(
                "[%s] Deleting master ZIP %s (%.2f MB) to save space",
                label,
                zip_path,
                size_mb,
            )
            zip_path.unlink()
        except Exception as exc:
            LOG.warning(
                "[%s] Failed to delete master ZIP %s: %s",
                label,
                zip_path,
                exc,
            )


def count_files(folder: Path) -> int:
    """Count all files (non-directories) under a folder (non-recursive)."""
    if not folder.exists():
        return 0
    return sum(1 for p in folder.iterdir() if p.is_file())


# -------------------------------------------------------------------
# CORE PER-DATE PIPELINE
# -------------------------------------------------------------------

def process_single_date(sess: requests.Session, trade_date: dt.date) -> None:
    """
    For a single trade_date:

        YYYY/MM/DD/
            zipem/
            archives/

    (Master ZIPs are optionally deleted after extraction based on
    KEEP_MASTER_ZIPS flag.)
    """
    LOG.info("[%s] ===== FO downloads starting =====", trade_date)
    date_folder = ensure_date_folder(trade_date)

    # --- 1) ZIPEM DAILY CLUSTER (FO all reports) ---
    zipem_zip = date_folder / f"fo_zipem_{trade_date.isoformat()}.zip"
    zipem_folder = date_folder / "zipem"

    zipem_url = build_zipem_url(trade_date)
    download_to_file(sess, zipem_url, zipem_zip, label=f"{trade_date} | ZIPEM")
    extract_zip(zipem_zip, zipem_folder, label=f"{trade_date} | ZIPEM")
    maybe_delete(zipem_zip, label=f"{trade_date} | ZIPEM")
    zipem_count = count_files(zipem_folder)
    LOG.info("[%s | ZIPEM] Files in zipem folder: %d", trade_date, zipem_count)

    # --- 2) DERIVATIVES ARCHIVES CLUSTER ---
    archives_zip = date_folder / f"fo_archives_{trade_date.isoformat()}.zip"
    archives_folder = date_folder / "archives"

    archives_url = build_archives_url(trade_date)
    download_to_file(
        sess,
        archives_url,
        archives_zip,
        label=f"{trade_date} | ARCHIVES",
    )
    extract_zip(archives_zip, archives_folder, label=f"{trade_date} | ARCHIVES")
    maybe_delete(archives_zip, label=f"{trade_date} | ARCHIVES")
    archives_count = count_files(archives_folder)
    LOG.info("[%s | ARCHIVES] Files in archives folder: %d", trade_date, archives_count)

    LOG.info(
        "[%s] ✅ FO downloads completed. zipem=%d, archives=%d, total=%d",
        trade_date,
        zipem_count,
        archives_count,
        zipem_count + archives_count,
    )


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Download NSE Derivatives (F&O) ALL-REPORTS bundles via zipem + archives."
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start trade date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        help="End trade date (YYYY-MM-DD). If omitted, only --start is processed.",
    )

    args = parser.parse_args(argv)

    start_date = dt.date.fromisoformat(args.start)
    end_date = dt.date.fromisoformat(args.end) if args.end else start_date

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    LOG.info("F&O ALL-REPORTS download from %s to %s", start_date, end_date)
    LOG.info("Output root: %s", DATA_ROOT.resolve())
    LOG.info("KEEP_MASTER_ZIPS = %s", KEEP_MASTER_ZIPS)

    sess = make_session()

    for d in daterange(start_date, end_date):
        # Skip weekends – F&O is closed
        if d.weekday() >= 5:  # 5 = Sat, 6 = Sun
            LOG.info("[%s] Weekend – skipping", d)
            continue

        try:
            process_single_date(sess, d)
        except Exception as exc:
            LOG.error("[%s] ❌ ERROR in F&O download: %s", d, exc, exc_info=True)

    LOG.info("F&O download loop complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
