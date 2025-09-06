"""
Retention Audit Script

Identifies Kaltura media entries covered by UCSD's 2-year / 4-year media
policies.
Input: one KMC CSV export or three merged exports
(ALL + optional Quizzes/YouTube).
Output: CSV of candidate entries, with optional non-ready rows.

Notes:
- Uses `media.get.lastPlayedAt` for recency (no analytics fallback).
- No storage/flavor calculations; all filtering is in-memory.
"""

from __future__ import annotations
import io
import re
import os
import time
import math
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from dotenv import load_dotenv
import random
import csv
import threading
from dateutil import parser as dateparser
from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.exceptions import KalturaClientException
from KalturaClient.Plugins.Core import KalturaSessionType
import platform
import subprocess

# ---- Compact warning aggregation (reduces noisy duplicate WARN lines) ----
WARN_COUNTS = {"dns": 0, "sdk_none": 0, "timeout": 0, "http": 0, "other": 0}
_LAST_WARN_SUMMARY_TS = 0.0

# ---- File logging (mirrors console) ----
LOG_FH = None  # set in __main__ once we know the logs folder


def _classify_warn(ex: Exception) -> str:
    s = str(ex)
    if "NameResolutionError" in s or "Failed to resolve" in s:
        return "dns"
    if "Read timed out" in s or "Operation timed out" in s:
        return "timeout"
    if "Max retries exceeded" in s or "HTTPSConnectionPool" in s:
        return "http"
    if "NoneType" in s and ("has no attribute" in s):
        return "sdk_none"
    return "other"


def _bump_warn(ex: Exception):
    kind = _classify_warn(ex)
    WARN_COUNTS[kind] = WARN_COUNTS.get(kind, 0) + 1


# ---- Thread-safe error log (CSV) ----
_ERR_LOCK = threading.Lock()
ERR_LOG_PATH: Optional[str] = None  # set at runtime


def init_error_log(path: str):
    """Create CSV error log with header (idempotent)."""
    global ERR_LOG_PATH
    ERR_LOG_PATH = path
    try:
        # Create/overwrite with header
        with open(path, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(["timestamp", "entry_id", "stage", "error"])
    except Exception as ex:
        log(f"[WARN] Could not initialize error log at {path}: {ex}")


def append_error(entry_id: str, stage: str, error: str):
    """Append a single error row; safe to call from multiple threads."""
    if not ERR_LOG_PATH:
        return
    try:
        with _ERR_LOCK:
            with open(ERR_LOG_PATH, 'a', newline='') as f:
                w = csv.writer(f)
                w.writerow([ts(), entry_id, stage, error])
    except Exception as ex:
        # Last-ditch stderr note; avoid raising
        log(
            f"[WARN] Failed to write to error log for {entry_id} ({stage}): "
            f"{ex}"
            )


# ---- Lightweight logging / progress helpers ----
START_TS = time.time()


def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def hhmmss(seconds: float) -> str:
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def log(msg: str):
    line = f"[{ts()}] {msg}"
    print(line, flush=True)
    if LOG_FH:
        try:
            LOG_FH.write(line + "\n")
            LOG_FH.flush()
        except Exception:
            pass


# ---- Console number formatting helpers (with thousands separators) ----
def fmt_int(n) -> str:
    """Format integers with commas (e.g., 12,345). Falls back gracefully."""
    try:
        return f"{int(n):,}"
    except Exception:
        try:
            return f"{float(n):,.0f}"
        except Exception:
            return str(n)


def fmt_int_w(n, width: int) -> str:
    """Right-align integer with commas to a fixed width (e.g., width 9)."""
    try:
        return f"{int(n):>{width},}"
    except Exception:
        try:
            return f"{float(n):>{width},.0f}"
        except Exception:
            s = str(n)
            return s.rjust(width)


# ---- Retry / backoff knobs ----
RETRIES = int(os.getenv("HTTP_RETRIES", "7"))
BACKOFF_BASE = float(os.getenv("HTTP_BACKOFF_BASE", "1.5"))
BACKOFF_MIN = float(os.getenv("HTTP_BACKOFF_MIN", "1.0"))
BACKOFF_MAX = float(os.getenv("HTTP_BACKOFF_MAX", "8.0"))
THROTTLE_MS = int(os.getenv("THROTTLE_MS", "0"))
# ---- Parallelism and batch knobs ----
# ---- Parallelism and batch knobs ----
# Auto-tune lookup workers: default = min(32, cpu*2), env overrides if set
_CPU = os.cpu_count() or 4
_DEFAULT_LOOKUP = max(4, min(32, _CPU * 2))
REPORT_LOOKUP_WORKERS = int(
    os.getenv("REPORT_LOOKUP_WORKERS", str(_DEFAULT_LOOKUP))
    )
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "200"))
PROGRESS_EVERY_SEC = float(os.getenv("PROGRESS_EVERY_SEC", "2.0"))

# Progress style: "multiline" (default) or "singleline"
PROGRESS_STYLE = os.getenv("PROGRESS_STYLE", "multiline").strip().lower()
# Verbosity for retry/backoff logs: set to 1/true to log every retry attempt,
# else periodic summary only
PROGRESS_VERBOSE_RETRY = os.getenv(
    "PROGRESS_VERBOSE_RETRY", "0"
    ).strip() not in ("0", "", "false", "False", "FALSE")
# Suppress periodic WARN summaries if set (default: True)
SUPPRESS_WARN_SUMMARY = os.getenv(
    "SUPPRESS_WARN_SUMMARY", "1"
    ).strip() not in ("0", "", "false", "False", "FALSE")


def log_progress_block(
    total_written: int,
    zero_seen: int,
    zero_written: int,
    api_submitted: int,
    api_completed: int,
    inflight: int,
    avg_rate: float,
    elapsed_sec: float,
):
    """
    Print progress either in a compact one-liner or a multi-line, neatly]
    aligned block. Controlled by PROGRESS_STYLE env var
    ("multiline" | "singleline").
    """
    global WARN_COUNTS
    c = WARN_COUNTS
    if PROGRESS_STYLE.startswith("single"):
        log(
            f"  …progress {fmt_int(total_written)} rows written "
            f"(zeros: seen {fmt_int(zero_seen)}, "
            f"written {fmt_int(zero_written)}; "
            f"API: submitted {fmt_int(api_submitted)}, "
            f"completed {fmt_int(api_completed)}, "
            f"in-flight {fmt_int(inflight)}; "
            f"avg {avg_rate:.1f} rows/s, elapsed {hhmmss(elapsed_sec)}; "
            f"warns dns={fmt_int(c['dns'])} sdk_none={fmt_int(c['sdk_none'])} "
            f"http={fmt_int(c['http'])} timeout={fmt_int(c['timeout'])} "
            f"other={fmt_int(c['other'])}"
            f", total_elapsed {hhmmss(time.time() - START_TS)})"
        )
        return

    # Multiline, aligned block
    header = "…progress (pipelined)"
    line1 = (
        f"  Written: {fmt_int_w(total_written, 9)} rows    "
        f"Avg: {avg_rate:>6.1f} rows/s    "
        f"Elapsed: {hhmmss(elapsed_sec)}"
        f"    Total Elapsed: {hhmmss(time.time() - START_TS)}"
    )
    line2 = (
        f"  Zero-play:  seen {fmt_int_w(zero_seen, 9)}  |  "
        f"written {fmt_int_w(zero_written, 9)}"
    )
    line3 = (
        f"  API:        submitted {fmt_int_w(api_submitted, 9)}  |  "
        f"completed {fmt_int_w(api_completed, 9)}  |  "
        f"in-flight {fmt_int_w(inflight, 7)}"
    )
    line4 = (
        f"  Warns:      dns {fmt_int_w(c['dns'], 6)}  |  "
        f"sdk_none {fmt_int_w(c['sdk_none'], 6)}  |  "
        f"http {fmt_int_w(c['http'], 6)}  |  "
        f"timeout {fmt_int_w(c['timeout'], 6)}  |  "
        f"other {fmt_int_w(c['other'], 6)}"
    )
    log("\n".join([header, line1, line2, line3, line4]))


# ---- CSV-mode progress logging helper ----
def log_csv_progress(
    out_written: int,
    zero_scanned: int,
    api_processed: int,
    api_written: int,
    zero_total: int,
    nonzero_total: int,
    baseline_ts: float,
    api_phase: bool,
):
    """
    Progress printer for split-CSV mode.

    Terminology:
      - "Candidates" = rows that have been written to the OUTPUT CSV
        (eligible for contact).
      - "Scanned" = rows examined so far (zero or nonzero input CSVs),
        whether or not they became candidates.
      - The average rows/sec resets when API phase begins (api_phase=True),
        using baseline_ts captured at that moment.
    """
    c = WARN_COUNTS
    elapsed = max(0.0, time.time() - baseline_ts)
    total = zero_total + nonzero_total
    scanned = zero_scanned + api_processed
    remaining_total = max(0, total - scanned)
    zero_left = max(0, zero_total - zero_scanned)
    api_left = max(0, nonzero_total - api_processed)

    # Phase-specific rate + ETA (reset at API start)
    if api_phase:
        phase_processed = max(0, api_processed)
        phase_left = api_left
        avg_rate = (phase_processed / elapsed) if elapsed > 0 else 0.0
    else:
        phase_processed = max(0, zero_scanned)
        phase_left = zero_left
        avg_rate = (phase_processed / elapsed) if elapsed > 0 else 0.0

    eta_sec = (
        int(phase_left / avg_rate) if avg_rate > 0 and phase_left > 0 else None
    )

    # Emit a true blank line (no timestamp) to separate blocks
    print("")  # deliberate: raw newline, not timestamped

    if PROGRESS_STYLE.startswith("single"):
        # Concise single-line variant if requested
        msg = (
            "…csv-progress "
            f"candidates {fmt_int(out_written)}/{fmt_int(total)} | "
            f"scanned {fmt_int(scanned)}/{fmt_int(total)} | "
            f"left {fmt_int(remaining_total)} (zero {fmt_int(zero_left)}"
            f" | api {fmt_int(api_left)}) ; "
            f"avg {avg_rate:.1f} rows/s ; "
            + (f"eta {hhmmss(eta_sec)} ; " if eta_sec is not None else "")
            + f"elapsed {hhmmss(elapsed)} ; "
            f"warns dns={fmt_int(c['dns'])} sdk_none={fmt_int(c['sdk_none'])} "
            f"http={fmt_int(c['http'])} "
            f"timeout={fmt_int(c['timeout'])} other={fmt_int(c['other'])}"
        )
        log(msg)
        return

    # Multi-line, aligned block (default)
    header = "…csv-progress"
    line1 = (
        f"  Candidates: {fmt_int_w(out_written, 9)} / {fmt_int_w(total, 9)} "
        f"rows   "
        f"Remaining: {fmt_int_w(remaining_total, 9)}   "
        f"Avg: {avg_rate:>6.1f} rows/s   "
        + (f"ETA: {hhmmss(eta_sec)}   " if eta_sec is not None else "")
        + f"Elapsed: {hhmmss(elapsed)}"
    )
    line2 = (
        f"  Scanned:    {fmt_int_w(scanned, 9)} / {fmt_int_w(total, 9)}   "
        f"(Zero {fmt_int_w(zero_scanned, 9)} / {fmt_int_w(zero_total, 9)} | "
        f"API  {fmt_int_w(api_processed, 9)} / {fmt_int_w(nonzero_total, 9)})"
    )
    line3 = (
        f"  Left:       Zero {fmt_int_w(zero_left, 9)}   |  "
        f"API {fmt_int_w(api_left, 9)}"
    )
    line4 = (
        f"  Warns:      dns {fmt_int_w(c['dns'], 6)}  |  "
        f"sdk_none {fmt_int_w(c['sdk_none'], 6)}  |  "
        f"http {fmt_int_w(c['http'], 6)}  |  "
        f"timeout {fmt_int_w(c['timeout'], 6)}  |  "
        f"other {fmt_int_w(c['other'], 6)}"
    )
    log("\n".join([header, line1, line2, line3, line4]))


def _is_retryable_error(ex: Exception) -> bool:
    s = str(ex)
    # Treat common transient/network and SDK null-response issues as retryable
    return (
        "Read timed out" in s
        or "Operation timed out" in s
        or "HTTPSConnectionPool" in s
        or "Max retries exceeded" in s
        or (
            "NoneType" in s and (
                "has no attribute" in s or "object has no attribute" in s
                )
            )
        or (
            isinstance(
                ex, KalturaClientException
                ) and getattr(ex, "code", None) in (-4,)
            )
    )


def retry_call(fn, *args, ctx: str = "", **kwargs):
    """Call fn with retries + exponential backoff on transient network errors.
    Args:
        ctx: short context label for logs, e.g., "media.get <entryId>".
    """
    attempt = 0
    global _LAST_WARN_SUMMARY_TS
    while True:
        try:
            return fn(*args, **kwargs)
        except Exception as ex:
            attempt += 1
            # classify and count
            _bump_warn(ex)
            is_retryable = _is_retryable_error(ex)
            if attempt > RETRIES or not is_retryable:
                label = ctx or getattr(fn, "__name__", "call")
                log(
                    f"[ERROR] Permanent failure after {attempt-1} retries: "
                    f"{label}: {ex}"
                    )
                try:
                    label = ctx or getattr(fn, "__name__", "call")
                    # If ctx includes an entry id, try to extract it;
                    # otherwise, pass empty id
                    entry_id_hint = ""
                    if ctx and ctx.startswith("media.get "):
                        entry_id_hint = ctx.split(" ", 1)[1]
                    append_error(entry_id_hint, label, str(ex))
                except Exception:
                    pass
                raise
            # backoff
            sleep_s = min(
                BACKOFF_MAX,
                BACKOFF_MIN * (BACKOFF_BASE ** (attempt - 1))
                )
            sleep_s = sleep_s * (0.7 + 0.6 * random.random())

            # Logging policy: either verbose per-attempt,
            # or periodic summary only
            if PROGRESS_VERBOSE_RETRY:
                label = ctx or getattr(fn, "__name__", "call")
                log(
                    f"[WARN] Retryable error ({label}): {ex} — "
                    f"backing off {sleep_s:.1f}s"
                    )
            else:
                if not SUPPRESS_WARN_SUMMARY:
                    now = time.time()
                    if now - _LAST_WARN_SUMMARY_TS >= max(
                        5.0, PROGRESS_EVERY_SEC * 3
                    ):
                        _LAST_WARN_SUMMARY_TS = now
                        c = WARN_COUNTS
                        log(
                            "[WARN] Retrying… "
                            f"dns={fmt_int(c['dns'])} "
                            f"sdk_none={fmt_int(c['sdk_none'])} "
                            f"http={fmt_int(c['http'])} "
                            f"timeout={fmt_int(c['timeout'])} "
                            f"other={fmt_int(c['other'])}"
                        )

            time.sleep(sleep_s)


# ---- ENV / CONFIG ----
load_dotenv()


def _int_from_env(*names: str, default: int = 0) -> int:
    """
    Return the first env var among *names that parses to an int.
    Missing/blank/non-numeric → ignored. Falls back to `default`.
    """
    for n in names:
        raw = os.getenv(n, "")
        if raw is None:
            continue
        s = str(raw).strip()
        if not s:
            continue
        try:
            return int(s)
        except ValueError:
            continue
    return default


# Helper: get first non-empty env var among names (string)
def _str_from_env(*names: str) -> str:
    """Return the first non-empty env var among *names, else empty string."""
    for n in names:
        v = os.getenv(n, "")
        if v and str(v).strip():
            return str(v).strip()
    return ""


# Prevent system sleep while this script runs (macOS only via `caffeinate`).
# Controlled by .env key PREVENT_COMPUTER_SLEEP with literal values True/False.
def _bool_from_env(name: str, default: bool = True) -> bool:
    raw = os.getenv(name, str(default))
    if raw is None:
        return default
    s = str(raw).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


PREVENT_COMPUTER_SLEEP = _bool_from_env("PREVENT_COMPUTER_SLEEP", default=True)

PARTNER_ID = int(os.getenv("PARTNER_ID", "0"))
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")
USER_ID = os.getenv("USER_ID", "")
PRIVILEGES = os.getenv("PRIVILEGES", "all:*,disableentitlement")

# Custom metadata profile ID for retention extensions
# (ExtendedUntil/extendedBy/requestDate).
# If unset/blank/non-numeric, treat as 0 (disabled).
CUSTOM_METADATA_PROFILE_ID = _int_from_env(
    "CUSTOM_METADATA_PROFILE_ID", default=0
    )
log(
    "Custom metadata profile: "
    + (
        "disabled"
        if not CUSTOM_METADATA_PROFILE_ID
        else str(CUSTOM_METADATA_PROFILE_ID)
    )
)

# Chunking knobs (week-by-week to avoid 10k match cap)

# Time zone handling (configurable via .env TIMEZONE, default PT)
TIMEZONE = os.getenv(
    "TIMEZONE", "America/Los_Angeles"
    ).strip() or "America/Los_Angeles"
try:
    TZ = ZoneInfo(TIMEZONE)
except Exception:
    log(
        f"[WARN] Invalid TIMEZONE '{TIMEZONE}', "
        f"falling back to America/Los_Angeles"
        )
    TZ = ZoneInfo("America/Los_Angeles")


# ---- Helper: prevent sleep with caffeinate (macOS only) ----
def keep_awake_with_caffeinate():
    """
    On macOS, launch `caffeinate` bound to this process so the system won't
    sleep. Returns the Popen handle (or None on non-Darwin or failure).
    Automatically exits when this process exits.
    """
    try:
        if platform.system() != "Darwin":
            return None
        pid = os.getpid()
        # -d: prevent display sleep, -i: prevent idle sleep,
        # -m: prevent disk sleep, # -s: prevent system sleep,
        # -u: declare user is active, -w PID: exit when PID exits
        return subprocess.Popen(["caffeinate", "-dimus", "-w", str(pid)])
    except Exception as ex:
        log(f"[WARN] Could not start caffeinate: {ex}")
        return None


# ---- Kaltura Client Bootstrap ----
def get_client() -> KalturaClient:
    # Fail-fast if credentials are missing (defensive – in case get_client is
    # called outside __main__)
    if PARTNER_ID == 0 or not (ADMIN_SECRET and ADMIN_SECRET.strip()):
        raise RuntimeError(
            "Missing PARTNER_ID or ADMIN_SECRET – cannot initialize "
            "Kaltura client."
        )
    cfg = KalturaConfiguration()
    cfg.serviceUrl = os.getenv("SERVICE_URL", "https://www.kaltura.com")
    cfg.timeout = int(os.getenv("HTTP_TIMEOUT", "900"))
    client = KalturaClient(cfg)
    ks = client.session.start(
        ADMIN_SECRET,
        USER_ID,
        KalturaSessionType.ADMIN,
        PARTNER_ID,
        expiry=86400,
        privileges=PRIVILEGES,
    )
    client.setKs(ks)
    return client


# ---- Helpers ----
def now_epoch() -> int:
    return int(time.time())


def to_pt_str(epoch: Optional[int]) -> str:
    if not epoch or epoch <= 0:
        return ""
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(TZ)
    return dt.strftime("%Y-%m-%d %I:%M %p")


# --- Helper: PT timestamp label for filenames ---
def now_label_pt() -> str:
    """Return a local timestamp label (per TIMEZONE), e.g., 2025-08-27-0946."""
    return datetime.now(
        tz=timezone.utc
    ).astimezone(TZ).strftime("%Y-%m-%d-%H%M")


# --- Helper: compute end-of-day PT epoch for REPORT_DATE ---
def asof_epoch_from_report_date(report_date_str: str) -> int:
    # Parse REPORT_DATE as end-of-day local time and return UTC epoch seconds.
    rpt_dt_local = dateparser.parse(report_date_str)
    if rpt_dt_local.tzinfo is None:
        rpt_dt_local = rpt_dt_local.replace(tzinfo=TZ)
    else:
        rpt_dt_local = rpt_dt_local.astimezone(TZ)
    rpt_dt_local = rpt_dt_local.replace(
        hour=23, minute=59, second=59, microsecond=0
    )
    return int(rpt_dt_local.astimezone(timezone.utc).timestamp())


# --- Helper: parse any KMC Excel date string or epoch to UTC epoch seconds ---
def parse_any_dt_to_epoch(val: str) -> Optional[int]:
    """Best-effort parse for KMC export date strings or epoch-like ints.
    Returns UTC epoch seconds or None."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    try:
        # numeric epoch seconds
        if isinstance(val, (int, float)):
            v = int(val)
            # Heuristic: treat 13-digit as ms
            if v > 10_000_000_000:
                v = int(v / 1000)
            return v
        s = str(val).strip()
        if not s:
            return None
        # Common KMC excel formats – let dateutil guess, assume naive as UTC
        dt = dateparser.parse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def parse_kmc_duration_to_seconds(val) -> int:
    """
    Convert KMC 'Duration' strings (typically MM:SS, sometimes H:MM:SS) into
    seconds. Accepts ints/floats (already seconds). Returns 0 on failure.
    """
    if val is None:
        return 0
    if isinstance(val, (int, float)) and not (
        isinstance(val, float) and math.isnan(val)
    ):
        return int(val)
    s = str(val).strip()
    if not s:
        return 0
    parts = s.split(":")
    try:
        if len(parts) == 2:
            mm, ss = int(parts[0]), int(parts[1])
            return mm * 60 + ss
        if len(parts) == 3:
            hh, mm, ss = int(parts[0]), int(parts[1]), int(parts[2])
            return hh * 3600 + mm * 60 + ss
        return int(float(s))
    except Exception:
        return 0


# --- Helpers for KMC Excel column normalization ---
def _norm(s: str) -> str:
    return (s or "").strip().lower().replace("_", " ")


def find_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cand = {_norm(c) for c in candidates}
    for col in df.columns:
        if _norm(col) in cand:
            return col
    return None


# ---- Policy checks ----
SEC_PER_YEAR = 365 * 24 * 3600
SEC_2Y = 2 * SEC_PER_YEAR
SEC_4Y = 4 * SEC_PER_YEAR


def classify_policy(
        created_at: int, last_play: Optional[int]
        ) -> Optional[str]:
    """Return '2year', '4year', or None if not in scope by age/watch rules."""
    age = now_epoch() - created_at
    last_gap = (now_epoch() - last_play) if last_play else None

    # 4-year: age >= 4y AND (last >= 4y OR never watched)
    if age >= SEC_4Y and (last_gap is None or last_gap >= SEC_4Y):
        return "4year"

    # Per contract: never-watched assets that are ≥2y old count as"not called
    # for playback for >24 months" and should be 2-year candidates. So, for
    # 2-year: 2y <= age < 4y AND (never watched OR last >= 2y)
    if SEC_2Y <= age < SEC_4Y and (last_gap is None or last_gap >= SEC_2Y):
        return "2year"

    return None


# --- Policy check at a specified report date ---
def classify_policy_at(
        created_at: int, last_play: Optional[int], asof_epoch: int
        ) -> Optional[str]:
    age = asof_epoch - created_at
    last_gap = (asof_epoch - last_play) if last_play else None
    if age >= SEC_4Y and (last_gap is None or last_gap >= SEC_4Y):
        return "4year"
    # Per contract: never-watched assets that are ≥2y old count as "not called
    # for playback for >24 months" and should be 2-year candidates. So, for
    # 2-year: 2y <= age < 4y AND (never watched OR last >= 2y)
    if SEC_2Y <= age < SEC_4Y and (last_gap is None or last_gap >= SEC_2Y):
        return "2year"
    return None


# --- CSV output for "formal" contact report ---
OUTPUT_CSV_HEADERS = [
    "policy",              # 2year | 4year | nonready
    "entry_id",            # from KMC "Entry ID"
    "entry_name",          # from KMC "Name"/"Title"
    "media_type",          # from KMC "Media Type" (string as-is)
    "created_on",          # PT string
    "last_updated",        # PT string (from KMC if present or SDK if fetched)
    "duration_seconds",    # parsed from KMC "Duration"
    "plays",               # from KMC "Plays"
    "status",              # from KMC "Status"
    "owner",               # from KMC "Owner" if present (else empty/SDK)
    "lastPlayedAt",        # PT string (empty for zero-plays)
    "reason"               # 0 plays | nonready | date watched | extended
]


def init_output_csv(path: str):
    # Create/overwrite CSV with header
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(OUTPUT_CSV_HEADERS)


def append_csv_rows(path: str, rows: List[Dict]):
    if not rows:
        return
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow([r.get(h, "") for h in OUTPUT_CSV_HEADERS])


# --- Input loader: CSV-only ---
def load_kmc_table(
        kmc_path: str, usecols: Optional[List[str]] = None
        ) -> pd.DataFrame:
    """Load a KMC export from .csv without mangling IDs/dates.
    We read as strings and coerce types later (e.g., plays).
    If `usecols` is provided, only those columns are loaded
    (faster & less memory).
    """
    ext = os.path.splitext(kmc_path)[1].lower()
    if ext != ".csv":
        raise ValueError(f"Only .csv input is supported, got: {ext}")
    return pd.read_csv(
        kmc_path,
        dtype=str,
        keep_default_na=True,
        usecols=usecols  # pandas will ignore if any names don't match
    ) if usecols else pd.read_csv(kmc_path, dtype=str, keep_default_na=True)


# ---- CSV input helpers for three-file merge mode ----
_ILLEGAL_XLSX_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def scrub_text(s: str) -> str:
    s = _ILLEGAL_XLSX_RE.sub("", s or "")
    return s.replace("\u2028", " ").replace("\u2029", " ")


def load_csv_as_text(path: str) -> pd.DataFrame:
    """Read CSV as raw text with liberal decoding and scrub control chars.
    Returns a pandas DataFrame with all columns as strings (no NA coercion)."""
    with open(path, "rb") as f:
        raw = f.read()
    for enc in ("utf-8-sig", "utf-8", "utf-16", "latin1"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise UnicodeDecodeError(
            "could not decode", b"", 0, 0, "unsupported encoding"
            )

    text = scrub_text(text)
    buf = io.StringIO(text)
    df = pd.read_csv(
        buf,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        quoting=csv.QUOTE_MINIMAL,
    )
    df.columns = [c.lstrip("\ufeff").strip() for c in df.columns]
    return df


def detect_id_col(df: pd.DataFrame, override: Optional[str] = None) -> str:
    if override and override in df.columns:
        return override
    col = find_col(df, ["entry id", "entryid", "id"]) or "Entry ID"
    if col not in df.columns:
        raise ValueError(
            "Could not locate Entry ID column. "
            f"Columns present: {list(df.columns)}"
        )
    return col


def detect_media_col(df: pd.DataFrame, override: Optional[str] = None) -> str:
    if override and override in df.columns:
        return override
    col = find_col(
        df, ["media type", "type", "entry media type", "media"]
        ) or "Media Type"
    if col not in df.columns:
        # Create it so we can assign values later
        df[col] = ""
    return col


def load_id_set(path: str, id_hint: Optional[str] = None) -> set[str]:
    df = load_csv_as_text(path)
    id_col = detect_id_col(df, id_hint)
    return set(df[id_col].astype(str))


def merge_media_subtypes(
        all_path: str, quizzes_path: Optional[str], youtube_path: Optional[str]
        ) -> pd.DataFrame:
    """Return a DataFrame based on ALL, with Media Type set to
    Quiz/YouTube/YouTube Quiz based on ID membership. Quizzes/YouTube paths
    are optional."""
    if not os.path.isfile(all_path):
        raise FileNotFoundError(f"ALL CSV not found: {all_path}")
    df_all = load_csv_as_text(all_path)
    id_col = detect_id_col(df_all)
    media_col = detect_media_col(df_all)

    quiz_ids: set[str] = set()
    yt_ids: set[str] = set()

    if quizzes_path and os.path.isfile(quizzes_path):
        quiz_ids = load_id_set(quizzes_path, id_col)
        log(f"Loaded {fmt_int(len(quiz_ids))} quiz IDs from {quizzes_path}")
    else:
        if quizzes_path:
            log(
                f"Info: quizzes CSV not found → continuing without quizzes. "
                f"(path: {quizzes_path})"
                )

    if youtube_path and os.path.isfile(youtube_path):
        yt_ids = load_id_set(youtube_path, id_col)
        log(f"Loaded {fmt_int(len(yt_ids))} YouTube IDs from {youtube_path}")
    else:
        if youtube_path:
            log(
                f"Info: YouTube CSV not found → continuing without YouTube. "
                f"(path: {youtube_path})"
                )

    if not quiz_ids and not yt_ids:
        log(
            "WARN: neither quizzes nor youtube lists were provided; "
            "media types will remain as-is from ALL CSV"
        )

    ids_series = df_all[id_col].astype(str)
    mask_yt = ids_series.isin(yt_ids)
    mask_quiz = ids_series.isin(quiz_ids)
    mask_both = mask_yt & mask_quiz
    mask_yt_only = mask_yt & ~mask_quiz
    mask_quiz_only = mask_quiz & ~mask_yt

    if yt_ids or quiz_ids:
        df_all.loc[mask_both, media_col] = "YouTube Quiz"
        df_all.loc[mask_yt_only, media_col] = "YouTube"
        df_all.loc[mask_quiz_only, media_col] = "Quiz"

    # scrub cells for safety
    for c in df_all.columns:
        df_all[c] = df_all[c].astype(str).map(scrub_text)
    return df_all


# --- Utility: coerce plays column to numeric ints ---
def plays_to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(int)


# --- Helper to build a unified output row from a Series/Dict ---
# Accepts either a pandas Series or a plain dict for `rowlike`.
# `plays_override` lets callers force plays to "0" for zero-play rows.
def build_out_row(
    rowlike: Dict,
    cols: Dict[str, Optional[str]],
    *,
    policy: str,
    created_epoch: int,
    last_play_epoch: Optional[int],
    reason: str,
    plays_override: Optional[str] = None,
) -> Dict:
    def _cell(key: str, default: str = "") -> str:
        col = cols.get(key, "")
        try:
            return str(rowlike.get(col, default))
        except Exception:
            return default

    plays_val = (
        plays_override if plays_override is not None else _cell("plays")
    )

    return {
        "policy": policy,
        "entry_id": _cell("entry"),
        "entry_name": _cell("title"),
        "media_type": _cell("media_type"),
        "created_on": to_pt_str(int(created_epoch)),
        "last_updated": to_pt_str(
            parse_any_dt_to_epoch(
                str(rowlike.get(cols.get("last_update", ""), ""))
            )
        ),
        "duration_seconds": parse_kmc_duration_to_seconds(
            rowlike.get(cols.get("duration", ""), "")
        ),
        "plays": plays_val,
        "status": _cell("status"),
        "owner": _cell("owner"),
        "lastPlayedAt": (
            to_pt_str(last_play_epoch) if last_play_epoch else ""
        ),
        "reason": reason,
    }


# --- Helper to resolve KMC columns for CSV pipeline ---
def resolve_kmc_columns(df: pd.DataFrame):
    """Return a dict of resolved column names from a KMC export DataFrame."""
    cols = {}
    cols["entry"] = find_col(df, [
        "entry id", "entryid", "id",
    ]) or "Entry ID"
    cols["title"] = find_col(df, [
        "title", "name",
    ]) or "Title"
    cols["created"] = find_col(df, [
        "creation date", "created at", "created on", "created_on",
        "created", "createdat", "creation time",
    ]) or "Created On"
    cols["plays"] = find_col(df, [
        "plays", "number of plays", "total plays",
    ]) or "Plays"
    cols["media_type"] = find_col(df, [
        "media type", "type", "entry media type", "media",
    ]) or None
    cols["last_update"] = find_col(df, [
        "last update date", "last updated", "last update", "update date",
    ]) or None
    cols["status"] = find_col(df, [
        "status", "entry status",
    ]) or None
    cols["owner"] = find_col(df, [
        "owner", "user id", "owner id", "uploader", "entry owner",
    ]) or None
    cols["duration"] = find_col(df, [
        "duration", "length", "entry duration",
    ]) or None
    return cols


def run_audit_from_kmc_single(
    kmc_export_path: str,
    report_date_str: str,
    out_csv_path: str,
    include_nonready: bool = True,
):
    # Delegate to the unified DataFrame path to avoid code duplication
    df = load_kmc_table(kmc_export_path, usecols=None)
    return run_audit_from_dataframe(
        df=df,
        report_date_str=report_date_str,
        out_csv_path=out_csv_path,
        include_nonready=include_nonready,
    )


# --- CSV pipeline for in-memory DataFrame (three-file merge mode) ---
def run_audit_from_dataframe(
    df: pd.DataFrame,
    report_date_str: str,
    out_csv_path: str,
    include_nonready: bool = True,
):
    asof_epoch = asof_epoch_from_report_date(report_date_str)
    asof_cutoff_2y = asof_epoch - SEC_2Y

    start_ts = time.time()
    init_output_csv(out_csv_path)
    log(f"CSV-mode (merged in-memory): REPORT_DATE={report_date_str}")
    log(f"asof={to_pt_str(asof_epoch)}")

    cols = resolve_kmc_columns(df)
    required = [cols["entry"], cols["created"], cols["plays"]]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Input is missing required column(s): {missing}. Found columns: "
            f"{list(df.columns)}"
            )

    df = df.copy()
    df["_created_epoch"] = (
        df[cols["created"]].apply(parse_any_dt_to_epoch).fillna(0).astype(int)
        )
    pre_rows = len(df)
    df = df[df["_created_epoch"] <= asof_cutoff_2y]
    log(
        f"Prefiltered by age: kept {fmt_int(len(df))}/{fmt_int(pre_rows)} "
        f"rows (created ≤ 2y before report date)"
        )

    status_series = (
        df[cols["status"]].astype(str).str.strip().str.lower()
        if cols.get("status") and cols["status"] in df.columns
        else pd.Series(["ready"] * len(df), index=df.index)
    )
    plays_series = plays_to_int(df[cols["plays"]])
    is_ready = status_series.eq("ready")
    is_nonready = ~is_ready
    is_zero = plays_series.eq(0) & is_ready
    is_nonzero = plays_series.gt(0) & is_ready

    zero_total = int(is_zero.sum())
    nonzero_total = int(is_nonzero.sum())
    zero_scanned = 0
    zero_written = 0
    api_processed = 0
    api_written = 0
    out_written = 0
    _last_progress = time.time()
    FEEDBACK_EVERY_SEC = PROGRESS_EVERY_SEC
    api_start_ts = None

    if include_nonready and is_nonready.any():
        try:
            nrd = df.loc[is_nonready]
            nr_batch: List[Dict] = []
            nr_written = 0
            for _, row in nrd.iterrows():
                out = build_out_row(
                    row, cols,
                    policy="nonready",
                    created_epoch=int(row["_created_epoch"]),
                    last_play_epoch=None,
                    reason="non_ready_status",
                )
                nr_batch.append(out)
                if len(nr_batch) >= FLUSH_EVERY:
                    append_csv_rows(out_csv_path, nr_batch)
                    out_written += len(nr_batch)
                    nr_written += len(nr_batch)
                    nr_batch = []
                    if (time.time() - _last_progress) >= FEEDBACK_EVERY_SEC:
                        log("\n")
                        log("…csv-progress (non-ready pass)")
                        log(
                            f"  Written non-ready: {fmt_int_w(nr_written, 9)}"
                            f"   Elapsed: {hhmmss(time.time() - start_ts)}"
                        )
                        _last_progress = time.time()
            append_csv_rows(out_csv_path, nr_batch)
            out_written += len(nr_batch)
            nr_written += len(nr_batch)
            if nr_batch:
                log("\n")
                log("…csv-progress (non-ready pass)")
                log(
                    f"  Written non-ready: {fmt_int_w(nr_written, 9)}   "
                    f"Elapsed: {hhmmss(time.time() - start_ts)}"
                )
            log(f"Wrote non-ready candidates to {out_csv_path}")
            log(f"(rows: {fmt_int(nr_written)})")
        except Exception as ex:
            log(f"[WARN] Failed to process non-ready rows in-memory: {ex}")

    zdf = df.loc[is_zero].copy()
    zero_batch: List[Dict] = []
    for _, row in zdf.iterrows():
        zero_scanned += 1
        policy = classify_policy_at(
            int(row["_created_epoch"]), None, asof_epoch
            )
        if not policy:
            continue
        out = build_out_row(
            row, cols,
            policy=policy,
            created_epoch=int(row["_created_epoch"]),
            last_play_epoch=None,
            reason="zero_plays",
            plays_override="0",
        )
        zero_batch.append(out)
        if len(zero_batch) >= FLUSH_EVERY:
            append_csv_rows(out_csv_path, zero_batch)
            out_written += len(zero_batch)
            zero_written += len(zero_batch)
            zero_batch = []
            if (time.time() - _last_progress) >= FEEDBACK_EVERY_SEC:
                log_csv_progress(
                    out_written, zero_scanned, api_processed, api_written,
                    zero_total, nonzero_total, start_ts, False
                    )
                _last_progress = time.time()

    # Final flush
    if zero_batch:
        append_csv_rows(out_csv_path, zero_batch)
        out_written += len(zero_batch)
        zero_written += len(zero_batch)
        zero_batch = []
        if (time.time() - _last_progress) >= FEEDBACK_EVERY_SEC:
            log_csv_progress(
                out_written, zero_scanned, api_processed, api_written,
                zero_total, nonzero_total, start_ts, False
                )
            _last_progress = time.time()

    log(f"Wrote zero-plays candidates to {out_csv_path}")
    log_csv_progress(
        out_written, zero_scanned, api_processed, api_written, zero_total,
        nonzero_total, start_ts, False
        )

    ndf = df.loc[is_nonzero].copy()
    api_start_ts = time.time()
    client: Optional[KalturaClient] = None

    def _client() -> KalturaClient:
        nonlocal client
        if client is None:
            client = get_client()
        return client

    lock = threading.Lock()
    api_buf: List[Dict] = []

    def _process_row(row_dict: Dict):
        entry_id = str(row_dict.get(cols["entry"], ""))
        created = int(row_dict.get("_created_epoch", 0))
        try:
            e = retry_call(
                _client().media.get, entry_id, ctx=f"media.get {entry_id}"
                )
            if e is None:
                raise RuntimeError("media.get returned None")
            lp = getattr(e, "lastPlayedAt", None)
            last_play = int(lp) if lp else None
        except Exception as ex:
            _bump_warn(ex)
            append_error(entry_id, "media.get", str(ex))
            e = None
            last_play = None

        policy = classify_policy_at(created, last_play, asof_epoch)
        if not policy:
            return None

        return build_out_row(
            row_dict, cols,
            policy=policy,
            created_epoch=created,
            last_play_epoch=last_play,
            reason="not_watched_within_window",
        )

    with ThreadPoolExecutor(max_workers=REPORT_LOOKUP_WORKERS) as pool:
        futures = []
        for row in ndf.to_dict(orient="records"):
            futures.append(pool.submit(_process_row, row))
            if len(futures) % (FLUSH_EVERY // 2 or 1) == 0:
                for fut in list(futures):
                    if fut.done():
                        futures.remove(fut)
                        r = fut.result()
                        api_processed += 1
                        if r:
                            api_buf.append(r)
                if len(api_buf) >= FLUSH_EVERY:
                    with lock:
                        append_csv_rows(out_csv_path, api_buf)
                        api_written += len(api_buf)
                        out_written += len(api_buf)
                        api_buf = []
                if (time.time() - _last_progress) >= FEEDBACK_EVERY_SEC:
                    log_csv_progress(
                        out_written, zero_scanned, api_processed, api_written,
                        zero_total, nonzero_total, api_start_ts, True
                        )
                    _last_progress = time.time()

        for fut in as_completed(futures):
            r = fut.result()
            api_processed += 1
            if r:
                api_buf.append(r)
            if len(api_buf) >= FLUSH_EVERY:
                with lock:
                    append_csv_rows(out_csv_path, api_buf)
                    api_written += len(api_buf)
                    out_written += len(api_buf)
                    api_buf = []
            if (time.time() - _last_progress) >= FEEDBACK_EVERY_SEC:
                log_csv_progress(
                    out_written, zero_scanned, api_processed, api_written,
                    zero_total, nonzero_total, api_start_ts, True
                    )
                _last_progress = time.time()

    append_csv_rows(out_csv_path, api_buf)
    api_written += len(api_buf)
    out_written += len(api_buf)

    log_csv_progress(
        out_written,
        zero_scanned,
        api_processed,
        api_written,
        zero_total,
        nonzero_total,
        api_start_ts,
        True,
    )
    log(
        f"CSV pipeline (merged in-memory) complete in "
        f"{hhmmss(time.time() - start_ts)}. Output → {out_csv_path}"
    )
    log(
        f"Summary: zero {fmt_int(zero_written)}/{fmt_int(zero_total)}, "
        f"api processed {fmt_int(api_processed)}/{fmt_int(nonzero_total)}, "
        f"written total {fmt_int(out_written)}"
    )


def init_logfile(base_dir: str) -> str:
    """Initialize logs folder and logfile, assigning handle to global LOG_FH.
    Returns the absolute path to the logfile."""
    global LOG_FH
    logs_dir = os.path.join(base_dir, "logs")
    try:
        os.makedirs(logs_dir, exist_ok=True)
    except Exception:
        # Fall back to base_dir if we can't create the folder
        logs_dir = base_dir
    log_name = f"media-retention-report_{now_label_pt()}.log"
    logfile = os.path.join(logs_dir, log_name)
    try:
        LOG_FH = open(logfile, "a", encoding="utf-8")
        print(f"[{ts()}] Logging to {logfile}", flush=True)
    except Exception:
        LOG_FH = None  # continue without file logging
    return logfile


if __name__ == "__main__":
    # --- Environment flags / filenames ---
    # Prefer RETENTION_INPUT_FILENAME for clarity; fall back to legacy
    # INPUT_CSV_FILENAME for compatibility
    RETENTION_INPUT_FILENAME = (
        os.getenv("RETENTION_INPUT_FILENAME", "").strip()
        or os.getenv("INPUT_CSV_FILENAME", "").strip()
    )
    OUTPUT_CSV_FILENAME = os.getenv("OUTPUT_CSV_FILENAME", "").strip()
    REPORT_DATE = os.getenv("REPORT_DATE", "").strip()

    # --- Credential sanity check (fail-fast) ---
    # We require PARTNER_ID (non-zero) and ADMIN_SECRET (non-empty) for any
    # Kaltura API lookups. USER_ID remains optional.
    if PARTNER_ID == 0 or not (ADMIN_SECRET and ADMIN_SECRET.strip()):
        log(
            "[ERROR] Missing Kaltura credentials. Please set PARTNER_ID "
            "(non-zero) and ADMIN_SECRET in your .env."
            )
        log("Example:")
        log("  PARTNER_ID=1234567")
        log("  ADMIN_SECRET=*****")
        raise SystemExit(1)

    # Timestamped outputs: default ON. Set OUTPUT_TIMESTAMP=0 to disable.

    INCLUDE_NONREADY = (
        os.getenv("INCLUDE_NONREADY", "1").strip().lower()
        not in ("0", "false", "no", "n")
    )
    OUTPUT_TIMESTAMP = (
        os.getenv("OUTPUT_TIMESTAMP", "1").strip().lower()
        not in ("0", "false", "")
    )

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Prevent system sleep on macOS while the script runs (optional via .env)
    _caff = None
    if PREVENT_COMPUTER_SLEEP:
        _caff = keep_awake_with_caffeinate()
        if _caff:
            log(
                "macOS caffeinate engaged (system sleep disabled for the "
                "duration of this run)."
                )

    # Initialize logs folder and logfile
    _ = init_logfile(base_dir)

    def _abs_or_join(p: str) -> str:
        return (
            p
            if (p and os.path.isabs(p))
            else (os.path.join(base_dir, p) if p else "")
        )

    def _maybe_timestamp(path: str) -> str:
        if not path:
            return path
        if not OUTPUT_TIMESTAMP:
            return path
        root, ext = os.path.splitext(path)
        return f"{root}_{now_label_pt()}{ext or ''}"

    # Initialize compact error log alongside the output CSV/XLSX we write.
    def _init_errlog_for(out_path: str):
        if not out_path:
            return
        err_path = os.path.splitext(out_path)[0] + "_errors.csv"
        init_error_log(err_path)
        log(f"Error log → {err_path}")

    # Three-file merge mode inputs (ALL required, Quizzes/YouTube optional)
    KMC_EXPORT_ALL_FILENAME = os.getenv("KMC_EXPORT_ALL_FILENAME", "").strip()
    KMC_EXPORT_QUIZ_FILENAME = os.getenv(
        "KMC_EXPORT_QUIZ_FILENAME", ""
        ).strip()
    KMC_EXPORT_YOUTUBE_FILENAME = os.getenv(
        "KMC_EXPORT_YOUTUBE_FILENAME", ""
        ).strip()

    # ------------------------------
    # Three-file merge mode (ALL + optional Quizzes/YouTube), all in-memory
    # ------------------------------
    if (
        not RETENTION_INPUT_FILENAME
        and KMC_EXPORT_ALL_FILENAME
        and OUTPUT_CSV_FILENAME
        and REPORT_DATE
    ):
        all_path = _abs_or_join(KMC_EXPORT_ALL_FILENAME)
        quizzes_path = (
            _abs_or_join(KMC_EXPORT_QUIZ_FILENAME)
            if KMC_EXPORT_QUIZ_FILENAME else ""
        )
        youtube_path = (
            _abs_or_join(KMC_EXPORT_YOUTUBE_FILENAME)
            if KMC_EXPORT_YOUTUBE_FILENAME else ""
        )

        if not os.path.exists(all_path):
            log(f"ERROR: ALL CSV not found at: {all_path}")
            raise SystemExit(2)

        out_csv = _maybe_timestamp(_abs_or_join(OUTPUT_CSV_FILENAME))
        _init_errlog_for(out_csv)

        log("Merging media sub-types from KMC exports (in-memory)…")
        df_merged = merge_media_subtypes(all_path, quizzes_path, youtube_path)
        run_audit_from_dataframe(
            df=df_merged,
            report_date_str=REPORT_DATE,
            out_csv_path=out_csv,
            include_nonready=INCLUDE_NONREADY,
        )
        raise SystemExit(0)

    # ------------------------------
    # Single-input mode (one CSV; no temp split files)
    # ------------------------------
    if RETENTION_INPUT_FILENAME and OUTPUT_CSV_FILENAME and REPORT_DATE:
        in_path = _abs_or_join(RETENTION_INPUT_FILENAME)
        if not os.path.exists(in_path):
            log(f"ERROR: Input CSV not found at: {in_path}")
            raise SystemExit(2)

        out_csv = _maybe_timestamp(_abs_or_join(OUTPUT_CSV_FILENAME))
        _init_errlog_for(out_csv)
        run_audit_from_kmc_single(
            kmc_export_path=in_path,
            report_date_str=REPORT_DATE,
            out_csv_path=out_csv,
            include_nonready=INCLUDE_NONREADY,
        )
        raise SystemExit(0)

    log("ERROR: Missing required env vars.")
    log(
        "  Single-file mode requires: RETENTION_INPUT_FILENAME, "
        "OUTPUT_CSV_FILENAME, REPORT_DATE."
    )
    log(
        "  Three-file mode requires: KMC_EXPORT_ALL_FILENAME "
        "[optional KMC_EXPORT_QUIZ_FILENAME, KMC_EXPORT_YOUTUBE_FILENAME], "
        "OUTPUT_CSV_FILENAME, REPORT_DATE."
    )
    raise SystemExit(2)
