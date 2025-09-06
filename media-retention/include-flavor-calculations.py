# pyright: reportMissingImports=false
"""
include-flavor-calculations.py

Enriches a media-retention-report candidates CSV with two columns:
  - number_of_flavors
  - bytes_saved

Reads input/output settings and performance knobs from .env.
"""

from __future__ import annotations
import csv
import os
import time
import queue
import threading
import platform
import subprocess
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from dotenv import load_dotenv
from KalturaClient import KalturaConfiguration, KalturaClient
from KalturaClient.Plugins.Core import (
    KalturaSessionType,
    KalturaFilterPager,
    KalturaAssetFilter,
)

# -------------------- ENV / CONFIG --------------------
load_dotenv()


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def _bool(name: str, default: bool) -> bool:
    val = os.getenv(name, str(default))
    if val is None:
        return default
    s = str(val).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


PARTNER_ID = int(os.getenv("PARTNER_ID", "0") or 0)
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "").strip()
USER_ID = os.getenv("USER_ID", "").strip() or "admin@kaltura.com"
PRIVILEGES = os.getenv("PRIVILEGES", "all:*,disableentitlement").strip()

INPUT_CSV = os.getenv(
    "FLAVOR_INPUT_FILENAME", "retention_candidates.csv"
    ).strip()
OUTPUT_CSV = os.getenv("FLAVOR_OUTPUT_FILENAME", "").strip()
FLAVOR_LOOKUP_WORKERS = _int("FLAVOR_LOOKUP_WORKERS", 6)
THROTTLE_MS = _int("THROTTLE_MS", 0)
HTTP_RETRIES = _int("HTTP_RETRIES", 3)

PREVENT_COMPUTER_SLEEP = _bool("PREVENT_COMPUTER_SLEEP", True)
PROGRESS_EVERY_SEC = float(os.getenv("PROGRESS_EVERY_SEC", "2.0"))
PROGRESS_STYLE = os.getenv("PROGRESS_STYLE", "singleline").strip().lower()

TIMEZONE = os.getenv(
    "TIMEZONE", "America/Los_Angeles"
    ).strip() or "America/Los_Angeles"
try:
    TZ = ZoneInfo(TIMEZONE)
except Exception:
    TZ = ZoneInfo("America/Los_Angeles")


def _now_label_tz() -> str:
    # Return timestamp label in configured TIMEZONE, e.g., 2025-09-04-161027.
    return datetime.now(
        tz=timezone.utc
        ).astimezone(TZ).strftime("%Y-%m-%d-%H%M%S")


ts = _now_label_tz()
if OUTPUT_CSV:
    base, ext = os.path.splitext(OUTPUT_CSV)
    OUTPUT_CSV = f"{base}_{ts}{ext or '.csv'}"
else:
    base, ext = os.path.splitext(INPUT_CSV or "retention_candidates.csv")
    OUTPUT_CSV = f"{base}_with_flavors_{ts}.csv"


# -------------------- LOGGING --------------------

def log(msg: str) -> None:
    ts = datetime.now(
        tz=timezone.utc
        ).astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


# -------------------- Kaltura Session --------------------

def get_client() -> KalturaClient:
    if not PARTNER_ID or not ADMIN_SECRET:
        log("ERROR: PARTNER_ID and ADMIN_SECRET must be set in .env")
        raise SystemExit(2)
    conf = KalturaConfiguration(PARTNER_ID)
    conf.serviceUrl = os.getenv(
        "KALTURA_SERVICE_URL", "https://www.kaltura.com/"
        ).strip() or "https://www.kaltura.com/"
    client = KalturaClient(conf)
    ks = client.session.start(
        ADMIN_SECRET,
        USER_ID,
        KalturaSessionType.ADMIN,
        PARTNER_ID,
        86400,
        PRIVILEGES,
    )
    client.setKs(ks)
    return client


# -------------------- Flavor calculations --------------------

def _is_source_flavor(asset) -> bool:
    try:
        if getattr(asset, "isOriginal", None) in (1, True):
            return True
    except Exception:
        pass
    try:
        tags = (getattr(asset, "tags", "") or "").lower()
        if "source" in tags:
            return True
    except Exception:
        pass
    return False


def flavor_assets_for_entry(client: KalturaClient, entry_id: str) -> List:
    """Return list of flavor assets for entry (handles paging)."""
    flt = KalturaAssetFilter()
    flt.entryIdEqual = entry_id
    pager = KalturaFilterPager(pageSize=500, pageIndex=1)

    out = []
    while True:
        last_err: Optional[Exception] = None
        for attempt in range(HTTP_RETRIES):
            try:
                resp = client.flavorAsset.list(flt, pager)
                objs = resp.objects or []
                out.extend(objs)
                break
            except Exception as ex:  # SDK wraps many errors
                last_err = ex
                time.sleep(min(2 ** attempt, 8))
        else:
            raise last_err  # type: ignore

        if THROTTLE_MS:
            time.sleep(THROTTLE_MS / 1000.0)
        if len(resp.objects or []) < pager.pageSize:
            break
        pager.pageIndex += 1
    return out


def _asset_size_bytes(a) -> int:
    """Return asset size in bytes; fall back to KB field when sizeInBytes==0.
    The Kaltura SDK/endpoint sometimes returns `sizeInBytes=0` while `size`
    (KB) is populated (as shown in KMC). We normalize here.
    """
    try:
        sib = int(getattr(a, "sizeInBytes", 0) or 0)
    except Exception:
        sib = 0
    if sib and sib > 0:
        return sib
    try:
        sz_kb = int(getattr(a, "size", 0) or 0)
    except Exception:
        sz_kb = 0
    if sz_kb and sz_kb > 0:
        return sz_kb * 1024
    return 0


def compute_counts_and_bytes(
        client: KalturaClient, entry_id: str, policy: str
        ) -> Tuple[int, int]:
    """Return (number_of_flavors, bytes_saved) per rules above."""
    try:
        assets = flavor_assets_for_entry(client, entry_id)
    except Exception as ex:
        log(f"[WARN] flavor list failed for {entry_id}: {ex}")
        return 0, 0

    n = len(assets)
    if n == 0:
        return 0, 0

    policy_l = (policy or "").strip().lower()
    total = 0
    for a in assets:
        size = _asset_size_bytes(a)
        if size <= 0:
            continue
        if policy_l == "2year" and _is_source_flavor(a):
            # In 2-year policy, keep source; savings excludes source flavor
            continue
        total += size
    return n, total


def keep_awake_with_caffeinate():
    """On macOS, keep the machine awake while this process runs."""
    try:
        if platform.system() != "Darwin":
            return None
        pid = os.getpid()
        return subprocess.Popen(["caffeinate", "-dimus", "-w", str(pid)])
    except Exception as ex:
        log(f"[WARN] Could not start caffeinate: {ex}")
        return None

# -------------------- Worker pool --------------------


class WorkerPool:
    def __init__(self, client_factory, workers: int = 6):
        self.q: "queue.Queue[Tuple[int, dict]]" = (
            queue.Queue(maxsize=workers * 4)
        )
        self.results: Dict[int, Tuple[int, int]] = {}
        self.errs: Dict[int, str] = {}
        self._threads: List[threading.Thread] = []
        self._stop = threading.Event()
        self.client_factory = client_factory
        self.workers = max(1, workers)
        self.processed = 0
        self.lock = threading.Lock()
        self.total = 0
        self.queued = 0
        self.fast_processed = 0  # rows that short-circuit (e.g., no media)
        self.api_processed = 0   # rows that required API calls

    def _run(self):
        client = self.client_factory()
        while not self._stop.is_set():
            try:
                idx, row = self.q.get(timeout=0.25)
            except queue.Empty:
                continue
            try:
                entry_id = row.get("entry_id", "") or row.get("id", "")
                policy = row.get("policy", "")
                status = (row.get("status", "") or "").strip().lower()
                if "no media" in status:
                    self.results[idx] = (0, 0)
                    with self.lock:
                        self.fast_processed += 1
                else:
                    self.results[idx] = (
                        compute_counts_and_bytes(client, entry_id, policy)
                    )
                    with self.lock:
                        self.api_processed += 1
            except Exception as ex:
                self.errs[idx] = str(ex)
                self.results[idx] = (0, 0)
            finally:
                with self.lock:
                    self.processed += 1
                self.q.task_done()

    def start(self):
        for _ in range(self.workers):
            t = threading.Thread(target=self._run, daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self):
        self._stop.set()
        for t in self._threads:
            t.join(timeout=1.0)


def _fmt_int(n: int) -> str:
    return f"{n:,}"


def _hhmmss(sec: float) -> str:
    sec = int(max(0, sec))
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def start_progress_thread(
        pool: "WorkerPool", start_ts: float, stop_evt: threading.Event
        ) -> threading.Thread:
    def run():
        while not stop_evt.is_set():
            elapsed = time.time() - start_ts
            with pool.lock:
                done = pool.processed
                queued = pool.queued
                fast = pool.fast_processed
                api = pool.api_processed
            total = pool.total
            backlog = max(0, queued - done)
            # Overall rate (all processed entries per second)
            rate_all = (done / elapsed) if elapsed > 0 else 0.0
            # Separate observed rates
            rate_fast = (fast / elapsed) if elapsed > 0 else 0.0
            rate_api = (api / elapsed) if elapsed > 0 else 0.0

            left = max(0, total - done)
            # Estimate composition of remaining work using observed mix so far
            fast_ratio = (fast / done) if done > 0 else 0.0
            est_fast_left = int(left * fast_ratio)
            est_api_left = max(0, left - est_fast_left)

            # Compute ETAs for each bucket and blend
            eta_fast = (est_fast_left / rate_fast) if rate_fast > 0 else 0.0
            eta_api = (est_api_left / rate_api) if rate_api > 0 else 0.0
            eta_blended = None
            if eta_fast or eta_api:
                # If one is zero (unknown), fall back to the other
                eta_blended = (eta_fast or 0.0) + (eta_api or 0.0)

            parts = [
                "flavors-progress processed ",
                f"{_fmt_int(done)}/{_fmt_int(total)} | ",
                f"queued {_fmt_int(queued)} | backlog {_fmt_int(backlog)} ; ",
                (
                    f"rate {rate_all:.1f}/s (api {rate_api:.1f}/s, "
                    f"fast {rate_fast:.1f}/s) ; "
                ),
            ]
            if eta_blended is not None and eta_blended > 0:
                parts.append(f"eta {_hhmmss(eta_blended)} ; ")
            parts.append(f"elapsed {_hhmmss(elapsed)}")
            line = "".join(parts)

            if PROGRESS_STYLE == "singleline":
                print("\r" + line + " ", end="", flush=True)
            else:
                log(line)
            stop_evt.wait(PROGRESS_EVERY_SEC)
        if PROGRESS_STYLE == "singleline":
            print()
    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t

# -------------------- fast row-count helper --------------------
# Return number of data rows (excluding header) without loading
# the file into memory.


def count_csv_rows(path: str) -> int:
    try:
        with open(path, 'r', encoding='utf-8', newline='') as f:
            # Count lines; subtract one for header if file not empty
            total_lines = sum(1 for _ in f)
            return max(0, total_lines - 1)
    except Exception:
        return 0


# -------------------- CSV I/O --------------------

def enrich_csv():
    if not os.path.isfile(INPUT_CSV):
        log(f"ERROR: Input CSV not found: {INPUT_CSV}")
        raise SystemExit(2)

    total_entries = count_csv_rows(INPUT_CSV)

    log(f"Reading candidates: {INPUT_CSV}")
    log(f"Total entries detected: {total_entries}")
    log(f"Flavor lookup workers: {FLAVOR_LOOKUP_WORKERS}")
    with open(INPUT_CSV, newline="", encoding="utf-8") as rf:
        reader = csv.DictReader(rf)
        base_fieldnames = list(reader.fieldnames or [])
        # Ensure required columns exist
        required = {"entry_id", "policy", "status"}
        missing = [c for c in required if c not in base_fieldnames]
        if missing:
            log(f"ERROR: Input CSV missing required columns: {missing}")
            raise SystemExit(2)

        out_fields = base_fieldnames + ["number_of_flavors", "bytes_saved"]
        log(f"Writing enriched CSV: {OUTPUT_CSV}")
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as wf:
            writer = csv.DictWriter(wf, fieldnames=out_fields)
            writer.writeheader()

            pool = WorkerPool(get_client, workers=FLAVOR_LOOKUP_WORKERS)
            pool.total = total_entries  # fixed total based on CSV rows
            pool.start()

            start_ts = time.time()
            stop_evt = threading.Event()
            prog_t = start_progress_thread(pool, start_ts, stop_evt)

            in_rows: List[dict] = []
            for idx, row in enumerate(reader):
                in_rows.append(row)
                pool.q.put((idx, row))
                with pool.lock:
                    pool.queued += 1
                if idx % 10000 == 0 and idx > 0:
                    log(f"Queued {idx:,} entries…")

            # Wait for completion
            pool.q.join()

            stop_evt.set()
            prog_t.join(timeout=1.0)

            # Write rows in original order with appended columns
            for idx, row in enumerate(in_rows):
                nf, bs = pool.results.get(idx, (0, 0))
                row["number_of_flavors"] = nf
                row["bytes_saved"] = bs
                writer.writerow(row)
                if THROTTLE_MS:
                    time.sleep(THROTTLE_MS / 1000.0)
            pool.stop()

    log("Done.\n  Input : %s\n  Output: %s" % (INPUT_CSV, OUTPUT_CSV))

# -------------------- main --------------------


if __name__ == "__main__":
    try:
        _caff = None
        if PREVENT_COMPUTER_SLEEP:
            _caff = keep_awake_with_caffeinate()
            if _caff:
                log(
                    "macOS caffeinate engaged (system sleep disabled for this "
                    "run)."
                    )
        enrich_csv()
    except KeyboardInterrupt:
        log("Interrupted by user.")
        raise SystemExit(130)
