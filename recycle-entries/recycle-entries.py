

"""recycle-entries.py

Recycle Kaltura media entries in bulk from a CSV list of entry IDs.

- Reads INPUT_FILENAME and COLUMN_HEADER_ENTRY_ID from .env
- Recycles each entry via baseEntry.recycle
- Writes an output CSV in an output/ subfolder with all original columns plus:
    - recycled_success (true/false)
    - failure_reason (stable enum)
    - failure_detail (human readable)
    - recycled_at (timestamp in TIMEZONE)

Notes:
- DRY_RUN=true will not recycle; it will only report what WOULD happen.
- VALIDATE_ENTRY_EXISTS=true will call baseEntry.get before recycle to provide
  clearer failure reasons and to optionally detect already-recycled entries.
- REQUEST_TIMEOUT_SEC sets the timeout for Kaltura API requests.
- ADMIN_SECRET is prompted at runtime and never read from .env.
- RECYCLE_RATE_PER_SEC paces recycle calls to stay under Kaltura's recycle
  throttle, which rejects the excess with ACTION_BLOCKED rather than queueing
  it. ACTION_BLOCKED responses are retried with backoff.
"""

from __future__ import annotations

import csv
import dataclasses
import getpass
import os
import signal
import sys
import threading
import time
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    # Python 3.8 fallback (shouldn't be needed for most modern macOS installs)
    ZoneInfo = None  # type: ignore


# Optional dependency; we fail with a friendly message if not installed.
try:
    from dotenv import load_dotenv
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: python-dotenv. Install with: pip install python-dotenv"
    ) from exc


try:
    from wakepy import keep
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: wakepy. Install with: pip install wakepy"
    ) from exc


try:
    from KalturaClient import KalturaClient, KalturaConfiguration
    from KalturaClient.Plugins.Core import (
        KalturaSessionType,
        # These may not exist in all client versions; we only use if present.
        KalturaEntryStatus,
    )
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: KalturaClient. Ensure the Kaltura Python client is installed."
    ) from exc


# -----------------------------
# Failure reason enums
# -----------------------------
FAIL_INVALID_ENTRY_ID = "invalid_entry_id"
FAIL_ENTRY_DOES_NOT_EXIST = "entry_does_not_exist"
FAIL_PERMISSION_DENIED = "permission_denied"
FAIL_RATE_LIMITED = "rate_limited"
FAIL_KALTURA_ERROR = "kaltura_error"
FAIL_UNKNOWN_ERROR = "unknown_error"
FAIL_CONNECTION_ERROR = "connection_error"
FAIL_ALREADY_RECYCLED_OR_INVALID_STATUS = "already_recycled_or_invalid_status"
FAIL_CHILD_ENTRY = "child_entry"
FAIL_ACTION_BLOCKED = "action_blocked"


STOP_EVENT = threading.Event()


@dataclass(frozen=True)
class EnvConfig:
    partner_id: int
    admin_secret: str
    user_id: str
    service_url: str
    privileges: str

    timezone: str

    input_filename: str
    column_header_entry_id: str

    dry_run: bool
    validate_entry_exists: bool
    validate_entry_exists_in_dry_run: bool

    max_workers: int
    recycle_rate_per_sec: float
    blocked_retries: int
    blocked_retry_delay: float
    max_retries: int
    backoff_base_sec: float
    request_delay_sec: float
    request_timeout_sec: int
    request_connect_timeout_sec: int
    progress_every: int


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    return int(val.strip())


def _env_float(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    return float(val.strip())


def load_env() -> EnvConfig:
    load_dotenv()

    partner_id = os.getenv("PARTNER_ID")
    user_id = os.getenv("USER_ID")
    service_url = os.getenv("SERVICE_URL")
    privileges = os.getenv("PRIVILEGES", "")

    timezone = os.getenv("TIMEZONE", "America/Los_Angeles")

    input_filename = os.getenv("INPUT_FILENAME")
    column_header_entry_id = os.getenv("COLUMN_HEADER_ENTRY_ID")

    missing = [
        name
        for name, val in [
            ("PARTNER_ID", partner_id),
            ("USER_ID", user_id),
            ("SERVICE_URL", service_url),
            ("INPUT_FILENAME", input_filename),
            ("COLUMN_HEADER_ENTRY_ID", column_header_entry_id),
        ]
        if not val
    ]
    if missing:
        raise RuntimeError("Missing required env vars: " + ", ".join(missing))

    dry_run = _env_bool("DRY_RUN", default=False)
    validate_entry_exists = _env_bool("VALIDATE_ENTRY_EXISTS", default=True)
    validate_entry_exists_in_dry_run = _env_bool(
        "VALIDATE_ENTRY_EXISTS_IN_DRY_RUN",
        default=False,
    )
    max_workers = max(1, _env_int("MAX_WORKERS", default=3))
    # Kaltura throttles recycles. Measured on a production account: while the
    # script attempted ~22 calls/sec, successful recycles held at ~2.0/sec
    # and the excess (~91%) came back as ACTION_BLOCKED. Pacing the calls
    # below that ceiling means nearly all of them land. 0 disables pacing.
    recycle_rate_per_sec = max(
        0.0, _env_float("RECYCLE_RATE_PER_SEC", default=1.5)
    )
    # ACTION_BLOCKED is usually a throttle response, not a permanent state,
    # so it is worth retrying after a pause.
    blocked_retries = max(0, _env_int("BLOCKED_RETRIES", default=4))
    blocked_retry_delay = max(1.0, _env_float("BLOCKED_RETRY_DELAY", default=10))
    max_retries = max(0, _env_int("MAX_RETRIES", default=3))
    backoff_base_sec = max(0.0, _env_float("BACKOFF_BASE_SEC", default=0.5))
    request_delay_sec = max(0.0, _env_float("REQUEST_DELAY_SEC", default=0.0))
    request_timeout_sec = max(5, _env_int("REQUEST_TIMEOUT_SEC", default=30))
    request_connect_timeout_sec = max(
        3,
        _env_int("REQUEST_CONNECT_TIMEOUT_SEC", default=10),
    )
    progress_every = max(1, _env_int("PROGRESS_EVERY", default=1))

    return EnvConfig(
        partner_id=int(partner_id),
        admin_secret="",  # set interactively in main() via getpass
        user_id=str(user_id),
        service_url=str(service_url),
        privileges=str(privileges),
        timezone=str(timezone),
        input_filename=str(input_filename),
        column_header_entry_id=str(column_header_entry_id),
        dry_run=dry_run,
        validate_entry_exists=validate_entry_exists,
        validate_entry_exists_in_dry_run=validate_entry_exists_in_dry_run,
        max_workers=max_workers,
        recycle_rate_per_sec=recycle_rate_per_sec,
        blocked_retries=blocked_retries,
        blocked_retry_delay=blocked_retry_delay,
        max_retries=max_retries,
        backoff_base_sec=backoff_base_sec,
        request_delay_sec=request_delay_sec,
        request_timeout_sec=request_timeout_sec,
        request_connect_timeout_sec=request_connect_timeout_sec,
        progress_every=progress_every,
    )


def now_in_tz(timezone_name: str) -> datetime:
    if ZoneInfo is None:
        # Fallback: naive local time
        return datetime.now()

    try:
        tz = ZoneInfo(timezone_name)
    except Exception:
        # If TIMEZONE is invalid, fallback to local time.
        return datetime.now()

    return datetime.now(tz)


def timestamp_string(timezone_name: str) -> str:
    # Format: YYYY-MM-DD-TTTT (e.g., 2026-03-04-0724)
    dt = now_in_tz(timezone_name)
    return dt.strftime("%Y-%m-%d-%H%M")


class RateLimiter:
    """Spaces calls evenly across all worker threads.

    Threads reserve the next slot under a lock, then sleep outside it, so a
    waiting thread never holds up the others' reservations.
    """

    def __init__(self, per_sec: float):
        self.interval = 1.0 / per_sec if per_sec > 0 else 0.0
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        if not self.interval:
            return
        with self._lock:
            slot = max(time.monotonic(), self._next)
            self._next = slot + self.interval
        _safe_sleep(slot - time.monotonic())


def build_client_from_env(cfg: EnvConfig) -> KalturaClient:
    config = KalturaConfiguration()
    config.serviceUrl = cfg.service_url
    config.partnerId = int(cfg.partner_id)
    config.timeout = cfg.request_timeout_sec
    client = KalturaClient(config)

    try:
        ks = client.session.start(
            cfg.admin_secret,
            cfg.user_id,
            KalturaSessionType.ADMIN,
            int(cfg.partner_id),
            privileges=cfg.privileges,
        )
    except Exception as e:
        if getattr(e, "code", "") == "START_SESSION_ERROR":
            print(
                "\n❌ Could not log in to Kaltura. Partner ID "
                f"[{cfg.partner_id}] and the Admin Secret were not accepted.\n"
                "   Double-check both values — the secret must be the "
                "Administrator secret (not the User secret),\n"
                "   copied exactly from KMC → Settings → Integration Settings.\n"
            )
        elif type(e).__name__ == "KalturaClientException":
            print(
                "\n❌ Could not reach Kaltura to start a session.\n"
                f"   {e}\n   Check your internet connection and try again.\n"
            )
        else:
            print(f"\n❌ Could not start Kaltura session: {e}\n")
        raise SystemExit(1)
    client.setKs(ks)
    return client


def get_thread_client(
    cfg: EnvConfig,
    thread_local: threading.local,
) -> KalturaClient:
    client = getattr(thread_local, "client", None)
    if client is None:
        client = build_client_from_env(cfg)
        thread_local.client = client
    return client


def _raw_api_url(cfg: EnvConfig, action: str) -> str:
    base_url = cfg.service_url.rstrip("/")
    return f"{base_url}/api_v3/service/baseentry/action/{action}"


def _raw_baseentry_call(
    cfg: EnvConfig,
    client: KalturaClient,
    action: str,
    entry_id: str,
) -> requests.Response:
    ks = client.getKs()
    if not ks:
        raise RuntimeError("Kaltura session key (KS) is missing")

    data = {"entryId": entry_id, "ks": ks}

    return requests.post(
        _raw_api_url(cfg, action),
        data=data,
        timeout=(cfg.request_connect_timeout_sec, cfg.request_timeout_sec),
    )


def _normalize_headers(row: Dict[str, Any]) -> Dict[str, Any]:
    # Preserve original keys in output; this is for internal lookup only.
    return {str(k).strip().lower(): v for k, v in row.items()}


def _extract_entry_id(
    row: Dict[str, Any],
    entry_header: str,
) -> str:
    norm = _normalize_headers(row)
    target = entry_header.strip().lower()
    val = norm.get(target, "")
    if val is None:
        return ""
    return str(val).strip()


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    # Heuristic; Kaltura error codes vary by environment/client.
    return any(
        token in msg
        for token in [
            "rate",
            "throttle",
            "too many",
            "exceeded",
            "service temporarily unavailable",
        ]
    )


def _is_permission_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(token in msg for token in ["permission", "forbidden", "not allowed", "denied"]) 


def _is_connection_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(
        token in msg
        for token in [
            "name resolution",
            "failed to resolve",
            "max retries exceeded",
            "httpsconnectionpool",
            "connection error",
            "temporarily unavailable",
            "nodename nor servname provided",
        ]
    )


def _is_invalid_recycle_status_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(
        token in msg
        for token in [
            "invalid_entry_status_for_recycle",
            "displayinstatus is invalid for recycle",
            "invalid for recycle",
        ]
    )


def _kaltura_code_message(exc: Exception) -> Tuple[Optional[str], str]:
    # Try common attribute patterns in different Kaltura client versions.
    code = None
    for attr in ("code", "error_code", "errorCode"):
        if hasattr(exc, attr):
            try:
                code = str(getattr(exc, attr))
            except Exception:
                code = None
            break
    # Some exceptions may have "message" separate from __str__
    detail = ""
    if hasattr(exc, "message"):
        try:
            detail = str(getattr(exc, "message"))
        except Exception:
            detail = ""
    if not detail:
        detail = str(exc)
    return code, detail


def _xml_field(text: str, tag: str) -> str:
    start = text.find(f"<{tag}>")
    end = text.find(f"</{tag}>")
    if start != -1 and end != -1 and end > start:
        return text[start + len(tag) + 2:end].strip()
    return ""


def _raw_response_error(response: requests.Response) -> Tuple[Optional[str], str]:
    text = response.text or ""

    code = None
    code_start = text.find("<code>")
    code_end = text.find("</code>")
    if code_start != -1 and code_end != -1 and code_end > code_start:
        code = text[code_start + 6:code_end].strip()

    message = ""
    msg_start = text.find("<message>")
    msg_end = text.find("</message>")
    if msg_start != -1 and msg_end != -1 and msg_end > msg_start:
        message = text[msg_start + 9:msg_end].strip()

    return code, message or text.strip()


def _safe_sleep(seconds: float) -> None:
    if seconds <= 0:
        return
    # Sleep in small increments so CTRL+C can stop quickly.
    end = time.time() + seconds
    while time.time() < end and not STOP_EVENT.is_set():
        time.sleep(min(0.1, end - time.time()))


def _entry_status_value(entry: Any) -> Optional[int]:
    # Kaltura entries usually expose .status
    try:
        return int(getattr(entry, "status"))
    except Exception:
        return None


def _status_is_recycled(status_val: Optional[int]) -> bool:
    if status_val is None:
        return False

    # Prefer enum if present; fall back to a conservative check.
    try:
        if hasattr(KalturaEntryStatus, "RECYCLED"):
            return status_val == int(getattr(KalturaEntryStatus, "RECYCLED"))
    except Exception:
        pass

    # If enum is unavailable, we cannot reliably detect recycled status.
    return False


def recycle_one(
    cfg: EnvConfig,
    thread_local: threading.local,
    row_index: int,
    total_rows: int,
    row: Dict[str, Any],
    seen_ids: set,
    lock: threading.Lock,
    rate_limiter: RateLimiter,
) -> Dict[str, Any]:
    """Process a single CSV row.

    Returns a dict of output columns (original + appended fields).
    """
    out: Dict[str, Any] = dict(row)

    entry_id = _extract_entry_id(row, cfg.column_header_entry_id)
    out["entry_id_normalized"] = entry_id
    out["recycled_at"] = now_in_tz(cfg.timezone).isoformat()

    display_entry_id = entry_id or "[blank]"
    print(f"[{row_index}/{total_rows}] Starting: {display_entry_id}")

    if not entry_id:
        out["recycled_success"] = "false"
        out["failure_reason"] = FAIL_INVALID_ENTRY_ID
        out["failure_detail"] = (
            f"Blank or missing entry ID in column "
            f"'{cfg.column_header_entry_id}'"
        )
        print(f"[{row_index}/{total_rows}] Failed immediately: blank entry ID")
        return out

    # De-dupe: first occurrence proceeds; subsequent rows are marked as skipped.
    with lock:
        if entry_id in seen_ids:
            out["recycled_success"] = "true"
            out["failure_reason"] = ""
            out["failure_detail"] = "duplicate_row_same_entry_id_skipped"
            print(f"[{row_index}/{total_rows}] Skipped duplicate: {entry_id}")
            return out
        seen_ids.add(entry_id)

    if STOP_EVENT.is_set():
        out["recycled_success"] = "false"
        out["failure_reason"] = "stopped"
        out["failure_detail"] = "Stopped before processing"
        print(f"[{row_index}/{total_rows}] Stopped before processing: {entry_id}")
        return out

    try:
        client = get_thread_client(cfg, thread_local)
    except Exception as exc:
        code, detail = _kaltura_code_message(exc)
        out["recycled_success"] = "false"
        out["failure_reason"] = FAIL_CONNECTION_ERROR
        out["failure_detail"] = detail
        out["kaltura_error_code"] = "" if code is None else code
        print(
            f"[{row_index}/{total_rows}] Failed to initialize Kaltura client for "
            f"{entry_id}: {detail}"
        )
        return out

    should_validate = cfg.validate_entry_exists and (
        not cfg.dry_run or cfg.validate_entry_exists_in_dry_run
    )

    # Optional preflight lookup.
    if should_validate:
        print(f"[{row_index}/{total_rows}] Checking whether entry exists: {entry_id}")
        try:
            response = _raw_baseentry_call(cfg, client, "get", entry_id)
            code, detail = _raw_response_error(response)

            if code == "ENTRY_ID_NOT_FOUND":
                out["recycled_success"] = "false"
                out["failure_reason"] = FAIL_ENTRY_DOES_NOT_EXIST
                out["failure_detail"] = "entry_does_not_exist"
                out["kaltura_error_code"] = code
                print(f"[{row_index}/{total_rows}] Entry does not exist: {entry_id}")
                return out

            if code:
                out["recycled_success"] = "false"
                out["failure_reason"] = FAIL_KALTURA_ERROR
                out["failure_detail"] = detail
                out["kaltura_error_code"] = code
                print(
                    f"[{row_index}/{total_rows}] Kaltura error on raw get for "
                    f"{entry_id}: {detail}"
                )
                return out

            # Check for child entry — parent recycle should handle children.
            root_entry_id = _xml_field(response.text or "", "rootEntryId")
            if root_entry_id and root_entry_id != entry_id:
                out["recycled_success"] = "false"
                out["failure_reason"] = FAIL_CHILD_ENTRY
                out["failure_detail"] = (
                    f"child_entry_skipped; recycle parent {root_entry_id} instead"
                )
                print(
                    f"[{row_index}/{total_rows}] Skipped child entry "
                    f"{entry_id} (parent: {root_entry_id})"
                )
                return out

            out["entry_status"] = "validated_by_raw_get"
        except requests.RequestException as exc:
            code, detail = _kaltura_code_message(exc)
            out["recycled_success"] = "false"
            out["failure_reason"] = FAIL_CONNECTION_ERROR
            out["failure_detail"] = detail
            out["kaltura_error_code"] = "" if code is None else code
            print(f"[{row_index}/{total_rows}] Connection error on raw get: {entry_id}")
            return out
        except Exception as exc:
            code, detail = _kaltura_code_message(exc)
            out["recycled_success"] = "false"
            out["failure_reason"] = FAIL_KALTURA_ERROR
            out["failure_detail"] = detail
            out["kaltura_error_code"] = "" if code is None else code
            print(
                f"[{row_index}/{total_rows}] Kaltura error on raw get for "
                f"{entry_id}: {detail}"
            )
            return out
        
    if cfg.dry_run and cfg.validate_entry_exists and not should_validate:
        out["recycled_success"] = "true"
        out["failure_reason"] = ""
        out["failure_detail"] = "dry_run_skipped_validation"
        print(
            f"[{row_index}/{total_rows}] DRY RUN: skipped existence check and "
            f"would recycle {entry_id}"
        )
        return out
    
    if cfg.dry_run:
        out["recycled_success"] = "true"
        out["failure_reason"] = ""
        out["failure_detail"] = "dry_run_would_recycle"
        print(f"[{row_index}/{total_rows}] DRY RUN: would recycle {entry_id}")
        return out

    last_exc: Optional[Exception] = None
    for attempt in range(cfg.max_retries + 1):
        if STOP_EVENT.is_set():
            break

        print(
            f"[{row_index}/{total_rows}] Recycle attempt {attempt + 1}/"
            f"{cfg.max_retries + 1}: {entry_id}"
        )

        try:
            for blocked_attempt in range(cfg.blocked_retries + 1):
                rate_limiter.wait()
                response = _raw_baseentry_call(cfg, client, "recycle", entry_id)
                code, detail = _raw_response_error(response)
                # ACTION_BLOCKED usually means we outran Kaltura's throttle,
                # not that this entry can't be recycled. Back off and retry.
                if (
                    code == "ACTION_BLOCKED"
                    and blocked_attempt < cfg.blocked_retries
                    and not STOP_EVENT.is_set()
                ):
                    delay = cfg.blocked_retry_delay * (blocked_attempt + 1)
                    print(
                        f"[{row_index}/{total_rows}] Throttled (ACTION_BLOCKED): "
                        f"{entry_id}; retry {blocked_attempt + 1}/"
                        f"{cfg.blocked_retries} in {delay:g}s"
                    )
                    _safe_sleep(delay)
                    continue
                break

            if not code:
                out["recycled_success"] = "true"
                out["failure_reason"] = ""
                out["failure_detail"] = ""
                out["attempt_number"] = str(attempt + 1)
                print(f"[{row_index}/{total_rows}] Recycled successfully: {entry_id}")
                return out

            out["kaltura_error_code"] = code
            out["attempt_number"] = str(attempt + 1)

            if code == "ENTRY_ID_NOT_FOUND":
                out["recycled_success"] = "false"
                out["failure_reason"] = FAIL_ENTRY_DOES_NOT_EXIST
                out["failure_detail"] = "entry_does_not_exist"
                print(
                    f"[{row_index}/{total_rows}] Recycle failed; entry does "
                    f"not exist: {entry_id}"
                )
                return out

            if code == "INVALID_ENTRY_STATUS_FOR_RECYCLE":
                out["recycled_success"] = "false"
                out["failure_reason"] = FAIL_ALREADY_RECYCLED_OR_INVALID_STATUS
                out["failure_detail"] = detail
                print(
                    f"[{row_index}/{total_rows}] Already recycled or invalid "
                    f"status for recycle: {entry_id}"
                )
                return out

            if code == "ACTION_BLOCKED":
                out["recycled_success"] = "false"
                out["failure_reason"] = FAIL_ACTION_BLOCKED
                out["failure_detail"] = detail
                print(
                    f"[{row_index}/{total_rows}] Still ACTION_BLOCKED after "
                    f"{cfg.blocked_retries} throttle retries: {entry_id}"
                )
                return out

            if _is_permission_error(Exception(detail)):
                out["recycled_success"] = "false"
                out["failure_reason"] = FAIL_PERMISSION_DENIED
                out["failure_detail"] = detail
                print(
                    f"[{row_index}/{total_rows}] Permission error on recycle: "
                    f"{entry_id}"
                )
                return out

            out["recycled_success"] = "false"
            out["failure_reason"] = FAIL_KALTURA_ERROR
            out["failure_detail"] = detail
            print(
                f"[{row_index}/{total_rows}] Attempt {attempt + 1} failed for "
                f"{entry_id}: {detail}"
            )
            return out
        except requests.RequestException as exc:
            last_exc = exc
            code, detail = _kaltura_code_message(exc)
            out["kaltura_error_code"] = "" if code is None else code
            out["attempt_number"] = str(attempt + 1)

            if _is_connection_error(exc):
                out["recycled_success"] = "false"
                out["failure_reason"] = FAIL_CONNECTION_ERROR
                out["failure_detail"] = detail
                print(
                    f"[{row_index}/{total_rows}] Connection error on raw recycle: "
                    f"{entry_id}"
                )
                return out

            if _is_rate_limit_error(exc):
                out["failure_reason"] = FAIL_RATE_LIMITED
            else:
                out["failure_reason"] = FAIL_CONNECTION_ERROR

            out["failure_detail"] = detail
            print(
                f"[{row_index}/{total_rows}] Attempt {attempt + 1} failed for "
                f"{entry_id}: {detail}"
            )

            if attempt < cfg.max_retries:
                sleep_for = cfg.backoff_base_sec * (2 ** attempt)
                _safe_sleep(sleep_for)
        except Exception as exc:
            last_exc = exc
            code, detail = _kaltura_code_message(exc)
            out["kaltura_error_code"] = "" if code is None else code
            out["attempt_number"] = str(attempt + 1)
            out["recycled_success"] = "false"
            out["failure_reason"] = FAIL_KALTURA_ERROR
            out["failure_detail"] = detail
            print(
                f"[{row_index}/{total_rows}] Attempt {attempt + 1} failed for "
                f"{entry_id}: {detail}"
            )
            return out

        finally:
            _safe_sleep(cfg.request_delay_sec)

    out["recycled_success"] = "false"
    if STOP_EVENT.is_set():
        out["failure_reason"] = "stopped"
        out["failure_detail"] = "Stopped during retry"
    elif last_exc is None:
        out["failure_reason"] = FAIL_UNKNOWN_ERROR
        out["failure_detail"] = "Unknown failure"
    print(f"[{row_index}/{total_rows}] Failed after retries: {entry_id}")
    return out


def _install_signal_handlers() -> None:
    def _handle_sigint(signum: int, frame: Any) -> None:  # noqa: ARG001
        STOP_EVENT.set()

    signal.signal(signal.SIGINT, _handle_sigint)


def _read_csv_rows(input_filename: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    input_path = Path(input_filename)
    if not input_path.exists():
        raise RuntimeError(
            "Input CSV not found: "
            f"{input_filename}\n"
            "Please check INPUT_FILENAME in .env and confirm the file exists."
        )

    if not input_path.is_file():
        raise RuntimeError(
            "Input path is not a file: "
            f"{input_filename}\n"
            "Please check INPUT_FILENAME in .env."
        )

    with open(input_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise RuntimeError("Input CSV has no headers")
        rows = list(reader)
        headers = list(reader.fieldnames)
    return rows, headers


def _write_output_header(
    out_path: str,
    base_headers: List[str],
    extra_headers: List[str],
) -> None:
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=base_headers + extra_headers)
        writer.writeheader()


def _append_output_row(
    out_path: str,
    fieldnames: List[str],
    row: Dict[str, Any],
) -> None:
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        # Ensure any missing keys are written as blanks.
        safe_row = {k: row.get(k, "") for k in fieldnames}
        writer.writerow(safe_row)


def main() -> None:
    _install_signal_handlers()

    cfg = load_env()

    if os.getenv("ADMIN_SECRET"):
        print(
            "⚠️  ADMIN_SECRET found in .env — it is ignored. Please delete "
            "that line from .env.\n"
        )
    admin_secret = getpass.getpass("Enter your Kaltura admin secret: ")
    if not admin_secret.strip():
        raise RuntimeError("An admin secret is required.")
    cfg = dataclasses.replace(cfg, admin_secret=admin_secret.strip())

    # Log in once up front so a mistyped secret fails immediately, before
    # any worker threads start.
    build_client_from_env(cfg)

    with keep.running(on_fail="warn") as wakepy_mode:
        if wakepy_mode.active:
            print(
                "System sleep prevented for the duration of this run "
                f"(wakepy, method: {wakepy_mode.active_method})."
            )

        run_ts = timestamp_string(cfg.timezone)
        output_dir = Path(__file__).resolve().parent / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        out_csv = str(output_dir / f"{run_ts}_recycledEntries.csv")

        print("=== Recycle Entries (raw baseEntry API calls via KS) ===")
        print(f"Timestamp: {run_ts} ({cfg.timezone})")
        print(f"Input CSV: {cfg.input_filename}")
        print(f"Output CSV: {out_csv}")
        print(f"Entry ID column: {cfg.column_header_entry_id}")
        print(f"DRY_RUN: {cfg.dry_run}")
        print(
            f"VALIDATE_ENTRY_EXISTS: {cfg.validate_entry_exists} | "
            f"VALIDATE_ENTRY_EXISTS_IN_DRY_RUN: "
            f"{cfg.validate_entry_exists_in_dry_run}"
        )
        print(
            f"MAX_WORKERS: {cfg.max_workers} | MAX_RETRIES: {cfg.max_retries}"
        )
        print(
            f"RECYCLE_RATE_PER_SEC: {cfg.recycle_rate_per_sec:g} | "
            f"BLOCKED_RETRIES: {cfg.blocked_retries} | "
            f"BLOCKED_RETRY_DELAY: {cfg.blocked_retry_delay:g}"
        )
        print(
            f"BACKOFF_BASE_SEC: {cfg.backoff_base_sec} | "
            f"REQUEST_DELAY_SEC: {cfg.request_delay_sec} | "
            f"REQUEST_TIMEOUT_SEC: {cfg.request_timeout_sec} | "
            f"REQUEST_CONNECT_TIMEOUT_SEC: {cfg.request_connect_timeout_sec}"
        )
        print(f"PROGRESS_EVERY: {cfg.progress_every}")
        if cfg.progress_every == 1:
            print(
                "Progress reporting is set to every completed row. "
                "For less terminal noise, try PROGRESS_EVERY=5 or 10."
            )
        print("-----------------------------------------")

        rows, base_headers = _read_csv_rows(cfg.input_filename)

        # Extra output fields (appended to keep original columns stable)
        extra_headers = [
            "entry_id_normalized",
            "entry_status",
            "recycled_success",
            "failure_reason",
            "failure_detail",
            "kaltura_error_code",
            "attempt_number",
            "recycled_at",
        ]

        # Ensure base headers are unique and stable
        base_headers_unique = []
        seen = set()
        for h in base_headers:
            if h in seen:
                continue
            base_headers_unique.append(h)
            seen.add(h)

        fieldnames = base_headers_unique + extra_headers
        _write_output_header(out_csv, base_headers_unique, extra_headers)

        if not rows:
            print("No rows found in input CSV.")
            print(f"Output CSV created: {out_csv}")
            return

        lock = threading.Lock()
        seen_ids: set = set()
        thread_local = threading.local()
        rate_limiter = RateLimiter(cfg.recycle_rate_per_sec)

        total = len(rows)
        start = time.monotonic()

        def _elapsed() -> str:
            secs = int(time.monotonic() - start)
            h, rem = divmod(secs, 3600)
            m, s = divmod(rem, 60)
            if h:
                return f"{h}h{m:02d}m{s:02d}s"
            if m:
                return f"{m}m{s:02d}s"
            return f"{s}s"

        print(f"Loaded {total} row(s). Processing...")
        if cfg.recycle_rate_per_sec > 0 and not cfg.dry_run:
            eta_min = total / cfg.recycle_rate_per_sec / 60
            print(
                f"Pacing recycles at {cfg.recycle_rate_per_sec:g}/sec to stay "
                f"under Kaltura's throttle — roughly {eta_min:.1f} min minimum."
            )
        print("Writing results incrementally to output CSV...")
        if cfg.max_workers > 1:
            print(
                "Note: each worker creates its own Kaltura session. "
                "If DNS/network resolution is flaky, lower MAX_WORKERS to 1 or 2."
            )

        completed = 0
        success_count = 0
        fail_count = 0

        with ThreadPoolExecutor(max_workers=cfg.max_workers) as pool:
            futures = {}
            for idx, row in enumerate(rows, start=1):
                if STOP_EVENT.is_set():
                    break
                fut = pool.submit(
                    recycle_one,
                    cfg,
                    thread_local,
                    idx,
                    total,
                    row,
                    seen_ids,
                    lock,
                    rate_limiter,
                )
                futures[fut] = idx

            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    result = fut.result()
                except Exception as exc:
                    result = {
                        "recycled_success": "false",
                        "failure_reason": FAIL_UNKNOWN_ERROR,
                        "failure_detail": f"Unhandled worker exception: {exc}",
                        "attempt_number": "",
                        "kaltura_error_code": "",
                        "recycled_at": now_in_tz(cfg.timezone).isoformat(),
                    }
                    print(f"[{idx}/{total}] Unhandled worker exception: {exc}")

                _append_output_row(out_csv, fieldnames, result)
                completed += 1

                if str(result.get("recycled_success", "")).lower() == "true":
                    success_count += 1
                else:
                    fail_count += 1

                if completed % cfg.progress_every == 0 or completed == total:
                    print(
                        f"Progress: {completed}/{total} [{_elapsed()}] processed "
                        f"(success {success_count}, failed {fail_count})"
                    )

                if STOP_EVENT.is_set():
                    break

        if STOP_EVENT.is_set():
            print("\nStopped early (Ctrl+C). Results written so far.")

        print("\n=== Summary ===")
        print(f"Total rows: {total}")
        print(f"Succeeded: {success_count}")
        print(f"Failed: {fail_count}")
        print(f"Elapsed: {_elapsed()}")
        print(f"Output CSV: {out_csv}")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        print("\nERROR:")
        print(exc)
        sys.exit(1)