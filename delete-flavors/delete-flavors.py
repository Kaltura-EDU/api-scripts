#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Delete Non‑Source Flavors – safer, flexible bulk tool.

Deletes all non‑source flavors for selected entries, preserving the source flavor 
(using isOriginal, tags, or largest file fallback). Skips single‑flavor entries 
and writes preview/results CSV reports.
"""

import csv
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv, find_dotenv

from KalturaClient import KalturaClient, KalturaConfiguration
from KalturaClient.Plugins.Core import (
    KalturaSessionType,
    KalturaFilterPager,
    KalturaMediaEntryFilter,
)
from KalturaClient.Plugins.Core import KalturaFlavorAssetFilter
from KalturaClient.Plugins.Core import KalturaMediaEntryFilter
from KalturaClient.exceptions import KalturaException


# ==== Env / config ===========================================================
load_dotenv(find_dotenv())

def require_env_int(name: str) -> int:
    raw = os.getenv(name, "").strip()
    if not raw.isdigit():
        print(f"[ERROR] Missing or invalid {name} in .env", file=sys.stderr)
        sys.exit(2)
    return int(raw)

def get_env_csv(name: str) -> List[str]:
    raw = os.getenv(name, "") or ""
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts

def now_stamp() -> str:
    # e.g., 2025-08-28-1412 (YYYY-MM-DD-HHMM, 24-hour clock)
    return datetime.now().strftime("%Y-%m-%d-%H%M")

PARTNER_ID = require_env_int("PARTNER_ID")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "").strip()
if not ADMIN_SECRET:
    print("[ERROR] Missing ADMIN_SECRET in .env", file=sys.stderr)
    sys.exit(2)

USER_ID = os.getenv("USER_ID", "").strip()  # optional
SERVICE_URL = os.getenv("SERVICE_URL", "https://www.kaltura.com").rstrip("/")
PRIVILEGES = os.getenv("PRIVILEGES", "all:*,disableentitlement")

ENTRY_IDS = get_env_csv("ENTRY_IDS")
TAGS = get_env_csv("TAGS")
CATEGORY_IDS = get_env_csv("CATEGORY_IDS")

TS = now_stamp()

# Ensure outputs go into a "reports" subfolder alongside this script
REPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

# Filenames start with the timestamp, e.g., 2025-08-28-1412_deleted_flavors_PREVIEW.csv
PREVIEW_CSV = os.path.join(REPORTS_DIR, f"{TS}_deleted_flavors_PREVIEW.csv")
RESULT_CSV  = os.path.join(REPORTS_DIR, f"{TS}_deleted_flavors_RESULT.csv")


# ==== Kaltura client bootstrap ===============================================
cfg = KalturaConfiguration(PARTNER_ID)
cfg.serviceUrl = SERVICE_URL
client = KalturaClient(cfg)
ks = client.session.start(ADMIN_SECRET, USER_ID, KalturaSessionType.ADMIN, PARTNER_ID, privileges=PRIVILEGES)
client.setKs(ks)


# ==== Utilities ==============================================================

def _as_int(x) -> int:
    try:
        return int(x)
    except Exception:
        try:
            # sizeInBytes sometimes returned as string
            return int(str(x).strip())
        except Exception:
            return 0

def pick_source_flavor(flavor_objects) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (source_flavor_id, reason) where reason in {'isOriginal','tags:source','largest'}.
    If none can be determined, returns (None, None).
    """
    if not flavor_objects:
        return None, None

    # 1) isOriginal == True
    for fa in flavor_objects:
        try:
            if bool(getattr(fa, "isOriginal", False)):
                return getattr(fa, "id", None), "isOriginal"
        except Exception:
            pass

    # 2) tags contains 'source'
    for fa in flavor_objects:
        try:
            tags = (getattr(fa, "tags", "") or "").lower()
            if "source" in tags:
                return getattr(fa, "id", None), "tags:source"
        except Exception:
            pass

    # 3) largest by sizeInBytes
    max_fa = None
    max_size = -1
    for fa in flavor_objects:
        size_b = _as_int(getattr(fa, "sizeInBytes", 0))
        if size_b > max_size:
            max_size = size_b
            max_fa = fa
    if max_fa is not None:
        return getattr(max_fa, "id", None), "largest"

    return None, None


def list_flavors(entry_id: str):
    ff = KalturaFlavorAssetFilter()
    ff.entryIdEqual = entry_id
    pager = KalturaFilterPager(pageSize=500, pageIndex=1)
    resp = client.flavorAsset.list(ff, pager)
    return [] if not resp else (resp.objects or [])


def list_children(entry_id: str):
    """
    Return a list of child entries (multi-stream components) for a given parent entry.
    Uses media.list with parentEntryIdEqual.
    """
    mf = KalturaMediaEntryFilter()
    mf.parentEntryIdEqual = entry_id
    pager = KalturaFilterPager(pageSize=500, pageIndex=1)
    children = []
    page = 0
    while True:
        page += 1
        try:
            resp = client.media.list(mf, pager)
        except KalturaException as ex:
            print(f"[WARN] media.list (children) failed for parent {entry_id} on page {page}: {ex}")
            break
        if not resp or not getattr(resp, "objects", None):
            break
        children.extend(resp.objects or [])
        if len(resp.objects) < pager.pageSize:
            break
        pager.pageIndex += 1
    return children


def iter_selected_entries() -> List:
    """
    Return a list of KalturaMediaEntry objects that match the selection.
    """
    selected = []

    # Shortcut: explicit ENTRY_IDS
    if ENTRY_IDS:
        for eid in ENTRY_IDS:
            try:
                e = client.media.get(eid)
                selected.append(e)
            except Exception as ex:
                print(f"[WARN] media.get failed for {eid}: {ex}")
        return selected

    # Otherwise: build filter
    f = KalturaMediaEntryFilter()
    if TAGS:
        # CSV of tags – server matches ANY
        f.tagsLikeAny = ",".join(TAGS)
    if CATEGORY_IDS:
        # CSV of category IDs – "match OR" semantics
        f.categoriesIdsMatchOr = ",".join(CATEGORY_IDS)

    pager = KalturaFilterPager(pageSize=500, pageIndex=1)
    page = 0
    while True:
        page += 1
        try:
            resp = client.media.list(f, pager)
        except KalturaException as ex:
            print(f"[ERROR] media.list failed on page {page}: {ex}")
            break
        if not resp or not resp.objects:
            break
        selected.extend(resp.objects or [])
        if len(resp.objects) < pager.pageSize:
            break
        pager.pageIndex += 1

    return selected


def write_csv(path: str, rows: List[Dict[str, str]]):
    if not rows:
        # still write header
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "role","entry_id","parent_entry_id","entry_name","owner_user_id","conversion_profile_id",
                "total_flavors","source_flavor_id","source_reason","flavors_to_delete",
                "flavors_deleted_count","bytes_saved","is_multistream","child_count","status","error"
            ])
            w.writeheader()
        return

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "role","entry_id","parent_entry_id","entry_name","owner_user_id","conversion_profile_id",
            "total_flavors","source_flavor_id","source_reason","flavors_to_delete",
            "flavors_deleted_count","bytes_saved","is_multistream","child_count","status","error"
        ])
        w.writeheader()
        w.writerows(rows)


def build_preview_rows_for_entry(e) -> List[Dict[str, str]]:
    """
    Builds preview rows for a single parent entry, including any children (multi‑stream).
    Returns a list of row dicts (parent first, then children).
    """
    rows: List[Dict[str, str]] = []

    parent_id = getattr(e, "id", "")
    name = getattr(e, "name", "")
    owner = getattr(e, "userId", "")
    conv = getattr(e, "conversionProfileId", "")

    # List flavors for the parent
    try:
        flavors = list_flavors(parent_id)
    except Exception as ex:
        rows.append({
            "role": "PARENT",
            "entry_id": parent_id,
            "parent_entry_id": "",
            "entry_name": name,
            "owner_user_id": owner,
            "conversion_profile_id": conv,
            "total_flavors": "0",
            "source_flavor_id": "",
            "source_reason": "",
            "flavors_to_delete": "",
            "flavors_deleted_count": "0",
            "bytes_saved": "0",
            "is_multistream": "",
            "child_count": "",
            "status": "ERROR",
            "error": f"flavorAsset.list failed: {ex}",
        })
        return rows

    total = len(flavors)
    # Gather children (multi-stream)
    children = list_children(parent_id)
    is_multi = "YES" if children else "NO"
    if is_multi == "YES":
        print(f"[INFO] Entry {parent_id} has {len(children)} child(ren); will apply deletion plan to each.")

    if total <= 1:
        rows.append({
            "role": "PARENT",
            "entry_id": parent_id,
            "parent_entry_id": "",
            "entry_name": name,
            "owner_user_id": owner,
            "conversion_profile_id": conv,
            "total_flavors": str(total),
            "source_flavor_id": "",
            "source_reason": "",
            "flavors_to_delete": "",
            "flavors_deleted_count": "0",
            "bytes_saved": "0",
            "is_multistream": is_multi,
            "child_count": str(len(children)) if children else "0",
            "status": "SKIPPED_SINGLE_FLAVOR",
            "error": "",
        })
    else:
        src_id, src_reason = pick_source_flavor(flavors)
        if not src_id:
            rows.append({
                "role": "PARENT",
                "entry_id": parent_id,
                "parent_entry_id": "",
                "entry_name": name,
                "owner_user_id": owner,
                "conversion_profile_id": conv,
                "total_flavors": str(total),
                "source_flavor_id": "",
                "source_reason": "",
                "flavors_to_delete": "",
                "flavors_deleted_count": "0",
                "bytes_saved": "0",
                "is_multistream": is_multi,
                "child_count": str(len(children)) if children else "0",
                "status": "SKIPPED_NO_SOURCE_DETECTED",
                "error": "",
            })
        else:
            to_delete_ids: List[str] = []
            bytes_saved = 0
            for fa in flavors:
                fa_id = getattr(fa, "id", "")
                if fa_id == src_id:
                    continue
                to_delete_ids.append(fa_id)
                bytes_saved += _as_int(getattr(fa, "sizeInBytes", 0))
            rows.append({
                "role": "PARENT",
                "entry_id": parent_id,
                "parent_entry_id": "",
                "entry_name": name,
                "owner_user_id": owner,
                "conversion_profile_id": conv,
                "total_flavors": str(total),
                "source_flavor_id": src_id or "",
                "source_reason": src_reason or "",
                "flavors_to_delete": ",".join(to_delete_ids),
                "flavors_deleted_count": str(len(to_delete_ids)),
                "bytes_saved": str(bytes_saved),
                "is_multistream": is_multi,
                "child_count": str(len(children)) if children else "0",
                "status": "READY",
                "error": "",
            })

    # Process each child similarly (note: we do NOT recurse to grandchildren)
    for c in children:
        cid = getattr(c, "id", "")
        cname = getattr(c, "name", "")
        cowner = getattr(c, "userId", "")
        cconv = getattr(c, "conversionProfileId", "")
        try:
            cflavors = list_flavors(cid)
        except Exception as ex:
            rows.append({
                "role": "CHILD",
                "entry_id": cid,
                "parent_entry_id": parent_id,
                "entry_name": cname,
                "owner_user_id": cowner,
                "conversion_profile_id": cconv,
                "total_flavors": "0",
                "source_flavor_id": "",
                "source_reason": "",
                "flavors_to_delete": "",
                "flavors_deleted_count": "0",
                "bytes_saved": "0",
                "is_multistream": "",
                "child_count": "",
                "status": "ERROR",
                "error": f"flavorAsset.list failed: {ex}",
            })
            continue

        ctotal = len(cflavors)
        if ctotal <= 1:
            rows.append({
                "role": "CHILD",
                "entry_id": cid,
                "parent_entry_id": parent_id,
                "entry_name": cname,
                "owner_user_id": cowner,
                "conversion_profile_id": cconv,
                "total_flavors": str(ctotal),
                "source_flavor_id": "",
                "source_reason": "",
                "flavors_to_delete": "",
                "flavors_deleted_count": "0",
                "bytes_saved": "0",
                "is_multistream": "",
                "child_count": "",
                "status": "SKIPPED_SINGLE_FLAVOR",
                "error": "",
            })
            continue

        csrc_id, csrc_reason = pick_source_flavor(cflavors)
        if not csrc_id:
            rows.append({
                "role": "CHILD",
                "entry_id": cid,
                "parent_entry_id": parent_id,
                "entry_name": cname,
                "owner_user_id": cowner,
                "conversion_profile_id": cconv,
                "total_flavors": str(ctotal),
                "source_flavor_id": "",
                "source_reason": "",
                "flavors_to_delete": "",
                "flavors_deleted_count": "0",
                "bytes_saved": "0",
                "is_multistream": "",
                "child_count": "",
                "status": "SKIPPED_NO_SOURCE_DETECTED",
                "error": "",
            })
            continue

        c_to_delete: List[str] = []
        c_bytes_saved = 0
        for fa in cflavors:
            fa_id = getattr(fa, "id", "")
            if fa_id == csrc_id:
                continue
            c_to_delete.append(fa_id)
            c_bytes_saved += _as_int(getattr(fa, "sizeInBytes", 0))

        rows.append({
            "role": "CHILD",
            "entry_id": cid,
            "parent_entry_id": parent_id,
            "entry_name": cname,
            "owner_user_id": cowner,
            "conversion_profile_id": cconv,
            "total_flavors": str(ctotal),
            "source_flavor_id": csrc_id or "",
            "source_reason": csrc_reason or "",
            "flavors_to_delete": ",".join(c_to_delete),
            "flavors_deleted_count": str(len(c_to_delete)),
            "bytes_saved": str(c_bytes_saved),
            "is_multistream": "",
            "child_count": "",
            "status": "READY",
            "error": "",
        })

    return rows


# ==== Main workflow ==========================================================

def main():
    print("[INFO] Selecting entries …")
    entries = iter_selected_entries()
    print(f"[INFO] Found {len(entries)} candidate entries")

    preview_rows: List[Dict[str, str]] = []

    for e in entries:
        rows_for_entry = build_preview_rows_for_entry(e)
        preview_rows.extend(rows_for_entry)

    write_csv(PREVIEW_CSV, preview_rows)
    print(f"[INFO] Wrote pre‑deletion plan → {PREVIEW_CSV}")

    # Any actually deletable entries?
    ready = [r for r in preview_rows if r["status"] == "READY" and r["flavors_to_delete"]]
    if not ready:
        print("[INFO] No entries require deletion. Exiting.")
        return

    parents_ready = sum(1 for r in ready if r.get("role") == "PARENT")
    children_ready = sum(1 for r in ready if r.get("role") == "CHILD")
    total_flavors_to_delete = sum(int(r.get("flavors_deleted_count","0") or 0) for r in ready)
    total_bytes_to_save = sum(_as_int(r.get("bytes_saved","0")) for r in ready)
    print(f"[PLAN] Parents ready: {parents_ready} | Children ready: {children_ready} | Flavors to delete: {total_flavors_to_delete} | Bytes potentially saved: {total_bytes_to_save}")

    confirm = input("\nType 'DELETE' to permanently delete the listed flavors: ").strip().upper()
    if confirm != "DELETE":
        print("[ABORTED] No deletions performed.")
        return

    # Perform deletions
    result_rows: List[Dict[str, str]] = []
    for r in preview_rows:
        if r["status"] != "READY" or not r["flavors_to_delete"]:
            # carry row forward unchanged
            result_rows.append(r)
            continue

        eid = r["entry_id"]
        deleted_count = 0
        error = ""
        try:
            for flv in r["flavors_to_delete"].split(","):
                flv = flv.strip()
                if not flv:
                    continue
                try:
                    client.flavorAsset.delete(flv)
                    deleted_count += 1
                    print(f"[DELETED] {flv} for entry {eid} (role={r.get('role','')}, parent={r.get('parent_entry_id','')})")
                except KalturaException as ex:
                    # continue but mark error
                    error = f"{error}; delete {flv} failed: {ex}"
        except Exception as ex:
            error = f"{error}; unexpected error: {ex}"

        new_status = "DELETED" if deleted_count == int(r["flavors_deleted_count"]) and not error else "PARTIAL" if deleted_count > 0 else "FAILED"
        rr = dict(r)
        rr["flavors_deleted_count"] = str(deleted_count)
        rr["status"] = new_status
        rr["error"] = error.strip("; ")
        result_rows.append(rr)

    write_csv(RESULT_CSV, result_rows)
    print(f"\n[INFO] Wrote results → {RESULT_CSV}")
    print("[DONE]")

if __name__ == "__main__":
    main()
