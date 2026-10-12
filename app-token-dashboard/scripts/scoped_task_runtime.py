#!/usr/bin/env python3
"""Independent, standard-library task implementations inspired by Kaltura-EDU/api-scripts.

The dashboard embeds one task manifest in TASK_CONFIG. This file never accepts an
admin secret and never starts an admin-secret session. See scoped-tasks.md.
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

TASK_CONFIG = None  # __TASK_CONFIG__
DEFAULT_URL = "https://www.kaltura.com/api_v3"


class TaskError(RuntimeError):
    pass


def load_credentials(path: Path, profile: dict) -> dict[str, str]:
    values = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator:
            raise TaskError("Invalid .env line")
        value = value.strip()
        values[key.strip()] = json.loads(value) if value.startswith('"') else value
    for key in ("KALTURA_PARTNER_ID", "KALTURA_APP_TOKEN_ID", "KALTURA_APP_TOKEN_VALUE",
                "KALTURA_APP_TOKEN_HASH_TYPE", "KALTURA_APP_TOKEN_USER_ID",
                "KALTURA_APP_TOKEN_SESSION_TYPE", "KALTURA_APP_TOKEN_SESSION_DURATION",
                "KALTURA_ROLE_ID", "KALTURA_ROLE_PERMISSIONS"):
        if not isinstance(values.get(key), str) or not values[key]:
            raise TaskError(f"Missing {key} in token credential .env")
    if values["KALTURA_PARTNER_ID"] != str(profile["partner_id"]):
        raise TaskError("Token credential PID does not match this task bundle")
    actual = set(values["KALTURA_ROLE_PERMISSIONS"].split(","))
    if actual != set(profile["permissions"]):
        raise TaskError("Local role manifest differs from this task's exact permissions; provision its dedicated token")
    if values["KALTURA_APP_TOKEN_HASH_TYPE"] != "SHA256":
        raise TaskError("This runner requires SHA256 AppTokens")
    # These values are a local check, not a proof of the remote token's role.
    if values.get("KALTURA_SERVICE_URL", DEFAULT_URL).rstrip("/") != DEFAULT_URL:
        raise TaskError("This bundle expects https://www.kaltura.com/api_v3")
    if values["KALTURA_APP_TOKEN_SESSION_TYPE"] != str(profile["session_type"]):
        raise TaskError("Session type differs from the reviewed task bundle")
    return values


class Client:
    def __init__(self, credentials: dict[str, str], profile: dict):
        self.credentials = credentials
        self.allowed = {tuple(pair) for pair in profile["api_actions"]}
        self.ks = ""

    def _post(self, service: str, action: str, data: dict[str, Any]) -> Any:
        url = f"{DEFAULT_URL}/service/{service}/action/{action}"
        request = urllib.request.Request(url, data=urllib.parse.urlencode({"format": 1, **data}).encode(),
                                        headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=30, context=ssl.create_default_context()) as response:
                result = json.load(response)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise TaskError(f"{service}.{action}: request failed; response omitted to protect credentials. "
                            "For a mutation, its outcome may be unknown; inspect Kaltura before rerunning.") from exc
        if isinstance(result, dict):
            result = result.get("result", result)
            if isinstance(result, dict) and (result.get("objectType") == "KalturaAPIException" or "error" in result or result.get("code")):
                error = result.get("error", result)
                code = error.get("code") if isinstance(error, dict) else None
                if code == "QUERY_EXCEEDED_MAX_MATCHES_ALLOWED":
                    raise TaskError("Kaltura's query match limit was exceeded; narrow the UTC date range or filters")
                raise TaskError(f"{service}.{action}: API rejected request; verify role permissions, object access, and input. Response omitted.")
        return result

    def authenticate(self) -> None:
        widget = self._post("session", "startWidgetSession", {"widgetId": f"_{self.credentials['KALTURA_PARTNER_ID']}"})
        widget_ks = widget.get("ks") if isinstance(widget, dict) else None
        if not isinstance(widget_ks, str) or not widget_ks:
            raise TaskError("Widget session did not return a KS")
        digest = hashlib.sha256((widget_ks + self.credentials["KALTURA_APP_TOKEN_VALUE"]).encode()).hexdigest()
        session = self._post("appToken", "startSession", {
            "ks": widget_ks, "id": self.credentials["KALTURA_APP_TOKEN_ID"], "tokenHash": digest,
            "userId": self.credentials["KALTURA_APP_TOKEN_USER_ID"],
            "type": self.credentials["KALTURA_APP_TOKEN_SESSION_TYPE"],
            "expiry": self.credentials["KALTURA_APP_TOKEN_SESSION_DURATION"],
        })
        self.ks = session.get("ks") if isinstance(session, dict) else session
        if not isinstance(self.ks, str) or not self.ks:
            raise TaskError("AppToken exchange did not return a scoped KS")

    def call(self, service: str, action: str, **data) -> Any:
        if (service, action) not in self.allowed:
            raise TaskError(f"{service}.{action} is outside this task's action manifest")
        if not self.ks:
            raise TaskError("Authenticate with the scoped token before content operations")
        return self._post(service, action, {"ks": self.ks, **data})


def paginate(client: Client, service: str, filter_type: str, filters: dict) -> list[dict]:
    objects = []
    page = 1
    while True:
        result = client.call(service, "list", **{"filter[objectType]": filter_type,
                             **{f"filter[{key}]": value for key, value in filters.items()},
                             "pager[pageSize]": 100, "pager[pageIndex]": page})
        if not isinstance(result, dict) or not isinstance(result.get("objects"), list):
            raise TaskError(f"{service}.list returned an unexpected response")
        batch = result["objects"]
        if any(not isinstance(obj, dict) or not obj.get("id") for obj in batch):
            raise TaskError(f"{service}.list returned an object without an ID")
        objects.extend(batch)
        total = result.get("totalCount")
        if len(batch) < 100:
            if total is not None and len(objects) < int(total):
                raise TaskError("Incomplete list; narrow the filters or date range before relying on this report")
            break
        if len(objects) >= 10000:
            raise TaskError("10,000-match boundary reached; narrow the date range or filters. No complete report was produced")
        page += 1
    if len({str(obj["id"]) for obj in objects}) != len(objects):
        raise TaskError("Repeated IDs while paging; rerun against a stable dataset with narrower filters")
    return objects


def timestamp(raw: str, end: bool = False) -> int:
    try:
        date = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise TaskError("Dates must be YYYY-MM-DD") from exc
    return int(date.timestamp()) + (86399 if end else 0)


def read_input(path: Path | None, profile: dict) -> list[dict[str, str]]:
    if not path:
        if profile["input_columns"]:
            raise TaskError("This task requires --input input.csv")
        return []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not set(profile["input_columns"]).issubset(reader.fieldnames or []):
            raise TaskError("Input CSV needs columns: " + ", ".join(profile["input_columns"]))
        rows = []
        seen = set()
        for number, row in enumerate(reader, 2):
            if None in row or any(value is None for value in row.values()):
                raise TaskError(f"Malformed CSV row {number}")
            row = {key: value.strip() for key, value in row.items()}
            if not any(row.values()):
                continue
            for column in profile["input_columns"]:
                if not row[column]:
                    raise TaskError(f"Missing {column} at row {number}")
            if not re.fullmatch(r"[A-Za-z0-9_-]+", row.get("entry_id", "")):
                raise TaskError(f"Invalid entry_id at row {number}")
            if profile["id"] == "caption-visibility" and row["display_on_player"] not in {"0", "1"}:
                raise TaskError("display_on_player must be 0 or 1")
            key = (row["entry_id"], row.get("caption_id", ""))
            if key in seen:
                raise TaskError(f"Repeated input target at row {number}")
            seen.add(key); rows.append(row)
    if not rows:
        raise TaskError("Input CSV is empty")
    return rows


def entry_filters(args) -> dict:
    filters = {}
    for key, value in (("tagsLike", args.tag), ("userIdEqual", args.owner), ("categoriesIdsMatchOr", args.category)):
        if value:
            filters[key] = value
    if args.start:
        filters["createdAtGreaterThanOrEqual"] = timestamp(args.start)
    if args.end:
        filters["createdAtLessThanOrEqual"] = timestamp(args.end, True)
    if args.start and args.end and timestamp(args.start) > timestamp(args.end, True):
        raise TaskError("Start date must precede end date")
    return filters


def report_entries(client, args, task) -> list[dict]:
    entries = paginate(client, "media", "KalturaMediaEntryFilter", entry_filters(args))
    rows = [{"entry_id": entry["id"], "name": entry.get("name", ""), "owner": entry.get("userId", ""),
             "duration_seconds": entry.get("duration") or 0, "created_at": entry.get("createdAt", ""),
             "categories": entry.get("categories", ""), "tags": entry.get("tags", "")} for entry in entries]
    if task == "count-duration":
        print(f"Entries: {len(rows)}; total duration: {sum(float(row['duration_seconds']) for row in rows)} seconds")
    return rows


def caption_rows(client, inputs) -> list[dict]:
    rows = []
    for item in inputs:
        assets = paginate(client, "caption_captionAsset", "KalturaCaptionAssetFilter", {"entryIdEqual": item["entry_id"]})
        if not assets:
            rows.append({"entry_id": item["entry_id"], "caption_id": "", "label": "", "language": "", "format": "", "caption_count": 0})
        for asset in assets:
            rows.append({"entry_id": item["entry_id"], "caption_id": asset["id"], "label": asset.get("label", ""),
                         "language": asset.get("language", ""), "format": asset.get("format", ""), "caption_count": len(assets)})
    return rows


class HttpsDownloads(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        if urllib.parse.urlsplit(newurl).scheme != "https":
            raise TaskError("Refusing a download redirect to an insecure URL")
        return super().redirect_request(request, fp, code, message, headers, newurl)


def save_download(url: str, path: Path) -> None:
    if not isinstance(url, str) or urllib.parse.urlsplit(url).scheme != "https":
        raise TaskError("Download URL must use HTTPS")
    path.parent.mkdir(parents=True, exist_ok=True)
    # Reserve the final filename before network access; never overwrite existing files.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    opener = urllib.request.build_opener(HttpsDownloads(), urllib.request.HTTPSHandler(context=ssl.create_default_context()))
    try:
        with os.fdopen(descriptor, "wb") as output, opener.open(url, timeout=60) as response:
            while block := response.read(1024 * 1024):
                output.write(block)
    except Exception as exc:
        path.unlink(missing_ok=True)
        raise TaskError("Download failed; URL and response omitted to protect scoped sessions") from exc


def downloads(client, inputs, args, task) -> list[dict]:
    rows = []
    for item in inputs:
        entry_id = item["entry_id"]
        captions = task == "download-captions"
        service = "caption_captionAsset" if captions else "flavorAsset"
        filter_type = "KalturaCaptionAssetFilter" if captions else "KalturaFlavorAssetFilter"
        filters = {"entryIdEqual": entry_id}
        if not captions:
            filters.update({"statusEqual": 2, "isOriginalEqual": 1})
        assets = paginate(client, service, filter_type, filters)
        if not assets:
            rows.append({"entry_id": entry_id, "asset_id": "", "file": "", "status": "no matching assets"})
        for asset in assets:
            asset_id = str(asset["id"])
            if not re.fullmatch(r"[A-Za-z0-9_-]+", asset_id):
                raise TaskError("Unexpected asset ID")
            url = client.call(service, "getUrl", id=asset_id)
            if isinstance(url, dict):
                url = url.get("url")
            extension = {"1": "srt", "2": "ttml", "3": "vtt", "4": "cap", "5": "scc"}.get(str(asset.get("format")), "txt") if captions else str(asset.get("fileExt") or "bin")
            if not re.fullmatch(r"[A-Za-z0-9]{1,12}", extension):
                extension = "bin"
            destination = args.output / "downloads" / f"{entry_id}_{asset_id}.{extension}"
            save_download(url, destination)
            rows.append({"entry_id": entry_id, "asset_id": asset_id, "file": str(destination), "status": "downloaded"})
    return rows


def mutation_plan(client, inputs, task) -> list[dict]:
    plan = []
    for item in inputs:
        if task == "caption-visibility":
            assets = paginate(client, "caption_captionAsset", "KalturaCaptionAssetFilter", {"entryIdEqual": item["entry_id"]})
            asset = next((asset for asset in assets if str(asset["id"]) == item["caption_id"]), None)
            if asset is None or asset.get("label", "") != item["expected_label"]:
                raise TaskError(f"Caption not found or expected label mismatch for {item['entry_id']}")
            plan.append({**item, "old_value": asset.get("displayOnPlayer", ""), "new_value": item["display_on_player"]})
        else:
            entry = client.call("baseEntry", "get", entryId=item["entry_id"])
            if not isinstance(entry, dict) or entry.get("id") != item["entry_id"]:
                raise TaskError(f"Unexpected entry response for {item['entry_id']}")
            if entry.get("name", "") != item["expected_name"]:
                raise TaskError(f"Expected name mismatch for {item['entry_id']}; review the input")
            plan.append({**item, "old_value": entry.get("name", ""), "new_value": item.get("new_name", "DELETE")})
    return plan


def apply_mutations(client, plan, task, log_path: Path) -> list[dict]:
    # Create and flush the audit log before every mutation. Never retry writes automatically.
    rows = []
    with log_path.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["entry_id", "caption_id", "old_value", "new_value", "status"])
        writer.writeheader(); handle.flush()
        for item in plan:
            row = {key: item.get(key, "") for key in writer.fieldnames}
            row["status"] = "attempting"
            writer.writerow(safe_row(row)); handle.flush()
            try:
                # Recheck the precondition immediately before the mutation.
                mutation_plan(client, [item], task)
                if task == "rename-entries":
                    result = client.call("baseEntry", "update", **{"entryId": item["entry_id"], "baseEntry[objectType]": "KalturaBaseEntry", "baseEntry[name]": item["new_name"]})
                    if not isinstance(result, dict) or result.get("id") != item["entry_id"]:
                        raise TaskError("Unexpected update response; inspect the entry before retrying")
                elif task == "delete-entries":
                    client.call("baseEntry", "delete", entryId=item["entry_id"])
                elif task == "caption-visibility":
                    result = client.call("caption_captionAsset", "update", **{"id": item["caption_id"], "captionAsset[objectType]": "KalturaCaptionAsset", "captionAsset[displayOnPlayer]": item["display_on_player"]})
                    if not isinstance(result, dict) or str(result.get("id")) != item["caption_id"]:
                        raise TaskError("Unexpected caption update response; inspect the asset before retrying")
                row["status"] = "success"
            except Exception as exc:
                row["status"] = "failed or outcome unknown; inspect before retry"
                writer.writerow(safe_row(row)); handle.flush()
                raise TaskError(f"Stopped at entry {item['entry_id']}. Inspect {log_path} and Kaltura before resuming.") from exc
            writer.writerow(safe_row(row)); handle.flush(); rows.append(row)
    return rows


def safe_row(row: dict) -> dict:
    return {key: "'" + value if isinstance(value, str) and value.startswith(("=", "+", "-", "@", "\t", "\r")) else value for key, value in row.items()}


def write_report(path: Path, rows: list[dict]) -> None:
    with path.open("x", encoding="utf-8", newline="") as handle:
        if not rows:
            handle.write("status\nno results\n")
            return
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(safe_row(row) for row in rows)


def main() -> int:
    if TASK_CONFIG is None:
        raise TaskError("Download a task-specific runner from the local dashboard")
    profile = TASK_CONFIG
    parser = argparse.ArgumentParser(description=profile["description"])
    parser.add_argument("--credentials", type=Path, default=Path(__file__).resolve().parent / "credentials" / f"{profile['credential_slug']}.env")
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parent / "reports")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Offline plan only (also the default)")
    mode.add_argument("--preview", action="store_true", help="Live read-only preview; no mutations or asset downloads")
    mode.add_argument("--execute", action="store_true", help="Run locally; mutations require terminal confirmation")
    parser.add_argument("--tag")
    parser.add_argument("--owner")
    parser.add_argument("--category")
    parser.add_argument("--start", help="YYYY-MM-DD, UTC inclusive")
    parser.add_argument("--end", help="YYYY-MM-DD, UTC inclusive")
    args = parser.parse_args()
    inputs = read_input(args.input, profile)
    filters = entry_filters(args)
    if profile["input_columns"] and filters:
        raise TaskError("This task selects entries from its input CSV; report filters are not supported")
    print(json.dumps({"task": profile["id"], "permissions": profile["permissions"], "role_capability": profile["capability"],
                      "api_actions": profile["api_actions"], "input_rows": len(inputs), "filters": filters}, indent=2))
    if not args.execute and not args.preview:
        print("Offline plan only. No network calls. Use --preview for live reads or --execute to run.")
        return 0
    values = load_credentials(args.credentials, profile)
    client = Client(values, profile)
    client.authenticate()
    task = profile["id"]
    # Unique report directory; preserve every run and refuse collisions.
    args.output = args.output / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + task)
    args.output.mkdir(parents=True, exist_ok=False)
    if task in {"search-entries", "count-duration"}:
        rows = report_entries(client, args, task)
    elif task == "audit-captions" or (task == "download-captions" and args.preview):
        rows = caption_rows(client, inputs)
    elif task in {"download-captions", "download-media"}:
        if args.preview:
            rows = []
            for item in inputs:
                assets = paginate(client, "flavorAsset", "KalturaFlavorAssetFilter", {"entryIdEqual": item["entry_id"], "statusEqual": 2, "isOriginalEqual": 1})
                rows.append({"entry_id": item["entry_id"], "source_asset_count": len(assets)})
        else:
            rows = downloads(client, inputs, args, task)
    else:
        rows = mutation_plan(client, inputs, task)
        write_report(args.output / "plan.csv", rows)
        print(f"Reviewed {len(rows)} targets. Plan: {args.output / 'plan.csv'}")
        if args.execute:
            expected = "DELETE" if task == "delete-entries" else "APPLY"
            if input(f"Type {expected} to {profile['name']} on these targets: ").strip() != expected:
                print("Cancelled; no mutations made.")
                return 0
            rows = apply_mutations(client, rows, task, args.output / "mutation-log.csv")
    write_report(args.output / "results.csv", rows)
    print(f"Report: {args.output / 'results.csv'}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (TaskError, ValueError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
