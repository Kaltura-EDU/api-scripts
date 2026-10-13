"""Offline tests: mock Kaltura responses, never authenticate against a real account."""
import base64
import contextlib
import csv
import hashlib
import io
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import scoped_task_runtime as runtime
from app_token_dashboard import ASSETS

CATALOG = json.loads((ASSETS / "tasks.json").read_text())


def profile(task_id):
    return {**next(task for task in CATALOG if task["id"] == task_id), "partner_id": "123", "credential_slug": task_id, "session_type": 2}


def credentials(task_id):
    return {"KALTURA_PARTNER_ID": "123", "KALTURA_SERVICE_URL": runtime.DEFAULT_URL,
            "KALTURA_APP_TOKEN_ID": "1_token", "KALTURA_APP_TOKEN_VALUE": "offline-secret",
            "KALTURA_APP_TOKEN_HASH_TYPE": "SHA256", "KALTURA_APP_TOKEN_USER_ID": "task-user",
            "KALTURA_APP_TOKEN_SESSION_TYPE": "2", "KALTURA_APP_TOKEN_SESSION_DURATION": "900",
            "KALTURA_ROLE_ID": "42", "KALTURA_ROLE_PERMISSIONS": ",".join(profile(task_id)["permissions"])}


class TaskTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)

    def input_file(self, columns, rows):
        path = self.folder / "input.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle); writer.writerow(columns); writer.writerows(rows)
        return path

    def test_authentication_uses_hash_exchange_without_admin_secret(self):
        client = runtime.Client(credentials("search-entries"), profile("search-entries"))
        with patch.object(client, "_post", side_effect=[{"ks": "widget-ks"}, {"ks": "scoped-ks"}]) as api:
            client.authenticate()
        self.assertEqual(client.ks, "scoped-ks")
        self.assertEqual([call.args[:2] for call in api.call_args_list], [("session", "startWidgetSession"), ("appToken", "startSession")])
        exchange = api.call_args_list[1].args[2]
        self.assertEqual(exchange["tokenHash"], hashlib.sha256(b"widget-ksoffline-secret").hexdigest())
        self.assertNotIn("secret", exchange)
        self.assertNotIn("sessionPrivileges", exchange)

    def test_manifest_blocks_content_actions_outside_task(self):
        client = runtime.Client(credentials("search-entries"), profile("search-entries")); client.ks = "ks"
        with patch.object(client, "_post") as api:
            with self.assertRaisesRegex(runtime.TaskError, "outside"): client.call("baseEntry", "delete", entryId="1_entry")
            api.assert_not_called()

    def test_credential_pid_and_exact_grants(self):
        path = self.folder / ".env"
        values = credentials("search-entries")
        path.write_text("\n".join(f"{key}={json.dumps(value)}" for key, value in values.items()))
        self.assertEqual(runtime.load_credentials(path, profile("search-entries"))["KALTURA_APP_TOKEN_VALUE"], "offline-secret")
        values["KALTURA_ROLE_PERMISSIONS"] += ",CONTENT_MANAGE_DELETE"
        path.write_text("\n".join(f"{key}={value}" for key, value in values.items()))
        with self.assertRaisesRegex(runtime.TaskError, "exact permissions"): runtime.load_credentials(path, profile("search-entries"))
        values = credentials("search-entries"); values["KALTURA_PARTNER_ID"] = "456"
        path.write_text("\n".join(f"{key}={value}" for key, value in values.items()))
        with self.assertRaisesRegex(runtime.TaskError, "PID"): runtime.load_credentials(path, profile("search-entries"))

    def test_generated_runners_for_all_tasks_default_to_offline(self):
        source = Path(runtime.__file__).read_text(encoding="utf-8")
        for item in CATALOG:
            config = profile(item["id"])
            encoded = base64.b64encode(json.dumps(config).encode()).decode()
            generated = source.replace("TASK_CONFIG = None  # __TASK_CONFIG__", f'TASK_CONFIG = json.loads(base64.b64decode("{encoded}"))')
            module = types.ModuleType("generated")
            module.__file__ = str(self.folder / "run_task.py")
            exec(compile(generated, module.__file__, "exec"), module.__dict__)
            argv = [module.__file__]
            if config["input_columns"]:
                path = self.input_file(config["input_columns"], [config["sample_row"]])
                argv += ["--input", str(path)]
            with self.subTest(task=item["id"]), patch("sys.argv", argv), patch.object(module.Client, "authenticate") as auth, \
                    patch.object(module, "load_credentials") as load, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(module.main(), 0); auth.assert_not_called(); load.assert_not_called()

    def test_csv_rejects_duplicate_and_missing_targets(self):
        config = profile("rename-entries")
        path = self.input_file(config["input_columns"], [["1_entry", "Current", "New"], ["1_entry", "Current", "Other"]])
        with self.assertRaisesRegex(runtime.TaskError, "Repeated"): runtime.read_input(path, config)
        path = self.input_file(config["input_columns"], [["1_entry", "", "New"]])
        with self.assertRaisesRegex(runtime.TaskError, "Missing expected_name"): runtime.read_input(path, config)

    def test_caption_visibility_value_is_restricted(self):
        config = profile("caption-visibility")
        path = self.input_file(config["input_columns"], [["1_entry", "1_caption", "English", "false"]])
        with self.assertRaisesRegex(runtime.TaskError, "0 or 1"): runtime.read_input(path, config)

    def test_rename_sends_only_name_and_logs_success(self):
        client = Mock()
        client.call.side_effect = [{"id": "1_entry", "name": "Current"}, {"id": "1_entry", "name": "Current"}, {"id": "1_entry", "name": "New"}]
        inputs = [{"entry_id": "1_entry", "expected_name": "Current", "new_name": "New"}]
        plan = runtime.mutation_plan(client, inputs, "rename-entries")
        rows = runtime.apply_mutations(client, plan, "rename-entries", self.folder / "audit.csv")
        call = client.call.call_args_list[-1]
        self.assertEqual(call.args, ("baseEntry", "update"))
        self.assertEqual(call.kwargs, {"entryId": "1_entry", "baseEntry[objectType]": "KalturaBaseEntry", "baseEntry[name]": "New"})
        self.assertEqual(rows[0]["status"], "success")
        log = (self.folder / "audit.csv").read_text()
        self.assertIn("attempting", log); self.assertIn("success", log)

    def test_expected_name_mismatch_stops_before_update(self):
        client = Mock(); client.call.return_value = {"id": "1_entry", "name": "Changed"}
        with self.assertRaisesRegex(runtime.TaskError, "Expected name mismatch"):
            runtime.mutation_plan(client, [{"entry_id": "1_entry", "expected_name": "Current", "new_name": "New"}], "rename-entries")
        self.assertEqual(client.call.call_count, 1)
        self.assertEqual(client.call.call_args.args, ("baseEntry", "get"))

    def test_mutation_failure_stops_without_retry_or_next_target(self):
        client = Mock(); client.call.side_effect = [{"id": "1_entry", "name": "Current"}, runtime.TaskError("timeout")]
        plan = [{"entry_id": "1_entry", "expected_name": "Current", "old_value": "Current", "new_value": "DELETE"},
                {"entry_id": "1_next", "expected_name": "Next", "old_value": "Next", "new_value": "DELETE"}]
        with self.assertRaisesRegex(runtime.TaskError, "Stopped at entry 1_entry"):
            runtime.apply_mutations(client, plan, "delete-entries", self.folder / "audit.csv")
        self.assertEqual(client.call.call_count, 2)
        self.assertIn("outcome unknown", (self.folder / "audit.csv").read_text())

    def test_caption_update_has_no_delete_or_content_fields(self):
        client = Mock(); client.call.side_effect = [{"objects": [{"id": "1_caption", "label": "English"}]}, {"id": "1_caption"}]
        plan = [{"entry_id": "1_entry", "caption_id": "1_caption", "expected_label": "English", "display_on_player": "0", "old_value": 1, "new_value": "0"}]
        runtime.apply_mutations(client, plan, "caption-visibility", self.folder / "audit.csv")
        self.assertEqual(client.call.call_args.args, ("caption_captionAsset", "update"))
        self.assertEqual(set(client.call.call_args.kwargs), {"id", "captionAsset[objectType]", "captionAsset[displayOnPlayer]"})

    def test_pagination_refuses_incomplete_or_repeated_results(self):
        client = Mock(); client.call.return_value = {"objects": [{"id": "1_entry"}], "totalCount": 500}
        with self.assertRaisesRegex(runtime.TaskError, "Incomplete"): runtime.paginate(client, "media", "KalturaMediaEntryFilter", {})
        client.call.return_value = {"objects": [{"id": "1_entry"}, {"id": "1_entry"}], "totalCount": 2}
        with self.assertRaisesRegex(runtime.TaskError, "Repeated IDs"): runtime.paginate(client, "media", "KalturaMediaEntryFilter", {})

    def test_count_duration_needs_no_analytics_call(self):
        client = Mock(); client.call.return_value = {"objects": [{"id": "1_entry", "duration": 30}, {"id": "1_other", "duration": 40}], "totalCount": 2}
        args = types.SimpleNamespace(tag=None, owner=None, category=None, start="2026-01-01", end="2026-01-31")
        with contextlib.redirect_stdout(io.StringIO()) as output:
            rows = runtime.report_entries(client, args, "count-duration")
        self.assertEqual(len(rows), 2); self.assertIn("70.0 seconds", output.getvalue())
        self.assertEqual(client.call.call_args.args, ("media", "list"))

    def test_caption_audit_records_missing_assets(self):
        client = Mock(); client.call.return_value = {"objects": [], "totalCount": 0}
        rows = runtime.caption_rows(client, [{"entry_id": "1_entry"}])
        self.assertEqual(rows[0]["caption_count"], 0)

    def test_media_download_targets_only_ready_source(self):
        client = Mock(); client.call.side_effect = [{"objects": [{"id": "1_asset", "fileExt": "mp4"}], "totalCount": 1}, "https://cdn.example.invalid/file"]
        args = types.SimpleNamespace(output=self.folder)
        with patch.object(runtime, "save_download") as download:
            rows = runtime.downloads(client, [{"entry_id": "1_entry"}], args, "download-media")
        filters = client.call.call_args_list[0].kwargs
        self.assertEqual(filters["filter[isOriginalEqual]"], 1); self.assertEqual(filters["filter[statusEqual]"], 2)
        self.assertEqual(download.call_args.args[1].name, "1_entry_1_asset.mp4")
        self.assertEqual(rows[0]["status"], "downloaded")

    def test_report_escapes_formulas_and_never_overwrites(self):
        path = self.folder / "result.csv"
        runtime.write_report(path, [{"name": "=HYPERLINK()", "duration": 30}])
        self.assertIn("'=HYPERLINK()", path.read_text())
        with self.assertRaises(FileExistsError): runtime.write_report(path, [{"name": "replacement"}])

    def test_catalog_names_and_role_categories(self):
        catalog = {line.strip() for line in (ASSETS / "permissions/kaltura-reference-catalog.txt").read_text().splitlines() if line.strip() and not line.startswith("#")}
        for task in CATALOG:
            self.assertTrue(set(task["permissions"]).issubset(catalog))
            if task["capability"] == "read":
                self.assertFalse(set(task["permissions"]) & {"CAPTION_MODIFY", "CONTENT_MANAGE_METADATA", "CONTENT_MANAGE_DELETE"})
                self.assertTrue(all(action not in {"add", "update", "delete"} for _, action in task["api_actions"]))
        self.assertEqual(profile("caption-visibility")["capability"], "delete")
        self.assertNotIn("CONTENT_MANAGE_METADATA", profile("delete-entries")["permissions"])

    def test_live_preview_and_declined_execution_never_mutate(self):
        config = profile("rename-entries")
        path = self.input_file(config["input_columns"], [["1_entry", "Current", "New"]])
        for mode in ("--preview", "--execute"):
            with self.subTest(mode=mode), patch.object(runtime, "TASK_CONFIG", config), \
                    patch("sys.argv", ["run_task.py", "--input", str(path), "--output", str(self.folder / "reports"), mode]), \
                    patch.object(runtime, "load_credentials", return_value=credentials("rename-entries")), \
                    patch.object(runtime.Client, "authenticate"), \
                    patch.object(runtime.Client, "call", return_value={"id": "1_entry", "name": "Current"}) as api, \
                    patch("builtins.input", return_value="CANCEL") as prompt, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(runtime.main(), 0)
                self.assertEqual([call.args for call in api.call_args_list], [("baseEntry", "get")])
                if mode == "--preview": prompt.assert_not_called()

    def test_transport_reports_limits_without_echoing_error_secrets(self):
        client = runtime.Client(credentials("search-entries"), profile("search-entries"))
        for code, expected in [("QUERY_EXCEEDED_MAX_MATCHES_ALLOWED", "narrow"), ("SERVICE_FORBIDDEN", "API rejected")]:
            payload = {"objectType": "KalturaAPIException", "code": code, "message": "private-scoped-session"}
            with patch.object(runtime.urllib.request, "urlopen") as transport:
                transport.return_value.__enter__.return_value = io.BytesIO(json.dumps(payload).encode())
                with self.assertRaises(runtime.TaskError) as error: client._post("media", "list", {})
                self.assertIn(expected, str(error.exception))
                self.assertNotIn("private-scoped-session", str(error.exception))

    def test_download_requires_https_and_preserves_existing_file(self):
        path = self.folder / "media.mp4"
        with self.assertRaisesRegex(runtime.TaskError, "HTTPS"): runtime.save_download("http://example.invalid/file", path)
        path.write_bytes(b"existing")
        with patch.object(runtime.urllib.request, "build_opener") as opener:
            with self.assertRaises(FileExistsError): runtime.save_download("https://example.invalid/file", path)
            opener.assert_not_called()
        self.assertEqual(path.read_bytes(), b"existing")


if __name__ == "__main__":
    unittest.main()
