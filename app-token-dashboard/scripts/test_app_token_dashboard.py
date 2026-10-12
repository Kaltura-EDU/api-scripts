"""Offline verification of the generated provisioning script and local HTTP boundary."""
import base64
import contextlib
import io
import json
import tempfile
import threading
import types
import unittest
from pathlib import Path
from unittest.mock import patch
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer

from app_token_dashboard import Handler, provisioning_template, ASSETS


class GeneratorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.config = dict(partner_id="123", role_name="Inventory", permissions="BASE_USER_SESSION_PERMISSION,CONTENT_MANAGE_BASE,CAPTION_SEARCH",
                           permissions_file=None, description="Read inventory", user_id="inventory", session_type="admin",
                           session_duration=900, extra_privilege=[], service_url="https://www.kaltura.com/api_v3", output=None, yes=False)
        encoded = base64.b64encode(json.dumps(self.config).encode()).decode()
        self.module = types.ModuleType("generated")
        self.module.__file__ = str(self.folder / "provision_app_token.py")
        source = provisioning_template().replace("__CONFIG_BASE64__", encoded)
        exec(compile(source, self.module.__file__, "exec"), self.module.__dict__)
        self.secret = 'test-secret-with-"quotes"-and-\\slashes'
        (self.folder / ".env").write_text(f'KALTURA_PARTNER_ID="123"\nKALTURA_ADMIN_SECRET={json.dumps(self.secret)}\n', encoding="utf-8")

    def run_main(self, argv=None, answer="yes", effects=None):
        with patch("sys.argv", argv or ["provision_app_token.py"]), patch("builtins.input", return_value=answer), \
                patch.object(self.module, "api_post", side_effect=effects) as api, contextlib.redirect_stdout(io.StringIO()):
            result = self.module.main()
        return result, api

    def test_dry_run_never_reads_secret_or_calls_api(self):
        (self.folder / ".env").unlink()
        result, api = self.run_main(["provision_app_token.py", "--dry-run"])
        self.assertEqual(result, 0)
        api.assert_not_called()
        self.assertFalse((self.folder / "credentials").exists())

    def test_decline_never_calls_api(self):
        (self.folder / ".env").unlink()
        result, api = self.run_main(answer="no")
        self.assertEqual(result, 0)
        api.assert_not_called()

    def test_exact_scope_and_secret_handling(self):
        result, api = self.run_main(effects=["admin-ks", {"id": 42}, {"id": "token-id", "token": "token-secret"}])
        self.assertEqual(result, 0)
        self.assertEqual([call.args[1:3] for call in api.call_args_list], [("session", "start"), ("userRole", "add"), ("appToken", "add")])
        self.assertEqual(api.call_args_list[0].args[3]["secret"], self.secret)
        self.assertEqual(api.call_args_list[1].args[3]["userRole[permissionNames]"], self.config["permissions"])
        token = api.call_args_list[2].args[3]
        self.assertEqual(token["appToken[sessionPrivileges]"], "setrole:42")
        self.assertEqual(token["appToken[hashType]"], "SHA256")
        output = (self.folder / "credentials/inventory.env").read_text()
        self.assertIn("token-secret", output)
        self.assertNotIn(self.secret, output)
        with patch.object(self.module, "api_post") as api:
            with self.assertRaises(FileExistsError): self.module.main()
            api.assert_not_called()

    def test_pid_mismatch_stops_before_api(self):
        (self.folder / ".env").write_text('KALTURA_PARTNER_ID="456"\nKALTURA_ADMIN_SECRET="unused"\n')
        with patch("builtins.input", return_value="yes"), patch.object(self.module, "api_post") as api, contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, "PID"): self.module.main()
            api.assert_not_called()

    def test_partial_role_failure_reports_id(self):
        with patch("builtins.input", return_value="yes"), patch.object(self.module, "api_post", side_effect=["ks", {"id": 42}, RuntimeError("failed")]), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(self.module.KalturaError, "Role 42 was created"): self.module.main()

    def test_save_failure_reports_both_ids(self):
        with patch("builtins.input", return_value="yes"), patch.object(self.module, "api_post", side_effect=["ks", {"id": 42}, {"id": "token-id", "token": "secret"}]), \
                patch.object(self.module, "write_credentials", side_effect=OSError("full")), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(self.module.KalturaError, "Role 42 and AppToken token-id were created"): self.module.main()

    def test_read_groups_exclude_write_permissions(self):
        groups = json.loads((ASSETS / "groups.json").read_text())
        mutations = ("_ADD", "_UPDATE", "_DELETE", "_MODIFY", "_METADATA", "_MANAGE")
        for group in groups:
            if group["mode"] == "read":
                for permission in group["permissions"]:
                    self.assertFalse(permission.endswith(mutations), permission)


class HttpTests(unittest.TestCase):
    def test_assets_and_execution_boundary(self):
        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                for path, expected in [("/", 200), ("/app.js", 200), ("/style.css", 200), ("/groups.json", 200), ("/tasks.json", 200), ("/template.py", 200), ("/task-template.py", 200), ("/.env", 404), ("/../credentials/secret.env", 404)]:
                    conn = HTTPConnection("127.0.0.1", server.server_port)
                    conn.request("GET", path)
                    response = conn.getresponse()
                    self.assertEqual(response.status, expected)
                    if expected == 200: self.assertIn("connect-src 'self'", response.getheader("Content-Security-Policy"))
                    response.read(); conn.close()
                conn = HTTPConnection("127.0.0.1", server.server_port)
                conn.request("GET", "/", headers={"Host": "evil.example"})
                self.assertEqual(conn.getresponse().status, 403); conn.close()
                conn = HTTPConnection("127.0.0.1", server.server_port)
                conn.request("POST", "/run", body="ignored")
                self.assertEqual(conn.getresponse().status, 501); conn.close()
            finally:
                server.shutdown(); thread.join()


if __name__ == "__main__":
    unittest.main()
