#!/usr/bin/env python3
"""Serve a local script generator. This process never calls Kaltura."""
from __future__ import annotations

import argparse
import json
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT


def provisioning_template() -> str:
    source = (ROOT / "scripts/create_scoped_app_token.py").read_text(encoding="utf-8")
    source = source.replace("import argparse\n", "import argparse\nimport base64\n")
    source = source.replace("    args = parse_args()", '''    config = json.loads(base64.b64decode("__CONFIG_BASE64__"))
    args = argparse.Namespace(**config)
    args.output = Path(__file__).resolve().parent / "credentials" / f"{slugify(args.role_name)}.env"
    if "--dry-run" in sys.argv:
        print(json.dumps(config, indent=2))
        print("Dry run: no network calls or files written.")
        return 0''')
    source = source.replace('    admin_secret = getpass.getpass("Kaltura admin secret (input hidden): ")', '''    env_path = Path(__file__).resolve().with_name(".env")
    if not env_path.is_file():
        raise ValueError("Save the downloaded .env beside this script first")
    environment = {}
    for line in env_path.read_text(encoding="utf-8-sig").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator:
            raise ValueError("Invalid .env line")
        value = value.strip()
        environment[key.strip()] = json.loads(value) if value.startswith('"') else value
    if environment.get("KALTURA_PARTNER_ID") != str(partner_id):
        raise ValueError("The .env PID does not match the script's reviewed PID")
    admin_secret = environment.get("KALTURA_ADMIN_SECRET", "")''')
    source = source.replace('raise KalturaError(f"HTTP {exc.code} from {service}.{action}: {detail}")', 'raise KalturaError(f"HTTP {exc.code} from {service}.{action}; response omitted to protect credentials")')
    source = source.replace('        detail = exc.read().decode("utf-8", errors="replace")\n', '')
    source = source.replace('raise KalturaError(f"{service}.{action}: {code}: {message}")', 'raise KalturaError(f"{service}.{action}: API error (response omitted to protect credentials)")')
    source = source.replace('    write_credentials(\n', '    try:\n        write_credentials(\n')
    # Indent the existing credential write, retaining partial-success recovery identifiers.
    start = source.index('        write_credentials(\n') + len('        write_credentials(\n')
    end = source.index('    print(f"\\nCreated role', start)
    source = source[:start] + ''.join('    ' + line if line.strip() else line for line in source[start:end].splitlines(keepends=True)) + '''    except OSError as exc:
        raise KalturaError(
            f"Role {role_id} and AppToken {token_id} were created, but saving failed. "
            "Do not rerun blindly: review these objects in Kaltura before trying again."
        ) from exc
''' + source[end:]
    source = source.replace('The admin secret was not saved.', 'Remove the provisioning .env when finished; keep the generated token credentials private.')
    source = source.replace('(KalturaError, ValueError, FileExistsError)', '(KalturaError, ValueError, OSError)')
    return source


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        expected_host = f"127.0.0.1:{self.server.server_port}"
        if self.headers.get("Host") != expected_host:
            self.send_error(403)
            return
        files = {"/": ("index.html", "text/html"), "/app.js": ("app.js", "text/javascript"),
                 "/style.css": ("style.css", "text/css"), "/groups.json": ("groups.json", "application/json"),
                 "/tasks.json": ("tasks.json", "application/json")}
        if self.path == "/template.py":
            body, kind = provisioning_template().encode(), "text/plain"
        elif self.path == "/task-template.py":
            body, kind = (ROOT / "scripts/scoped_task_runtime.py").read_bytes(), "text/plain"
        elif self.path in files:
            filename, kind = files[self.path]
            body = (ASSETS / filename).read_bytes()
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", kind + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; connect-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args) -> None:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()
    with ThreadingHTTPServer(("127.0.0.1", args.port), Handler) as server:
        url = f"http://127.0.0.1:{server.server_port}"
        print(f"Kaltura AppToken dashboard: {url}\nPress Ctrl+C to stop.", flush=True)
        if not args.no_browser:
            webbrowser.open(url)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
