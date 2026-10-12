# Kaltura AppToken dashboard

A local browser dashboard that generates scoped Kaltura AppToken provisioning scripts and eight task-specific Python runners. It runs on macOS and Windows with **Python 3.10 or later** and a modern browser. No additional Python packages, Node.js, hosted service, or AI connection are required.

The dashboard itself never calls Kaltura. It generates files in your browser. You run the downloaded Python scripts separately to make API calls.

## Start on Windows

Extract the entire ZIP first, then double-click **Start-KalturaAppTokens.cmd** inside the `app-token-dashboard` folder. The launcher checks `py -3`, `python`, and `python3` for a compatible Python version and opens the dashboard.

Alternatively, open PowerShell in that folder:

```powershell
py -3 scripts/app_token_dashboard.py
```

If needed, install Python from [python.org](https://www.python.org/downloads/) and reopen the launcher. An existing Python virtual environment also works.

## Start on Mac

Extract the entire ZIP, then double-click **Start-KalturaAppTokens.command**. The launcher checks `python3` and `python` and opens the dashboard. It locates its own folder, including paths containing spaces.

If the launcher does not open, use Terminal in the extracted folder:

```bash
python3 scripts/app_token_dashboard.py
```

If its executable permission was lost during download or copying, restore it from that folder:

```bash
chmod +x Start-KalturaAppTokens.command
```

Install Python 3.10+ from [python.org](https://www.python.org/downloads/) if needed; the macOS system Python is not assumed to be suitable.

## Dashboard workflow

1. Choose a script, or select custom permission groups to generate a token without a task runner.
2. Enter the Partner ID (PID), role name, purpose, and admin secret. You may leave the secret blank and enter it in the downloaded `.env` later.
3. Review the exact permission names. Write/delete-capable roles require an explicit acknowledgement.
4. Click **Prepare ZIP**, then **Download ZIP**. Download this to a folder and unzip it. The extracted folder includes the provisioning script, correctly named `.env`, README, permission guide, `.gitignore`, and any selected task runner/input CSV.
5. Preview provisioning offline, then run it yourself and confirm the plan.
6. Remove the provisioning `.env` containing the admin secret after the token is created. The task runner uses only the token credentials saved under `credentials/`.

The dashboard opens at **http://127.0.0.1:8766**. Keep its terminal window open; stop it with Ctrl+C. For a different port, run `python3 scripts/app_token_dashboard.py --port 8770` on Mac or `py -3 scripts/app_token_dashboard.py --port 8770` on Windows. `--no-browser` suppresses automatic browser opening.

The secret stays in browser memory until you download the ZIP containing `.env`; it is never submitted to the local server or stored in browser storage. Do not enter secrets in issues or pull requests. The default API URL is `https://www.kaltura.com/api_v3`.

## Run generated files

From the extracted bundle folder, run these on Mac:

```bash
python3 provision_app_token.py --dry-run
python3 provision_app_token.py
python3 run_task.py --input input.csv --dry-run
python3 run_task.py --input input.csv --preview
python3 run_task.py --input input.csv --execute
```

On Windows, replace `python3` with `py -3` or your installed `python` command. Search and count/duration tasks do not require `--input`; they accept optional `--tag`, `--owner`, `--category`, `--start`, and `--end` filters instead.

The exact `.env` filename matters. Windows editors must not append `.txt`; Mac Finder may hide dotfiles, so use the downloaded file in Terminal or an editor. ZIP extraction preserves the exact `.env` filename, avoiding browser renaming to `env.txt`. Keep ZIPs containing your admin secret private. Do not overwrite an existing credential file for another task.

Task runners default to **offline**. `--preview` performs live reads without mutations or asset downloads. `--execute` performs the task; write/delete tasks produce a plan and require typed terminal confirmation. The runners never fall back to an admin secret, expand permissions, or automatically retry writes.

## Supported tasks and permissions

| Task | Role category |
| --- | --- |
| Search entries | Read |
| Count & duration report | Read |
| Audit captions | Read |
| Download captions | Read |
| Download original media | Read |
| Rename entries | Read + write/edit |
| Set caption visibility | Read + write/edit + delete capability |
| Delete entries | Read + write/edit + delete capability |

Categories are navigation labels, not cumulative permission bundles. Deleting an entry does not grant metadata-edit permission. Caption visibility only updates `displayOnPlayer`, but the required `CAPTION_MODIFY` permission also allows caption deletion, which is disclosed in its preset.

See [task permissions and limitations](docs/scoped-tasks.md), [all reference scripts audited](docs/reference-script-audit.md), and the per-task [machine-readable manifests](tasks.json). These are focused independent implementations of selected reference tasks, not full replacements for every optional upstream feature.

**Account verification is pending.** Permission names and operations were reviewed offline, and tests use mocked API responses. Successful operation and the minimum grants on your specific account still need operator verification, especially caption `getUrl` access. The scripts stop on denial. Read-only presets have no create/update/rename/delete grants.

## Offline tests

From this package folder:

```bash
python3 -m unittest discover -s scripts -p 'test_*.py'
```

On Windows:

```powershell
py -3 -m unittest discover -s scripts -p 'test_*.py'
```

Tests use temporary local files, a loopback HTTP server, and mocked Kaltura responses. They do not call Kaltura or require credentials.

## References

- [Kaltura API guides](https://kaltura.md/llms.txt)
- [AppTokens guide](https://kaltura.md/raw/KALTURA_APPTOKENS_API.md)
- [Read-only reference scripts](https://github.com/Kaltura-EDU/api-scripts)

Credentials, generated reports, and private workspace files are not included in this package. Maintainer contribution notes are in [CONTRIBUTING.md](CONTRIBUTING.md).
