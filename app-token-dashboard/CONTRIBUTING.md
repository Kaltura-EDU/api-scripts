# Contribution notes

This folder is designed to be contributed as `app-token-dashboard/` in Kaltura-EDU/api-scripts, without changing existing reference scripts. All task implementations are independently authored; upstream script paths and the audited commit are recorded in `docs/reference-script-audit.md` and generated permission guides.

Before merging a contribution:

1. Run the offline test commands in README.md.
2. Verify launching and downloading files on both Windows and macOS. Browser generation has been tested on Windows; native macOS validation remains pending.
3. Have an account operator verify the candidate scoped grants using a dedicated test token, starting with live previews. Test write/delete workflows only against designated test entries. Record permission changes with their exact operations; do not resolve a denial by granting broad administration to read-only tasks.
4. Confirm no `.env`, `credentials/`, or generated `reports/` are staged.
5. Preserve executable mode for `Start-KalturaAppTokens.command` and LF line endings. From the contribution checkout, use `git update-index --chmod=+x app-token-dashboard/Start-KalturaAppTokens.command` if needed.

Do not add a new license or assume a license for reference code without maintainer agreement. The package introduces no copied upstream task implementations; its permission-name catalog records public API identifiers with source attribution.

For a new preset, update `tasks.json`, implement its bounded operations in `scripts/scoped_task_runtime.py`, document its required permissions and limitations, and add offline tests. Distinguish script effects from the remote role's broader capabilities. Do not include credentials or live API calls in tests.
