#!/bin/bash
set -u
dashboard_directory="$(cd -- "$(dirname -- "$0")" && pwd)" || exit 1
cd "$dashboard_directory" || exit 1

dashboard_python=""
for candidate in python3 python; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import sys; sys.exit(sys.version_info < (3, 10))' >/dev/null 2>&1; then
        dashboard_python="$candidate"
        break
    fi
done

if [ -z "$dashboard_python" ]; then
    printf '%s\n' 'Python 3.10 or later is required. Install it from https://www.python.org/downloads/'
    printf '%s\n' 'Then reopen this launcher. No additional Python packages are required.'
    if [ -t 0 ]; then read -r -p 'Press Return to close.'; fi
    exit 1
fi

"$dashboard_python" scripts/app_token_dashboard.py "$@"
dashboard_exit=$?
if [ "$dashboard_exit" -ne 0 ] && [ -t 0 ]; then read -r -p 'The dashboard stopped. Press Return to close.'; fi
exit "$dashboard_exit"
