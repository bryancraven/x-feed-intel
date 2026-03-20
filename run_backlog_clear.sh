#!/bin/bash
# X Feed Intel -- Backlog-only processor wrapper (safe to schedule after fetch)
# Uses the same pipeline lock as run_fetch.sh and exits cleanly if busy.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCKFILE="/tmp/x_feed_intel_pipeline.lock"
LOGFILE="${SCRIPT_DIR}/logs/x_feed_intel.log"
PYTHON_BIN="${SCRIPT_DIR}/venv/bin/python3"

log_msg() {
  local level="$1"
  local msg="$2"
  printf '%s - %-8s - [x_feed_intel] - %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$level" "$msg" >> "$LOGFILE"
}

cd "$SCRIPT_DIR" || exit 1

exec 9>"$LOCKFILE"
if ! flock -n 9; then
  log_msg "INFO" "Pipeline lock busy; skipping backlog clear invocation"
  exit 0
fi

"$PYTHON_BIN" -m backlog_clearer "$@"
