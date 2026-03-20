#!/bin/bash
# X Feed Intel -- Cron wrapper for scheduled timeline fetch
# Runs via cron based on your preferred schedule.
# Also runs a backlog-only processor after fetch to clear leftover unclassified/unlinked posts.

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
  log_msg "INFO" "Pipeline lock busy; skipping scheduled fetch/backlog cycle"
  exit 0
fi

"$PYTHON_BIN" -m fetcher
FETCH_RC=$?

if [ "$FETCH_RC" -ne 0 ]; then
  log_msg "WARNING" "Fetcher exited non-zero ($FETCH_RC); skipping backlog clear for this cycle"
  exit "$FETCH_RC"
fi

"$PYTHON_BIN" -m backlog_clearer --max-passes 12
BACKLOG_RC=$?
if [ "$BACKLOG_RC" -ne 0 ]; then
  log_msg "WARNING" "Backlog clearer exited non-zero ($BACKLOG_RC) after successful fetch"
fi

exit "$BACKLOG_RC"
