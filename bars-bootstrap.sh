#!/usr/bin/env bash
# One-time: fill the archive as deep as the upstream will go.
#
#   ./bars-bootstrap.sh              pool first, then the whole universe
#   ./bars-bootstrap.sh --pool-only  first hundred only, ~35 minutes
#
# Yahoo serves two depths, and one run of this collects both:
#
#     1m   last  7 days
#     5m   last 58 days
#
# Which is worth knowing before waiting on it: every window the site
# draws - 1D, 3D, 7D, 20D, 2M - is inside those two depths. So after ONE
# successful run the boards could be served entirely from our own files.
# The archive's value from then on is not reaching further back, it is
# keeping what the upstream throws away going forward.
#
# Runs the pool first on purpose. A hundred stocks finishes in about half
# an hour and proves the whole path - fetch, bronze, parse, partition -
# while you are still awake to look at it. The universe then takes hours
# and can be left alone.
#
# Re-running is safe. Every partition is merged rather than replaced, so
# a run that dies at stock 400 loses nothing and the next one continues.

set -u -o pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ENV_FILE=${ENV_FILE:-/etc/intraday-archive.env}
LOG=${BARS_LOG:-$HOME/logs/bars-bootstrap.log}
PACE=${BARS_PACE:-10}

if [ -r "$ENV_FILE" ]; then
  set -a; . "$ENV_FILE"; set +a
fi

mkdir -p "$(dirname "$LOG")"
exec > >(tee -a "$LOG") 2>&1
echo
echo "=== bars bootstrap $(date '+%Y-%m-%d %H:%M:%S %Z') ================="

cd "$HERE" || exit 1

# -u so progress reaches the log while it is still useful. Python buffers
# stdout when it is a file, which would leave this empty for seven hours
# and then print everything at the moment you no longer need it.
PY="python3 -u"

# Checked once, up front. Without this a missing duckdb surfaced as the
# pool run failing, which sent the script down its no-database fallback -
# where it failed again, for the same reason, wearing a different message.
# A dependency problem must not be reported as a data problem.
$PY - <<'CHECK' || {
import duckdb  # noqa
CHECK
  echo "duckdb is not installed. On Debian/Ubuntu:" >&2
  echo "    apt install -y python3-pip" >&2
  echo "    pip3 install --break-system-packages duckdb" >&2
  exit 1
}

echo
echo "--- 1. a hundred stocks: proves the path, ~35 min ---"
$PY bars_fetch.py --all --limit 100 --pace "$PACE" || {
  echo "first run failed - stopping before the long one" >&2
  exit 1
}

if [ "${1:-}" = "--pool-only" ]; then
  echo
  echo "--pool-only: stopping here"
  exit 0
fi

echo
echo "--- 2. the universe: every liquid stock, several hours ---"
# Wider than the pool on purpose. The pool changes nightly, and a stock
# fetched only on the days it happened to be picked has holes exactly
# where a later study would want continuity.
$PY bars_fetch.py --all --pace "$PACE" || {
  echo "universe run did not finish - what was fetched is kept, re-run to continue" >&2
  exit 1
}

echo
echo "=== done $(date '+%H:%M:%S') ==========================================="
