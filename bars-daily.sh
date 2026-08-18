#!/usr/bin/env bash
# Nightly: yesterday's session, plus enough overlap to heal a missed night.
#
#   ./bars-daily.sh
#
# --days-back 3 rather than 1, and the reason is the whole point of the
# archive. With 1, a night that fails takes that session with it for good;
# the upstream keeps about seven days of 1-minute data and will not be
# asked twice. With 3, tonight's failure is repaired by tomorrow's run.
#
# The overlap is nearly free: it is ONE request either way, just a larger
# response, and at ten seconds between requests the pacing dominates
# completely. It is not set higher because a wider window means re-merging
# more old partitions every night, and a corporate action landing inside
# that window would put some days on a new price basis while older ones
# stayed on the old.
#
# Pushes 1-minute data to the trading box afterwards, if BARS_PEER is set.
# Fetch once and replicate, rather than both boxes fetching independently:
# half the upstream load, and two copies that are identical rather than
# merely similar.

set -u -o pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ENV_FILE=${ENV_FILE:-/etc/intraday-archive.env}
LOG=${BARS_LOG:-$HOME/logs/bars-daily.log}
PACE=${BARS_PACE:-10}
BACK=${BARS_DAYS_BACK:-3}

if [ -r "$ENV_FILE" ]; then
  set -a; . "$ENV_FILE"; set +a
fi

DATA=${DATA_DIR:-$HERE/data}

mkdir -p "$(dirname "$LOG")"
exec > >(tee -a "$LOG") 2>&1
echo
echo "=== bars daily $(date '+%Y-%m-%d %H:%M:%S %Z') ====================="

cd "$HERE" || exit 1

# Up front, so a missing dependency is reported as itself rather than as
# a night with no data.
python3 - <<'CHECK' || {
import duckdb  # noqa
CHECK
  echo "duckdb is not installed - see archive/README.md" >&2
  exit 1
}

# ------------------------------------------------------- corporate actions
# First, and never fatal. NSE publishes a rolling window of the next few
# weeks, so a night that misses this loses actions permanently - and an
# unrecorded split is indistinguishable from a real 5x move once the bars
# are in. But a stock that splits is still worth having bars for, so a
# refusal from NSE must not take the fetch down with it.
echo
echo "--- corporate actions ---"
python3 -u corp_actions.py || echo "  nse did not answer - carrying on"

echo
echo "--- fetch: last $BACK day(s), every liquid stock ---"
python3 -u bars_fetch.py --all --days-back "$BACK" --pace "$PACE"
RC=$?
if [ $RC -ne 0 ]; then
  echo "fetch did not finish cleanly (rc=$RC) - what was collected is kept" >&2
fi

# ------------------------------------------------------------- replicate
# 1-minute only. The trading box runs the engine, which reads minute bars;
# 5-minute exists for the website's longer boards and has no reader there.
# Sending it anyway would mix two grains in a store that only wants one.
if [ -n "${BARS_PEER:-}" ]; then
  echo
  echo "--- replicate 1m to $BARS_PEER ---"
  SRC="$DATA/silver/bars/interval=1m/"
  if [ -d "$SRC" ]; then
    # A dedicated key, not the login key. The receiving box holds the
    # broker credentials, so the one that reaches it from a public web
    # server should be able to do this and nothing else - restricted in
    # authorized_keys on the far side, named here.
    SSH_CMD="ssh"
    [ -n "${BARS_PEER_KEY:-}" ] && \
      SSH_CMD="ssh -i $BARS_PEER_KEY -o IdentitiesOnly=yes"
    # No --delete. This is a copy for redundancy, and a bug here that
    # emptied the source must not empty the replica too - that is the one
    # failure a second copy exists to survive.
    rsync -az --info=stats1 -e "$SSH_CMD" "$SRC" "$BARS_PEER" \
      || echo "  replication failed - the local archive is intact" >&2
  else
    echo "  nothing to replicate yet"
  fi
else
  echo
  echo "  BARS_PEER unset - skipping replication"
  echo "  set it to e.g. bars@replica-host:. - a bare dot, because"
  echo "  rrsync chroots the far side and appends anything you add"
fi

echo
echo "=== done $(date '+%H:%M:%S') ==========================================="
exit $RC
