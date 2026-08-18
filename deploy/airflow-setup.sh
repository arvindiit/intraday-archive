#!/usr/bin/env bash
# Install Airflow and register the archive DAG. Run once, as root.
#
#   ./deploy/airflow-setup.sh
#
# Choices worth knowing before you run it:
#
# SQLITE, NOT A DATABASE SERVER. Airflow's metadata store can be Postgres
# or MySQL, and for one DAG with one six-hour task it would buy nothing.
# SQLite forces SequentialExecutor, which runs tasks one at a time - the
# cost here is about two minutes, because corp_actions would otherwise run
# alongside a fetch measured in hours. Parallelism you do not need is a
# second thing that can break. The upgrade is a connection string and an
# executor name, not a rebuild.
#
# LOOPBACK ONLY. The Airflow UI can trigger jobs and read logs, so it is
# bound to 127.0.0.1 and reached over an SSH tunnel:
#
#     ssh -L 8081:127.0.0.1:8081 user@your-server
#
# A public Airflow with a default password is a remote shell wearing a
# web page.
#
# ITS OWN VIRTUALENV. Airflow pins a long list of libraries; sharing an
# interpreter with the website means one of them eventually wins an
# argument neither of you knew you were having.

set -eu -o pipefail

AIRFLOW_HOME=${AIRFLOW_HOME:-/opt/airflow}
ARCHIVE_DIR=${ARCHIVE_DIR:-/opt/intraday-archive}
DAGS_SRC=${DAGS_SRC:-/opt/intraday-archive/dags}
VERSION=${AIRFLOW_VERSION:-2.11.0}
PORT=${AIRFLOW_PORT:-8081}
PY_TAG=$(python3 -c 'import sys;print("%d.%d"%sys.version_info[:2])')

echo "=== airflow $VERSION on python $PY_TAG -> $AIRFLOW_HOME ==="

if [ "$PY_TAG" = "3.13" ] && [ "${VERSION%%.*}" = "2" ]; then
  echo "Airflow 2.x does not support Python 3.13." >&2
  echo "Either use python3.12, or AIRFLOW_VERSION=3.0.6 ./airflow-setup.sh" >&2
  exit 1
fi

# ------------------------------------------------------------------ install
apt-get install -y python3-venv >/dev/null 2>&1 || true
python3 -m venv "$AIRFLOW_HOME/venv"
PIP="$AIRFLOW_HOME/venv/bin/pip"
$PIP install --quiet --upgrade pip

# Constraints, not bare pip. Airflow depends on a few hundred packages and
# the combination that actually works is published per release; without
# this you get whatever resolved today, which is how an install that
# worked last month stops working.
$PIP install --quiet "apache-airflow==$VERSION" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$VERSION/constraints-$PY_TAG.txt"

export AIRFLOW_HOME
AF="$AIRFLOW_HOME/venv/bin/airflow"

# ------------------------------------------------------------------- config
# Through environment variables, not by editing airflow.cfg. The file is
# generated lazily - `airflow version` does not write it on 2.11 - so a
# script that seds it is a script that fails on a clean box. Every setting
# has an AIRFLOW__SECTION__KEY form, it wins over the file, and it survives
# an upgrade rewriting the config.
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_HOME/dags"
# The DAG carries its own timezone so the schedule is already right, but
# the UI renders in this one - and reading 14:00 for a job you know runs at
# 19:30 wastes an evening at least once.
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Kolkata
export AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=Asia/Kolkata
export AIRFLOW__WEBSERVER__WEB_SERVER_HOST=127.0.0.1
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT="$PORT"
# The scheduler rereads every DAG file on a timer; thirty seconds is the
# default and pointless for a file that changes when someone deploys.
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=300

$AF db migrate

# ---------------------------------------------------------------- the admin
# Created only if absent, so re-running this script does not reset a
# password somebody changed.
if ! $AF users list 2>/dev/null | grep -q admin; then
  PW=$(python3 -c "import secrets;print(secrets.token_urlsafe(12))")
  $AF users create --username admin --firstname a --lastname a \
      --role Admin --email admin@example.com --password "$PW" >/dev/null
  echo
  echo "  airflow admin password: $PW"
  echo "  (shown once - store it somewhere)"
fi

# ------------------------------------------------------------------- the DAG
mkdir -p "$AIRFLOW_HOME/dags"
# Symlinked, not copied, so `git pull` in the utilities checkout updates
# what the scheduler reads. A copy is a second version that drifts.
ln -sfn "$DAGS_SRC/intraday_archive.py" "$AIRFLOW_HOME/dags/intraday_archive.py"

# ----------------------------------------------------------------- services
# The settings above are exported in this shell; the services get their own
# copy, or they would start with airflow.cfg's defaults and quietly ignore
# every choice made here.
cat > "$AIRFLOW_HOME/env" <<ENV
AIRFLOW_HOME=$AIRFLOW_HOME
ARCHIVE_DIR=$ARCHIVE_DIR
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW_HOME/dags
AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Kolkata
AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=Asia/Kolkata
AIRFLOW__WEBSERVER__WEB_SERVER_HOST=127.0.0.1
AIRFLOW__WEBSERVER__WEB_SERVER_PORT=$PORT
AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=300
ENV

for svc in scheduler webserver; do
  cat > "/etc/systemd/system/airflow-$svc.service" <<EOF
[Unit]
Description=Airflow $svc
After=network.target

[Service]
EnvironmentFile=$AIRFLOW_HOME/env
# BARS_PEER and BARS_PEER_KEY live here. Without them the replicate task
# skips itself and says so, which is correct but not what you want on the
# box that does the fetching.
EnvironmentFile=-/etc/intraday-archive.env
ExecStart=$AIRFLOW_HOME/venv/bin/airflow $svc
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
done

systemctl daemon-reload
systemctl enable --now airflow-scheduler airflow-webserver

echo
echo "=== done ==="
echo "  systemctl status airflow-scheduler airflow-webserver"
echo
echo "  UI is on 127.0.0.1:$PORT - reach it with:"
echo "      ssh -L $PORT:127.0.0.1:$PORT user@your-server"
echo "  then open http://localhost:$PORT"
echo
echo "  The DAG is paused on arrival. Unpause it in the UI when you have"
echo "  looked at it, or:  $AF dags unpause intraday_archive"
