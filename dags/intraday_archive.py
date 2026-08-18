"""The nightly archive, as a DAG.

    fetch_bars ──► verify ──► replicate ──┐
                                          ├──► summary
    corp_actions ─────────────────────────┘

Four tasks, one real fan-in, and every decision below is one the shell
script had to make implicitly.

WHY catchup IS OFF, WHICH IS THE INTERESTING ONE
------------------------------------------------
Airflow's catchup exists to run the days a pipeline missed, and for most
pipelines that is exactly right - the source still has the data, so a
backfill recovers it.

It is meaningless here. Yahoo serves about seven days of 1-minute bars and
nothing older, so asking it for a session from three weeks ago returns
nothing at all. A catchup run would spend hours collecting empty responses
and report success. The overlap in `--days-back 3` is the recovery
mechanism instead: each night re-fetches the last three, so a failure is
repaired by the next run rather than by a backfill that cannot work.

That is the general point worth taking from this file: **backfill is only
a recovery strategy when the source can still answer.** For a perishable
source the only recovery is having collected it the first time.

WHY max_active_runs IS 1
------------------------
The fetch takes hours. On a daily schedule two runs would otherwise
overlap, both writing the same partitions, and the merge is safe per file
but the pacing is not - two runs mean twice the request rate at the
upstream, which is how a job that worked at 5 seconds starts collecting
429s for reasons nobody can reproduce.

WHY verify GATES replicate
--------------------------
Bad data that reached the other box is worse than no data that did not.
The checks run after the fetch and before anything downstream reads the
result, because a check that runs afterwards only tells you how long you
were wrong.
"""

import os
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

# Airflow 3 moved the built-in operators into the standard provider, and
# Airflow 2 does not support Python 3.13 - so which of these works depends
# on the interpreter the box happens to have. Trying both is two lines and
# removes a class of "works on my machine" that has nothing to do with the
# pipeline.
try:                                        # Airflow 3.x
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:                         # Airflow 2.x
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator

ARCHIVE = os.environ.get("ARCHIVE_DIR", "/opt/intraday-archive")

default_args = {
    "owner": "archive",
    # Retries with a widening gap. The upstream's refusals are temporary
    # and uncoordinated, so a fixed retry lands in the same crowded moment
    # every time.
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
    "depends_on_past": False,
}

with DAG(
    dag_id="intraday_archive",
    description="Fetch NSE intraday bars before the upstream forgets them",
    # Timezone-aware on purpose. Airflow schedules in UTC unless the
    # start_date carries a zone - so a bare datetime here would fire this
    # at 19:30 UTC, which is 01:00 the next morning in Mumbai. That is
    # exactly the mistake a stray CRON_TZ=UTC made on this box once
    # already, moving a nightly job by five and a half hours with nothing
    # to show it had happened.
    start_date=pendulum.datetime(2026, 8, 18, tz="Asia/Kolkata"),
    schedule="30 19 * * 1-5",          # 19:30 IST, after the close
    catchup=False,                     # see the module docstring
    max_active_runs=1,
    default_args=default_args,
    tags=["market-data", "archive"],
    # What a person may change when triggering a run by hand. Airflow
    # renders these as a form and REJECTS anything outside the bounds, so
    # the upstream's limits are enforced rather than written down
    # somewhere and hoped for.
    #
    # days_back caps at 7 because that is all Yahoo keeps of 1-minute
    # data. Asking for thirty returns nothing for the extra twenty-three
    # and takes just as long to find out. 5-minute reaches 58 days, but
    # collecting that depth is bars-bootstrap.sh's job, not the nightly
    # one - so the tighter of the two limits is the right one here.
    #
    # Note these are settings, unlike ARCHIVE above. ARCHIVE is where the
    # code lives on this box and is fixed at deploy; these change per run.
    params={
        "days_back": Param(3, type="integer", minimum=1, maximum=7,
                           description="Sessions to re-fetch. More than 1 so "
                                       "a failed night is repaired by the "
                                       "next run."),
        "pace": Param(5, type="number", minimum=2, maximum=30,
                      description="Seconds between requests. Below 2 the "
                                  "upstream refuses; 10 is safe but takes "
                                  "twelve hours for the full universe."),
        "scope": Param("all", enum=["all", "pool"],
                       description="Every liquid stock, or only tonight's "
                                   "screen pool."),
    },
) as dag:

    # ---------------------------------------------------------------- fetch
    # The long one, and the only one that cannot be redone later. Given a
    # generous timeout rather than a tight one: being killed at hour four
    # of six loses two hours of collection to save nothing.
    fetch_bars = BashOperator(
        task_id="fetch_bars",
        bash_command=(
            "cd %s && python3 -u bars_fetch.py "
            "--{{ params.scope }} "
            "--days-back {{ params.days_back }} "
            "--pace {{ params.pace }}" % ARCHIVE
        ),
        execution_timeout=timedelta(hours=8),
    )

    # -------------------------------------------------------- corp actions
    # Independent of the bars, and never allowed to fail the night. NSE
    # refuses datacentre addresses, so this is expected to be skipped on
    # the server - splits still arrive through Yahoo's events block inside
    # the fetch above. This task adds bonuses and face-value detail on the
    # days NSE does answer.
    corp_actions = BashOperator(
        task_id="corp_actions",
        bash_command=(
            "cd %s && python3 -u corp_actions.py || "
            "echo 'nse unavailable - splits still come from the bars fetch'"
            % ARCHIVE
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # --------------------------------------------------------------- verify
    # No retries. A failing check is a statement about the data, and
    # running it again produces the same answer more slowly.
    verify = BashOperator(
        task_id="verify",
        bash_command="cd %s && python3 -u checks.py" % ARCHIVE,
        retries=0,
        execution_timeout=timedelta(minutes=15),
    )

    # ------------------------------------------------------------ replicate
    # Only 1-minute partitions, and only once the checks have passed. Skips
    # itself when BARS_PEER is unset rather than failing, so the DAG is the
    # same on a box that has no peer to push to.
    replicate = BashOperator(
        task_id="replicate",
        bash_command=(
            'if [ -z "${BARS_PEER:-}" ]; then '
            '  echo "BARS_PEER unset - nothing to replicate"; exit 0; fi; '
            'rsync -az --info=stats1 '
            '${BARS_PEER_KEY:+-e "ssh -i $BARS_PEER_KEY -o IdentitiesOnly=yes"} '
            '%s/data/silver/bars/interval=1m/ "$BARS_PEER"' % ARCHIVE
        ),
        execution_timeout=timedelta(hours=1),
    )

    # -------------------------------------------------------------- summary
    # ALL_DONE, not the default. corp_actions is allowed to fail without
    # taking the night with it, and the default rule would mark this
    # upstream_failed and hide the fact that the bars arrived fine.
    summary = EmptyOperator(
        task_id="summary",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fetch_bars >> verify >> replicate >> summary
    corp_actions >> summary
