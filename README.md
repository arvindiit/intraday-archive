# intraday-archive

An archive for NSE intraday bars, built around one fact: **the source
forgets.** Yahoo serves about seven days of 1-minute bars and fifty-eight
of 5-minute, and nothing older. Every day nobody fetches is a day of
minute detail gone for good — not expensive to replace, impossible to
replace.

That single property drives every design decision below. This README
leads with the decisions rather than the usage, because the decisions are
the part worth reading.

```
bars_fetch.py     Yahoo → raw responses (bronze) → date-partitioned parquet (silver)
bars_read.py      the one read path — windows, aggregation, split adjustment
corp_actions.py   splits and bonuses, so a price series can stop lying
checks.py         seven gates that run before the data is believed
rebuild.py        regenerate silver from bronze, no network
dags/             the nightly pipeline, as an Airflow DAG
deploy/           a one-shot Airflow install (SQLite, loopback-only)
```

## What using it looks like

**Day one:**

```bash
git clone https://github.com/arvindiit/intraday-archive
pip install duckdb          # a library, not a server - nothing to run or configure
./bars-bootstrap.sh         # hours; fills data/ as deep as the upstream allows
```

**Every night after — pick one:**

```bash
30 19 * * 1-5 cd ~/intraday-archive && ./bars-daily.sh        # cron
# or run deploy/airflow-setup.sh once, and the DAG does the same
# with retries, logs and a UI
```

The archive grows by one session a day, and each night re-fetches the
last three so a failed night heals itself. Nothing to babysit.

**Reading what you have kept:**

```bash
python3 bars_read.py TCS --days 7           # from a shell
```
```python
from bars_read import load, window          # from your own code
```

And because silver is **plain parquet**, every data tool reads it without
this repo's help:

```python
import pandas as pd
pd.read_parquet("data/silver/bars/interval=1m/date=2026-08-18/part.parquet")
```

DuckDB is how this pipeline writes and queries; it is not a gatekeeper on
your data. The files are the product.

## The decisions

**1. Land the raw response before touching it.** The first version of this
fetcher parsed and reshaped in the same pass that fetched — so a bug in
the parsing could never be repaired, because the response was gone and the
source had moved on. Now every response is gzipped to disk untouched
before anything reads it. That copy paid for itself the first week: a
slow bulk-insert bug was fixed by re-parsing bronze in four seconds
instead of re-fetching for seven hours.

**2. Partition by date, sort by symbol.** One parquet file per interval
per day, holding every stock, ordered by symbol. Per-symbol files are
worse in both directions — reading one day means opening ~2,300 files,
and reading one stock's history means decompressing years of it to keep a
morning. Date partitions make "one day" one file, and the symbol sort
lets parquet's row-group statistics skip everything a one-stock query
does not want. No index; a consequence of write order.

Bronze and silver are partitioned on *different keys*, and it is not an
inconsistency. Bronze is keyed on when we fetched, because one response
covers seven days and belongs to no single one of them. Silver is keyed
on what the data is about, because that is what a reader asks for.

**3. Merge, never replace.** Every partition write is keyed on
`(symbol, timestamp)` with the newest fetch winning, written to a
temporary file and renamed. Re-running anything is safe; a partial
session fetched at noon is corrected by the same session fetched at six;
an interrupted run leaves the previous file whole. The nightly fetch asks
for **three** days, not one — so a night that fails is repaired by the
next run rather than becoming a permanent hole.

**4. Store bars as traded; adjust on read.** A stock that splits 1:5
closes at 3,400 on Tuesday and 680 on Wednesday, and both are correct.
Rewriting history onto the new basis destroys the record of what actually
traded — and the information needed to adjust *only exists near the
event*, so `corp_actions.py` records it daily and `bars_read.adjust()`
scales whichever window is being displayed. Volume scales the other way:
five times the shares at a fifth of the price is the same money.

**5. Check before anything downstream reads.** `checks.py` is seven
questions — duplicates, malformed candles, non-positive prices,
impossible bar counts, volumeless sessions, staleness, and whether the
session actually reaches the close. Each names the row that failed,
because a check that says only "FAILED" is a check somebody disables on a
Friday evening. In the DAG, verification **gates replication**: data that
fails never propagates.

**6. `catchup=False`, which is the opposite of the usual advice.**
Airflow's catchup exists to re-run the days a pipeline missed, and that
works when the source can still answer. This one forgets — a catchup run
would spend hours collecting empty responses and report success. The
three-day overlap does the recovering instead. **Backfill is only a
recovery strategy when the source can still answer.** For a perishable
source, the only recovery is having collected it the first time.

## Things that went wrong, kept here on purpose

Every one of these is in the commit history, and they are better
documentation than the design notes.

- **Yahoo 429s a full browser user-agent and serves a bare
  `Mozilla/5.0`.** Backwards from intuition. Tested on the same machine
  in the same minute against the same URL: bare returned 375 bars, the
  Chrome string returned HTTP 429. NSE's servers are the opposite and
  refuse minimal clients. One constant each.
- **`executemany` is not a bulk loader.** DuckDB binds one row at a time
  from Python: 3,566 rows/sec, against 271,868 through its CSV reader —
  76× — and the two calls look identical in the API. Eight million rows
  is the difference between half a minute and an evening.
- **Holding a full run in memory cost 3 GB.** Date partitions cannot be
  finished until every symbol has arrived, so the first version collected
  everything. Partitions merge, so flushing every 300 symbols gives an
  identical result with bounded memory.
- **The completeness check was measuring the wrong statistic.** The
  median last-bar time read 15:15 on a session where half the stocks were
  truncated — because the median stock happened to be one of the complete
  half. The fix was calibration against real sessions: on complete days
  98.7–99.8% of stocks trade past 15:00; on the truncated day, 59%. A
  threshold of 95% separates those cleanly. *A statistic can look
  sensible and sit exactly where it cannot see the thing you built it
  for.*
- **Two constants that had to agree, didn't.** The parser keeps 376 bars
  per session (09:15–15:30 inclusive); the checker hardcoded 375 and
  failed on every complete session. The cap is now derived from the same
  bounds the parser filters on.

## Use

```bash
pip install duckdb          # the only dependency; everything else is stdlib

./bars-bootstrap.sh         # one-time: as deep as the upstream allows
./bars-daily.sh             # nightly: last 3 sessions + replication
python3 checks.py           # the gates, runnable by hand
python3 bars_read.py TCS --days 7
```

Or orchestrated — `deploy/airflow-setup.sh` installs Airflow (SQLite +
SequentialExecutor, deliberately: one DAG, five tasks, and parallelism
would buy two minutes at the cost of a second database to maintain; the
UI binds to loopback because a public Airflow is a remote shell wearing a
web page) and registers the DAG:

```
fetch_bars ──► verify ──► replicate ──┐
                                      ├──► summary
corp_actions ─────────────────────────┘
```

Replication is one restricted rsync: a dedicated SSH key, caged on the
receiving side with `restrict,command="rrsync -wo <dir>"`, able to push
bars into one directory and do nothing else.

## Size

Roughly **10 GB/year** of bronze and **4 GB/year** of silver for the full
NSE equity list at 1-minute grain. The entire thing fits on one machine
for a decade, which is why the query engine is DuckDB and not a cluster.

## What this is not

Not a data source — it archives what a licensed/permitted source serves
*to you*; Yahoo's terms do not permit redistribution, which is why
`data/` is gitignored and no data ships with this repo. Not a trading
system — nothing here decides anything. It keeps what would otherwise be
lost, and proves what it kept.
