#!/usr/bin/env python3
"""What has to be true before the archive is believed.

    python3 checks.py                  every check, against the whole archive
    python3 checks.py --date 2026-08-18   just that session
    python3 checks.py --interval 5m

Exits non-zero when anything fails, so an orchestrator can gate on it.

These run AFTER the fetch and BEFORE anything downstream reads the result.
The ordering is the point: a check that runs after the data is in use
tells you how long you were wrong. The job exiting zero and the data being
right are different claims, and nearly every real data incident lives in
the gap between them - a pipeline that completed perfectly while writing
nonsense.

Each check answers one question and says which rows failed it. A check
that reports "FAILED" without naming a row is a check somebody will
disable the first time it fires on a Friday evening.

Standard library plus duckdb.
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(HERE, "data"))
SILVER = os.path.join(DATA_DIR, "silver", "bars")

# How many bars a full session can hold, DERIVED from the same bounds the
# parser filters on rather than written out again.
#
# The first version hardcoded 375 and 75, and failed on every session: the
# parser keeps minutes 555..930 inclusive, which is 376 one-minute bars,
# not 375. Two constants that had to agree, in two files, and they didn't.
#
# A stock can legitimately have far fewer - illiquid names do not trade
# every minute - but more than this means bars from outside the session or
# two sessions fused, and either is a bug rather than a quiet stock.
SESSION_OPEN_M, SESSION_CLOSE_M = 9 * 60 + 15, 15 * 60 + 30


def session_cap(interval):
    step = int(interval.rstrip("m")) or 1
    return (SESSION_CLOSE_M - SESSION_OPEN_M) // step + 1

# How stale the newest partition may be before it is worth saying so.
# Three calendar days covers a normal weekend; a long weekend will produce
# one false alarm a year, which is the right side to err on.
STALE_DAYS = 3

IST = timezone(timedelta(hours=5, minutes=30))

# What fraction of stocks must still be trading late in the session.
#
# Calibrated against real sessions rather than guessed. Five complete days
# and one truncated one, measured on 2,290 NSE names:
#
#     complete   98.7% - 99.8%  of stocks have a bar at or after 15:00
#     truncated  59.0%
#
# Forty points apart, so 95% separates them with room to spare.
#
# The first version of this check used the MEDIAN last bar and was useless:
# on the truncated day the median was 15:15, only fifteen minutes short,
# because the median stock happened to be one of the half fetched after the
# close. A statistic can look sensible and sit exactly where it cannot see
# the thing you built it for.
#
# 15:00 rather than 15:30 on purpose. Only about 11% of NSE names trade in
# the final minute at all, so "reaches 15:30" measures liquidity, not
# coverage.
REACH_BY_M = int(os.environ.get("BARS_REACH_BY", "900"))        # 15:00 IST
MIN_REACHING = float(os.environ.get("BARS_MIN_REACHING", "0.95"))


def glob_for(interval):
    return os.path.join(SILVER, "interval=%s" % interval, "date=*",
                        "part.parquet")


def partitions(interval):
    root = os.path.join(SILVER, "interval=%s" % interval)
    if not os.path.isdir(root):
        return []
    out = []
    for name in sorted(os.listdir(root)):
        if name.startswith("date=") and os.path.exists(
                os.path.join(root, name, "part.parquet")):
            out.append(name[5:])
    return out


class Result:
    def __init__(self):
        self.rows = []

    def add(self, name, ok, detail=""):
        self.rows.append((name, ok, detail))
        print("  %-22s %-6s %s" % (name, "ok" if ok else "FAILED", detail))
        return ok

    @property
    def failed(self):
        return [r for r in self.rows if not r[1]]


def run(interval="1m", only=None):
    """Every check for one interval. `only` limits to a single date."""
    res = Result()
    parts = partitions(interval)
    if only:
        parts = [p for p in parts if p == only]

    print("interval=%s  %d partition(s)%s"
          % (interval, len(parts), "  date=%s" % only if only else ""))

    if not res.add("partitions exist", bool(parts),
                   "" if parts else "nothing to check - has the fetch run?"):
        return res

    con = duckdb.connect()
    where = "WHERE date = DATE '%s'" % only if only else ""
    src = "read_parquet('%s', hive_partitioning=true)" % glob_for(interval)

    def q(sql):
        return con.execute(sql).fetchall()

    # 1. Nothing duplicated. Parquet cannot enforce a key, so the grain is
    #    a promise the writer keeps - and this is the only thing that
    #    checks the promise was kept.
    dupes = q("SELECT symbol, ts, count(*) c FROM %s %s "
              "GROUP BY 1,2 HAVING c > 1 ORDER BY c DESC LIMIT 5"
              % (src, where))
    res.add("no duplicate bars", not dupes,
            "" if not dupes else "e.g. %s at %s appears %d times" % dupes[0])

    # 2. A candle whose high is below its low, or whose high does not
    #    contain its open and close, is not a candle. Cheap, and it catches
    #    a whole class of parsing mistakes at once.
    bad = q("SELECT symbol, ts FROM %s %s %s h < l OR h < o OR h < c "
            "OR l > o OR l > c LIMIT 5"
            % (src, where, "AND" if where else "WHERE"))
    res.add("candles well formed", not bad,
            "" if not bad else "e.g. %s at ts %s" % bad[0])

    # 3. Prices above zero, volume not negative. Yahoo returns nulls for
    #    minutes that did not trade and we drop those on parse; a zero or
    #    a negative here means something else went wrong.
    nonsense = q("SELECT symbol, ts FROM %s %s %s o <= 0 OR h <= 0 OR l <= 0 "
                 "OR c <= 0 OR v < 0 LIMIT 5"
                 % (src, where, "AND" if where else "WHERE"))
    res.add("prices positive", not nonsense,
            "" if not nonsense else "e.g. %s at ts %s" % nonsense[0])

    # 4. No session may hold more bars for one stock than a session has.
    #    Fewer is normal - an illiquid name does not trade every minute -
    #    but more means bars from outside the session, or two days fused.
    cap = session_cap(interval)
    over = q("SELECT symbol, date, c FROM (SELECT symbol, date, count(*) c "
             "FROM %s %s GROUP BY 1,2) WHERE c > %d ORDER BY c DESC LIMIT 5"
             % (src, where, cap))
    res.add("bars within session", not over,
            "" if not over else "e.g. %s on %s has %d (max %d)"
            % (over[0][0], over[0][1], over[0][2], cap))

    # 5. Volume: a session where every bar is zero means the fetch returned
    #    a shape we parsed but a market that never traded, which is a
    #    holiday we should not have stored or a payload we misread.
    empty = q("SELECT date, sum(v) FROM %s %s GROUP BY 1 HAVING sum(v) = 0 "
              "LIMIT 5" % (src, where))
    res.add("sessions have volume", not empty,
            "" if not empty else "no volume at all on %s" % empty[0][0])

    # 6. Does the session actually reach the close?
    #
    # The check the others could not make. Every one above is satisfied by a
    # session that stops at eleven in the morning: the bars present are
    # well formed, unduplicated, positively priced and under the cap.
    # "Fewer bars than a full session" is indistinguishable from a quiet
    # stock, so nothing was watching for a fetch that simply stopped early.
    #
    # It happened on the first bootstrap. The run started at 11:24 and
    # walked the alphabet until 18:35, so stocks fetched before the close
    # hold half a session and stocks fetched after hold all of it - and
    # every other check passed.
    # Minute-of-day from raw epoch arithmetic, NOT from rendering a
    # timestamp. date_part() on a timestamp answers in the SESSION
    # timezone - so on a box set to Asia/Kolkata the 330-minute offset was
    # added twice, every stock appeared to trade until nine at night, and
    # this check waved through the exact truncated session it was written
    # to catch. The epoch never lies about anything: +19800s is IST, and
    # modulo a day is the wall clock, on any machine.
    reach = q("SELECT date, "
              "  count(*) AS stocks, "
              "  sum(CASE WHEN last_m >= %d THEN 1 ELSE 0 END) AS reaching "
              "FROM (SELECT symbol, date, "
              "        max(((ts + 19800) %% 86400) // 60) AS last_m "
              "      FROM %s %s GROUP BY 1,2) "
              "GROUP BY 1 ORDER BY 1" % (REACH_BY_M, src, where))
    short = [(d, 100.0 * r / n) for d, n, r in reach
             if n and r / float(n) < MIN_REACHING]
    res.add("session reaches close", not short,
            "" if not short else
            "%s: only %.1f%% of stocks trade past %02d:%02d (want %.0f%%)%s"
            % (short[0][0], short[0][1], REACH_BY_M // 60, REACH_BY_M % 60,
               MIN_REACHING * 100,
               " (+%d more)" % (len(short) - 1) if len(short) > 1 else ""))

    # 7. Freshness, and only when looking at the archive as a whole -
    #    asking whether one named past date is recent makes no sense.
    if not only:
        newest = max(parts)
        age = (date.today() - date.fromisoformat(newest)).days
        res.add("archive is fresh", age <= STALE_DAYS,
                "newest partition %s, %d day(s) old" % (newest, age))

    con.close()
    return res


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--interval", default=None,
                    help="1m or 5m; both when omitted")
    ap.add_argument("--date", metavar="YYYY-MM-DD",
                    help="check one session rather than the archive")
    args = ap.parse_args()

    intervals = [args.interval] if args.interval else ["1m", "5m"]
    failed = []
    for iv in intervals:
        res = run(iv, only=args.date)
        failed += [(iv, name) for name, ok, _ in res.rows if not ok]
        print()

    if failed:
        print("%d check(s) failed: %s"
              % (len(failed), ", ".join("%s/%s" % f for f in failed)))
        # Non-zero so the orchestrator stops rather than publishing. Bad
        # data that reaches a reader is worse than late data that does not.
        raise SystemExit(1)
    print("all checks passed")


if __name__ == "__main__":
    main()
