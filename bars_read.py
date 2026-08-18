#!/usr/bin/env python3
"""The one way anything reads bars.

    from bars_read import load, window

    load("TCS", "1m", since=date(2026, 8, 1))   -> [[epoch,o,h,l,c,v], ...]
    window("TCS", 7)                            -> (bars, interval_minutes)

Everything that wants intraday data comes through here - the boards, the
levels fill, the charts. One reader because the archive can hold more than
one version of a bar: a session fetched at noon and again at six differs,
and the rule that the newest wins has to live in exactly one place. Two
implementations of that rule is one more than can stay in agreement.

Reads are cheap for a reason that is worth knowing. The archive is
partitioned by date, so a query for one week opens seven directories and
ignores the rest - the filesystem answers that, at no cost. Within a file
the rows are sorted by symbol, so parquet's per-row-group min/max lets
DuckDB skip every group whose symbol range cannot contain the one asked
for. Neither trick needs an index; both are consequences of how the files
were laid out on write.

Standard library plus duckdb.
"""

import os
from datetime import datetime, timedelta, timezone

import duckdb

import corp_actions

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(HERE, "data"))
SILVER = os.path.join(DATA_DIR, "silver", "bars")
IST = timezone(timedelta(hours=5, minutes=30))

# A board holds about this many candles whatever the window, so the
# interval is chosen to land near it. Candles, not points on a line - at
# 375 a body is about two pixels and the volume mark inside it is
# invisible, which removes the only reason to draw candles at all.
BOARD_CANDLES = 120
WINDOWS = (
    # days, target interval, source interval
    (1,   1,  "1m"),
    (3,   3,  "1m"),
    (7,   7,  "1m"),      # sits on the 1m history limit - see the fallback
    (20,  20, "5m"),
    (44,  60, "5m"),      # ~2 months
)

_con = None


def _db():
    """One connection, reused. Opening one per call is not expensive, but
    a board that draws five windows would open five for no reason."""
    global _con
    if _con is None:
        _con = duckdb.connect()
    return _con


def _glob(interval):
    return os.path.join(SILVER, "interval=%s" % interval,
                        "date=*", "part.parquet")


def available(interval="1m"):
    """The session dates the archive holds, oldest first.

    Read from the directory names rather than the files: the partition
    layout already answers this, and opening a hundred parquet footers to
    learn what the paths say would be work for nothing.
    """
    root = os.path.join(SILVER, "interval=%s" % interval)
    if not os.path.isdir(root):
        return []
    out = []
    for name in os.listdir(root):
        if name.startswith("date=") and os.path.exists(
                os.path.join(root, name, "part.parquet")):
            try:
                out.append(datetime.strptime(name[5:], "%Y-%m-%d").date())
            except ValueError:
                continue
    return sorted(out)


def load(symbol, interval, since=None, upto=None):
    """Every stored bar for one stock, oldest first.

    An empty archive is not an error - it is a stock, or a day, we have
    not reached yet. Every caller has to handle that anyway, and raising
    here would only move the guard.
    """
    if not available(interval):
        return []
    where = ["symbol = ?"]
    args = [symbol.upper()]
    # Filtering on the partition column, not on the timestamp: DuckDB
    # prunes whole directories from this and never opens them. The same
    # filter written against ts would read every file to find out.
    if since:
        where.append("date >= ?")
        args.append(since)
    if upto:
        where.append("date <= ?")
        args.append(upto)
    rows = _db().execute(
        "SELECT ts, o, h, l, c, v FROM read_parquet(?, hive_partitioning=true)"
        " WHERE %s ORDER BY ts" % " AND ".join(where),
        [_glob(interval)] + args).fetchall()
    return [list(r) for r in rows]


def session_bars(day, interval="5m"):
    """{symbol: [[ts, o, h, l, c, v], ...]} for one session, every stock.

    The other way round from load(), and the difference matters. Anything
    computing a per-stock-per-day figure across the whole market - levels,
    a screen, a backfill - wants every symbol for one day, and asking for
    it stock by stock means opening the same partition 1,343 times.

    One query, one file, grouped in memory. Which is the shape the layout
    was chosen for: partitioned by date so a day is one place, sorted by
    symbol so the grouping is already done on disk.
    """
    p = os.path.join(SILVER, "interval=%s" % interval,
                     "date=%s" % day.isoformat(), "part.parquet")
    if not os.path.exists(p):
        return {}
    rows = _db().execute(
        "SELECT symbol, ts, o, h, l, c, v FROM read_parquet(?) "
        "ORDER BY symbol, ts", [p]).fetchall()
    out = {}
    for r in rows:
        out.setdefault(r[0], []).append(list(r[1:]))
    return out


def adjust(bars, symbol, upto):
    """Put every bar on the price basis in force at `upto`.

    A stock that split 1:5 inside the window has bars from before the date
    quoted five times too high. Left alone the chart shows an eighty
    percent crash that never happened, and anything reading it - a chart, a
    level, an engine - believes it.

    The stored bars are never touched. This scales on the way out, for the
    window being asked about, which is the only place the answer is even
    well defined: "the price of TCS" before a split means one thing on a
    one-day chart and another on a two-month one.

    Volume moves the opposite way. Five times the shares at a fifth of the
    price is the same money, so a share count from before the split is five
    times too small to compare - divided by the same factor the prices are
    multiplied by.
    """
    acts = corp_actions.load().get(symbol.upper(), [])
    if not acts or not bars:
        return bars
    out, cache = [], {}
    for b in bars:
        d = datetime.fromtimestamp(b[0], IST).date()
        f = cache.get(d)
        if f is None:
            f = cache[d] = corp_actions.factor_for(acts, d, upto)
        if f == 1.0:
            out.append(b)
            continue
        out.append([b[0], round(b[1] * f, 4), round(b[2] * f, 4),
                    round(b[3] * f, 4), round(b[4] * f, 4),
                    int(round(b[5] / f)) if f else b[5]])
    return out


def aggregate(bars, factor):
    """Coarser candles from finer ones. Exact, not resampled.

    Buckets break at session boundaries: a candle must never span the close
    of one day and the open of the next, or its high and low describe two
    sessions and its open is half an hour before its close in a different
    week.
    """
    if factor <= 1:
        return list(bars)
    out, bucket, day = [], [], None
    for b in bars:
        d = datetime.fromtimestamp(b[0], IST).date()
        if day is not None and (d != day or len(bucket) == factor):
            out.append(_fuse(bucket))
            bucket = []
        day = d
        bucket.append(b)
    if bucket:
        out.append(_fuse(bucket))
    return out


def _fuse(bucket):
    return [bucket[0][0], bucket[0][1], max(b[2] for b in bucket),
            min(b[3] for b in bucket), bucket[-1][4],
            sum(b[5] for b in bucket)]


def window(symbol, days, upto=None):
    """The bars a board of `days` should draw, at the right interval.

    Falls back to the coarser source when the finer one does not reach far
    enough - the seven-day board wants 1-minute data and the upstream keeps
    about seven days of it, so one holiday can put us over the edge. Better
    a board of 15-minute candles than no board.
    """
    spec = next((w for w in WINDOWS if w[0] >= days), WINDOWS[-1])
    _d, target, source = spec
    end = upto or datetime.now(IST).date()
    # Calendar days, generously: `days` trading sessions need more than
    # `days` calendar days once weekends and holidays are in the way, and
    # asking for too many costs nothing because the partitions that do not
    # exist are never opened.
    start = end - timedelta(days=days * 2 + 10)
    bars = load(symbol, source, since=start, upto=end)
    if source == "1m" and not bars:
        bars = load(symbol, "5m", since=start, upto=end)
        target = max(5, (target // 5) * 5) or 5
        source = "5m"
    if not bars:
        return [], target
    # Now trim to the last `days` SESSIONS. The widened date range above
    # exists only so weekends and holidays cannot leave us short; without
    # this the widening becomes the window, and a one-day board drew eight
    # sessions averaged into 41-minute candles while calling itself 1D.
    bars = _last_sessions(bars, days)
    # Before aggregating, not after. A coarse candle built from two price
    # bases has a high from one and a low from the other, and no later
    # scaling can separate them again.
    bars = adjust(bars, symbol, end)
    step = max(1, round(len(bars) / float(BOARD_CANDLES)))
    return aggregate(bars, step), step * (1 if source == "1m" else 5)


def _last_sessions(bars, n):
    seen = sorted({datetime.fromtimestamp(b[0], IST).date() for b in bars})
    keep = set(seen[-n:])
    return [b for b in bars
            if datetime.fromtimestamp(b[0], IST).date() in keep]


def main():
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("symbol")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--interval", default=None,
                    help="raw read at this interval instead of a board window")
    args = ap.parse_args()

    if args.interval:
        bars = load(args.symbol, args.interval)
        print("%s %s: %d bar(s)" % (args.symbol, args.interval, len(bars)))
    else:
        bars, interval = window(args.symbol, args.days)
        print("%s - %d days at %d-minute candles: %d bar(s)"
              % (args.symbol, args.days, interval, len(bars)))
    for b in bars[-6:]:
        t = datetime.fromtimestamp(b[0], IST)
        print("  %s  o %-9s h %-9s l %-9s c %-9s v %s"
              % (t.strftime("%d-%b %H:%M"), b[1], b[2], b[3], b[4], b[5]))


if __name__ == "__main__":
    main()
