#!/usr/bin/env python3
"""Yahoo intraday bars: land the raw response, then derive parquet.

    python3 bars_fetch.py --pool                tonight's screen pool
    python3 bars_fetch.py --all --limit 50      the liquid universe
    python3 bars_fetch.py --symbols TCS,INFY

Minute data is PERISHABLE upstream - Yahoo serves about seven days of
1-minute bars and nothing older. Every day nobody fetches is a day of
minute detail gone for good, not expensive to replace but impossible to
replace. That is the reason this file exists; speed and rate limits are
conveniences.

Two layers, and the split is the point:

    data/bronze/yahoo/interval=1m/fetched=2026-08-15/TCS.json.gz
    data/silver/bars/interval=1m/date=2026-08-15/part.parquet

BRONZE is exactly what the upstream returned, gzipped, never rewritten.
Its predecessor parsed and reshaped in the same pass that fetched, which
meant a bug in the parsing could never be repaired - the response was
gone and the source had moved on. Landing raw first costs a few
gigabytes a year and makes every later mistake recoverable.

Note the two directories are partitioned on different things, and it is
not an inconsistency. Bronze is keyed on WHEN WE FETCHED, because one
response covers seven days and belongs to no single one of them. Silver
is keyed on WHAT THE DATA IS ABOUT, because that is what a reader asks
for. Raw is organised by the act of collection; modelled data by the
thing it describes.

SILVER is date-partitioned and symbol-sorted, one file per interval per
day holding every stock. Per-symbol files were the obvious alternative
and are worse in both directions: reading a single day means opening
1,343 files, and reading one stock's history means decompressing years
of it to keep a morning. Sorted by symbol so parquet's per-row-group
min/max statistics can skip everything a query does not want.

Re-running is safe. Each partition is merged - keyed on (symbol,
timestamp), the newest fetch winning - then written to a temporary file
and renamed, so an interrupted run leaves the previous day intact rather
than a half-written one.

Standard library plus duckdb.
"""

import argparse
import csv
import glob
import gzip
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(HERE, "data"))
BRONZE = os.path.join(DATA_DIR, "bronze", "yahoo")
SILVER = os.path.join(DATA_DIR, "silver", "bars")

# Two upstreams, two user-agents, and the difference is not cosmetic.
#
# Yahoo 429s the detailed Chrome string and serves the bare one, which is
# backwards from what anyone would guess. Tested on the same box in the
# same minute against the same URL: bare returned 375 bars, Chrome
# returned HTTP 429. The likely reason is that a full browser signature
# invites the bot-verification path - cookies, a crumb, a session - while
# a minimal client is handled as what it says it is. The website has been
# fetching with the bare one for months without trouble; this file
# inherited the elaborate one from barstore.py and was refused everywhere.
#
# NSE is the opposite and wants the browser-shaped string: it refuses
# python-urllib's default outright. So they get one each rather than a
# shared constant that has to be wrong for one of them.
UA = {"User-Agent": "Mozilla/5.0"}
NSE_UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 "
                        "Safari/537.36"}
UPSTREAM = "https://query1.finance.yahoo.com/v8/finance/chart/"
NSE_LIST = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
IST = timezone(timedelta(hours=5, minutes=30))
SESSION_OPEN_M, SESSION_CLOSE_M = 9 * 60 + 15, 15 * 60 + 30

# The only two intervals the upstream offers that cannot be derived from
# something finer, so the only two worth storing. Everything coarser is an
# exact sum of these - first open, highest high, lowest low, last close,
# summed volume - not a resampling that approximates anything.
FETCH = (("1m", 7), ("5m", 58))

# Ten seconds, not two. A hundred stocks is two hundred requests, and at
# two seconds that is an implied 1,700 an hour, which trips the upstream
# limit part way through and leaves the rest of the run recording failures
# that were really one refusal repeated.
PACE_SEC = float(os.environ.get("BARS_PACE", "10.0"))

# A 429 means "wait", not "this stock has no data". Counting it as a
# failure and moving on is exactly wrong: the next request fails too, and
# the run reports ninety missing stocks when the truth is one block.
RETRY_WAITS = (60, 180, 420)

# How many stocks to hold before writing. Nothing magic about 300 - it
# bounds memory to a few hundred megabytes while keeping the number of
# partition rewrites small.
FLUSH_EVERY = int(os.environ.get("BARS_FLUSH_EVERY", "300"))


# ----------------------------------------------------------------- bronze

def land(symbol, interval, payload, fetched_on):
    """Write the upstream response exactly as it arrived.

    Before any parsing, and never touched again. This is the only copy of
    what Yahoo actually said, and the seven-day window means it is the
    only copy that will ever exist.
    """
    d = os.path.join(BRONZE, "interval=%s" % interval,
                     "fetched=%s" % fetched_on.isoformat())
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, "%s.json.gz" % symbol.upper())
    tmp = p + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    os.replace(tmp, p)
    return p


def fetch_raw(symbol, interval, days_back):
    """One upstream call. Returns the decoded payload, unmodified.

    period1/period2 rather than range=: the range parameter accepts only a
    fixed vocabulary (1d, 5d, 1mo ...) and anything else is a Bad Request
    with no explanation. Epochs say exactly what we want.
    """
    now = int(time.time())
    # events=split,div costs nothing - same request, a few extra bytes -
    # and it is how we learn about splits at all. NSE's own corporate
    # actions API refuses this box (the www host 403s datacentre IPs), so
    # the upstream we can actually reach is the one that has to tell us.
    # It arrives inside the payload we already land as bronze, which means
    # the record of what changed is stored alongside the prices it
    # explains rather than in a separate feed that can go missing.
    url = "%s%s.NS?period1=%d&period2=%d&interval=%s&events=split,div" % (
        UPSTREAM, urllib.parse.quote(symbol),
        now - days_back * 86400, now + 86400, interval)
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


# ----------------------------------------------------------------- silver

def parse(symbol, payload):
    """Rows of (symbol, epoch, o, h, l, c, v, session_date) from a payload.

    Bars outside the trading session are dropped - the upstream sometimes
    includes a pre-open stub, and a bar nobody could trade in is not a bar.
    A row with a null close is a minute that did not trade; skipped rather
    than carried forward, because an invented price is worse than a gap.
    """
    result = (payload.get("chart") or {}).get("result") or []
    if not result:
        return []
    node = result[0]
    stamps = node.get("timestamp") or []
    q = ((node.get("indicators") or {}).get("quote") or [{}])[0]
    out = []
    for i, ts in enumerate(stamps):
        try:
            c = q["close"][i]
            if c is None:
                continue
            t = datetime.fromtimestamp(ts, IST)
            minute = t.hour * 60 + t.minute
            if not (SESSION_OPEN_M <= minute <= SESSION_CLOSE_M):
                continue
            out.append((symbol.upper(), int(ts),
                        round(q["open"][i], 2), round(q["high"][i], 2),
                        round(q["low"][i], 2), round(c, 2),
                        int(q["volume"][i] or 0), t.date()))
        except (KeyError, IndexError, TypeError):
            continue
    return out


def parse_events(symbol, payload):
    """Splits and dividends the payload happens to mention.

    Yahoo states a split as numerator:denominator - "5:1" meaning five
    shares where there was one - so a price from before the date is five
    times too large and the factor that rebases it is 1/5. Written the
    same shape corp_actions.py stores, so both sources land in one file
    and the reader does not care which found it.
    """
    node = ((payload.get("chart") or {}).get("result") or [{}])[0]
    ev = node.get("events") or {}
    out = []
    for e in (ev.get("splits") or {}).values():
        num, den = e.get("numerator"), e.get("denominator")
        ts = e.get("date")
        if not (num and den and ts):
            continue
        out.append({
            "symbol": symbol.upper(),
            "ex_date": datetime.fromtimestamp(int(ts), IST).date().isoformat(),
            "kind": "split",
            "factor": round(float(den) / float(num), 8),
            "subject": "yahoo split %s" % (e.get("splitRatio")
                                           or "%s:%s" % (num, den))})
    for e in (ev.get("dividends") or {}).values():
        ts, amt = e.get("date"), e.get("amount")
        if not ts:
            continue
        # Recorded, never adjusted for - a null factor means the reader
        # leaves it alone. Kept so the decision can change later without
        # another year of waiting for the data to reappear.
        out.append({
            "symbol": symbol.upper(),
            "ex_date": datetime.fromtimestamp(int(ts), IST).date().isoformat(),
            "kind": "dividend", "factor": None,
            "subject": "yahoo dividend %s" % amt})
    return out


def part_path(interval, day):
    return os.path.join(SILVER, "interval=%s" % interval,
                        "date=%s" % day.isoformat(), "part.parquet")


def write_partitions(rows, interval):
    """Merge rows into their day partitions. Returns (days, bars_written).

    One file per day holding every symbol, so a day's fetch touches as many
    files as it has days of history - seven for the 1m window - rather than
    one per stock. Each is merged rather than replaced: the newest fetch
    wins per (symbol, timestamp), so a partial session collected at noon is
    corrected by the same session collected at six, and a re-run changes
    nothing.
    """
    if not rows:
        return 0, 0
    con = duckdb.connect()
    # Through a CSV, not executemany. DuckDB's executemany binds one row at a
    # time from Python and manages about 3,500 rows a second; its CSV reader
    # is multi-threaded and does 270,000. On a full universe run - eight
    # million rows - that is the difference between half a minute and several
    # hours, and the first version of this file spent an evening proving it.
    #
    # executemany is for parameterised statements, not bulk loading. The two
    # look identical in the API and differ by two orders of magnitude.
    spool = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False,
                                        newline="")
    try:
        csv.writer(spool).writerows(rows)
        spool.close()
        con.execute(
            "CREATE TABLE incoming AS SELECT * FROM read_csv(?, header=false, "
            "columns={'symbol':'VARCHAR','ts':'BIGINT','o':'DOUBLE',"
            "'h':'DOUBLE','l':'DOUBLE','c':'DOUBLE','v':'BIGINT','d':'DATE'})",
            [spool.name])
    finally:
        os.unlink(spool.name)

    days = [r[0] for r in con.execute(
        "SELECT DISTINCT d FROM incoming ORDER BY d").fetchall()]
    written = 0
    for day in days:
        p = part_path(interval, day)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = p + ".tmp"
        # src=1 for what just arrived, 0 for what was already on disk, so
        # the window function keeps the fresher of any duplicate pair. The
        # existing file is read INSIDE the same statement that writes the
        # temporary one, which is why the rename at the end is safe: the
        # old file is still whole until the instant it is replaced.
        existing = ("SELECT symbol, ts, o, h, l, c, v, 0 AS src FROM "
                    "read_parquet('%s')" % p) if os.path.exists(p) else None
        union = ("SELECT symbol, ts, o, h, l, c, v, 1 AS src FROM incoming "
                 "WHERE d = DATE '%s'" % day.isoformat())
        if existing:
            union += " UNION ALL " + existing
        con.execute(
            "COPY (SELECT symbol, ts, o, h, l, c, v FROM ("
            "  SELECT *, row_number() OVER "
            "         (PARTITION BY symbol, ts ORDER BY src DESC) AS rn"
            "  FROM (%s)) WHERE rn = 1"
            # Sorted so parquet's per-row-group min/max can skip: a query
            # for one symbol reads the two or three groups that could hold
            # it rather than every group in the file.
            "  ORDER BY symbol, ts) "
            "TO '%s' (FORMAT parquet, COMPRESSION zstd)" % (union, tmp))
        os.replace(tmp, p)
        written += con.execute(
            "SELECT count(*) FROM read_parquet('%s')" % p).fetchone()[0]
    con.close()
    return len(days), written


# ------------------------------------------------------------------ input

def universe(series=("EQ",)):
    """Every listed equity, from NSE's own master list.

    Self-contained on purpose: this repo runs on boxes that have neither a
    database nor a copy of the website's code, so it cannot borrow a symbol
    list from either. The exchange publishes one, and it is the
    authoritative version anyway.

    Wider than tonight's pool, deliberately. The pool changes nightly, and
    a stock fetched only on the days it happened to be picked has holes
    exactly where a later study would want continuity.
    """
    import csv
    import io
    req = urllib.request.Request(NSE_LIST, headers=NSE_UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        text = r.read().decode("utf-8", "replace")
    out = []
    for row in csv.DictReader(io.StringIO(text)):
        sym = (row.get("SYMBOL") or "").strip()
        # NSE's header has a leading space on SERIES in some vintages of
        # this file, and reading the wrong key silently returns every
        # series - debentures, warrants and all.
        ser = (row.get(" SERIES") or row.get("SERIES") or "").strip()
        if sym and (not series or ser in series):
            out.append(sym)
    return sorted(set(out))


def main():
    global PACE_SEC
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="every liquid stock")
    ap.add_argument("--symbols", help="comma separated, overrides the rest")
    # One choice today, on purpose. The seam for a second source exists -
    # bronze is already partitioned by source, and only fetch_raw() and
    # parse() know anything about Yahoo - but the interface gets extracted
    # when a second real implementation lands, not designed speculatively
    # before it. Until then this flag is the visible slot.
    ap.add_argument("--source", choices=["yahoo"], default="yahoo",
                    help="where bars come from (only yahoo yet)")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--days-back", type=int, metavar="N",
                    help="override how far each interval reaches. A daily "
                         "run wants a small number: yesterday's session is "
                         "all that is new, and asking for sixty days of it "
                         "every night is sixty times the request for one "
                         "day of data.")
    ap.add_argument("--pace", type=float, metavar="SEC")
    ap.add_argument("--dry", action="store_true",
                    help="fetch and land bronze, but write no parquet")
    args = ap.parse_args()

    if args.symbols:
        syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    elif args.all:
        syms = universe()
    else:
        ap.error("pass --symbols or --all")
    if args.limit:
        syms = syms[:args.limit]
    if not syms:
        raise SystemExit("no symbols to fetch")

    if args.pace is not None:
        PACE_SEC = args.pace
    plan = [(iv, args.days_back or back) for iv, back in FETCH]
    today = datetime.now(IST).date()
    print("Archiving %d stock(s): %s, %.0fs apart"
          % (len(syms), ", ".join("%s over %dd" % p for p in plan), PACE_SEC))
    print("  bronze  %s\n  silver  %s\n" % (BRONZE, SILVER))

    t0 = time.time()
    collected = {iv: [] for iv, _ in plan}
    events = []
    fails = 0
    for i, sym in enumerate(syms, 1):
        for interval, days_back in plan:
            payload, blocked = None, False
            for wait in (0,) + RETRY_WAITS:
                if wait:
                    sys.stderr.write("  rate limited - waiting %ds\n" % wait)
                    time.sleep(wait)
                try:
                    payload = fetch_raw(sym, interval, days_back)
                    break
                except urllib.error.HTTPError as exc:
                    if exc.code != 429:
                        sys.stderr.write("  %s %s: HTTP %s\n"
                                         % (sym, interval, exc.code))
                        break
                    blocked = True
                except Exception as exc:
                    sys.stderr.write("  %s %s: %s\n"
                                     % (sym, interval, type(exc).__name__))
                    break
            if payload is None:
                fails += 1
                if blocked:
                    # Still refused after every backoff. Carrying on would
                    # spend the rest of the run collecting the same refusal;
                    # stopping keeps what was gathered and the next run
                    # picks up from there.
                    print("\n  blocked after %d retries at %s - stopping. "
                          "What was fetched is kept." % (len(RETRY_WAITS), sym))
                    syms = syms[:i]
                    break
                continue
            # Land before parse, always. If the parse below throws, the
            # response is already safe on disk and the day is recoverable.
            land(sym, interval, payload, today)
            collected[interval].extend(parse(sym, payload))
            events.extend(parse_events(sym, payload))
            time.sleep(PACE_SEC)
        # Flushed as we go, not held to the end. A full universe is about
        # eight million rows, and keeping them as Python tuples costs three
        # gigabytes - which the first version of this file did, on a box
        # with twelve. Partitions merge rather than replace, so writing in
        # batches gives an identical result with bounded memory; the cost
        # is rewriting each day's file once per batch, which is seconds.
        if not args.dry and i % FLUSH_EVERY == 0:
            for iv in collected:
                if collected[iv]:
                    write_partitions(collected[iv], iv)
                    collected[iv] = []
        else:
            if i % 10 == 0 or i == len(syms):
                # Flushed, because this runs for hours under cron with
                # stdout redirected to a file - and Python buffers that.
                # An unflushed progress line means a log that stays empty
                # until the job ends, which is exactly when you no longer
                # need it.
                print("  %3d/%d  %-12s  %5.0fs" % (i, len(syms), sym,
                                                   time.time() - t0),
                      flush=True)
            continue
        break

    if args.dry:
        print("\n--dry: bronze landed, no parquet written")
        for iv, rows in collected.items():
            print("  %s: %d row(s) parsed" % (iv, len(rows)))
        print("  events: %d" % len(events))
        return

    print()
    for interval, _back in plan:
        write_partitions(collected[interval], interval)
        collected[interval] = []
        # Counted from the archive rather than from what this run wrote:
        # after batching, no single call knows the total, and the number
        # worth printing is what is actually there.
        parts = sorted(glob.glob(os.path.join(
            SILVER, "interval=%s" % interval, "date=*", "part.parquet")))
        total = 0
        if parts:
            con = duckdb.connect()
            total = con.execute(
                "SELECT count(*) FROM read_parquet(?)",
                [os.path.join(SILVER, "interval=%s" % interval,
                              "date=*", "part.parquet")]).fetchone()[0]
            con.close()
        print("  %-3s  %d day partition(s), %d bar(s) on disk"
              % (interval, len(parts), total))

    # Written even when there are none, and reported either way. A split
    # nobody recorded is indistinguishable from a real 5x move once the
    # bars are in, so "0 today" is information and silence is not.
    if events:
        import corp_actions
        total, added = corp_actions.save(events)
        splits = [e for e in events if e["kind"] == "split"]
        print("  act  %d action(s) seen, %d new, %d stored" %
              (len(events), added, total))
        for e in splits[:5]:
            print("       %s  %-12s x%.6g  %s"
                  % (e["ex_date"], e["symbol"], e["factor"], e["subject"]))
    else:
        print("  act  no splits or dividends in this window")

    size = sum(os.path.getsize(os.path.join(dp, f))
               for root in (BRONZE, SILVER) if os.path.isdir(root)
               for dp, _dn, fn in os.walk(root) for f in fn)
    print("\n%d fetch failure(s), archive now %.1f MB in %d s"
          % (fails, size / 1e6, time.time() - t0))


if __name__ == "__main__":
    main()
