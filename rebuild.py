#!/usr/bin/env python3
"""Rebuild silver from bronze. No network, no re-fetching.

    python3 rebuild.py                     everything on disk
    python3 rebuild.py --interval 1m
    python3 rebuild.py --fetched 2026-08-18
    python3 rebuild.py --batch 200         smaller batches, less memory

This is the reason bronze exists. The parse or the partition writer can be
wrong, or slow, or a run can die halfway - and none of it costs a session,
because the upstream's answer was written down before anything touched it.
Yahoo keeps seven days of 1-minute bars; without a raw copy, a bug found on
the eighth day is permanent.

Batched on purpose. Doing it in one pass means holding every row in memory -
eight million of them for a full universe, about three gigabytes - and the
first version of the fetcher did exactly that. Partitions merge rather than
replace, so writing in batches gives the same result with bounded memory;
the only cost is rewriting each day's file once per batch, which is
seconds.

Standard library plus duckdb.
"""

import argparse
import glob
import gzip
import json
import os
import sys
import time

import bars_fetch


def bronze_files(interval, fetched=None):
    """Every landed response for one interval, oldest fetch first."""
    pat = os.path.join(bars_fetch.BRONZE, "interval=%s" % interval,
                       "fetched=%s" % (fetched or "*"), "*.json.gz")
    return sorted(glob.glob(pat))


def rebuild(interval, fetched=None, batch=400, dry=False):
    files = bronze_files(interval, fetched)
    if not files:
        print("  %s: nothing landed%s" % (interval,
              " for fetched=%s" % fetched if fetched else ""))
        return 0, 0

    print("  %s: %d bronze file(s)" % (interval, len(files)))
    rows, parsed, days, t0 = [], 0, set(), time.time()
    for i, p in enumerate(files, 1):
        symbol = os.path.basename(p).replace(".json.gz", "")
        try:
            with gzip.open(p, "rt", encoding="utf-8") as f:
                payload = json.load(f)
        except (OSError, ValueError) as exc:
            # Named, not swallowed. A corrupt landing is worth knowing about
            # precisely because bronze is the copy of last resort.
            sys.stderr.write("    %s unreadable (%s)\n"
                             % (p, type(exc).__name__))
            continue
        got = bars_fetch.parse(symbol, payload)
        rows.extend(got)
        parsed += len(got)
        days.update(r[7] for r in got)

        if len(rows) and (i % batch == 0 or i == len(files)):
            if not dry:
                bars_fetch.write_partitions(rows, interval)
            print("    %4d/%d files  %8d row(s)  %4.0fs"
                  % (i, len(files), parsed, time.time() - t0), flush=True)
            rows = []
    return parsed, len(days)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--interval", help="1m or 5m; both when omitted")
    ap.add_argument("--fetched", metavar="YYYY-MM-DD",
                    help="only responses landed on that date")
    ap.add_argument("--batch", type=int, default=400,
                    help="files per write (default 400; lower it if memory "
                         "is tight)")
    ap.add_argument("--dry", action="store_true",
                    help="parse everything, write nothing")
    args = ap.parse_args()

    intervals = [args.interval] if args.interval else ["1m", "5m"]
    print("Rebuilding silver from %s" % bars_fetch.BRONZE)
    t0 = time.time()
    total = 0
    for iv in intervals:
        n, d = rebuild(iv, args.fetched, args.batch, args.dry)
        total += n
        if n:
            print("  %s: %d row(s) across %d session(s)\n" % (iv, n, d))
    print("%s %d row(s) in %.0fs"
          % ("would write" if args.dry else "wrote", total, time.time() - t0))


if __name__ == "__main__":
    main()
