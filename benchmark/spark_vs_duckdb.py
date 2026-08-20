#!/usr/bin/env python3
"""Spark against DuckDB, on this archive, timed honestly.

    python3 benchmark/spark_vs_duckdb.py

Three queries that cover the archive's real access patterns:

    scan      every symbol, every session - the cross-symbol aggregate
              nothing can prune (the screen's shape of work)
    point     one symbol, one day - the case partition pruning and
              row-group statistics exist for (the chart's shape)
    resample  1-minute to 5-minute for one symbol across the archive -
              the derive-on-read decision, exercised

Engine startup is timed separately from the queries, because it is the
whole argument: below some data size, the fixed cost of a distributed
engine exceeds the entire job, and the honest comparison names that cost
instead of hiding it in an average.

Nothing here is rigged. Both engines read the same parquet files in
place; neither gets a warm-up lap the other is denied; the same SQL runs
on both (Spark reads it through its SQL interface, DuckDB natively).
"""

import glob
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get("DATA_DIR", os.path.join(HERE, "..", "data"))
SILVER = os.path.join(DATA, "silver", "bars")


def dataset():
    files = glob.glob(os.path.join(SILVER, "interval=*", "date=*",
                                   "part.parquet"))
    size = sum(os.path.getsize(f) for f in files)
    return files, size


def pick_symbol_day():
    """A symbol and a date that actually exist, chosen from the data."""
    import duckdb
    con = duckdb.connect()
    g = os.path.join(SILVER, "interval=5m", "date=*", "part.parquet")
    sym, day = con.execute(
        "SELECT symbol, date FROM read_parquet(?, hive_partitioning=true) "
        "LIMIT 1", [g]).fetchone()
    con.close()
    return sym, str(day)


QUERIES = {
    "scan": """
        SELECT symbol, count(*) AS bars, avg(v) AS avg_vol
        FROM bars5
        GROUP BY symbol
        ORDER BY bars DESC
    """,
    "point": """
        SELECT count(*), min(l), max(h)
        FROM bars5
        WHERE symbol = '{sym}' AND date = DATE '{day}'
    """,
    "resample": """
        SELECT symbol,
               ts - (ts % 300) AS bucket,
               min(l) AS lo, max(h) AS hi, sum(v) AS vol
        FROM bars1
        WHERE symbol = '{sym}'
        GROUP BY 1, 2
    """,
}


def run_duckdb(sym, day):
    t0 = time.perf_counter()
    import duckdb
    con = duckdb.connect()
    g5 = os.path.join(SILVER, "interval=5m", "date=*", "part.parquet")
    g1 = os.path.join(SILVER, "interval=1m", "date=*", "part.parquet")
    con.execute("CREATE VIEW bars5 AS SELECT * FROM "
                "read_parquet('%s', hive_partitioning=true)" % g5)
    con.execute("CREATE VIEW bars1 AS SELECT * FROM "
                "read_parquet('%s', hive_partitioning=true)" % g1)
    startup = time.perf_counter() - t0

    times = {}
    for name, q in QUERIES.items():
        t0 = time.perf_counter()
        rows = con.execute(q.format(sym=sym, day=day)).fetchall()
        times[name] = (time.perf_counter() - t0, len(rows))
    con.close()
    return startup, times


def run_spark(sym, day):
    t0 = time.perf_counter()
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("bench")
             .config("spark.ui.enabled", "false")
             .master("local[*]").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.read.parquet(os.path.join(SILVER, "interval=5m")) \
         .createOrReplaceTempView("bars5")
    spark.read.parquet(os.path.join(SILVER, "interval=1m")) \
         .createOrReplaceTempView("bars1")
    startup = time.perf_counter() - t0

    times = {}
    for name, q in QUERIES.items():
        t0 = time.perf_counter()
        rows = spark.sql(q.format(sym=sym, day=day)).collect()
        times[name] = (time.perf_counter() - t0, len(rows))
    spark.stop()
    return startup, times


def main():
    files, size = dataset()
    if not files:
        raise SystemExit("no archive at %s - run the fetch first" % SILVER)
    sym, day = pick_symbol_day()
    print("archive: %d file(s), %.1f MB   probe: %s on %s\n"
          % (len(files), size / 1e6, sym, day))

    d_start, d_times = run_duckdb(sym, day)
    s_start, s_times = run_spark(sym, day)

    print("%-10s %14s %14s %8s" % ("", "duckdb", "spark", "ratio"))
    print("%-10s %12.2fs %12.2fs %7.0fx"
          % ("startup", d_start, s_start,
             s_start / d_start if d_start > 0 else 0))
    d_total, s_total = d_start, s_start
    for name in QUERIES:
        dt, dn = d_times[name]
        st, sn = s_times[name]
        flag = "" if dn == sn else "  ROWS DIFFER %d vs %d" % (dn, sn)
        print("%-10s %12.2fs %12.2fs %7.1fx%s"
              % (name, dt, st, st / dt if dt > 0 else 0, flag))
        d_total += dt
        s_total += st
    print("%-10s %12.2fs %12.2fs %7.1fx"
          % ("TOTAL", d_total, s_total,
             s_total / d_total if d_total > 0 else 0))
    print("\nThe number that matters: at %.0f MB, Spark's startup alone is "
          "%.1fx DuckDB's entire run." % (size / 1e6, s_start / d_total
                                          if d_total > 0 else 0))


if __name__ == "__main__":
    main()
