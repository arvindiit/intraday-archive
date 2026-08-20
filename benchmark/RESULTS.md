# Spark vs DuckDB, on this archive

The question this answers is not "which engine is better" — it is **where
the crossover sits for data of this shape and size**, measured rather
than assumed. Spark exists for data that no longer fits one machine;
below that, its fixed costs compete with the whole job.

Run it yourself:

```bash
pip install duckdb pyspark      # pyspark needs a JRE
python3 benchmark/spark_vs_duckdb.py
```

Three queries, matching the archive's real access patterns:

| query | shape | who runs it |
|---|---|---|
| `scan` | every symbol aggregated — nothing prunes | the nightly screen |
| `point` | one symbol, one day — pruning's showcase | every chart load |
| `resample` | 1m → 5m for one symbol | the derive-on-read decision |

Startup is timed separately from the queries on purpose: for small data
it **is** the result, and averaging it away is how benchmarks lie.

## Results

**Dev laptop** — Apple Silicon, macOS, Python 3.13, duckdb 1.5.5,
pyspark 4.2.0. Synthetic archive at production scale: 2,000 symbols,
6 sessions of 1-minute + 5-minute bars, 173 MB of parquet.

```
              duckdb    spark     ratio
startup        0.01s    5.85s     750x
scan           0.01s    1.86s     236x
point          0.01s    0.24s      45x
resample       0.01s    0.36s      40x
TOTAL          0.03s    8.30s     278x
```

At 173 MB, **Spark's startup alone is ~196x DuckDB's entire run** -
startup plus all three queries.

_pending: the production box (Ubuntu VPS, 12 GB, the real archive) -
same script, paste below._

## The reading

- Both engines return identical row counts on every query — this is a
  cost comparison, not a correctness one.
- DuckDB's advantage here is not cleverness, it is **absence of
  machinery**: no JVM, no session, no task scheduling, for a workload
  that fits comfortably in one process.
- The number at which this verdict flips: when the working set outgrows
  one machine's memory-plus-disk, or the nightly window stops fitting at
  full CPU. For this archive (~15 MB/day, ~4 GB/year) that is roughly a
  decade away — and the switch then is a different engine over the same
  parquet, which is the point of keeping the storage open-format.
