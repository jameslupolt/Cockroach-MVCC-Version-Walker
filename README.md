Count MVCC versions per key in a CockroachDB range by sampling historical snapshots.

## What it does

CockroachDB stores multiple MVCC versions of each row until garbage collection runs. When a range is unexpectedly large or hot, it's useful to know which keys have the most versions and how fast they're accumulating.

This script:

1. Connects to a CockroachDB cluster and identifies the primary key columns for the target table
2. Samples every second across the GC TTL window using `AS OF SYSTEM TIME`
3. Collects distinct `crdb_internal_mvcc_timestamp` values per key
4. Reports the number of observed versions and the update rate for each key

## Installation

Requires Python 3.10+ and `psycopg2`. Run directly with [uv](https://github.com/astral-sh/uv) — no manual install needed:

```bash
uv run mvcc_versions.py --help
```

## Usage

```bash
uv run mvcc_versions.py \
  --host <crdb-hostname> \
  --user <username> \
  --sslrootcert <path-to-ca.crt> \
  --db <database> \
  --table <table> \
  [--where "<predicate>"] \
  [--auto-ttl | --ttl <seconds>] \
  [--workers <N>]
```

### Typical workflow

1. Find the hot or bloated range:

   ```sql
   SHOW RANGES FROM TABLE system.jobs WITH DETAILS, KEYS;
   ```

2. Note the start/end keys and translate them into a SQL predicate on the table's primary key.

3. Run the script:

   ```bash
   uv run mvcc_versions.py \
     --host crdb.example.com --user dba \
     --sslrootcert ~/.certs/ca.crt \
     --db system --table jobs \
     --where "id < 1154217738623647977"
   ```

### Options

| Flag | Description | Default |
|------|-------------|---------|
| `--host` | CockroachDB hostname | required |
| `--user` | Database username (uses `.pgpass` for auth) | required |
| `--sslrootcert` | Path to CA certificate | required |
| `--db` | Database name | required |
| `--table` | Table name | required |
| `--where` | WHERE clause to target a specific range | whole table |
| `--ttl` | GC TTL in seconds | 7200 |
| `--auto-ttl` | Auto-detect GC TTL from zone configuration | off |
| `--workers` | Number of parallel connections | 8 |
| `--port` | CockroachDB port | 26257 |
| `--sslmode` | SSL mode | verify-full |

### Examples

```bash
# A specific range of a user table, auto-detect GC TTL
uv run mvcc_versions.py \
  --host crdb.example.com --user dba \
  --sslrootcert ~/.certs/ca.crt \
  --db mydb --table orders \
  --where "id >= 'a0f3...' AND id < 'b1e7...'" \
  --auto-ttl

# Whole small table
uv run mvcc_versions.py \
  --host crdb.example.com --user dba \
  --sslrootcert ~/.certs/ca.crt \
  --db mydb --table config --auto-ttl

# Faster scan with more workers, shorter TTL
uv run mvcc_versions.py \
  --host crdb.example.com --user dba \
  --sslrootcert ~/.certs/ca.crt \
  --db system --table jobs \
  --where "id < 1154217738623647977" \
  --ttl 3600 --workers 16
```

## Sample output

```
Primary key: (id)
GC TTL: 7200s (from zone config)
Found 68 live keys
Scanning 7200 seconds with 8 workers...
  500/7200 samples complete
  1000/7200 samples complete
  ...
Done in 37s (196 samples/s)

  versions   rate/s                       key            status          job_type
-----------------------------------------------------------------------------------------------
      6440    0.89       1096460340745011458           running        CHANGEFEED
      6424    0.89       1060446755271442608           running        CHANGEFEED
      6396    0.89       1016814707236929650           running        CHANGEFEED
       ...
         1    0.00                       100           running     KEY VISUALIZER
         1    0.00                       101           running   POLL JOBS STATS
-----------------------------------------------------------------------------------------------
    157061                           keys: 74
```

## How it works

CockroachDB exposes the hidden column `crdb_internal_mvcc_timestamp` on every table, returning the nanosecond MVCC timestamp of the current version of each row. By querying at every second within the GC TTL window via `AS OF SYSTEM TIME '-Ns'`, the script observes which timestamp was current at each sample point.

The count of distinct timestamps per key gives a lower bound on the number of MVCC versions — sub-second overwrites between sample points are not counted. For most workloads (heartbeats, status updates, changefeeds), per-second sampling captures the vast majority of versions.

### Why the version count may differ from span_stats

`span_stats` (from `SHOW RANGES ... WITH DETAILS`) reports at the KV layer:

- **`key_count`**: Number of unique KV keys, including separate column families. A table with 3 column families has 3 KV keys per row.
- **`val_count`**: Total MVCC versions across all KV keys, including sub-second overwrites.
- **`live_count`**: KV keys with a live (non-tombstone) value.

This script counts at the SQL row level, so:

- Rows found ≈ `live_count / column_families_per_row`
- Versions found ≤ `val_count` (missing sub-second updates and column-family multiplier)

## Performance

The script runs `TTL` queries in parallel across `--workers` connections. Each query returns only the rows matching the `--where` predicate, so the per-query cost is proportional to the number of keys in the target range.

| TTL | Workers | Typical runtime |
|-----|---------|-----------------|
| 7200 | 8 | ~35s |
| 7200 | 16 | ~20s |
| 3600 | 8 | ~18s |

## License

MIT
