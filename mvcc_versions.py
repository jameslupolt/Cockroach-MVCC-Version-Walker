#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["psycopg2-binary"]
# ///
"""
Count MVCC versions per key in a CockroachDB range.

Samples every second across the GC TTL window using AS OF SYSTEM TIME,
collecting distinct crdb_internal_mvcc_timestamp values per key.

Usage examples:

  # system.jobs range with keys below a specific job ID
  mvcc_versions.py --host crdb.example.com --user dba \
    --sslrootcert ~/.certs/ca.crt \
    --db system --table jobs --where "id < 1154217738623647977"

  # A specific range of a user table by UUID boundaries from SHOW RANGES
  mvcc_versions.py --host crdb.example.com --user dba \
    --sslrootcert ~/.certs/ca.crt \
    --db mydb --table orders \
    --where "id >= 'a0f3...' AND id < 'b1e7...'" --auto-ttl

  # Whole small table, auto-detect GC TTL
  mvcc_versions.py --host crdb.example.com --user dba \
    --sslrootcert ~/.certs/ca.crt \
    --db mydb --table config --auto-ttl
"""

import argparse
import re
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import psycopg2.sql


def get_primary_key_columns(conn, table: str) -> list[str]:
    """Auto-detect primary key columns for a table."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT kcu.column_name "
            "FROM information_schema.key_column_usage kcu "
            "JOIN information_schema.table_constraints tc "
            "  ON kcu.constraint_name = tc.constraint_name "
            "  AND kcu.table_schema = tc.table_schema "
            "WHERE kcu.table_name = %s AND tc.constraint_type = 'PRIMARY KEY' "
            "ORDER BY kcu.ordinal_position",
            (table,),
        )
        cols = [row[0] for row in cur.fetchall()]
    if not cols:
        raise SystemExit(f"Error: could not find primary key for table '{table}'")
    return cols


def get_gc_ttl(conn, db: str, table: str) -> int:
    """Extract gc.ttlseconds from the zone configuration."""
    with conn.cursor() as cur:
        cur.execute(f"SHOW ZONE CONFIGURATION FOR TABLE {db}.{table}")
        for row in cur.fetchall():
            config = str(row)
            match = re.search(r"gc\.ttlseconds\s*=\s*(\d+)", config)
            if match:
                return int(match.group(1))
    return 7200  # fallback


_progress_lock = threading.Lock()
_progress_done = 0


def scan_chunk(
    dsn: str,
    offsets: range,
    query_template: str,
    params: tuple,
    pk_count: int,
    total: int,
) -> dict[tuple, set]:
    global _progress_done
    conn = psycopg2.connect(dsn)
    conn.set_session(autocommit=True, readonly=True)
    versions: dict[tuple, set] = defaultdict(set)
    cur = conn.cursor()
    try:
        for offset in offsets:
            cur.execute(query_template.format(offset=offset), params)
            for row in cur:
                pk = row[:pk_count]
                mvcc_ts = row[pk_count]
                versions[pk].add(mvcc_ts)

            with _progress_lock:
                _progress_done += 1
                done = _progress_done
            if done % 500 == 0:
                print(f"  {done}/{total} samples complete", file=sys.stderr)
    finally:
        cur.close()
        conn.close()
    return versions


def main():
    parser = argparse.ArgumentParser(
        description="Count MVCC versions per key in a CockroachDB range",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--host", required=True, help="CockroachDB hostname")
    parser.add_argument("--user", required=True, help="Database username")
    parser.add_argument("--port", type=int, default=26257)
    parser.add_argument("--sslmode", default="verify-full")
    parser.add_argument("--sslrootcert", required=True, help="Path to CA cert")
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument(
        "--where",
        default=None,
        help="WHERE clause to target a specific range (e.g. \"id < 12345\")",
    )
    parser.add_argument("--ttl", type=int, default=None, help="GC TTL in seconds")
    parser.add_argument(
        "--auto-ttl",
        action="store_true",
        help="Auto-detect GC TTL from zone configuration",
    )
    parser.add_argument(
        "--workers", type=int, default=8, help="Parallel connections (default: 8)"
    )
    args = parser.parse_args()

    dsn = (
        f"postgresql://{args.user}@{args.host}:{args.port}/{args.db}"
        f"?sslmode={args.sslmode}&sslrootcert={args.sslrootcert}"
    )

    conn = psycopg2.connect(dsn)
    conn.set_session(autocommit=True, readonly=True)

    # Discover primary key.
    pk_cols = get_primary_key_columns(conn, args.table)
    pk_list = ", ".join(pk_cols)
    print(f"Primary key: ({pk_list})", file=sys.stderr)

    # Determine GC TTL.
    if args.ttl is not None:
        ttl = args.ttl
    elif args.auto_ttl:
        ttl = get_gc_ttl(conn, args.db, args.table)
        print(f"GC TTL: {ttl}s (from zone config)", file=sys.stderr)
    else:
        ttl = 7200
        print(f"GC TTL: {ttl}s (default; use --auto-ttl or --ttl)", file=sys.stderr)

    # Build queries.
    where = f"WHERE {args.where}" if args.where else ""

    # Extra columns to display (best-effort, non-fatal if missing).
    extra_cols = []
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s AND column_name IN ('status', 'job_type', 'type')",
            (args.table,),
        )
        extra_cols = [row[0] for row in cur.fetchall()]

    extra_list = (", " + ", ".join(extra_cols)) if extra_cols else ""

    current_query = (
        f"SELECT {pk_list}, crdb_internal_mvcc_timestamp{extra_list} "
        f"FROM {args.table} {where} ORDER BY {pk_list}"
    )

    # The {{offset}} placeholder is filled by str.format() in scan_chunk;
    # any %s placeholders are filled by psycopg2 (unused here since the
    # WHERE clause is baked in, but kept for safety).
    scan_query = (
        f"SELECT {pk_list}, crdb_internal_mvcc_timestamp "
        f"FROM {args.table} AS OF SYSTEM TIME '-{{offset}}s' {where}"
    )

    # Step 1: Get current keys.
    with conn.cursor() as cur:
        cur.execute(current_query)
        rows = cur.fetchall()

    current_keys: dict[tuple, dict] = {}
    for row in rows:
        pk = row[: len(pk_cols)]
        mvcc_ts = row[len(pk_cols)]
        extras = dict(zip(extra_cols, row[len(pk_cols) + 1 :]))
        current_keys[pk] = {"mvcc_ts": mvcc_ts, **extras}

    conn.close()
    print(f"Found {len(current_keys)} live keys\n", file=sys.stderr)

    # Seed versions with current state.
    versions: dict[tuple, set] = defaultdict(set)
    for pk, info in current_keys.items():
        versions[pk].add(info["mvcc_ts"])

    # Step 2: Parallel scan across TTL window.
    chunk_size = (ttl + args.workers - 1) // args.workers
    chunks = []
    for i in range(args.workers):
        start = 1 + i * chunk_size
        end = min(start + chunk_size, ttl + 1)
        if start < end:
            chunks.append(range(start, end))

    global _progress_done
    _progress_done = 0
    t0 = time.monotonic()
    print(f"Scanning {ttl} seconds with {len(chunks)} workers...", file=sys.stderr)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(
                scan_chunk, dsn, chunk, scan_query, (), len(pk_cols), ttl
            )
            for chunk in chunks
        ]
        for future in as_completed(futures):
            for pk, ts_set in future.result().items():
                versions[pk].update(ts_set)

    elapsed = time.monotonic() - t0
    print(f"Done in {elapsed:.0f}s ({ttl / elapsed:.0f} samples/s)\n", file=sys.stderr)

    # Step 3: Report.
    all_keys = set(current_keys) | set(versions)
    result_rows = []
    for pk in all_keys:
        count = len(versions.get(pk, set()))
        info = current_keys.get(pk, {})
        rate = count / ttl if ttl > 0 else 0
        result_rows.append((count, pk, rate, info))

    result_rows.sort(key=lambda r: -r[0])

    # Format header.
    pk_header = "key" if len(pk_cols) == 1 else "(" + ", ".join(pk_cols) + ")"
    extra_headers = [col.rjust(18) for col in extra_cols]
    print(
        f"{'versions':>10}  {'rate/s':>7}  {pk_header:>25}"
        + "".join(extra_headers)
    )
    print("-" * (45 + 18 * len(extra_cols)))

    total_versions = 0
    for count, pk, rate, info in result_rows:
        total_versions += count
        pk_str = str(pk[0]) if len(pk) == 1 else str(pk)
        extra_vals = "".join(
            str(info.get(col, "GONE")).rjust(18) for col in extra_cols
        )
        print(f"{count:>10}  {rate:>6.2f}  {pk_str:>25}{extra_vals}")

    print("-" * (45 + 18 * len(extra_cols)))
    print(f"{total_versions:>10}  {'':>7}  {'keys: ' + str(len(result_rows)):>25}")
    print(
        f"\nPer-second sampling; sub-second overwrites not counted.",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
