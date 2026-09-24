"""
Benchmarks for the numbers worth putting on a resume.

  1. load   - COPY vs pandas to_sql (row-by-row and chunked multi-insert)
              Uses a scratch schema (bench_tmp) that is dropped afterwards.
  2. dbt    - incremental vs --full-refresh runtime of stg_inventory_movements, plus a
              correctness check that the incremental running_balance matches a full rebuild.
              Inserts a temporary batch into raw.movements and removes it afterwards.

Run from the host (not inside Docker), with the virtual environment activated:

    python benchmark.py load --rows 100000
    python benchmark.py dbt --rows 50000

Credentials are read from airflow-warehouse-pipeline/.env. Its DB_HOST/DB_PORT point at
the Docker-internal address (warehouse-db:5432), so they are swapped for localhost and
DB_HOST_PORT (5433). Variables already set in the shell take precedence over .env.
dbt reads the same variables through profiles.yml, so no separate profile is needed.

Results are printed as a table and written to benchmark_results.json.
"""
import argparse
import io
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

HERE = Path(__file__).resolve().parent
ENV_FILE = HERE.parent / "airflow-warehouse-pipeline" / ".env"
SCRATCH = "bench_tmp"
SKUS = [f"SKU-{n:04d}" for n in range(1, 21)]
WAREHOUSES = ["WH-POZNAN", "WH-WARSZAWA"]
TYPES = ["receipt", "return", "pick", "shipment", "damage"]


def load_env():
    """Load .env (shell variables win) and point DB_HOST/DB_PORT at the host-mapped port."""
    host_set_in_shell = "DB_HOST" in os.environ
    if not ENV_FILE.exists():
        sys.exit(f"{ENV_FILE} not found - copy .env.example to .env and fill it in.")
    load_dotenv(ENV_FILE, override=False)
    if not host_set_in_shell:
        os.environ["DB_HOST"] = "localhost"
        os.environ["DB_PORT"] = os.environ.get("DB_HOST_PORT", "5433")
    missing = [k for k in ("DB_USER", "DB_PASSWORD", "DB_NAME") if not os.environ.get(k)]
    if missing:
        sys.exit(f"Missing in {ENV_FILE}: {', '.join(missing)}")
    print(f"Connecting to {os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']} "
          f"as {os.environ['DB_USER']}")


def get_engine():
    return create_engine(
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}"
        f"/{os.environ['DB_NAME']}"
    )


def make_events(n, start_id, start_ts, days=30, seed=0):
    rng = np.random.default_rng(seed)
    ts = pd.Timestamp(start_ts) + pd.to_timedelta(rng.integers(0, days * 24 * 60, n), unit="m")
    df = pd.DataFrame({
        "event_id": np.arange(start_id, start_id + n),
        "sku": rng.choice(SKUS, n),
        "warehouse_id": rng.choice(WAREHOUSES, n),
        "event_type": rng.choice(TYPES, n, p=[0.2, 0.1, 0.3, 0.3, 0.1]),
        "quantity": rng.integers(1, 51, n),
        "event_timestamp": ts,
    })
    return df.sort_values("event_timestamp").reset_index(drop=True)


def timed(fn):
    t = time.perf_counter()
    fn()
    return time.perf_counter() - t


def summarize(name, times, rows=None):
    med = statistics.median(times)
    out = {"name": name, "runs": len(times), "median_s": round(med, 3),
           "min_s": round(min(times), 3), "max_s": round(max(times), 3)}
    if rows:
        out["rows_per_s"] = round(rows / med)
    return out


def print_table(results):
    print(f"\n{'benchmark':<34}{'median s':>10}{'min':>9}{'max':>9}{'rows/s':>12}")
    for r in results:
        print(f"{r['name']:<34}{r['median_s']:>10}{r['min_s']:>9}{r['max_s']:>9}{r.get('rows_per_s', ''):>12}")


# --------------------------------------------------------------------------- load
def bench_load(args):
    engine = get_engine()
    df = make_events(args.rows, 1, "2030-01-01")
    ddl = f"""CREATE TABLE {SCRATCH}.m (event_id bigint, sku text, warehouse_id text,
              event_type text, quantity bigint, event_timestamp timestamp)"""

    def fresh_table():
        with engine.begin() as c:
            c.execute(text(f"DROP SCHEMA IF EXISTS {SCRATCH} CASCADE"))
            c.execute(text(f"CREATE SCHEMA {SCRATCH}"))
            c.execute(text(ddl))

    def copy():
        raw = engine.raw_connection()
        try:
            buf = io.StringIO()
            df.to_csv(buf, index=False, header=True)
            buf.seek(0)
            raw.cursor().copy_expert(f"COPY {SCRATCH}.m FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", buf)
            raw.commit()
        finally:
            raw.close()

    def to_sql_multi():
        df.to_sql("m", engine, schema=SCRATCH, if_exists="append", index=False,
                  method="multi", chunksize=10_000)

    def to_sql_default():
        df.to_sql("m", engine, schema=SCRATCH, if_exists="append", index=False)

    methods = [("COPY (copy_expert)", copy), ("to_sql multi, chunk=10k", to_sql_multi)]
    if not args.skip_slow:
        methods.append(("to_sql default (executemany)", to_sql_default))

    results = []
    try:
        for name, fn in methods:
            times = []
            for _ in range(args.repeats):
                fresh_table()
                times.append(timed(fn))
            with engine.connect() as c:
                loaded = c.execute(text(f"SELECT COUNT(*) FROM {SCRATCH}.m")).scalar()
            assert loaded == args.rows, f"{name}: loaded {loaded}, expected {args.rows}"
            results.append(summarize(name, times, args.rows))
    finally:
        with engine.begin() as c:
            c.execute(text(f"DROP SCHEMA IF EXISTS {SCRATCH} CASCADE"))

    base = next((r for r in results if r["name"].startswith("to_sql default")), results[-1])
    for r in results:
        r["speedup_vs_baseline"] = round(base["median_s"] / r["median_s"], 1)
        r["baseline"] = base["name"]
    return results


# --------------------------------------------------------------------------- dbt
def dbt(args, *extra):
    cmd = ["dbt", "run", "--select", "stg_inventory_movements",
           "--project-dir", str(HERE), "--profiles-dir", args.profiles_dir, *extra]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        sys.exit(f"dbt failed:\n{p.stdout}\n{p.stderr}")
    # Model execution time only, without dbt's startup/parse overhead
    run_results = json.loads((HERE / "target" / "run_results.json").read_text())
    return run_results["results"][0]["execution_time"]


def bench_dbt(args):
    engine = get_engine()
    stg = f"{args.dbt_schema}.stg_inventory_movements"

    with engine.connect() as c:
        max_id, max_ts, total = c.execute(text(
            "SELECT MAX(event_id), MAX(event_timestamp), COUNT(*) FROM raw.movements")).one()
    if not total:
        sys.exit("raw.movements is empty - run the pipeline once first.")

    print(f"raw.movements has {total:,} rows; syncing staging (untimed)...")
    dbt(args)  # bring staging up to date so the timed run only sees the new batch

    batch = make_events(args.rows, max_id + 1, pd.Timestamp(max_ts).normalize() + pd.Timedelta(days=1))
    raw = engine.raw_connection()
    try:
        buf = io.StringIO()
        batch.to_csv(buf, index=False, header=True)
        buf.seek(0)
        raw.cursor().copy_expert("COPY raw.movements FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", buf)
        raw.commit()
    finally:
        raw.close()

    results, mismatches = [], None
    try:
        t = time.perf_counter()
        inc_sql = dbt(args)
        inc = time.perf_counter() - t
        with engine.begin() as c:
            c.execute(text(f"DROP TABLE IF EXISTS {SCRATCH}_inc"))
            c.execute(text(f"CREATE TABLE {SCRATCH}_inc AS SELECT event_id, running_balance FROM {stg}"))
        t = time.perf_counter()
        full_sql = dbt(args, "--full-refresh")
        full = time.perf_counter() - t
        with engine.connect() as c:
            mismatches = c.execute(text(
                f"SELECT COUNT(*) FROM {SCRATCH}_inc i JOIN {stg} f USING (event_id) "
                f"WHERE i.running_balance IS DISTINCT FROM f.running_balance")).scalar()
        def row(name, s):
            return {"name": name, "median_s": round(s, 3), "min_s": round(s, 3), "max_s": round(s, 3)}
        results = [
            row(f"incremental, wall (+{args.rows:,})", inc),
            row(f"full-refresh, wall ({total + args.rows:,})", full),
            row(f"incremental, model SQL (+{args.rows:,})", inc_sql),
            row(f"full-refresh, model SQL ({total + args.rows:,})", full_sql),
        ]
        results[0]["full_refresh_vs_incremental_x"] = round(full / inc, 1)
        results[2]["full_refresh_vs_incremental_x"] = round(full_sql / inc_sql, 1)
        results[0]["running_balance_mismatches"] = mismatches
    finally:
        with engine.begin() as c:
            c.execute(text(f"DROP TABLE IF EXISTS {SCRATCH}_inc"))
            c.execute(text("DELETE FROM raw.movements WHERE event_id > :m"), {"m": max_id})
        print("removed benchmark rows from raw.movements; rebuilding staging (untimed)...")
        dbt(args, "--full-refresh")
    print(f"running_balance mismatches, incremental vs full rebuild: {mismatches}")
    return results


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    l = sub.add_parser("load")
    l.add_argument("--rows", type=int, default=100_000)
    l.add_argument("--repeats", type=int, default=3)
    l.add_argument("--skip-slow", action="store_true", help="skip default to_sql (minutes at 500k rows)")
    d = sub.add_parser("dbt")
    d.add_argument("--rows", type=int, default=50_000)
    d.add_argument("--profiles-dir", default=str(HERE))
    d.add_argument("--dbt-schema", default="dbt_dev")
    args = ap.parse_args()

    load_env()
    results = bench_load(args) if args.cmd == "load" else bench_dbt(args)
    print_table(results)
    out = HERE / "benchmark_results.json"
    existing = json.loads(out.read_text()) if out.exists() else {}
    existing[args.cmd] = {"rows": args.rows, "results": results}
    out.write_text(json.dumps(existing, indent=2))
    print(f"\nwritten to {out}")


if __name__ == "__main__":
    main()
