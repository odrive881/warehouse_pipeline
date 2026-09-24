# Warehouse Inventory Pipeline

A containerized data pipeline that ingests synthetic warehouse movement
event data, transforms it with dbt, and produces inventory reporting
tables for BI tools such as Power BI. Orchestrated with Apache Airflow,
running fully in Docker.

This is the second project in a personal data engineering portfolio,
following an accounting ledger pipeline built on the same stack. The
focus here is on event-log modeling patterns not present in the
accounting project: reconstructing running state from an append-only
event stream, incremental dbt models, and slowly changing product
master data.

## What this project does

- Generates synthetic warehouse movement event data via a CSV generator
  script that correctly maintains data continuity across runs -
  event IDs and timestamps never overlap between batches
- Loads event batches into PostgreSQL via Python using PostgreSQL's
  native COPY protocol for bulk-load performance, with full data
  continuity validation before insertion
- Cleans, types, and reconstructs running inventory balances in a dbt
  staging layer using window functions
- Materializes the staging model incrementally - only new events are
  processed on each run, not the full history
- Produces three reporting marts: current stock levels, inventory
  turnover by period, and reorder alerts for SKUs below safety stock
- Orchestrated on demand via Airflow (manual trigger), fully containerized with Docker Compose

## Architecture

```
CSV generator (local)
        │
        ▼
movements_batch.csv
        │
        ▼ loader.py (validates continuity, COPY protocol)
        │
        ▼
raw.movements (Postgres)
        │
        ▼ dbt seed
        │
dbt_dev.product_master
        │
        ▼ dbt build (incremental)
        │
dbt_dev.stg_inventory_movements
  (running balance via window functions)
        │
    ┌───┴───────────────┐──────────────────┐
    ▼                   ▼                  ▼
mart_stock_level  mart_turnover  mart_reorder_alerts
        │
        ▼
  Power BI / pgAdmin
```

Orchestrated by a four-task Airflow DAG:
`ensure_raw_schema` -> `load_raw` -> `seed` -> `build`

## New concepts vs the accounting pipeline

| Concept | Accounting pipeline | This pipeline |
|---|---|---|
| Data shape | Double-entry ledger | Append-only event log |
| State reconstruction | Direct aggregation | Running balance via `SUM() OVER` |
| dbt materialization | `view` / `table` | `incremental` |
| Reference data changes | Static chart of accounts | Product master (SCD-aware) |
| Bulk loading | pandas `to_sql` | psycopg2 `copy_expert` |

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow (Docker) |
| Transformation | dbt Core + dbt-postgres |
| Storage | PostgreSQL 16 (containerized) |
| Ingestion | Python (pandas, psycopg2, SQLAlchemy) |
| Reporting | Power BI |

## Project structure

```
warehouse-pipeline/
│
├── warehouse_pipeline/              # dbt project root
│   ├── dbt_project.yml
│   ├── profiles.yml.example         # copy to profiles.yml, values copied over from .env
│   ├── data/
│   │   ├── .gitkeep
│   │   ├── movements_large.csv      # gitignored - generate locally
│   │   └── csv_generator_script.py  # generates randomized .csv ingestion data
│   ├── seeds/
│   │   └── product_master.csv       # 20-SKU reference data, committed
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_inventory_movements.sql
│   │   │   └── source.yml
│   │   └── marts/
│   │       ├── mart_stock_level.sql
│   │       ├── mart_turnover.sql
│   │       ├── mart_reorder_alerts.sql
│   │       └── marts.yml
│   ├── tests/
│   │   ├── assert_positive_quantity.sql
│   │   └── assert_no_implausible_quantity.sql
│   └── loader.py
│
├── airflow-warehouse-pipeline/      # Docker / Airflow setup
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   ├── .env.example                 # copy to .env, fill in values
│   └── dags/
│       └── warehouse_pipeline_dag.py
│
├── .gitignore
├── LICENSE
└── README.md
```

## Setup

### Prerequisites

- Docker Desktop (4GB+ memory allocated)
- Python 3.12 (required, will not run otherwise), used through a virtual
  environment (step 2)
- Power BI Desktop (optional, for viewing reports)

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/warehouse-pipeline.git
cd warehouse-pipeline
```

### 2. Create a Python 3.12 virtual environment

The generator, loader and benchmark scripts run on your machine, so they
need their dependencies installed locally. Use a virtual environment so
they don't mix with other Python installs on your system.

Windows (PowerShell):

```powershell
cd warehouse_pipeline
py -3.12 -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
cd ..
```

macOS / Linux:

```bash
cd warehouse_pipeline
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cd ..
```

Keep the environment activated (your prompt shows `(.venv)`) for every
`python` command in the steps below. Use `python -m pip` rather than bare
`pip`, so packages go into the venv and not into another Python on your
PATH. If you get `No module named pip`, run `py -3.12 -m ensurepip --upgrade`
(or `python3.12 -m ensurepip --upgrade`) and try again.

Airflow is not installed here. It runs inside Docker (step 5).

### 3. Copy and fill in config files

```bash
cp warehouse_pipeline/profiles.yml.example warehouse_pipeline/profiles.yml
cp airflow-warehouse-pipeline/.env.example airflow-warehouse-pipeline/.env
```

Edit both files and replace placeholder values with real credentials.
The values in `.env` are automatically copied over to `profiles.yml`,
leave the values in `profiles.yml` unchanged.

### 4. Generate the initial movement data

```bash
cd warehouse_pipeline/data
python csv_generator_script.py
```

This creates `warehouse_pipeline/data/movements_large.csv` - the initial
full dataset. On subsequent runs it generates a new batch file containing
only new events, with non-overlapping event IDs and timestamps, ready to
be appended by the loader.

### 5. Build and start the Docker stack

```bash
cd..
cd..
cd airflow-warehouse-pipeline
docker compose up airflow-init
docker compose up -d
docker compose ps
```

All containers should report healthy. This typically takes 60-90 seconds
on first start.

### 6. Open the Airflow UI

Navigate to `http://localhost:8080` in your browser.
Default credentials: username `airflow`, password `airflow`.

### 7. Trigger the pipeline

Unpause the `warehouse_pipeline` DAG and click the play button to trigger
a manual run. The DAG is set to `schedule=None` - it only runs when
manually triggered, matching the real-world cadence of "place a new batch
file, then trigger the pipeline."

Watch the four tasks complete in order:
`ensure_raw_schema` -> `load_raw` -> `seed` -> `build`

### 8. Connect Power BI (optional)

In Power BI Desktop, connect via Get Data -> PostgreSQL:

```
Server:   localhost:5433
Database: warehouse_pipeline
```

Select tables from the `dbt_dev` schema. Use Import mode.

## Running subsequent batches

To simulate ongoing warehouse activity, with the virtual environment from
step 2 activated:

```bash
cd warehouse_pipeline/data	# generates new batch with fresh event IDs
python csv_generator_script.py  # and timestamps after existing data
```

Then trigger the DAG again - the loader validates continuity and appends
only the new rows, the incremental dbt model processes only the new
events, and marts update automatically.

## Connection reference

A common source of confusion that I experienced when working with containerized Postgres:

| Connecting from | Host | Port |
|---|---|---|
| dbt (`profiles.yml`) | `warehouse-db` | `5432` |
| `loader.py` | `warehouse-db` | `5432` |
| pgAdmin / Power BI / psql on Windows | `localhost` | `5433` |

Anything inside the Docker network uses the service name and internal
port. Anything on your Windows host uses `localhost` and the mapped port.

## Data model notes

- Movement events are append-only. The source table `raw.movements` is
  never truncated between runs - only new rows are added.
- Running inventory balance is reconstructed from the event log using
  `SUM() OVER (PARTITION BY sku ORDER BY event_timestamp)`. This is the
  core modeling pattern of the project.
- `mart_reorder_alerts` is built on top of `mart_stock_level` rather
  than directly on staging, demonstrating mart-on-mart composition.

## Sample data

This repository does not include real operational data. All movement
events are synthetically generated by `csv_generator_script.py`. The
product master (`seeds/product_master.csv`) is a hand-authored reference
file containing 20 fictional SKUs across five categories.

## Status

Personal learning project - built to practice incremental dbt models,
window-function-based state reconstruction, and bulk-loading patterns,
applied to a warehouse/inventory domain. Second project in a portfolio
that also includes an accounting ledger pipeline built on the same
dbt/Airflow/Docker stack.
