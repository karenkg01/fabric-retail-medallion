# Project Map: How the Pieces Connect

A quick reference for the Retail Medallion project in Microsoft Fabric: where everything lives, what each item does, and how they connect.

## Item types at a glance

The name prefix tells you what kind of item it is:

| Prefix | Type | What it is | Items in this project |
| --- | --- | --- | --- |
| `lh_` | **Lakehouse** | Storage: holds files and tables | `lh_source`, `lh_bronze`, `lh_silver`, `lh_gold`, `lh_framework` |
| `nb_` | **Notebook** | Code that does the work (PySpark) | `nb_setup_framework`, `nb_bronze_load` |
| `pl_` | **Pipeline** | Automation that runs notebooks in order | `pl_ingest_bronze` |
| (none) | **Table** | Data inside a lakehouse | e.g. `meta.ingestion_config`, `sales_system.orders` |

**Two halves to remember:**
- **The data's journey:** `lh_source` → `lh_bronze` → `lh_silver` → `lh_gold`
- **The control room:** `lh_framework` tells the journey what to do and records what happened.

## The data's journey

| Step | Item | Type | Task | Status | What it does |
| --- | --- | --- | --- | --- | --- |
| 1 | `lh_source` | Lakehouse | Batch Ingestion | Done | Plays the "outside systems." Holds ~4,400 raw CSV/JSON files from 7 systems in `Files/landing/`, in year/month/day folders, plus manifest files listing how many rows each file should have. No tables; nothing here is ever changed. |
| 2 | `pl_ingest_bronze` | Pipeline | Batch Ingestion | Building | The automation. **Lookup** reads the source list from `meta.ingestion_config`, **Filter** keeps only active sources, **ForEach** runs `nb_bronze_load` once per source. |
| 3 | `nb_bronze_load` | Notebook | Batch Ingestion | Done | Does the loading. Finds files newer than the watermark, loads them as text plus tracking columns (`_source_file`, `_business_date`, `_ingest_ts`, `_batch_id`, `_row_hash`), checks each file's row count against its manifest, moves the watermark forward, and writes the run log. Safe to rerun. |
| 4 | `lh_bronze` | Lakehouse | Bronze Layer | Done | Raw copy of the source data, 3.3M rows, all values stored as text. |
| 5 | `lh_silver` | Lakehouse | Silver Layer | Empty | Next: correct data types, duplicates removed, bad rows quarantined with a reason, customer and product history (SCD Type 2). |
| 6 | `lh_gold` | Lakehouse | Gold Layer | Empty | Later: star schema (fact and dimension tables) that feeds the Power BI report. Planned to be built with dbt. |

### Bronze tables by schema (Tables inside the `lh_bronze` Lakehouse)

| Schema | Tables |
| --- | --- |
| `sales_system` | orders, order_lines |
| `customer_system` | customers |
| `product_catalog` | products |
| `store_operations` | stores, returns, inventory |

## The control room

| Item | Type | Task | Status | What it does |
| --- | --- | --- | --- | --- |
| `lh_framework` | Lakehouse | Orchestration | Done | Holds the `meta` schema: the tables that drive and record every run. |
| `nb_setup_framework` | Notebook | Orchestration | Done | One-time setup. Created the `meta` schema and its 5 tables, and filled in the 7 config rows and 20 data-quality rules. |

### The 5 framework tables (Tables inside the `lh_framework` Lakehouse, `meta` schema)

| Table | What it holds | Who uses it (Notebook / Pipeline) |
| --- | --- | --- |
| `ingestion_config` | The 7 sources and how to read each (format, read options, target schema/table, primary keys, load order, `is_active`) | The pipeline reads it to know what to run |
| `watermark` | Last date loaded per source | `nb_bronze_load` reads it to find new files and updates it after a successful load |
| `pipeline_run_log` | A row for every run: success, failure or skipped | `nb_bronze_load` writes to it |
| `dq_rules` | The 20 data-quality rules | Silver will use it for its checks |
| `dq_results` | The result of each data-quality check | `nb_bronze_load` writes reconciliation results; Silver will add more |

## Outside Fabric

- **Your PC, `C:\fabric-data`:** where the Python generator created the files you uploaded to `lh_source`.
- **GitHub, `karenkg01/fabric-retail-medallion`:** notebook code, lakehouse and pipeline definitions, `docs/DESIGN.md`, and the data generator. No data; the generator can always recreate it.
