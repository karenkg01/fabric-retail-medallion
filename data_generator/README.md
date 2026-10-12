# Sample data generator

`generate_retail_data.py` simulates the source systems of **Harborline Retail** (24 stores + e-commerce) and writes daily extract files the way a real vendor feed would. The data contains realistic problems (duplicates, late-arriving orders, bad dates, orphan keys, messy text) so the Silver layer has real work to do. See [`docs/DESIGN.md`](../docs/DESIGN.md#known-source-data-problems-by-design) for the full list.

## Requirements
Python 3.10+ with `numpy` and `pandas` (`pip install -r requirements.txt`).

## Usage

```bash
# 2-year backfill: 2024-10-01 to 2026-09-30 (~1 minute, ~200 MB, ~4,400 files)
python generate_retail_data.py history

# Smaller, faster test set (a quarter of the volume)
python generate_retail_data.py history --scale 0.25 --overwrite

# Simulate the next day(s) of new files for incremental loads
python generate_retail_data.py daily --days 1
```

| Option | Default | Meaning |
|---|---|---|
| `--start` / `--end` | `2024-10-01` / `2026-09-30` | History date range |
| `--scale` | `1.0` | Multiplies order volume and customer base |
| `--seed` | `42` | Random seed; same seed = same data |
| `--inventory-days` | `120` | Inventory snapshots are written for the last N days of history only |
| `--out` | `output` | Output folder |

## Output

```
output/
├── landing/
│   ├── orders/YYYY/MM/DD/orders_YYYYMMDD.csv
│   ├── order_lines/…            customers/…        products/… (.json)
│   ├── stores/…                 inventory/…        returns/…
│   └── _manifests/YYYY/MM/DD/manifest_YYYYMMDD.json   ← file list + row counts
└── _state/generator_state.pkl   ← lets `daily` continue where `history` stopped
```

| Volume (default) | Rows |
|---|---|
| orders | ~474K |
| order_lines | ~1.14M |
| customers (incl. change records) | ~86K |
| returns | ~73K |
| inventory | ~1.5M |

Generation is deterministic per day: running `history` once produces exactly the same files as running `history` for the first day and `daily` for the rest.

## Loading it into Fabric
Upload the `landing` folder to `lh_source` → `Files/landing` (OneLake file explorer or **Upload folder** in the lakehouse). The script only needs numpy and pandas, so it can also run in a Fabric notebook with `--out /lakehouse/default/Files` to produce new daily files on a schedule.

`output/` is git-ignored; the data is always regenerated from the script.
