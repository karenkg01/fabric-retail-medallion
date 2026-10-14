# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ad271630-afe2-466d-a86e-b388e9c05f3b",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "7917d9a7-4bd6-4deb-94d5-6fa39f91701d",
# META       "known_lakehouses": [
# META         {
# META           "id": "a819bf62-ab49-42b2-98e9-2bf2c5dd8369"
# META         },
# META         {
# META           "id": "ad271630-afe2-466d-a86e-b388e9c05f3b"
# META         },
# META         {
# META           "id": "6c8efc6f-8d4c-476a-8cb3-6240bb939710"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_bronze_load
# Loads raw files from `lh_source/Files/landing` into Bronze Delta tables in `lh_bronze`, driven by `lh_framework.meta.ingestion_config`.
# 
# **For each active source, in `load_order`:**
# 1. Reads the watermark and the manifests, and keeps only files dated after the watermark
# 2. Reads the files with every value as text, and adds `_source_file`, `_business_date`, `_ingest_ts`, `_batch_id`, `_row_hash`
# 3. Reconciles each file's row count against its manifest (DQ001, Critical). On mismatch: log, stop, watermark unchanged
# 4. Writes to `<target_schema>.<target_table>` with `replaceWhere` on `_business_date`, so reruns never duplicate
# 5. Advances the watermark and logs to `pipeline_run_log` and `dq_results`
# 
# **Parameters:** `source_name` (`"all"` or one source), `run_id`, `pipeline_name`, all set by the calling pipeline.
# Safe to rerun at any time: with nothing new, every source logs `Skipped`.

# PARAMETERS CELL ********************

# Parameters: a pipeline can override these when it calls the notebook
source_name   = "all"      # one source (e.g. "orders") or "all" for every active source
run_id        = "manual"   # the pipeline passes its run ID; "manual" means run by hand
pipeline_name = "manual"   # which pipeline called this notebook

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Setup: imports, file locations, and a unique run ID for manual runs
from pyspark.sql import functions as F
from datetime import datetime, timezone

FRAMEWORK = "lh_framework.meta"   # where the config, watermark and logs live

# Build the path to lh_source's landing folder from its IDs (works even with spaces in the workspace name)
src = notebookutils.lakehouse.get("lh_source")
LANDING = f"abfss://{src.workspaceId}@onelake.dfs.fabric.microsoft.com/{src.id}/Files/landing"

# Manual runs get a timestamped ID so each one can be told apart in the logs
if run_id == "manual":
    run_id = f"manual-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"

print(f"Run ID:  {run_id}")
print(f"Landing: {LANDING}")

# Quick check: can we see the landing folders and the config?
for f in notebookutils.fs.ls(LANDING):
    print("  found folder:", f.name)
display(spark.table(f"{FRAMEWORK}.ingestion_config").select("source_name", "target_schema", "target_table", "load_order").orderBy("load_order"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 4: read every manifest once into a single inventory of delivered files
manifests = (
    spark.read.option("multiLine", "true")
        .json(f"{LANDING}/_manifests/*/*/*/*.json")            # year/month/day/manifest_YYYYMMDD.json
        .select(F.to_date("business_date").alias("business_date"),
                F.explode("files").alias("f"))                   # one row per file listed in each manifest
        .select("business_date",
                F.col("f.source").alias("source_name"),
                F.col("f.path").alias("relative_path"),
                F.col("f.row_count").cast("long").alias("expected_rows"))
)
manifests.cache()   # keep it in memory: we'll reuse it for every source

print(f"Files listed across all manifests: {manifests.count():,}")
display(manifests.groupBy("source_name")
        .agg(F.count("*").alias("files"),
             F.sum("expected_rows").alias("expected_rows"),
             F.min("business_date").alias("first_day"),
             F.max("business_date").alias("last_day"))
        .orderBy("source_name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 5: compare the delivery inventory with each source's watermark to find files still waiting
watermarks = spark.table(f"{FRAMEWORK}.watermark").select("source_name", "last_business_date")

pending = (manifests
           .join(watermarks, "source_name")                                   # attach each source's bookmark
           .where(F.col("business_date") > F.col("last_business_date")))      # keep only days after it

display(pending.groupBy("source_name")
        .agg(F.count("*").alias("files_to_load"),
             F.sum("expected_rows").alias("rows_to_load"),
             F.min("business_date").alias("from_day"),
             F.max("business_date").alias("to_day"))
        .orderBy("source_name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 6a: a reusable function that reads a source's files as raw text and stamps the metadata columns
import json

def read_raw(cfg, relative_paths):
    """Read the given landing files for one source and add Bronze metadata columns."""
    paths = [f"{LANDING}/{p}" for p in relative_paths]
    opts  = json.loads(cfg["read_options"] or "{}")          # e.g. {"header": "true"} from the config

    if cfg["file_format"] == "csv":
        df = (spark.read.options(**opts)
                .option("inferSchema", "false")                # keep every value as text
                .csv(paths)
                .withColumn("_source_file", F.col("_metadata.file_path")))
    else:
        # JSON: the catalog extract wraps the records in a "products" list, so unpack it
        df = (spark.read.options(**opts)
                .option("primitivesAsString", "true")          # numbers and true/false stay as text
                .json(paths)
                .select("extract_type",
                        F.explode("products").alias("rec"),
                        F.col("_metadata.file_path").alias("_source_file"))
                .select("extract_type", "rec.*", "_source_file"))

    data_cols = [c for c in df.columns if c != "_source_file"]
    return (df
        .withColumn("_business_date",
                    F.to_date(F.regexp_extract("_source_file", r"/(\d{4}/\d{2}/\d{2})/", 1), "yyyy/MM/dd"))        
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_batch_id",  F.lit(run_id))
        .withColumn("_row_hash",  F.sha2(F.concat_ws("||",
                    *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in data_cols]), 256)))

print("read_raw is ready")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 7a: helpers that write to the run log and the data-quality results
def log_run(source, status, business_date, rows_read, rows_written, start_ts, error=None):
    """Append one row to pipeline_run_log for this source."""
    end_ts = datetime.now(timezone.utc)
    row = [(run_id, pipeline_name, "nb_bronze_load", "bronze", source, business_date, status,
            rows_read, rows_written, 0, start_ts, end_ts, (end_ts - start_ts).total_seconds(),
            error, end_ts)]
    (spark.createDataFrame(row, spark.table(f"{FRAMEWORK}.pipeline_run_log").schema)
          .write.mode("append").saveAsTable(f"{FRAMEWORK}.pipeline_run_log"))

def log_dq(table_name, business_date, rows_checked, rows_failed):
    """Append the DQ001 reconciliation result (rows loaded vs. manifest) to dq_results."""
    passed = rows_failed == 0
    row = [(run_id, "DQ001", table_name, business_date, rows_checked, rows_failed,
            (rows_failed / rows_checked) if rows_checked else 0.0, "Critical",
            "none" if passed else "fail_run", "Passed" if passed else "Failed",
            datetime.now(timezone.utc))]
    (spark.createDataFrame(row, spark.table(f"{FRAMEWORK}.dq_results").schema)
          .write.mode("append").saveAsTable(f"{FRAMEWORK}.dq_results"))
    return passed

print("logging helpers are ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 7b: load one source end to end: find new files, read, reconcile, write to Bronze, move watermark, log
def load_source(source):
    start_ts = datetime.now(timezone.utc)
    cfg = config[source]
    target = f"{cfg['target_schema']}.{cfg['target_table']}"     # e.g. sales_system.orders in lh_bronze

    # 1. Fresh watermark, then the files delivered after it
    wm = (spark.table(f"{FRAMEWORK}.watermark")
               .where(F.col("source_name") == source).first()["last_business_date"])
    files = (manifests.where((F.col("source_name") == source) & (F.col("business_date") > F.lit(wm)))
                      .select("business_date", "relative_path", "expected_rows").collect())
    if not files:
        print(f"{source}: nothing new after {wm}")
        log_run(source, "Skipped", None, 0, 0, start_ts)
        return

    d_from = min(f["business_date"] for f in files)
    d_to   = max(f["business_date"] for f in files)
    try:
        # 2. Read the raw files and stamp the metadata
        df = read_raw(cfg, [f["relative_path"] for f in files])

        # 3. Reconcile every file against its manifest row count (rule DQ001)
        actual = {r["file_name"]: r["count"] for r in
                  df.groupBy(F.element_at(F.split("_source_file", "/"), -1).alias("file_name")).count().collect()}
        expected = {f["relative_path"].split("/")[-1]: f["expected_rows"] for f in files}
        rows_expected = sum(expected.values())
        rows_off = sum(abs(n - actual.get(name, 0)) for name, n in expected.items())
        if not log_dq(target, d_to, rows_expected, rows_off):
            bad = [name for name, n in expected.items() if actual.get(name, 0) != n]
            raise ValueError(f"DQ001 failed: {len(bad)} file(s) don't match their manifest, e.g. {bad[:3]}")

        # 4. Write to Bronze; replaceWhere makes reruns of the same days safe (no duplicates)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg['target_schema']}")
        writer = df.write.format("delta").option("mergeSchema", "true")
        if spark.catalog.tableExists(target):
            (writer.mode("overwrite")
                   .option("replaceWhere", f"_business_date BETWEEN '{d_from}' AND '{d_to}'")
                   .saveAsTable(target))
        else:
            writer.mode("overwrite").saveAsTable(target)

        # 5. Move the watermark forward, only after the write succeeded
        last_file = max(files, key=lambda f: f["business_date"])["relative_path"]
        spark.sql(f"""
            UPDATE {FRAMEWORK}.watermark
            SET last_business_date = DATE'{d_to}', last_file_path = '{last_file}',
                last_rows_loaded = {rows_expected}, last_run_id = '{run_id}', updated_at = current_timestamp()
            WHERE source_name = '{source}'
        """)

        # 6. Log success
        log_run(source, "Succeeded", d_to, rows_expected, rows_expected, start_ts)
        print(f"{source}: loaded {len(files)} files, {rows_expected:,} rows ({d_from} to {d_to}) into {target}")

    except Exception as e:
        log_run(source, "Failed", d_to, 0, 0, start_ts, str(e)[:4000])   # record the failure...
        raise                                                           # ...then stop with the error

print("load_source is ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 9: load every active source in load_order (or only the one passed in the source_name parameter)
config = {r["source_name"]: r.asDict() for r in spark.table(f"{FRAMEWORK}.ingestion_config").collect()}
sources = [r["source_name"] for r in
           spark.table(f"{FRAMEWORK}.ingestion_config").where("is_active").orderBy("load_order").collect()]
if source_name != "all":
    sources = [s for s in sources if s == source_name]

failed = []
for s in sources:
    try:
        load_source(s)
    except Exception as e:
        print(f"{s}: FAILED - {e}")
        failed.append(s)      # keep going with the other sources

if failed:
    raise RuntimeError(f"Bronze load failed for: {failed}")   # fail the notebook so a pipeline sees it
print("Bronze load complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 10: verify the Bronze load
# 1. Rows in each Bronze table vs. the manifests
counts = [(s, f"{c['target_schema']}.{c['target_table']}",
           spark.table(f"{c['target_schema']}.{c['target_table']}").count()) for s, c in config.items()]
counts_df   = spark.createDataFrame(counts, "source_name string, bronze_table string, bronze_rows long")
expected_df = manifests.groupBy("source_name").agg(F.sum("expected_rows").alias("manifest_rows"))
display(counts_df.join(expected_df, "source_name")
                 .withColumn("match", F.col("bronze_rows") == F.col("manifest_rows"))
                 .orderBy("source_name"))

# 2. Watermarks after the load
display(spark.table(f"{FRAMEWORK}.watermark").orderBy("source_name"))

# 3. The most recent log entries (all runs)
display(spark.table(f"{FRAMEWORK}.pipeline_run_log").orderBy(F.desc("logged_at")).limit(20))
display(spark.table(f"{FRAMEWORK}.dq_results").orderBy(F.desc("checked_at")).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
