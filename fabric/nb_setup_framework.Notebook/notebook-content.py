# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a819bf62-ab49-42b2-98e9-2bf2c5dd8369",
# META       "default_lakehouse_name": "lh_framework",
# META       "default_lakehouse_workspace_id": "7917d9a7-4bd6-4deb-94d5-6fa39f91701d",
# META       "known_lakehouses": [
# META         {
# META           "id": "a819bf62-ab49-42b2-98e9-2bf2c5dd8369"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_setup_framework
# Creates the framework tables in `lh_framework.meta` that drive and monitor the medallion pipeline.
# 
# | Table | Type | Purpose |
# |---|---|---|
# | `ingestion_config` | Setting (overwrite) | What to load, from where, to which Bronze table |
# | `watermark` | Setting (overwrite) | How far each source has been loaded (enables incremental loads) |
# | `pipeline_run_log` | History (create if not exists) | One row per pipeline activity run |
# | `dq_rules` | Setting (overwrite) | Data-quality rulebook |
# | `dq_results` | History (create if not exists) | Result of every rule on every run |
# 
# **Warning:** rerunning the watermark cell resets every source to 2024-09-30 and forces a full reload.

# CELL ********************

# Cell 1: create a schema to group all framework tables
spark.sql("CREATE SCHEMA IF NOT EXISTS meta")
print("Schema 'meta' is ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: ingestion_config, the control table that drives the metadata-driven pipeline
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import current_timestamp

config_schema = StructType([
    StructField("source_name",    StringType(),  False),  # folder name under Files/landing
    StructField("source_system",  StringType(),  False),  # which operational system it comes from
    StructField("file_format",    StringType(),  False),  # csv | json
    StructField("read_options",   StringType(),  True),   # Spark reader options as JSON text
    StructField("extract_type",   StringType(),  False),  # how the source delivers data
    StructField("target_schema",  StringType(),  False),  # Bronze schema in lh_bronze
    StructField("target_table",   StringType(),  False),  # Bronze table name
    StructField("primary_keys",   StringType(),  False),  # used by Silver for dedupe + MERGE
    StructField("load_order",     IntegerType(), False),  # order the pipeline processes sources
    StructField("is_active",      BooleanType(), False),  # switch a source off without deleting it
    StructField("description",    StringType(),  True),
])

csv_opts  = '{"header": "true"}'
json_opts = '{"multiLine": "true"}'

config_rows = [
    ("stores",      "store_operations", "csv",  csv_opts,  "full_snapshot",  "store_operations", "stores",      "store_id",                          10, True, "Store master list; full extract when stores change"),
    ("products",    "product_catalog",  "json", json_opts, "change_records", "product_catalog",  "products",    "product_id",                        20, True, "Product catalog API; full on day 1, then changed products only"),
    ("customers",   "customer_system",  "csv",  csv_opts,  "change_records", "customer_system",  "customers",   "customer_id",                       30, True, "Customer system (CRM); full on day 1, then new and changed customers"),
    ("orders",      "sales_system",     "csv",  csv_opts,  "incremental",    "sales_system",     "orders",      "order_id",                          40, True, "Sales system (POS + e-commerce) order headers"),
    ("order_lines", "sales_system",     "csv",  csv_opts,  "incremental",    "sales_system",     "order_lines", "order_line_id",                     50, True, "Order line items"),
    ("returns",     "store_operations", "csv",  csv_opts,  "incremental",    "store_operations", "returns",     "return_id",                         60, True, "Returns and refunds"),
    ("inventory",   "store_operations", "csv",  csv_opts,  "daily_snapshot", "store_operations", "inventory",   "snapshot_date,store_id,product_id", 70, True, "Daily on-hand stock per store and product"),
]

config_df = (spark.createDataFrame(config_rows, config_schema)
                  .withColumn("updated_at", current_timestamp()))

(config_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("meta.ingestion_config"))

display(spark.table("meta.ingestion_config").orderBy("load_order"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: watermark, tracks how far each source has been loaded (enables incremental loads)
from pyspark.sql.functions import lit, to_date, current_timestamp
from pyspark.sql.types import StringType, LongType

INITIAL_WATERMARK = "2024-09-30"   # the day before the data starts, so the first run loads everything

watermark_df = (spark.table("meta.ingestion_config")
    .select("source_name")
    .withColumn("last_business_date", to_date(lit(INITIAL_WATERMARK)))  # last day folder loaded
    .withColumn("last_file_path",     lit(None).cast(StringType()))     # last file loaded (for tracing)
    .withColumn("last_rows_loaded",   lit(None).cast(LongType()))       # rows loaded on the last run
    .withColumn("last_run_id",        lit(None).cast(StringType()))     # pipeline run that moved it
    .withColumn("updated_at",         current_timestamp()))

(watermark_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("meta.watermark"))

display(spark.table("meta.watermark").orderBy("source_name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Cell 4: pipeline_run_log, one row per activity run (the audit trail used for monitoring and alerts)
# MAGIC -- CREATE TABLE IF NOT EXISTS: history must never be overwritten, so reruns leave existing rows alone
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS meta.pipeline_run_log (
# MAGIC     run_id            STRING     COMMENT 'Pipeline run ID from Fabric; groups all activities of one run',
# MAGIC     pipeline_name     STRING     COMMENT 'e.g. pl_master_daily',
# MAGIC     activity_name     STRING     COMMENT 'e.g. nb_bronze_load',
# MAGIC     layer             STRING     COMMENT 'bronze | silver | gold | serving',
# MAGIC     source_name       STRING     COMMENT 'Which source this activity processed (NULL if not source-specific)',
# MAGIC     business_date     DATE       COMMENT 'Business day the data belongs to',
# MAGIC     status            STRING     COMMENT 'Succeeded | Failed | Skipped',
# MAGIC     rows_read         BIGINT     COMMENT 'Rows read from the input',
# MAGIC     rows_written      BIGINT     COMMENT 'Rows written to the target',
# MAGIC     rows_rejected     BIGINT     COMMENT 'Rows sent to quarantine',
# MAGIC     start_ts          TIMESTAMP  COMMENT 'Activity start (UTC)',
# MAGIC     end_ts            TIMESTAMP  COMMENT 'Activity end (UTC)',
# MAGIC     duration_seconds  DOUBLE     COMMENT 'end_ts - start_ts in seconds',
# MAGIC     error_message     STRING     COMMENT 'Error text when status = Failed',
# MAGIC     logged_at         TIMESTAMP  COMMENT 'When this row was written (UTC)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Audit log: one row per pipeline activity run';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE TABLE meta.pipeline_run_log

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: dq_rules, the rulebook of data-quality checks (a setting, so overwrite is safe)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
from pyspark.sql.functions import current_timestamp

rules_schema = StructType([
    StructField("rule_id",         StringType(),  False),  # e.g. DQ020
    StructField("layer",           StringType(),  False),  # bronze | silver | gold
    StructField("table_name",      StringType(),  False),  # table the rule checks (schema.table)
    StructField("rule_name",       StringType(),  False),  # plain-language description
    StructField("check_type",      StringType(),  False),  # how the rule is tested
    StructField("column_name",     StringType(),  True),   # column being checked
    StructField("expression",      StringType(),  True),   # SQL condition a GOOD row must satisfy
    StructField("reference_table", StringType(),  True),   # for referential checks: where the value must exist
    StructField("severity",        StringType(),  False),  # Critical | Error | Warning
    StructField("action",          StringType(),  False),  # fail_run | quarantine | flag
    StructField("threshold",       DoubleType(),  False),  # max failure rate allowed before the rule fails (0 = none)
    StructField("is_active",       BooleanType(), False),
])

rules = [
    # rule_id  layer     table_name               rule_name                                                check_type        column_name         expression                                                              reference_table           severity    action        threshold active
    ("DQ001", "bronze", "*",                     "Rows loaded match the manifest row count",              "reconciliation", None,               None,                                                                   None,                     "Critical", "fail_run",   0.0,  True),
    ("DQ002", "bronze", "*",                     "Expected file exists for the business date",            "completeness",   None,               None,                                                                   None,                     "Critical", "fail_run",   0.0,  True),
    ("DQ010", "silver", "sales.orders",          "Order timestamp is a valid date and time",              "row_condition",  "order_ts",         "order_ts IS NOT NULL",                                                 None,                     "Error",    "quarantine", 0.0,  True),
    ("DQ011", "silver", "sales.orders",          "Order ID is unique after deduplication",                "unique",         "order_id",         None,                                                                   None,                     "Critical", "fail_run",   0.0,  True),
    ("DQ012", "silver", "sales.orders",          "Customer exists (guest orders allowed)",                "referential",    "customer_id",      None,                                                                   "customers.customers",    "Warning",  "flag",       0.01, True),
    ("DQ013", "silver", "sales.orders",          "Store exists",                                          "referential",    "store_id",         None,                                                                   "stores.stores",          "Error",    "quarantine", 0.0,  True),
    ("DQ014", "silver", "sales.orders",          "Order status is an allowed value",                      "row_condition",  "order_status",     "order_status IN ('Completed', 'Cancelled', 'Pending')",                None,                     "Error",    "quarantine", 0.0,  True),
    ("DQ020", "silver", "sales.order_lines",     "Quantity is greater than zero",                         "row_condition",  "quantity",         "quantity > 0",                                                         None,                     "Error",    "quarantine", 0.0,  True),
    ("DQ021", "silver", "sales.order_lines",     "Product exists in the catalog",                         "referential",    "product_id",       None,                                                                   "products.products",      "Error",    "quarantine", 0.0,  True),
    ("DQ022", "silver", "sales.order_lines",     "Line amount = quantity x price - discount (1 cent)",    "row_condition",  "line_amount",      "abs(line_amount - (quantity * unit_price - discount_amount)) <= 0.01", None,                     "Warning",  "flag",       0.01, True),
    ("DQ023", "silver", "sales.order_lines",     "Every order line has a parent order",                   "referential",    "order_id",         None,                                                                   "sales.orders",           "Error",    "quarantine", 0.0,  True),
    ("DQ030", "silver", "customers.customers",   "Email looks valid when present",                        "row_condition",  "email",            "email IS NULL OR email LIKE '%_@_%._%'",                               None,                     "Warning",  "flag",       0.05, True),
    ("DQ031", "silver", "customers.customers",   "State is a 2-letter code",                              "row_condition",  "state",            "length(state) = 2",                                                    None,                     "Error",    "quarantine", 0.0,  True),
    ("DQ040", "silver", "products.products",     "Price is positive and above cost",                      "row_condition",  "list_price",       "list_price > 0 AND unit_cost < list_price",                            None,                     "Error",    "quarantine", 0.0,  True),
    ("DQ050", "silver", "stores.inventory",      "Stock on hand is not negative",                         "row_condition",  "quantity_on_hand", "quantity_on_hand >= 0",                                                None,                     "Error",    "quarantine", 0.0,  True),
    ("DQ051", "silver", "stores.inventory",      "Every open store has a snapshot for the day",           "completeness",   "store_id",         None,                                                                   "stores.stores",          "Warning",  "flag",       0.0,  True),
    ("DQ060", "silver", "sales.returns",         "Return points to a real order line",                    "referential",    "order_line_id",    None,                                                                   "sales.order_lines",      "Error",    "quarantine", 0.0,  True),
    ("DQ061", "silver", "sales.returns",         "Refund is not more than the original line amount",      "custom",         "refund_amount",    None,                                                                   "sales.order_lines",      "Warning",  "flag",       0.0,  True),
    ("DQ070", "gold",   "analytics.fact_sales",  "Daily sales within 3 std devs of the 28-day average",   "anomaly",        "net_amount",       None,                                                                   None,                     "Warning",  "flag",       0.0,  True),
    ("DQ071", "gold",   "analytics.*",           "No orphan keys in facts (all keys resolve or map to -1)","referential",   None,               None,                                                                   None,                     "Critical", "fail_run",   0.0,  True),
]

rules_df = spark.createDataFrame(rules, rules_schema).withColumn("updated_at", current_timestamp())

(rules_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("meta.dq_rules"))

display(spark.table("meta.dq_rules").orderBy("rule_id"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Cell 6: dq_results, the report card for every rule on every run (history: never overwrite)
# MAGIC CREATE TABLE IF NOT EXISTS meta.dq_results (
# MAGIC     run_id         STRING     COMMENT 'Pipeline run that executed the check',
# MAGIC     rule_id        STRING     COMMENT 'Rule from meta.dq_rules',
# MAGIC     table_name     STRING     COMMENT 'Table that was checked',
# MAGIC     business_date  DATE       COMMENT 'Business day of the data checked',
# MAGIC     rows_checked   BIGINT     COMMENT 'Rows evaluated',
# MAGIC     rows_failed    BIGINT     COMMENT 'Rows that broke the rule',
# MAGIC     failure_rate   DOUBLE     COMMENT 'rows_failed / rows_checked',
# MAGIC     severity       STRING     COMMENT 'Copied from the rule at check time',
# MAGIC     action_taken   STRING     COMMENT 'fail_run | quarantine | flag | none',
# MAGIC     status         STRING     COMMENT 'Passed | Failed',
# MAGIC     checked_at     TIMESTAMP  COMMENT 'When the check ran (UTC)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Data-quality results: one row per rule per run';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
