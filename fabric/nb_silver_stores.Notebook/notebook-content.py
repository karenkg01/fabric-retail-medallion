# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "12385485-b82d-47a3-bbf6-67888e920fe7",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "7917d9a7-4bd6-4deb-94d5-6fa39f91701d",
# META       "known_lakehouses": [
# META         {
# META           "id": "ad271630-afe2-466d-a86e-b388e9c05f3b"
# META         },
# META         {
# META           "id": "12385485-b82d-47a3-bbf6-67888e920fe7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_silver_stores
# Cleans `lh_bronze.store_operations.stores` into `lh_silver.stores.stores` (SCD Type 1).
# 
# 1. Keeps the latest delivery of each store (full snapshots arrive when a store opens)
# 2. Standardizes `state` to 2-letter codes and `region` to one spelling per region
# 3. Converts `open_date` to a date and `square_feet` to an integer; `zip_code` stays text (leading zeros)
# 4. Rows whose state can't be mapped go to `quality.quarantine` under rule DQ031
# 5. MERGE on `store_id`, updating only rows whose `_hash_diff` changed, so reruns change nothing

# CELL ********************

# Step 1: profile the Bronze data (exploration only: counts, spellings, versions per store)
from pyspark.sql import functions as F

BRONZE = "lh_bronze.store_operations"
df = spark.table(f"{BRONZE}.stores")

# 1. How many rows vs how many distinct stores?
print("rows:", df.count(), "| distinct stores:", df.select("store_id").distinct().count())

# 2. Every spelling of state and region that exists
display(df.groupBy("state").count().orderBy("state"))
display(df.groupBy("region").count().orderBy("region"))

# 3. How many versions of each store, and from which dates
display(df.groupBy("store_id")
          .agg(F.count("*").alias("versions"),
               F.collect_set("_business_date").alias("delivered_on"))
          .orderBy("store_id"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Window

# --- Lookup tables: every messy spelling -> one standard value ---
STATE_NAMES = {"ARIZONA": "AZ", "CALIFORNIA": "CA", "COLORADO": "CO", "FLORIDA": "FL",
               "GEORGIA": "GA", "ILLINOIS": "IL", "INDIANA": "IN", "MASSACHUSETTS": "MA",
               "MINNESOTA": "MN", "NORTH CAROLINA": "NC", "NEW YORK": "NY", "OHIO": "OH",
               "OREGON": "OR", "PENNSYLVANIA": "PA", "TENNESSEE": "TN", "TEXAS": "TX",
               "WASHINGTON": "WA"}
REGIONS = {"southeast": "Southeast", "southcentral": "South Central", "midwest": "Midwest",
           "northeast": "Northeast", "west": "West", "national": "National"}

state_map  = F.create_map([F.lit(x) for kv in STATE_NAMES.items() for x in kv])
region_map = F.create_map([F.lit(x) for kv in REGIONS.items() for x in kv])

# 1. Keep only the latest version of each store
w = Window.partitionBy("store_id").orderBy(F.col("_business_date").desc(), F.col("_ingest_ts").desc())
latest = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# 2. Standardize, convert types, add business flags
state_up = F.upper(F.trim("state"))
clean = latest.select(
    F.trim("store_id").alias("store_id"),
    F.trim("store_name").alias("store_name"),
    F.trim("store_type").alias("store_type"),
    F.trim("address_line1").alias("address_line1"),
    F.trim("city").alias("city"),
    F.when(F.length(state_up) == 2, state_up).otherwise(state_map[state_up]).alias("state"),
    F.trim("zip_code").alias("zip_code"),                                   # stays text: leading zeros
    region_map[F.regexp_replace(F.lower("region"), "[^a-z]", "")].alias("region"),
    F.to_date("open_date").alias("open_date"),
    F.col("square_feet").cast("double").cast("int").alias("square_feet"),
    F.trim("manager_name").alias("manager_name"),
    (F.lower(F.trim("store_type")) == "online").alias("is_online"),
    F.col("_business_date").alias("_source_business_date"),
    "_source_file",
    "_batch_id",
)

print("stores after cleaning:", clean.count())
display(clean.orderBy("store_id"))

# 3. Anything the lookups couldn't fix? These would be quarantined.
display(clean.filter(F.col("state").isNull() | F.col("region").isNull()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

SILVER_TABLE = "stores.stores"          # lh_silver is the default lakehouse
QUARANTINE   = "quality.quarantine"

spark.sql("CREATE SCHEMA IF NOT EXISTS stores")
spark.sql("CREATE SCHEMA IF NOT EXISTS quality")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {QUARANTINE} (
        quarantine_id STRING, source_table STRING, rule_id STRING,
        record STRING, _batch_id STRING, quarantined_at TIMESTAMP
    ) USING DELTA
""")

# 1. Split good rows from bad (rule DQ031: state must map to a valid 2-letter code)
bad_ids = clean.filter(F.col("state").isNull()).select("store_id")
good    = clean.filter(F.col("state").isNotNull())

# 2. Bad rows -> quarantine, keeping the ORIGINAL raw row so we can see what was wrong
bad_raw = latest.withColumn("store_id", F.trim("store_id")).join(bad_ids, "store_id")
(bad_raw.select(
     F.expr("uuid()").alias("quarantine_id"),
     F.lit("store_operations.stores").alias("source_table"),
     F.lit("DQ031").alias("rule_id"),
     F.to_json(F.struct(*bad_raw.columns)).alias("record"),
     F.col("_batch_id"),
     F.current_timestamp().alias("quarantined_at"))
 .write.mode("append").saveAsTable(QUARANTINE))

# 3. A fingerprint of the business columns, to detect real changes
business_cols = [c for c in good.columns if not c.startswith("_")]
good = (good
        .withColumn("_hash_diff", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>"))
                                                            for c in business_cols]), 256))
        .withColumn("_silver_updated_at", F.current_timestamp()))

# 4. MERGE: update stores that changed, insert new stores, leave the rest alone
if not spark.catalog.tableExists(SILVER_TABLE):
    good.write.format("delta").saveAsTable(SILVER_TABLE)
    print(f"created {SILVER_TABLE} with {good.count()} stores")
else:
    (DeltaTable.forName(spark, SILVER_TABLE).alias("t")
        .merge(good.alias("s"), "t.store_id = s.store_id")
        .whenMatchedUpdateAll(condition="t._hash_diff <> s._hash_diff")
        .whenNotMatchedInsertAll()
        .execute())
    print("merge complete")

print("quarantined:", bad_ids.count())
display(spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE}")
        .select("version", "timestamp", "operation", "operationMetrics").limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
