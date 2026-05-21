# =============================================================
# NB_01_bronze_ingestion
# Reads raw CSVs from ADLS raw zone
# Writes 4 Bronze Delta tables with ingestion metadata
# Expected output:
#   bronze_claims               → 2,200 rows
#   bronze_claim_status_updates → 1,600 rows
#   bronze_policy_master        → 1,500 rows
#   bronze_customer_master      → 1,000 rows
# =============================================================

# ── Cell 1: Load config ──────────────────────────────────────
# %run ./NB_00_config_loader

# ── Cell 2: Imports + run ID ─────────────────────────────────
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from pyspark.sql.types import (StructType, StructField, StringType,
                                DateType, DecimalType, TimestampType)
from datetime import datetime

dbutils.widgets.text("runId", "manual_run")
RUN_ID = dbutils.widgets.get("runId")

print(f"Run ID: {RUN_ID}")
print(f"Start time: {datetime.now()}")

# ── Cell 3: Create Bronze database ───────────────────────────
spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze_insclm LOCATION '{BRONZE_PATH}'")
print("✅ bronze_insclm database ready")

# ── Cell 4: Reusable ingest function ─────────────────────────
def ingest_to_bronze(source_path, target_table, schema):
    print(f"\n📥 Reading from : {source_path}")
    df = (spark.read
            .option("header", "true")
            .option("dateFormat", "yyyy-MM-dd")
            .schema(schema)
            .csv(source_path)
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file",         input_file_name())
            .withColumn("_pipeline_run_id",      lit(RUN_ID)))

    row_count = df.count()
    print(f"   Rows read    : {row_count:,}")

    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table))

    saved_count = spark.table(target_table).count()
    print(f"✅ {target_table} → {saved_count:,} rows saved")
    return saved_count

# ── Cell 5: Define schemas ────────────────────────────────────
claims_schema = StructType([
    StructField("claim_id",        StringType(),      False),
    StructField("policy_id",       StringType(),      False),
    StructField("customer_id",     StringType(),      False),
    StructField("claim_date",      DateType(),        True),
    StructField("claim_amount",    DecimalType(18,2), True),
    StructField("claim_reason",    StringType(),      True),
    StructField("document_status", StringType(),      True),
    StructField("ingestion_date",  DateType(),        True),
])

status_schema = StructType([
    StructField("status_update_id", StringType(), False),
    StructField("claim_id",         StringType(), False),
    StructField("old_status",       StringType(), True),
    StructField("new_status",       StringType(), True),
    StructField("status_date",      DateType(),   True),
    StructField("remarks",          StringType(), True),
])

policy_schema = StructType([
    StructField("policy_id",       StringType(),      False),
    StructField("customer_id",     StringType(),      False),
    StructField("policy_type",     StringType(),      True),
    StructField("coverage_amount", DecimalType(18,2), True),
    StructField("premium_amount",  DecimalType(18,2), True),
    StructField("policy_status",   StringType(),      True),
    StructField("start_date",      DateType(),        True),
    StructField("end_date",        DateType(),        True),
    StructField("last_updated",    TimestampType(),   True),
])

customer_schema = StructType([
    StructField("customer_id",   StringType(),   False),
    StructField("customer_name", StringType(),   True),
    StructField("email",         StringType(),   True),
    StructField("phone",         StringType(),   True),
    StructField("city",          StringType(),   True),
    StructField("state",         StringType(),   True),
    StructField("dob",           DateType(),     True),
    StructField("risk_category", StringType(),   True),
    StructField("last_updated",  TimestampType(),True),
])

print("✅ Schemas defined")

# ── Cell 6: Ingest all 4 datasets ────────────────────────────
print("\n" + "=" * 55)
print("STARTING BRONZE INGESTION")
print("=" * 55)

start = datetime.now()

c1 = ingest_to_bronze(
    RAW_PATH + "/claims/*.csv",
    "bronze_insclm.bronze_claims",
    claims_schema
)

c2 = ingest_to_bronze(
    RAW_PATH + "/claim_status_updates/*.csv",
    "bronze_insclm.bronze_claim_status_updates",
    status_schema
)

c3 = ingest_to_bronze(
    RAW_PATH + "/policy_master/*.csv",
    "bronze_insclm.bronze_policy_master",
    policy_schema
)

c4 = ingest_to_bronze(
    RAW_PATH + "/customer_master/*.csv",
    "bronze_insclm.bronze_customer_master",
    customer_schema
)

end = datetime.now()

# ── Cell 7: Summary ───────────────────────────────────────────
print("\n" + "=" * 55)
print("BRONZE INGESTION SUMMARY")
print("=" * 55)
print(f"bronze_claims               : {c1:,}  (expected 2,200)")
print(f"bronze_claim_status_updates : {c2:,}  (expected 1,600)")
print(f"bronze_policy_master        : {c3:,}  (expected 1,500)")
print(f"bronze_customer_master      : {c4:,}  (expected 1,000)")
print(f"Total time                  : {end - start}")
print("=" * 55)

all_correct = (c1 == 2200 and c2 == 1600 and c3 == 1500 and c4 == 1000)
if all_correct:
    print("✅ All row counts match. Bronze layer complete.")
    print("   Next: Run NB_02_silver_transformation")
else:
    print("❌ Row count mismatch — check raw zone CSV files.")
