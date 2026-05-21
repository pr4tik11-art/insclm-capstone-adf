# =============================================================
# NB_05_merge_claim_status
# Builds silver_claim_status_history using Delta MERGE
#
# Delta MERGE = upsert:
#   - If claim_id already exists → UPDATE with new status
#   - If claim_id is new → INSERT as new row
#
# This means re-running this notebook is safe — it won't
# create duplicates. It just keeps the table up to date.
# =============================================================

# ── Cell 1: Load config ──────────────────────────────────────
# %run ./NB_00_config_loader

# ── Cell 2: Imports ───────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

print("=" * 55)
print("STARTING CLAIM STATUS MERGE")
print("=" * 55)

# ── Cell 3: Load bronze status updates ───────────────────────
print("\n📥 Loading bronze_claim_status_updates...")
bronze_status = spark.table("bronze_insclm.bronze_claim_status_updates")
print(f"   Rows: {bronze_status.count():,}  (expected: 1,600)")

# ── Cell 4: Prepare source dataframe ─────────────────────────
print("\n📥 Preparing source dataframe...")

source = (bronze_status
    .withColumn("is_terminal_status",
        F.col("new_status").isin("Approved", "Rejected"))
    .withColumn("_silver_loaded_at", F.current_timestamp()))

print("is_terminal_status distribution:")
source.groupBy("is_terminal_status", "new_status") \
    .count().orderBy("is_terminal_status", "new_status").show()

# ── Cell 5: MERGE into silver_claim_status_history ───────────
TABLE_NAME = "silver_insclm.silver_claim_status_history"

if not spark.catalog.tableExists(TABLE_NAME):
    # First time — just write directly
    print(f"\n📥 {TABLE_NAME} does not exist.")
    print("   Performing initial load...")

    source.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABLE_NAME)

    count = spark.table(TABLE_NAME).count()
    print(f"✅ Initial load complete → {count:,} rows")

else:
    # Subsequent runs — use MERGE
    print(f"\n📥 {TABLE_NAME} exists.")
    print("   Performing Delta MERGE (upsert)...")

    before_count = spark.table(TABLE_NAME).count()
    print(f"   Rows before MERGE: {before_count:,}")

    target = DeltaTable.forName(spark, TABLE_NAME)

    (target.alias("t")
        .merge(
            source.alias("s"),
            "t.status_update_id = s.status_update_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

    after_count = spark.table(TABLE_NAME).count()
    print(f"   Rows after MERGE : {after_count:,}")
    print(f"   Net new rows     : {after_count - before_count:,}")

# ── Cell 6: Verify MERGE results ─────────────────────────────
print("\n" + "=" * 55)
print("CLAIM STATUS HISTORY VERIFICATION")
print("=" * 55)

status_hist = spark.table(TABLE_NAME)
total = status_hist.count()
print(f"Total rows: {total:,}  (expected: 1,600)")

print("\nnew_status distribution:")
status_hist.groupBy("new_status") \
    .count().orderBy(F.desc("count")).show()

print("old_status distribution:")
status_hist.groupBy("old_status") \
    .count().orderBy(F.desc("count")).show()

print("is_terminal_status breakdown:")
status_hist.groupBy("is_terminal_status") \
    .count().show()

# ── Cell 7: Show MERGE history from Delta ────────────────────
print("Delta MERGE history:")
DeltaTable.forName(spark, TABLE_NAME) \
    .history(5) \
    .select("version", "timestamp", "operation", "operationMetrics") \
    .show(truncate=False)

print("=" * 55)
print("✅ NB_05 complete.")
print("   Next: Run NB_06_gold_tables")
