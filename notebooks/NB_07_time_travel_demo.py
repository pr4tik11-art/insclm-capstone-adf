# =============================================================
# NB_07_time_travel_demo
# Demonstrates Delta Lake Time Travel capabilities
#
# Time Travel lets you query data AS OF a past version
# or timestamp. Key business use case:
#   "What did the policy look like on the day the claim
#    was filed?" — not what it looks like today.
#
# Two methods:
#   1. VERSION AS OF  → query by Delta version number
#   2. TIMESTAMP AS OF → query by date/time
# =============================================================

# ── Cell 1: Load config ──────────────────────────────────────
# %run ./NB_00_config_loader

# ── Cell 2: Imports ───────────────────────────────────────────
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

print("=" * 55)
print("DELTA LAKE TIME TRAVEL DEMONSTRATION")
print("=" * 55)

# ── Cell 3: Show full Delta history of policy_dim ────────────
print("\n📋 Full history of silver_policy_dim:")
print("   Every write operation is recorded as a version.\n")

DeltaTable.forName(spark, "silver_insclm.silver_policy_dim") \
    .history() \
    .select(
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "operationMetrics"
    ).show(10, truncate=False)

# ── Cell 4: Time Travel by VERSION NUMBER ────────────────────
print("\n📋 Time Travel — by VERSION number")
print("   Reading version 0 = the very first load\n")

v0_df = (spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .table("silver_insclm.silver_policy_dim"))

v0_count = v0_df.count()
print(f"   Version 0 row count : {v0_count:,}")
print(f"   Current row count   : "
      f"{spark.table('silver_insclm.silver_policy_dim').count():,}")
print(f"   Difference          : "
      f"{spark.table('silver_insclm.silver_policy_dim').count() - v0_count:,} rows added since v0")

print("\nSample rows from Version 0:")
v0_df.select(
    "policy_id", "policy_type", "policy_status",
    "coverage_amount", "is_current"
).show(5, truncate=False)

# ── Cell 5: Time Travel by TIMESTAMP ─────────────────────────
print("\n📋 Time Travel — by TIMESTAMP")
print("   Reading data as it looked on 2026-01-01\n")

try:
    ts_df = (spark.read
        .format("delta")
        .option("timestampAsOf", "2026-01-01 00:00:00")
        .table("silver_insclm.silver_policy_dim"))

    print(f"   Rows as of 2026-01-01: {ts_df.count():,}")
    ts_df.select(
        "policy_id", "policy_status",
        "coverage_amount", "effective_date", "is_current"
    ).show(5, truncate=False)

except Exception as e:
    print(f"   Note: {e}")
    print("   This means the table was created after 2026-01-01.")
    print("   Time Travel works from the first write onwards.")

# ── Cell 6: Business use case ────────────────────────────────
# The key requirement: for each claim, what was the policy
# state ON THE DATE the claim was filed — not today's state
print("\n📋 Business Use Case:")
print("   Policy state at claim date (time-aware join)")
print("   This is already implemented in silver_claims_fact")
print("   via the effective_date/expiry_date join in NB_02.\n")

# Show evidence — pick a claim and show its policy state at claim time
print("Sample: claims with their policy_status_at_claim")
print("(This shows the policy status AS OF the claim date,")
print(" not the current status)\n")

spark.table("silver_insclm.silver_claims_fact") \
    .select(
        "claim_id",
        "policy_id",
        "claim_date",
        "policy_status_at_claim",
        "coverage_amount",
        "policy_type"
    ).orderBy("claim_date") \
    .show(10, truncate=False)

# ── Cell 7: Compare — current vs at claim date ───────────────
print("\n📋 Comparing current policy status vs status at claim date:")
print("   If they differ → policy changed after claim was filed\n")

current_policy = (spark.table("silver_insclm.silver_policy_dim")
    .filter(F.col("is_current") == True)
    .select(
        "policy_id",
        F.col("policy_status").alias("current_status"),
        F.col("coverage_amount").alias("current_coverage")
    ))

claims_snapshot = (spark.table("silver_insclm.silver_claims_fact")
    .select(
        "claim_id",
        "policy_id",
        "claim_date",
        "policy_status_at_claim",
        "coverage_amount"
    ).distinct())

comparison = (claims_snapshot
    .join(current_policy, "policy_id", "left")
    .withColumn("status_changed",
        F.col("policy_status_at_claim") != F.col("current_status"))
    .filter(F.col("status_changed") == True))

changed_count = comparison.count()
print(f"Claims where policy status changed after filing: {changed_count:,}")

if changed_count > 0:
    print("\nSample — policies that changed after claim was filed:")
    comparison.select(
        "claim_id", "policy_id", "claim_date",
        "policy_status_at_claim", "current_status"
    ).show(10, truncate=False)
else:
    print("No status changes detected in this dataset.")

# ── Cell 8: DESCRIBE HISTORY on claims_fact ──────────────────
print("\n📋 Delta history of silver_claims_fact:")
DeltaTable.forName(spark, "silver_insclm.silver_claims_fact") \
    .history() \
    .select("version", "timestamp", "operation", "operationMetrics") \
    .show(5, truncate=False)

print("\n" + "=" * 55)
print("✅ NB_07 complete.")
print("   Next: Run NB_08_describe_history_optimize")
print("=" * 55)
