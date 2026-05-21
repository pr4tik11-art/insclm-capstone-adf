# =============================================================
# NB_04_scd_policy_dim
# Implements SCD Type 2 on the policy dimension
# Creates: silver_insclm.silver_policy_dim
#
# SCD Type 2 means: instead of overwriting old policy data,
# we keep ALL versions with effective/expiry dates so we can
# query what a policy looked like at ANY point in the past.
#
# New columns added:
#   policy_sk      → surrogate key (unique per version)
#   effective_date → when this version became active
#   expiry_date    → when this version was replaced
#                    (9999-12-31 = still active)
#   is_current     → True for the latest version only
#   record_hash    → MD5 of tracked columns (detects changes)
# =============================================================

# ── Cell 1: Load config ──────────────────────────────────────
# %run ./NB_00_config_loader

# ── Cell 2: Imports ───────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime

spark.sql(f"CREATE DATABASE IF NOT EXISTS silver_insclm LOCATION '{SILVER_PATH}'")
print("✅ silver_insclm database ready")

# ── Cell 3: Load bronze_policy_master ────────────────────────
print("\n📥 Loading bronze_policy_master...")
bronze_policy = spark.table("bronze_insclm.bronze_policy_master")
print(f"   Rows: {bronze_policy.count():,}  (expected: 1,500)")

# ── Cell 4: Define SCD tracked columns + compute hash ────────
# These are the columns that trigger a new SCD version
# when any of them change
SCD_COLS = ["policy_type", "coverage_amount", "premium_amount", "policy_status"]

print(f"\nSCD tracked columns: {SCD_COLS}")
print("Any change in these → new row in policy_dim")

hash_expr = F.md5(
    F.concat_ws("||", *[F.col(c).cast("string") for c in SCD_COLS])
)

source_df = (bronze_policy
    .withColumn("record_hash",    hash_expr)
    .withColumn("effective_date", F.col("last_updated"))
    .withColumn("expiry_date",
        F.lit("9999-12-31 00:00:00").cast(TimestampType()))
    .withColumn("is_current",     F.lit(True))
    .withColumn("created_at",     F.current_timestamp())
    .withColumn("updated_at",     F.current_timestamp()))

print("✅ Source dataframe prepared with hash and SCD columns")

# ── Cell 5: Initial load OR incremental MERGE ─────────────────
TABLE_NAME = "silver_insclm.silver_policy_dim"

if not spark.catalog.tableExists(TABLE_NAME):
    # ── FIRST TIME: Initial load ──────────────────────────────
    print(f"\n📥 {TABLE_NAME} does not exist.")
    print("   Performing initial SCD load...")

    initial_df = (source_df
        .withColumn("policy_sk",
            F.monotonically_increasing_id())
        .select(
            "policy_sk",
            "policy_id",
            "customer_id",
            "policy_type",
            "coverage_amount",
            "premium_amount",
            "policy_status",
            "start_date",
            "end_date",
            "effective_date",
            "expiry_date",
            "is_current",
            "record_hash",
            "created_at",
            "updated_at"
        ))

    initial_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABLE_NAME)

    count = spark.table(TABLE_NAME).count()
    print(f"✅ Initial SCD load complete → {count:,} rows")

else:
    # ── SUBSEQUENT RUNS: SCD Type 2 MERGE ────────────────────
    print(f"\n📥 {TABLE_NAME} already exists.")
    print("   Performing SCD Type 2 MERGE for changed records...")

    target = DeltaTable.forName(spark, TABLE_NAME)
    target_df = target.toDF()

    # Find policies where any tracked column has changed
    current_rows = target_df.filter(F.col("is_current") == True)

    changed_policies = (source_df.alias("s")
        .join(current_rows.alias("t"), "policy_id", "inner")
        .filter(F.col("s.record_hash") != F.col("t.record_hash"))
        .select(F.col("s.policy_id")))

    changed_count = changed_policies.count()
    print(f"   Changed policies found: {changed_count:,}")

    if changed_count > 0:
        # Step 1: Expire old rows for changed policies
        print("   Step 1: Expiring old rows...")
        (target.alias("t")
            .merge(
                changed_policies.alias("c"),
                "t.policy_id = c.policy_id AND t.is_current = true"
            )
            .whenMatchedUpdate(set={
                "is_current":   "false",
                "expiry_date":  "current_timestamp()",
                "updated_at":   "current_timestamp()"
            })
            .execute())
        print("   ✅ Old rows expired")

        # Step 2: Insert new versions for changed policies
        print("   Step 2: Inserting new versions...")
        max_sk = target_df.agg(F.max("policy_sk")).first()[0] or 0

        new_versions = (source_df
            .join(changed_policies, "policy_id", "inner")
            .withColumn("policy_sk",
                F.monotonically_increasing_id() + F.lit(max_sk + 1))
            .select(
                "policy_sk", "policy_id", "customer_id",
                "policy_type", "coverage_amount", "premium_amount",
                "policy_status", "start_date", "end_date",
                "effective_date", "expiry_date", "is_current",
                "record_hash", "created_at", "updated_at"
            ))

        new_versions.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(TABLE_NAME)

        print(f"   ✅ {new_versions.count():,} new version rows inserted")
    else:
        print("   No changes detected — policy_dim is up to date")

# ── Cell 6: Verify SCD results ───────────────────────────────
print("\n" + "=" * 55)
print("SCD TYPE 2 VERIFICATION")
print("=" * 55)

policy_dim = spark.table(TABLE_NAME)
total      = policy_dim.count()
current    = policy_dim.filter(F.col("is_current") == True).count()
historical = policy_dim.filter(F.col("is_current") == False).count()

print(f"Total rows in policy_dim : {total:,}")
print(f"Current rows (is_current=True)  : {current:,}  (expected: 1,500)")
print(f"Historical rows (is_current=False): {historical:,}")

# Validate: exactly 1 current row per policy_id
duplicates = (policy_dim
    .filter(F.col("is_current") == True)
    .groupBy("policy_id")
    .count()
    .filter(F.col("count") > 1)
    .count())

print(f"\nDuplicate current rows check: {duplicates}  (expected: 0)")

if duplicates == 0:
    print("✅ SCD integrity check passed — 1 current row per policy")
else:
    print("❌ SCD integrity issue — multiple current rows per policy!")

# Show sample — one policy with history if available
print("\nSample policy_dim rows:")
policy_dim.select(
    "policy_id", "policy_type", "coverage_amount",
    "policy_status", "effective_date", "expiry_date",
    "is_current", "policy_sk"
).orderBy("policy_id", "effective_date").show(5, truncate=False)

print("=" * 55)
print("✅ NB_04 complete.")
print("   Member 2 can now run NB_02 Cells 5-7 (time-aware join)")
print("   Next for you: Run NB_05_merge_claim_status")
