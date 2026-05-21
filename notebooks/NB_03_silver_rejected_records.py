# =============================================================
# NB_03_silver_rejected_records
# Applies rejection logic to status updates, policy, customer
# Expected: 0 rejections for all 3 (data is verified clean)
# But we still create the tables for completeness + audit
# =============================================================

# ── Cell 1: Load config ──────────────────────────────────────
# %run ./NB_00_config_loader

# ── Cell 2: Imports ───────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

print("=" * 55)
print("STARTING REJECTED RECORDS — OTHER SOURCES")
print("=" * 55)

# ── Cell 3: Rejected status updates ──────────────────────────
print("\n📥 Checking bronze_claim_status_updates...")
bronze_status = spark.table("bronze_insclm.bronze_claim_status_updates")
print(f"   Total rows: {bronze_status.count():,}")

rejected_status = (bronze_status
    .filter(
        F.col("status_update_id").isNull() |
        F.col("claim_id").isNull()         |
        F.col("new_status").isNull()       |
        F.col("old_status").isNull()       |
        F.col("status_date").isNull()
    )
    .withColumn("rejection_reason",
        F.when(F.col("status_update_id").isNull(),
                                    "MISSING_STATUS_UPDATE_ID")
        .when(F.col("claim_id").isNull(),
                                    "MISSING_CLAIM_ID")
        .when(F.col("new_status").isNull(),
                                    "MISSING_NEW_STATUS")
        .when(F.col("old_status").isNull(),
                                    "MISSING_OLD_STATUS")
        .when(F.col("status_date").isNull(),
                                    "MISSING_STATUS_DATE")
        .otherwise("UNKNOWN"))
    .withColumn("rejected_at", F.current_timestamp()))

rejected_status.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("rejected_insclm.rejected_status_updates")

rej_status_count = rejected_status.count()
print(f"✅ rejected_status_updates → {rej_status_count:,} rows  (expected: 0)")

if rej_status_count > 0:
    print("⚠️ Unexpected rejections found:")
    rejected_status.groupBy("rejection_reason").count().show()

# ── Cell 4: Rejected policies ─────────────────────────────────
print("\n📥 Checking bronze_policy_master...")
bronze_policy = spark.table("bronze_insclm.bronze_policy_master")
print(f"   Total rows: {bronze_policy.count():,}")

rejected_policy = (bronze_policy
    .filter(
        F.col("policy_id").isNull()       |
        F.col("customer_id").isNull()     |
        F.col("coverage_amount").isNull() |
        F.col("premium_amount").isNull()  |
        (F.col("coverage_amount") <= 0)   |
        (F.col("premium_amount") <= 0)    |
        F.col("policy_status").isNull()   |
        F.col("start_date").isNull()      |
        F.col("end_date").isNull()
    )
    .withColumn("rejection_reason",
        F.when(F.col("policy_id").isNull(),
                                    "MISSING_POLICY_ID")
        .when(F.col("customer_id").isNull(),
                                    "MISSING_CUSTOMER_ID")
        .when(F.col("coverage_amount").isNull() |
              (F.col("coverage_amount") <= 0),
                                    "INVALID_COVERAGE_AMOUNT")
        .when(F.col("premium_amount").isNull() |
              (F.col("premium_amount") <= 0),
                                    "INVALID_PREMIUM_AMOUNT")
        .when(F.col("policy_status").isNull(),
                                    "MISSING_POLICY_STATUS")
        .when(F.col("start_date").isNull(),
                                    "MISSING_START_DATE")
        .when(F.col("end_date").isNull(),
                                    "MISSING_END_DATE")
        .otherwise("UNKNOWN"))
    .withColumn("rejected_at", F.current_timestamp()))

rejected_policy.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("rejected_insclm.rejected_policy")

rej_policy_count = rejected_policy.count()
print(f"✅ rejected_policy → {rej_policy_count:,} rows  (expected: 0)")

if rej_policy_count > 0:
    print("⚠️ Unexpected rejections found:")
    rejected_policy.groupBy("rejection_reason").count().show()

# ── Cell 5: Rejected customers ────────────────────────────────
print("\n📥 Checking bronze_customer_master...")
bronze_customer = spark.table("bronze_insclm.bronze_customer_master")
print(f"   Total rows: {bronze_customer.count():,}")

rejected_customer = (bronze_customer
    .filter(
        F.col("customer_id").isNull()   |
        F.col("customer_name").isNull() |
        F.col("email").isNull()         |
        F.col("phone").isNull()         |
        F.col("risk_category").isNull() |
        F.col("dob").isNull()
    )
    .withColumn("rejection_reason",
        F.when(F.col("customer_id").isNull(),
                                    "MISSING_CUSTOMER_ID")
        .when(F.col("customer_name").isNull(),
                                    "MISSING_CUSTOMER_NAME")
        .when(F.col("email").isNull(),
                                    "MISSING_EMAIL")
        .when(F.col("phone").isNull(),
                                    "MISSING_PHONE")
        .when(F.col("risk_category").isNull(),
                                    "MISSING_RISK_CATEGORY")
        .when(F.col("dob").isNull(),
                                    "MISSING_DOB")
        .otherwise("UNKNOWN"))
    .withColumn("rejected_at", F.current_timestamp()))

rejected_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("rejected_insclm.rejected_customers")

rej_cust_count = rejected_customer.count()
print(f"✅ rejected_customers → {rej_cust_count:,} rows  (expected: 0)")

if rej_cust_count > 0:
    print("⚠️ Unexpected rejections found:")
    rejected_customer.groupBy("rejection_reason").count().show()

# ── Cell 6: Final summary ─────────────────────────────────────
print("\n" + "=" * 55)
print("REJECTED RECORDS SUMMARY — ALL SOURCES")
print("=" * 55)
print(f"rejected_claims         : already in NB_02 (~120 rows)")
print(f"rejected_status_updates : {rej_status_count:,}  (expected: 0)")
print(f"rejected_policy         : {rej_policy_count:,}  (expected: 0)")
print(f"rejected_customers      : {rej_cust_count:,}  (expected: 0)")
print("=" * 55)

all_clean = (rej_status_count == 0 and
             rej_policy_count == 0 and
             rej_cust_count   == 0)

if all_clean:
    print("✅ All clean — data quality confirmed.")
else:
    print("⚠️ Unexpected rejections found — investigate above.")

print("\n✅ NB_03 complete.")
print("   Next: Run NB_04_scd_policy_dim (Sreya's notebook)")
