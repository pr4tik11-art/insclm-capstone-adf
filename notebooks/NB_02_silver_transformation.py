# =============================================================
# NB_02_silver_transformation
# Bronze → Silver layer
# Creates:
#   silver_insclm.silver_customer_dim   → cleaned customer data
#   silver_insclm.silver_claims_fact    → joined + flagged claims
#   rejected_insclm.rejected_claims     → ~120 bad records
#
# ⚠️ IMPORTANT: Run Cells 1–4 first independently.
#    Then wait for NB_04 (silver_policy_dim) to finish.
#    Only then run Cells 5–7.
# =============================================================

# ── Cell 1: Load config ──────────────────────────────────────
# %run ./NB_00_config_loader

# ── Cell 2: Imports + database setup ─────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

spark.sql(f"CREATE DATABASE IF NOT EXISTS silver_insclm   LOCATION '{SILVER_PATH}'")
spark.sql(f"CREATE DATABASE IF NOT EXISTS rejected_insclm LOCATION '{REJECTED_PATH}'")
print("✅ silver_insclm and rejected_insclm databases ready")

# ── Cell 3: Build silver_customer_dim ────────────────────────
# No dependencies — run this immediately after Bronze is done

print("\n📥 Building silver_customer_dim...")
bronze_customer = spark.table("bronze_insclm.bronze_customer_master")
print(f"   Bronze customer rows: {bronze_customer.count():,}")

silver_customer = (bronze_customer
    .withColumn("age_years",
        F.floor(F.months_between(F.current_date(), F.col("dob")) / 12))
    .withColumn("email_valid",
        F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"))
    .withColumn("phone_valid",
        F.length(F.col("phone")) == 10)
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .dropDuplicates(["customer_id"]))

silver_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_insclm.silver_customer_dim")

cust_count = spark.table("silver_insclm.silver_customer_dim").count()
print(f"✅ silver_customer_dim → {cust_count:,} rows  (expected 1,000)")

# ── Cell 4: Rejection logic on bronze_claims ─────────────────
# No dependencies — run this immediately after Bronze is done

print("\n📥 Applying rejection logic on bronze_claims...")
bronze_claims = spark.table("bronze_insclm.bronze_claims")
print(f"   Bronze claims rows: {bronze_claims.count():,}")

rejected_conditions = (
    F.col("claim_id").isNull()                          |
    F.col("policy_id").isNull()                         |
    F.col("customer_id").isNull()                       |
    F.col("claim_amount").isNull()                      |
    (F.col("claim_amount") <= 0)                        |
    F.col("document_status").isNull()                   |
    (F.col("document_status") == "Missing")             |
    (F.col("claim_date") > F.current_date())
)

rejected_claims = (bronze_claims
    .filter(rejected_conditions)
    .withColumn("rejection_reason",
        F.when(F.col("claim_id").isNull(),
                                        "MISSING_CLAIM_ID")
        .when(F.col("policy_id").isNull(),
                                        "ORPHAN_POLICY_ID")
        .when(F.col("customer_id").isNull(),
                                        "ORPHAN_CUSTOMER_ID")
        .when(F.col("claim_amount").isNull(),
                                        "MISSING_CLAIM_AMOUNT")
        .when(F.col("claim_amount") <= 0,
                                        "NEGATIVE_CLAIM_AMOUNT")
        .when(F.col("document_status") == "Missing",
                                        "MISSING_DOCUMENT_STATUS")
        .when(F.col("claim_date") > F.current_date(),
                                        "FUTURE_CLAIM_DATE")
        .otherwise("UNKNOWN"))
    .withColumn("rejected_at", F.current_timestamp()))

good_claims = bronze_claims.filter(~rejected_conditions)

rejected_claims.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("rejected_insclm.rejected_claims")

rej_count  = rejected_claims.count()
good_count = good_claims.count()

print(f"✅ rejected_claims → {rej_count:,}  (expected ~120)")
print(f"✅ good_claims     → {good_count:,}  (expected ~2,080)")
print("\nRejection breakdown:")
rejected_claims.groupBy("rejection_reason").count() \
    .orderBy(F.desc("count")).show(truncate=False)

print("\n" + "=" * 55)
print("⏳ STOP HERE.")
print("   Wait for NB_04_scd_policy_dim to finish.")
print("   Ask in team chat: silver_insclm.silver_policy_dim ready?")
print("   Verify: spark.table('silver_insclm.silver_policy_dim').count()")
print("   Expected: 1,500+ rows")
print("   Once confirmed → run Cell 5 onwards")
print("=" * 55)

# ── Cell 5: Time-aware join ───────────────────────────────────
# ⚠️ Run ONLY after NB_04 is complete

print("\n📥 Loading silver_policy_dim and silver_customer_dim...")
policy_dim   = spark.table("silver_insclm.silver_policy_dim")
customer_dim = spark.table("silver_insclm.silver_customer_dim")

print(f"   policy_dim rows   : {policy_dim.count():,}")
print(f"   customer_dim rows : {customer_dim.count():,}")

# Time-aware join: get the policy version that was
# active on the exact date the claim was filed
print("\n📥 Joining claims to policy_dim (time-aware)...")
claims_with_policy = (good_claims.alias("c")
    .join(policy_dim.alias("p"),
          (F.col("c.policy_id") == F.col("p.policy_id")) &
          (F.col("c.claim_date") >= F.to_date(F.col("p.effective_date"))) &
          (F.col("c.claim_date") <  F.to_date(F.col("p.expiry_date"))),
          "left"))

print("📥 Joining to customer_dim...")
claims_with_all = (claims_with_policy
    .join(customer_dim.alias("cu"),
          F.col("c.customer_id") == F.col("cu.customer_id"),
          "left"))

print(f"✅ Joined rows: {claims_with_all.count():,}")

# ── Cell 6: Business flags + suspicious score ─────────────────
print("\n📥 Applying business flags...")

customer_window = Window.partitionBy("c.customer_id")

silver_claims_fact = (claims_with_all
    .withColumn("customer_claim_count",
        F.count("c.claim_id").over(customer_window))
    .withColumn("amount_exceeds_coverage_flag",
        F.when(F.col("c.claim_amount") > F.col("p.coverage_amount"),
               True).otherwise(False))
    .withColumn("inactive_policy_flag",
        F.col("p.policy_status").isin("Cancelled","Lapsed","Expired"))
    .withColumn("incomplete_docs_flag",
        F.col("c.document_status").isin("Incomplete","Missing"))
    .withColumn("high_frequency_flag",
        F.col("customer_claim_count") > 5)
    .withColumn("suspicious_score",
        F.col("amount_exceeds_coverage_flag").cast("int") +
        F.col("inactive_policy_flag").cast("int") +
        F.col("incomplete_docs_flag").cast("int") +
        F.col("high_frequency_flag").cast("int"))
    .withColumn("_silver_loaded_at", F.current_timestamp())
    .select(
        F.col("c.claim_id"),
        F.col("c.policy_id"),
        F.col("p.policy_sk"),
        F.col("c.customer_id"),
        F.col("cu.customer_name"),
        F.col("cu.city"),
        F.col("cu.state"),
        F.col("cu.risk_category"),
        F.col("p.policy_type"),
        F.col("p.coverage_amount"),
        F.col("p.premium_amount"),
        F.col("p.policy_status").alias("policy_status_at_claim"),
        F.col("c.claim_date"),
        F.col("c.claim_amount"),
        F.col("c.claim_reason"),
        F.col("c.document_status"),
        "amount_exceeds_coverage_flag",
        "inactive_policy_flag",
        "incomplete_docs_flag",
        "high_frequency_flag",
        "customer_claim_count",
        "suspicious_score",
        F.col("c.ingestion_date"),
        "_silver_loaded_at"
    ))

# ── Cell 7: Write silver_claims_fact ─────────────────────────
print("\n📥 Writing silver_claims_fact...")

silver_claims_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy(F.year("claim_date").alias("claim_year")) \
    .saveAsTable("silver_insclm.silver_claims_fact")

fact_count = spark.table("silver_insclm.silver_claims_fact").count()
print(f"✅ silver_claims_fact → {fact_count:,} rows")

# ── Cell 8: Validation summary ────────────────────────────────
print("\n" + "=" * 55)
print("SILVER TRANSFORMATION SUMMARY")
print("=" * 55)

spark.table("silver_insclm.silver_claims_fact").agg(
    F.count("claim_id").alias("total_claims"),
    F.sum(F.col("amount_exceeds_coverage_flag").cast("int"))
     .alias("exceeds_coverage"),
    F.sum(F.col("inactive_policy_flag").cast("int"))
     .alias("inactive_policy"),
    F.sum(F.col("incomplete_docs_flag").cast("int"))
     .alias("incomplete_docs"),
    F.sum(F.col("high_frequency_flag").cast("int"))
     .alias("high_frequency"),
    F.avg("suspicious_score").alias("avg_suspicious_score")
).show()

print("Suspicious score distribution:")
spark.table("silver_insclm.silver_claims_fact") \
    .groupBy("suspicious_score").count() \
    .orderBy("suspicious_score").show()

print("✅ NB_02 complete.")
print("   Next: Run NB_03_silver_rejected_records")
print("   Then: Message Sreya — silver_claims_fact is ready for Gold")
