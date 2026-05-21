# =============================================================
# NB_06_gold_tables
# Builds 3 Gold Delta tables from Silver layer
#
# Gold tables are final business-ready aggregations:
#   1. gold_claim_summary          → claims by policy + reason
#   2. gold_policy_history_summary → policy version history
#   3. gold_suspicious_claim_summary → all flagged claims
#
# ⚠️ Depends on:
#   silver_insclm.silver_claims_fact       (NB_02)
#   silver_insclm.silver_policy_dim        (NB_04)
#   silver_insclm.silver_claim_status_history (NB_05)
# =============================================================

# ── Cell 1: Load config ──────────────────────────────────────
# %run ./NB_00_config_loader

# ── Cell 2: Imports + database setup ─────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

spark.sql(f"CREATE DATABASE IF NOT EXISTS gold_insclm LOCATION '{GOLD_PATH}'")
print("✅ gold_insclm database ready")

# ── Cell 3: Load silver tables ───────────────────────────────
print("\n📥 Loading silver tables...")

claims_fact  = spark.table("silver_insclm.silver_claims_fact")
policy_dim   = spark.table("silver_insclm.silver_policy_dim")
status_hist  = spark.table("silver_insclm.silver_claim_status_history")

print(f"   silver_claims_fact              : {claims_fact.count():,}")
print(f"   silver_policy_dim               : {policy_dim.count():,}")
print(f"   silver_claim_status_history     : {status_hist.count():,}")

# ── Cell 4: Get latest status per claim ──────────────────────
print("\n📥 Computing latest status per claim...")

status_window = Window.partitionBy("claim_id").orderBy(F.desc("status_date"))

latest_status = (status_hist
    .withColumn("rn", F.row_number().over(status_window))
    .filter(F.col("rn") == 1)
    .select(
        "claim_id",
        F.col("new_status").alias("final_status"),
        F.col("status_date").alias("final_status_date"),
        F.col("remarks").alias("final_remarks"),
        "is_terminal_status"
    ))

print(f"   Latest status rows: {latest_status.count():,}")

claims_with_status = claims_fact.join(latest_status, "claim_id", "left")
print(f"   Claims with status joined: {claims_with_status.count():,}")

# ── Cell 5: Gold Table 1 — gold_claim_summary ────────────────
print("\n📥 Building gold_claim_summary...")
print("   Business question: How many claims, total amount,")
print("   approval/rejection rates by policy type + claim reason?")

gold_claim_summary = (claims_with_status
    .groupBy("policy_type", "claim_reason")
    .agg(
        F.count("claim_id")
         .alias("total_claims"),
        F.sum(F.when(F.col("final_status") == "Approved", 1)
              .otherwise(0))
         .alias("approved_claims"),
        F.sum(F.when(F.col("final_status") == "Rejected", 1)
              .otherwise(0))
         .alias("rejected_claims"),
        F.sum(F.when(F.col("final_status") == "Pending", 1)
              .otherwise(0))
         .alias("pending_claims"),
        F.sum(F.when(F.col("final_status") == "Investigation", 1)
              .otherwise(0))
         .alias("investigation_claims"),
        F.sum("claim_amount")
         .alias("total_claim_amount"),
        F.avg("claim_amount")
         .alias("avg_claim_amount"),
        F.min("claim_amount")
         .alias("min_claim_amount"),
        F.max("claim_amount")
         .alias("max_claim_amount"),
    )
    .withColumn("approval_rate_pct",
        F.round(F.col("approved_claims") /
                F.col("total_claims") * 100, 2))
    .withColumn("rejection_rate_pct",
        F.round(F.col("rejected_claims") /
                F.col("total_claims") * 100, 2))
    .withColumn("avg_claim_amount",
        F.round(F.col("avg_claim_amount"), 2))
    .withColumn("total_claim_amount",
        F.round(F.col("total_claim_amount"), 2))
    .withColumn("_gold_loaded_at", F.current_timestamp()))

gold_claim_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_insclm.gold_claim_summary")

g1_count = spark.table("gold_insclm.gold_claim_summary").count()
print(f"✅ gold_claim_summary → {g1_count:,} rows")

print("\nPreview — top 5 by total claims:")
spark.table("gold_insclm.gold_claim_summary") \
    .orderBy(F.desc("total_claims")) \
    .select("policy_type", "claim_reason", "total_claims",
            "approved_claims", "rejected_claims",
            "approval_rate_pct", "rejection_rate_pct") \
    .show(5, truncate=False)

# ── Cell 6: Gold Table 2 — gold_policy_history_summary ───────
print("\n📥 Building gold_policy_history_summary...")
print("   Business question: For each policy, what is the")
print("   full version history and coverage change %?")

# Aggregate policy dimension (all versions)
policy_agg = (policy_dim
    .groupBy("policy_id", "customer_id")
    .agg(
        F.count("*")
         .alias("total_versions"),
        F.min("effective_date")
         .alias("first_effective_date"),
        F.max("effective_date")
         .alias("last_effective_date"),
        F.max(F.when(F.col("is_current") == True,
                     F.col("policy_status")))
         .alias("current_policy_status"),
        F.max(F.when(F.col("is_current") == True,
                     F.col("coverage_amount")))
         .alias("current_coverage"),
        F.max(F.when(F.col("is_current") == True,
                     F.col("premium_amount")))
         .alias("current_premium"),
        F.max(F.when(F.col("is_current") == True,
                     F.col("policy_type")))
         .alias("current_policy_type"),
        F.min(F.when(F.col("is_current") == False,
                     F.col("coverage_amount")))
         .alias("original_coverage"),
    ))

# Aggregate claims per policy
claims_agg = (claims_fact
    .groupBy("policy_id")
    .agg(
        F.count("claim_id")
         .alias("total_claims_against_policy"),
        F.sum("claim_amount")
         .alias("total_claim_amount_against_policy"),
        F.avg("claim_amount")
         .alias("avg_claim_amount_against_policy"),
    ))

gold_policy_history = (policy_agg
    .join(claims_agg, "policy_id", "left")
    .fillna(0, ["total_claims_against_policy",
                "total_claim_amount_against_policy",
                "avg_claim_amount_against_policy"])
    .withColumn("coverage_change_pct",
        F.when(
            F.col("original_coverage").isNotNull() &
            (F.col("original_coverage") > 0),
            F.round(
                (F.col("current_coverage") -
                 F.col("original_coverage")) /
                F.col("original_coverage") * 100, 2)
        ).otherwise(F.lit(0.0)))
    .withColumn("total_claim_amount_against_policy",
        F.round("total_claim_amount_against_policy", 2))
    .withColumn("avg_claim_amount_against_policy",
        F.round("avg_claim_amount_against_policy", 2))
    .withColumn("_gold_loaded_at", F.current_timestamp()))

gold_policy_history.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_insclm.gold_policy_history_summary")

g2_count = spark.table("gold_insclm.gold_policy_history_summary").count()
print(f"✅ gold_policy_history_summary → {g2_count:,} rows")

print("\nPolicies with more than 1 version (SCD changes):")
spark.table("gold_insclm.gold_policy_history_summary") \
    .filter(F.col("total_versions") > 1) \
    .select("policy_id", "total_versions", "current_policy_status",
            "current_coverage", "coverage_change_pct") \
    .show(5, truncate=False)

# ── Cell 7: Gold Table 3 — gold_suspicious_claim_summary ─────
print("\n📥 Building gold_suspicious_claim_summary...")
print("   Business question: Which claims are suspicious")
print("   and what is their risk level?")

gold_suspicious = (claims_fact
    .filter(F.col("suspicious_score") >= 1)
    .withColumn("suspicion_level",
        F.when(F.col("suspicious_score") == 1, "LOW")
        .when(F.col("suspicious_score") == 2, "MEDIUM")
        .otherwise("HIGH"))
    .join(latest_status.select(
              "claim_id", "final_status",
              "final_status_date", "final_remarks"),
          "claim_id", "left")
    .select(
        "claim_id",
        "policy_id",
        "customer_id",
        "customer_name",
        "state",
        "risk_category",
        "claim_amount",
        "coverage_amount",
        "policy_status_at_claim",
        "document_status",
        "claim_reason",
        "claim_date",
        "customer_claim_count",
        "amount_exceeds_coverage_flag",
        "inactive_policy_flag",
        "incomplete_docs_flag",
        "high_frequency_flag",
        "suspicious_score",
        "suspicion_level",
        "final_status",
        "final_status_date",
        "final_remarks",
    )
    .withColumn("_gold_loaded_at", F.current_timestamp()))

gold_suspicious.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_insclm.gold_suspicious_claim_summary")

g3_count = spark.table("gold_insclm.gold_suspicious_claim_summary").count()
print(f"✅ gold_suspicious_claim_summary → {g3_count:,} rows")

print("\nSuspicion level breakdown:")
spark.table("gold_insclm.gold_suspicious_claim_summary") \
    .groupBy("suspicion_level").count() \
    .orderBy("suspicion_level").show()

# ── Cell 8: Final summary ─────────────────────────────────────
print("\n" + "=" * 55)
print("GOLD LAYER SUMMARY")
print("=" * 55)
print(f"gold_claim_summary              : {g1_count:,} rows")
print(f"gold_policy_history_summary     : {g2_count:,} rows")
print(f"gold_suspicious_claim_summary   : {g3_count:,} rows")
print("=" * 55)
print("✅ NB_06 complete.")
print("   Next: Run NB_07_time_travel_demo")
