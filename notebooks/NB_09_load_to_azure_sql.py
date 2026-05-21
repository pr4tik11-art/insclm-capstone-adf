# =============================================================
# NB_09_load_to_azure_sql
# Writes Gold Delta tables to Azure SQL reporting schema
#
# Output tables in Azure SQL:
#   reporting.fact_claim_summary
#   reporting.fact_suspicious_claims
#   reporting.dim_policy_history
#
# Then runs 3 analytical queries to verify the data
#
# ⚠️ Depends on:
#   gold_insclm.gold_claim_summary          (NB_06)
#   gold_insclm.gold_suspicious_claim_summary (NB_06)
#   silver_insclm.silver_policy_dim          (NB_04)
# =============================================================

# ── Cell 1: Load config ──────────────────────────────────────
# %run ./NB_00_config_loader

# ── Cell 2: Imports ───────────────────────────────────────────
from pyspark.sql import functions as F
from datetime import datetime

print("=" * 55)
print("LOADING GOLD TABLES TO AZURE SQL")
print("=" * 55)

# ── Cell 3: JDBC connection properties ───────────────────────
# All values come from Key Vault via NB_00_config_loader
# Nothing hardcoded here

connection_properties = {
    "user":     SQL_USER,
    "password": SQL_PASSWORD,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

print("✅ JDBC connection properties loaded from Key Vault")
print(f"   JDBC URL: {SQL_JDBC_URL[:60]}...")

# ── Cell 4: Helper function to write to SQL ───────────────────
def write_to_sql(df, table_name, mode="overwrite"):
    start = datetime.now()
    row_count = df.count()
    print(f"\n📥 Writing {row_count:,} rows to {table_name}...")

    (df.write
        .format("jdbc")
        .option("url",      SQL_JDBC_URL)
        .option("dbtable",  table_name)
        .option("user",     SQL_USER)
        .option("password", SQL_PASSWORD)
        .option("driver",   "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("batchsize", 10000)
        .mode(mode)
        .save())

    end = datetime.now()
    print(f"✅ {table_name} → {row_count:,} rows written in "
          f"{(end-start).seconds}s")
    return row_count

# ── Cell 5: Write reporting.fact_claim_summary ───────────────
print("\n── Table 1: reporting.fact_claim_summary ────────────")

fact_claim = (spark
    .table("gold_insclm.gold_claim_summary")
    .withColumn("loaded_at", F.current_timestamp())
    .drop("_gold_loaded_at"))

c1 = write_to_sql(fact_claim, "reporting.fact_claim_summary")

# ── Cell 6: Write reporting.fact_suspicious_claims ───────────
print("\n── Table 2: reporting.fact_suspicious_claims ────────")

fact_suspicious = (spark
    .table("gold_insclm.gold_suspicious_claim_summary")
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
        "customer_claim_count",
        "suspicious_score",
        "suspicion_level",
        "final_status",
        "final_status_date",
    )
    .withColumn("loaded_at", F.current_timestamp()))

c2 = write_to_sql(fact_suspicious, "reporting.fact_suspicious_claims")

# ── Cell 7: Write reporting.dim_policy_history ───────────────
print("\n── Table 3: reporting.dim_policy_history ────────────")

dim_policy = (spark
    .table("silver_insclm.silver_policy_dim")
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
        "record_hash"
    )
    .withColumn("loaded_at", F.current_timestamp()))

c3 = write_to_sql(dim_policy, "reporting.dim_policy_history")

# ── Cell 8: Verify row counts in Azure SQL ────────────────────
print("\n" + "=" * 55)
print("VERIFYING ROW COUNTS IN AZURE SQL")
print("=" * 55)

def read_from_sql(table_name):
    return (spark.read
        .format("jdbc")
        .option("url",      SQL_JDBC_URL)
        .option("dbtable",  table_name)
        .option("user",     SQL_USER)
        .option("password", SQL_PASSWORD)
        .option("driver",
                "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load())

v1 = read_from_sql("reporting.fact_claim_summary").count()
v2 = read_from_sql("reporting.fact_suspicious_claims").count()
v3 = read_from_sql("reporting.dim_policy_history").count()

print(f"reporting.fact_claim_summary      : {v1:,}  (written: {c1:,})")
print(f"reporting.fact_suspicious_claims  : {v2:,}  (written: {c2:,})")
print(f"reporting.dim_policy_history      : {v3:,}  (written: {c3:,})")

all_match = (v1 == c1 and v2 == c2 and v3 == c3)
if all_match:
    print("\n✅ All row counts match — data loaded correctly")
else:
    print("\n❌ Row count mismatch — investigate above")

# ── Cell 9: Analytical Query 1 ───────────────────────────────
print("\n" + "=" * 55)
print("ANALYTICAL QUERY 1")
print("Approval and Rejection Rate by Policy Type")
print("=" * 55)

query1 = """
    SELECT
        policy_type,
        SUM(total_claims)      AS total_claims,
        SUM(approved_claims)   AS approved,
        SUM(rejected_claims)   AS rejected,
        SUM(pending_claims)    AS pending,
        ROUND(AVG(approval_rate_pct), 2)  AS avg_approval_rate,
        ROUND(AVG(rejection_rate_pct), 2) AS avg_rejection_rate,
        ROUND(SUM(total_claim_amount), 2) AS total_amount
    FROM reporting.fact_claim_summary
    GROUP BY policy_type
    ORDER BY total_claims DESC
"""

result1 = read_from_sql(f"({query1}) q1")
print("Result:")
result1.show(truncate=False)

# ── Cell 10: Analytical Query 2 ──────────────────────────────
print("\n" + "=" * 55)
print("ANALYTICAL QUERY 2")
print("Top 10 High-Risk States by Suspicious Claims")
print("=" * 55)

query2 = """
    SELECT TOP 10
        state,
        COUNT(*)                    AS suspicious_claims,
        SUM(suspicious_score)       AS total_score,
        AVG(CAST(suspicious_score AS FLOAT))
                                    AS avg_score,
        SUM(claim_amount)           AS total_claim_amount,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(CASE WHEN suspicion_level = 'HIGH'
                 THEN 1 ELSE 0 END) AS high_risk_count,
        SUM(CASE WHEN suspicion_level = 'MEDIUM'
                 THEN 1 ELSE 0 END) AS medium_risk_count,
        SUM(CASE WHEN suspicion_level = 'LOW'
                 THEN 1 ELSE 0 END) AS low_risk_count
    FROM reporting.fact_suspicious_claims
    GROUP BY state
    ORDER BY suspicious_claims DESC
"""

result2 = read_from_sql(f"({query2}) q2")
print("Result:")
result2.show(truncate=False)

# ── Cell 11: Analytical Query 3 ──────────────────────────────
print("\n" + "=" * 55)
print("ANALYTICAL QUERY 3")
print("Top 10 Customers by Suspicious Claim Amount")
print("with Ranking")
print("=" * 55)

query3 = """
    WITH customer_totals AS (
        SELECT
            customer_id,
            customer_name,
            state,
            risk_category,
            COUNT(DISTINCT claim_id)      AS total_suspicious_claims,
            SUM(claim_amount)             AS total_suspicious_amount,
            MAX(customer_claim_count)     AS overall_claim_count,
            MAX(suspicious_score)         AS max_suspicious_score,
            SUM(CASE WHEN suspicion_level = 'HIGH'
                     THEN 1 ELSE 0 END)  AS high_risk_claims
        FROM reporting.fact_suspicious_claims
        GROUP BY
            customer_id,
            customer_name,
            state,
            risk_category
    )
    SELECT TOP 10
        customer_id,
        customer_name,
        state,
        risk_category,
        total_suspicious_claims,
        ROUND(total_suspicious_amount, 2) AS total_suspicious_amount,
        overall_claim_count,
        max_suspicious_score,
        high_risk_claims,
        RANK() OVER (
            ORDER BY total_suspicious_amount DESC
        ) AS suspicious_rank
    FROM customer_totals
    ORDER BY total_suspicious_amount DESC
"""

result3 = read_from_sql(f"({query3}) q3")
print("Result:")
result3.show(truncate=False)

# ── Cell 12: Final summary ────────────────────────────────────
print("\n" + "=" * 55)
print("AZURE SQL REPORTING SUMMARY")
print("=" * 55)
print(f"reporting.fact_claim_summary     : {v1:,} rows")
print(f"reporting.fact_suspicious_claims : {v2:,} rows")
print(f"reporting.dim_policy_history     : {v3:,} rows")
print("\n3 Analytical Queries executed successfully")
print("=" * 55)
print("✅ NB_09 complete.")
print("   Next: Run NB_10_audit_logger")
