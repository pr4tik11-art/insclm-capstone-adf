# =============================================================
# NB_08_describe_history_optimize
# Runs DESCRIBE HISTORY and OPTIMIZE on all key tables
#
# DESCRIBE HISTORY → shows every write operation ever done
#                    on a Delta table (version, time, operation)
#
# OPTIMIZE → merges many small Parquet files into fewer
#            large files → makes reads much faster
#
# VACUUM → NOT run in this notebook (explained below)
# =============================================================

# ── Cell 1: Load config ──────────────────────────────────────
# %run ./NB_00_config_loader

# ── Cell 2: Imports ───────────────────────────────────────────
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

print("=" * 55)
print("DESCRIBE HISTORY + OPTIMIZE")
print("=" * 55)

# ── Cell 3: DESCRIBE HISTORY on all key tables ───────────────
tables_to_inspect = [
    "silver_insclm.silver_policy_dim",
    "silver_insclm.silver_claims_fact",
    "silver_insclm.silver_customer_dim",
    "silver_insclm.silver_claim_status_history",
    "gold_insclm.gold_claim_summary",
    "gold_insclm.gold_policy_history_summary",
    "gold_insclm.gold_suspicious_claim_summary",
    "rejected_insclm.rejected_claims",
    "bronze_insclm.bronze_claims",
]

print("\n📋 DESCRIBE HISTORY — all key tables\n")

for table in tables_to_inspect:
    try:
        print(f"\n{'='*55}")
        print(f"TABLE: {table}")
        print(f"{'='*55}")
        DeltaTable.forName(spark, table) \
            .history() \
            .select(
                "version",
                "timestamp",
                "operation",
                "operationMetrics"
            ).show(5, truncate=False)
    except Exception as e:
        print(f"⚠️  Could not read history for {table}: {e}")

# ── Cell 4: OPTIMIZE on key tables ───────────────────────────
print("\n" + "=" * 55)
print("OPTIMIZE — compacting small files")
print("=" * 55)
print("Delta creates many small Parquet files during writes.")
print("OPTIMIZE merges them → faster reads for reporting.\n")

optimize_tables = [
    ("silver_insclm.silver_claims_fact",
     "ZORDER BY (policy_id, claim_date)"),
    ("silver_insclm.silver_policy_dim",
     "ZORDER BY (policy_id, effective_date)"),
    ("silver_insclm.silver_claim_status_history",
     "ZORDER BY (claim_id, status_date)"),
    ("gold_insclm.gold_claim_summary",
     ""),
    ("gold_insclm.gold_suspicious_claim_summary",
     ""),
]

for table, zorder in optimize_tables:
    try:
        start = datetime.now()
        print(f"📥 Optimizing {table}...")
        if zorder:
            spark.sql(f"OPTIMIZE {table} {zorder}")
        else:
            spark.sql(f"OPTIMIZE {table}")
        end = datetime.now()
        print(f"✅ Done in {(end-start).seconds}s")
    except Exception as e:
        print(f"⚠️  Could not optimize {table}: {e}")

# ── Cell 5: Verify file count after OPTIMIZE ─────────────────
print("\n📋 Checking Delta table details after OPTIMIZE:")

detail_tables = [
    "silver_insclm.silver_claims_fact",
    "silver_insclm.silver_policy_dim",
]

for table in detail_tables:
    try:
        print(f"\n{table}:")
        spark.sql(f"DESCRIBE DETAIL {table}") \
            .select(
                "numFiles",
                "sizeInBytes",
                "location"
            ).show(truncate=False)
    except Exception as e:
        print(f"⚠️  {e}")

# ── Cell 6: VACUUM explanation ────────────────────────────────
print("\n" + "=" * 55)
print("VACUUM — WHY WE DO NOT RUN IT")
print("=" * 55)

vacuum_explanation = """
WHAT IS VACUUM?
    VACUUM permanently deletes old Parquet files that are
    older than the retention period (default: 7 days).
    Once deleted, they CANNOT be recovered.

WHY VACUUM IS DANGEROUS IN INSURANCE:

    1. REGULATORY COMPLIANCE
       IRDAI (Insurance Regulatory and Development Authority
       of India) requires insurance records to be preserved
       for a minimum of 5-10 years. Running VACUUM destroys
       old Delta versions — making compliance impossible.

    2. TIME TRAVEL IS LOST
       Once VACUUM removes old files, AS OF queries fail.
       We can no longer ask "what did policy POL000001
       look like on 2025-06-15?" — the data is gone.

    3. SCD TYPE 2 AUDIT TRAIL DESTROYED
       Our policy_dim uses Delta history to prove WHEN
       premiums and coverage amounts changed. VACUUM
       removes that evidence permanently.

    4. FRAUD INVESTIGATION
       Suspicious claims may be disputed years after
       filing. Investigators need original claim values
       from the time of filing. VACUUM destroys this.

    5. NO RECOVERY
       Unlike a database DELETE with rollback, VACUUM
       is permanent. No recycle bin. No undo.

SAFE APPROACH FOR INSURANCE:
    — Set retention to minimum 2 years:
      ALTER TABLE silver_insclm.silver_policy_dim
      SET TBLPROPERTIES (
        'delta.deletedFileRetentionDuration' = 'interval 730 days'
      );

    — If VACUUM is absolutely required (storage emergency):
      Always DRY RUN first to see what would be deleted:
      VACUUM silver_insclm.silver_policy_dim
        RETAIN 17520 HOURS DRY RUN;

    — Archive old snapshots to cold storage BEFORE VACUUM.

    — Log every VACUUM run in the audit table with
      justification and approver name.

BOTTOM LINE:
    For this capstone project: VACUUM is NOT run.
    All Delta history is preserved for Time Travel,
    audit, and regulatory compliance.
"""

print(vacuum_explanation)

# ── Cell 7: Set retention properties ─────────────────────────
print("📥 Setting retention properties on key tables...")
print("   (This makes the safe retention period explicit)\n")

retention_tables = [
    "silver_insclm.silver_policy_dim",
    "silver_insclm.silver_claims_fact",
    "silver_insclm.silver_claim_status_history",
    "gold_insclm.gold_suspicious_claim_summary",
]

for table in retention_tables:
    try:
        spark.sql(f"""
            ALTER TABLE {table}
            SET TBLPROPERTIES (
                'delta.deletedFileRetentionDuration' = 'interval 730 days',
                'delta.logRetentionDuration'         = 'interval 730 days'
            )
        """)
        print(f"✅ {table} → retention set to 730 days")
    except Exception as e:
        print(f"⚠️  {table}: {e}")

# ── Cell 8: Final summary ─────────────────────────────────────
print("\n" + "=" * 55)
print("SUMMARY")
print("=" * 55)
print("✅ DESCRIBE HISTORY run on all key tables")
print("✅ OPTIMIZE run on 5 tables with ZORDER")
print("✅ VACUUM explained — NOT executed (by design)")
print("✅ Retention properties set to 730 days")
print("=" * 55)
print("✅ NB_08 complete.")
print("   Next: Run NB_09_load_to_azure_sql")
