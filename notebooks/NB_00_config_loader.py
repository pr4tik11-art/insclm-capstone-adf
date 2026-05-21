# =============================================================
# NB_00_config_loader
# Single source of truth for all paths and credentials.
# Every other notebook starts with: %run ./NB_00_config_loader
# NO hardcoded values — everything comes from Key Vault.
# =============================================================

KV_SCOPE = "kv-insclm"

# ── ADLS Configuration ───────────────────────────────────────
ADLS_ACCOUNT_NAME = dbutils.secrets.get(KV_SCOPE, "adls-account-name")
ADLS_ACCOUNT_KEY  = dbutils.secrets.get(KV_SCOPE, "adls-account-key")
ADLS_CONTAINER    = dbutils.secrets.get(KV_SCOPE, "adls-container-name")
ADLS_ABFSS_BASE   = dbutils.secrets.get(KV_SCOPE, "adls-abfss-base")

# ── Zone Paths ───────────────────────────────────────────────
RAW_PATH      = ADLS_ABFSS_BASE + dbutils.secrets.get(KV_SCOPE, "adls-raw-path")
BRONZE_PATH   = ADLS_ABFSS_BASE + dbutils.secrets.get(KV_SCOPE, "adls-bronze-path")
SILVER_PATH   = ADLS_ABFSS_BASE + dbutils.secrets.get(KV_SCOPE, "adls-silver-path")
GOLD_PATH     = ADLS_ABFSS_BASE + dbutils.secrets.get(KV_SCOPE, "adls-gold-path")
REJECTED_PATH = ADLS_ABFSS_BASE + dbutils.secrets.get(KV_SCOPE, "adls-rejected-path")
AUDIT_PATH    = ADLS_ABFSS_BASE + dbutils.secrets.get(KV_SCOPE, "adls-audit-path")

# ── Configure Spark to access ADLS ───────────────────────────
spark.conf.set(
    f"fs.azure.account.key.{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
    ADLS_ACCOUNT_KEY
)

# ── SQL Configuration ─────────────────────────────────────────
SQL_JDBC_URL = dbutils.secrets.get(KV_SCOPE, "sql-jdbc-url")
SQL_USER     = dbutils.secrets.get(KV_SCOPE, "sql-admin-username")
SQL_PASSWORD = dbutils.secrets.get(KV_SCOPE, "sql-admin-password")

# ── Source File Names ─────────────────────────────────────────
FILE_CLAIMS          = dbutils.secrets.get(KV_SCOPE, "file-claims")
FILE_CLAIM_STATUS    = dbutils.secrets.get(KV_SCOPE, "file-claim-status-updates")
FILE_POLICY_MASTER   = dbutils.secrets.get(KV_SCOPE, "file-policy-master")
FILE_CUSTOMER_MASTER = dbutils.secrets.get(KV_SCOPE, "file-customer-master")

print("=" * 50)
print("✅ Config loaded from Key Vault successfully.")
print(f"   ADLS Account : {ADLS_ACCOUNT_NAME}")
print(f"   Container    : {ADLS_CONTAINER}")
print(f"   RAW path     : {RAW_PATH}")
print(f"   BRONZE path  : {BRONZE_PATH}")
print(f"   SILVER path  : {SILVER_PATH}")
print(f"   GOLD path    : {GOLD_PATH}")
print("=" * 50)
