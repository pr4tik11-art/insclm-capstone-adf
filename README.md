# Capstone 06 — Insurance Claims Policy History Lakehouse

## Project Overview

An end-to-end Azure Data Engineering Lakehouse that analyses insurance policyholder history, policy changes, claims, approvals, rejections, and suspicious claim indicators — while preserving the **correct policy state at the time of each claim**.

---

## Team

| Member | Role | Owns |
|--------|------|------|
| Pratik | Azure Source + ADF Engineer | Azure infrastructure, Key Vault, ADF pipelines, raw zone ingestion |
| Manas | Databricks Claims Engineer | Bronze Delta, Silver Delta, cleaning, joins, business flags |
| Sreya | SCD + Gold + Reporting Engineer | SCD Type 2, Delta MERGE, Gold tables, Time Travel, Azure SQL reporting |

---

## Architecture

```
Azure SQL DB                    ADLS Raw Zone (CSV files)
(policy_master,                 (claims.csv,
 customer_master)                claim_status_updates.csv)
        |                               |
        +----------ADF Pipelines--------+
                        |
                        v
               ADLS Raw Zone
                        |
                        v
              Databricks PySpark
                        |
           +------------+------------+
           |                         |
           v                         v
    Bronze Delta               Bronze Delta
    (claims,                   (policy_master,
     claim_status_updates)      customer_master)
           |
           v
    Silver Delta
    (silver_claims_fact,
     silver_customer_dim,
     silver_policy_dim,          <-- SCD Type 2
     silver_claim_status_history) <-- Delta MERGE
           |
           v
    Gold Delta
    (gold_claim_summary,
     gold_policy_history_summary,
     gold_suspicious_claim_summary)
           |
           v
    Azure SQL Reporting
    (fact_claim_summary,
     fact_suspicious_claims,
     dim_policy_history)
           |
           v
    3 Analytical SQL Queries
```

---

## Azure Resources

| Resource | Name | Region |
|----------|------|--------|
| Resource Group | rg-insclm-capstone | East US |
| Storage Account (ADLS Gen2) | stinsclmcappsp | East US |
| ADLS Container | lakehouse | — |
| SQL Server | sql-insclm-cap-psp | East US |
| SQL Database | sqldb-insclm-capstone | East US |
| Key Vault | kv-insclm-cap-psp | East US |
| Data Factory | adf-insclm-capstone | East US |
| Databricks Workspace | dbw-insclm-capstone | East US |

> **Security rule:** No hardcoded passwords, keys, or connection strings anywhere in code. Everything comes from Azure Key Vault via Databricks secret scope `kv-insclm`.

---

## Source Data

| File | Rows | Destination | Owner |
|------|------|-------------|-------|
| claims.csv | 2,200 | ADLS raw/claims/ | Pratik uploads |
| claim_status_updates.csv | 1,600 | ADLS raw/claim_status_updates/ | Pratik uploads |
| policy_master_seed.csv | 1,500 | Azure SQL dbo.policy_master | Pratik imports |
| insurance_customer_master_seed.csv | 1,000 | Azure SQL dbo.customer_master | Pratik imports |

---

## ADLS Folder Structure

```
lakehouse/
├── raw/
│   ├── claims/
│   ├── claim_status_updates/
│   ├── policy_master/
│   └── customer_master/
├── bronze/
│   ├── bronze_claims/
│   ├── bronze_claim_status_updates/
│   ├── bronze_policy_master/
│   └── bronze_customer_master/
├── silver/
│   ├── silver_claims_fact/
│   ├── silver_customer_dim/
│   ├── silver_policy_dim/
│   └── silver_claim_status_history/
├── gold/
│   ├── gold_claim_summary/
│   ├── gold_policy_history_summary/
│   └── gold_suspicious_claim_summary/
├── rejected/
│   ├── rejected_claims/
│   ├── rejected_policy/
│   └── rejected_status_updates/
└── audit/
    └── audit_pipeline_log/
```

---

## Delta Table Catalogue

| Layer | Database | Table | Rows | Owner |
|-------|----------|-------|------|-------|
| Bronze | bronze_insclm | bronze_claims | 2,200 | Member 2 |
| Bronze | bronze_insclm | bronze_claim_status_updates | 1,600 | Member 2 |
| Bronze | bronze_insclm | bronze_policy_master | 1,500 | Member 2 |
| Bronze | bronze_insclm | bronze_customer_master | 1,000 | Member 2 |
| Silver | silver_insclm | silver_customer_dim | 1,000 | Member 2 |
| Silver | silver_insclm | silver_claims_fact | ~2,080 | Member 2 |
| Silver | silver_insclm | silver_policy_dim | 1,500+ | Sreya |
| Silver | silver_insclm | silver_claim_status_history | 1,600 | Sreya |
| Rejected | rejected_insclm | rejected_claims | ~120 | Member 2 |
| Rejected | rejected_insclm | rejected_status_updates | 0 | Member 2 |
| Rejected | rejected_insclm | rejected_policy | 0 | Member 2 |
| Gold | gold_insclm | gold_claim_summary | — | Sreya |
| Gold | gold_insclm | gold_policy_history_summary | — | Sreya |
| Gold | gold_insclm | gold_suspicious_claim_summary | — | Sreya |
| Audit | audit_insclm | audit_pipeline_log | 16 | Sreya |

---

## Azure SQL Reporting Tables

| Schema | Table | Purpose |
|--------|-------|---------|
| reporting | fact_claim_summary | Claim KPIs by policy type and reason |
| reporting | fact_suspicious_claims | Suspicious claim detail for fraud team |
| reporting | dim_policy_history | SCD Type 2 policy dimension history |

---

## Key Vault Secrets

| Secret Name | Purpose |
|-------------|---------|
| sql-server-fqdn | Azure SQL server hostname |
| sql-database-name | Database name |
| sql-admin-username | SQL admin login |
| sql-admin-password | SQL admin password |
| sql-jdbc-url | Full JDBC connection string |
| adls-account-name | Storage account name |
| adls-account-key | Storage account key |
| adls-container-name | Container name |
| adls-abfss-base | ABFSS base URL |
| adls-raw-path | Raw zone path |
| adls-bronze-path | Bronze zone path |
| adls-silver-path | Silver zone path |
| adls-gold-path | Gold zone path |
| adls-rejected-path | Rejected zone path |
| adls-audit-path | Audit zone path |
| file-claims | claims.csv filename |
| file-claim-status-updates | claim_status_updates.csv filename |
| file-policy-master | policy_master_seed.csv filename |
| file-customer-master | insurance_customer_master_seed.csv filename |
| sql-table-policy-master | dbo.policy_master |
| sql-table-customer-master | dbo.customer_master |
| databricks-workspace-url | Databricks workspace URL |

---

## Databricks Notebooks

| # | Notebook | Owner | Purpose |
|---|----------|-------|---------|
| NB_00 | NB_00_config_loader | All | Loads all config from Key Vault. Run at top of every notebook. |
| NB_01 | NB_01_bronze_ingestion | Member 2 | Reads raw CSVs from ADLS, writes 4 Bronze Delta tables |
| NB_02 | NB_02_silver_transformation | Member 2 | Cleans data, joins all 4 sources, adds 4 business flags |
| NB_03 | NB_03_silver_rejected_records | Member 2 | Rejection logic for status updates, policy, customer |
| NB_04 | NB_04_scd_policy_dim | Sreya | SCD Type 2 on policy dimension with record hash |
| NB_05 | NB_05_merge_claim_status | Sreya | Delta MERGE upsert on claim status history |
| NB_06 | NB_06_gold_tables | Sreya | Builds 3 Gold aggregation tables |
| NB_07 | NB_07_time_travel_demo | Sreya | Delta Time Travel queries — policy state at claim date |
| NB_08 | NB_08_describe_history_optimize | Sreya | DESCRIBE HISTORY, OPTIMIZE, VACUUM explanation |
| NB_09 | NB_09_load_to_azure_sql | Sreya | Writes Gold tables to Azure SQL via JDBC + 3 analytical queries |
| NB_10 | NB_10_audit_logger | Sreya | Logs all 16 pipeline stages to audit Delta table |

### Notebook Execution Order

```
NB_00 → NB_01 → NB_02 (Cells 1-4) → NB_04 → NB_02 (Cells 5-7)
     → NB_03 → NB_05 → NB_06 → NB_07 → NB_08 → NB_09 → NB_10
```

> NB_02 Cells 5-7 depend on NB_04 (silver_policy_dim) being ready first.

---

## ADF Components

### Linked Services

| Name | Type | Auth |
|------|------|------|
| LS_KeyVault | Azure Key Vault | Managed Identity |
| LS_AzureSQL_FromKV | Azure SQL Database | SQL auth via Key Vault |
| LS_ADLS_FromKV | ADLS Gen2 | Account key via Key Vault |
| LS_Databricks_FromKV | Azure Databricks | PAT via Key Vault |

### Pipelines

| Name | Purpose |
|------|---------|
| PL_00_GetConfigFromKV | Loads config from Key Vault |
| PL_01_Ingest_SQL_To_Raw | Copies SQL tables to ADLS raw zone |
| PL_02_Ingest_CSV_To_Raw | Copies CSVs to dated raw subfolders |
| PL_03_Trigger_Databricks_Notebook | Triggers notebook runs |
| PL_99_Master_Orchestrator | End-to-end orchestration |

---

## Business Flags in silver_claims_fact

| Flag Column | Condition | Business Meaning |
|-------------|-----------|-----------------|
| amount_exceeds_coverage_flag | claim_amount > coverage_amount | Possible fraud or data error |
| inactive_policy_flag | policy_status IN (Cancelled, Lapsed, Expired) | Claim on dead policy |
| incomplete_docs_flag | document_status IN (Incomplete, Missing) | Follow-up required |
| high_frequency_flag | customer_claim_count > 5 | Repeat claimant pattern |
| suspicious_score | Sum of all 4 flags (0-4) | 1=LOW, 2=MEDIUM, 3+=HIGH |

---

## SQL Scripts

| File | Purpose |
|------|---------|
| 01_create_source_tables.sql | DDL for dbo.policy_master and dbo.customer_master |
| 02_create_reporting_tables.sql | DDL for reporting schema and 3 reporting tables |
| 03_analytical_queries.sql | 3 analytical queries for business reporting |

### Analytical Queries

1. **Approval and Rejection Rate by Policy Type** — shows which policy types perform best
2. **Top 10 High-Risk States by Suspicious Claims** — geographic fraud hotspot analysis
3. **Top 10 Customers by Suspicious Claim Amount with Ranking** — highest priority fraud cases

---

## GitHub Branch Strategy

| Branch | Purpose |
|--------|---------|
| main | Final deliverables — protected |
| develop | ADF collaboration branch |
| feat/pratik-ingestion | Pratik working branch |
| feat/member2-databricks | Member 2 working branch |
| feat/sreya-scd-gold | Sreya working branch |
| adf_publish | ADF auto-publish (do not edit manually) |

---

## Eight Project Dimensions Demonstrated

| # | Dimension | How |
|---|-----------|-----|
| 1 | Ingestion | ADF pipelines move data from SQL and CSV to ADLS raw zone |
| 2 | Transformation | PySpark cleans, joins, and enriches all 4 data sources |
| 3 | Historical tracking | SCD Type 2 on policy_dim + Delta Time Travel |
| 4 | Auditability | audit_pipeline_log Delta table + DESCRIBE HISTORY |
| 5 | Security | Azure Key Vault — zero hardcoded secrets anywhere |
| 6 | Recovery awareness | VACUUM risk documented in NB_08 — not executed |
| 7 | Reporting readiness | 3 Gold tables + 3 Azure SQL reporting tables + 3 queries |
| 8 | Quality control | Rejected records logic with 8 rejection reason codes |

---

## Critical Rules

- **No hardcoded values** — every credential comes from Key Vault secret scope `kv-insclm`
- **VACUUM is NOT run** — insurance regulatory compliance requires full Delta history
- **Time-aware joins only** — silver_claims_fact uses policy state at claim_date, not current state
- **Region: East US** — all Azure resources must be in East US
- **Shared cluster** — `cl-insclm-shared` is used by all three members

---

## Handoff Points

| From | To | Artifact | Verification |
|------|----|----------|--------------|
| Pratik | Member 2 | Raw zone populated | `dbutils.fs.ls(RAW_PATH)` shows 4 folders |
| Pratik | All | Key Vault secrets added | `dbutils.secrets.list("kv-insclm")` lists ~22 secrets |
| Sreya | Member 2 | silver_policy_dim ready | `spark.table("silver_insclm.silver_policy_dim").count()` = 1500+ |
| Member 2 | Sreya | silver_claims_fact ready | `spark.table("silver_insclm.silver_claims_fact").count()` = ~2080 |
| Sreya | Group | Reporting tables in Azure SQL | Query `reporting.fact_claim_summary` from SSMS |

---

*Last updated: Capstone 06 — Team: Pratik · Member 2 · Sreya*
