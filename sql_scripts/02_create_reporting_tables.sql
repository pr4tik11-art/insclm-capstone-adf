-- =============================================================
-- 02_create_reporting_tables.sql
-- Creates reporting schema and 3 reporting tables in Azure SQL
-- These tables receive data from Databricks Gold layer
-- via JDBC write in NB_09_load_to_azure_sql
--
-- Database : sqldb-insclm-capstone
-- Schema   : reporting
-- Tables   : fact_claim_summary
--            fact_suspicious_claims
--            dim_policy_history
-- =============================================================

USE sqldb-insclm-capstone;
GO

-- ── Step 1: Create reporting schema ──────────────────────────
IF NOT EXISTS (
    SELECT 1 FROM sys.schemas
    WHERE name = 'reporting'
)
BEGIN
    EXEC('CREATE SCHEMA reporting');
    PRINT 'reporting schema created';
END
ELSE
BEGIN
    PRINT 'reporting schema already exists';
END
GO

-- ── Step 2: Drop tables if they exist (clean run) ────────────
IF OBJECT_ID('reporting.fact_claim_summary', 'U') IS NOT NULL
    DROP TABLE reporting.fact_claim_summary;
GO

IF OBJECT_ID('reporting.fact_suspicious_claims', 'U') IS NOT NULL
    DROP TABLE reporting.fact_suspicious_claims;
GO

IF OBJECT_ID('reporting.dim_policy_history', 'U') IS NOT NULL
    DROP TABLE reporting.dim_policy_history;
GO

-- ── Step 3: Create fact_claim_summary ────────────────────────
-- Receives data from gold_insclm.gold_claim_summary
-- Business purpose: Approval/rejection rates by policy type
CREATE TABLE reporting.fact_claim_summary (
    policy_type          VARCHAR(20)   NOT NULL,
    claim_reason         VARCHAR(50)   NOT NULL,
    total_claims         BIGINT        NOT NULL,
    approved_claims      BIGINT        NOT NULL,
    rejected_claims      BIGINT        NOT NULL,
    pending_claims       BIGINT        NOT NULL,
    investigation_claims BIGINT        NOT NULL,
    total_claim_amount   DECIMAL(18,2) NOT NULL,
    avg_claim_amount     DECIMAL(18,2) NOT NULL,
    min_claim_amount     DECIMAL(18,2) NOT NULL,
    max_claim_amount     DECIMAL(18,2) NOT NULL,
    approval_rate_pct    DECIMAL(5,2)  NOT NULL,
    rejection_rate_pct   DECIMAL(5,2)  NOT NULL,
    loaded_at            DATETIME2     NOT NULL,
    CONSTRAINT PK_fact_claim_summary
        PRIMARY KEY (policy_type, claim_reason)
);
GO

-- Index for faster filtering on policy_type
CREATE INDEX IX_fact_claim_summary_policy_type
    ON reporting.fact_claim_summary(policy_type);
GO

-- ── Step 4: Create fact_suspicious_claims ────────────────────
-- Receives data from gold_insclm.gold_suspicious_claim_summary
-- Business purpose: Fraud detection and risk analysis
CREATE TABLE reporting.fact_suspicious_claims (
    claim_id              VARCHAR(20)   NOT NULL,
    policy_id             VARCHAR(20)   NOT NULL,
    customer_id           VARCHAR(20)   NOT NULL,
    customer_name         VARCHAR(100)  NOT NULL,
    state                 VARCHAR(50)   NOT NULL,
    risk_category         VARCHAR(10)   NOT NULL,
    claim_amount          DECIMAL(18,2) NOT NULL,
    coverage_amount       DECIMAL(18,2) NOT NULL,
    policy_status_at_claim VARCHAR(20)  NOT NULL,
    document_status       VARCHAR(20)   NOT NULL,
    claim_reason          VARCHAR(50)   NOT NULL,
    claim_date            DATE          NOT NULL,
    customer_claim_count  INT           NOT NULL,
    suspicious_score      INT           NOT NULL,
    suspicion_level       VARCHAR(10)   NOT NULL,
    final_status          VARCHAR(20)   NULL,
    final_status_date     DATE          NULL,
    loaded_at             DATETIME2     NOT NULL,
    CONSTRAINT PK_fact_suspicious_claims
        PRIMARY KEY (claim_id)
);
GO

-- Index for faster filtering on suspicion_level
CREATE INDEX IX_fact_suspicious_suspicion_level
    ON reporting.fact_suspicious_claims(suspicion_level);
GO

-- Index for faster filtering on state
CREATE INDEX IX_fact_suspicious_state
    ON reporting.fact_suspicious_claims(state);
GO

-- Index for faster filtering on risk_category
CREATE INDEX IX_fact_suspicious_risk_category
    ON reporting.fact_suspicious_claims(risk_category);
GO

-- ── Step 5: Create dim_policy_history ────────────────────────
-- Receives data from silver_insclm.silver_policy_dim
-- Business purpose: Full SCD Type 2 policy history
CREATE TABLE reporting.dim_policy_history (
    policy_sk      BIGINT        NOT NULL,
    policy_id      VARCHAR(20)   NOT NULL,
    customer_id    VARCHAR(20)   NOT NULL,
    policy_type    VARCHAR(20)   NOT NULL,
    coverage_amount DECIMAL(18,2) NOT NULL,
    premium_amount  DECIMAL(18,2) NOT NULL,
    policy_status   VARCHAR(20)   NOT NULL,
    start_date      DATE          NOT NULL,
    end_date        DATE          NOT NULL,
    effective_date  DATETIME2     NOT NULL,
    expiry_date     DATETIME2     NOT NULL,
    is_current      BIT           NOT NULL,
    record_hash     VARCHAR(32)   NOT NULL,
    loaded_at       DATETIME2     NOT NULL,
    CONSTRAINT PK_dim_policy_history
        PRIMARY KEY (policy_sk)
);
GO

-- Index for faster joins on policy_id
CREATE INDEX IX_dim_policy_history_policy_id
    ON reporting.dim_policy_history(policy_id);
GO

-- Index for faster filtering on is_current
CREATE INDEX IX_dim_policy_history_is_current
    ON reporting.dim_policy_history(is_current);
GO

-- Index for time-aware queries
CREATE INDEX IX_dim_policy_history_effective_expiry
    ON reporting.dim_policy_history(effective_date, expiry_date);
GO

-- ── Step 6: Verify all 3 tables created ──────────────────────
SELECT
    s.name  AS schema_name,
    t.name  AS table_name,
    COUNT(c.name) AS column_count
FROM sys.tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
JOIN sys.columns c
    ON t.object_id = c.object_id
WHERE s.name = 'reporting'
GROUP BY s.name, t.name
ORDER BY t.name;
GO
-- Expected: 3 rows — fact_claim_summary, fact_suspicious_claims,
--           dim_policy_history

-- ── Step 7: Verify row counts after NB_09 runs ───────────────
-- Run this AFTER NB_09_load_to_azure_sql completes
SELECT 'fact_claim_summary'     AS table_name,
        COUNT(*)                 AS row_count
FROM reporting.fact_claim_summary
UNION ALL
SELECT 'fact_suspicious_claims',
        COUNT(*)
FROM reporting.fact_suspicious_claims
UNION ALL
SELECT 'dim_policy_history',
        COUNT(*)
FROM reporting.dim_policy_history;
GO
