-- =============================================================
-- 01_create_source_tables.sql
-- Creates source tables in Azure SQL Database
-- Run this FIRST before importing any CSV data
--
-- Database : sqldb-insclm-capstone
-- Schema   : dbo
-- Tables   : policy_master, customer_master
-- =============================================================

-- ── Step 1: Verify you are in the right database ─────────────
USE sqldb-insclm-capstone;
GO

-- ── Step 2: Drop tables if they exist (clean run) ────────────
IF OBJECT_ID('dbo.policy_master', 'U') IS NOT NULL
    DROP TABLE dbo.policy_master;
GO

IF OBJECT_ID('dbo.customer_master', 'U') IS NOT NULL
    DROP TABLE dbo.customer_master;
GO

-- ── Step 3: Create policy_master ─────────────────────────────
CREATE TABLE dbo.policy_master (
    policy_id       VARCHAR(20)   NOT NULL,
    customer_id     VARCHAR(20)   NOT NULL,
    policy_type     VARCHAR(20)   NOT NULL,
    coverage_amount DECIMAL(18,2) NOT NULL,
    premium_amount  DECIMAL(18,2) NOT NULL,
    policy_status   VARCHAR(20)   NOT NULL,
    start_date      DATE          NOT NULL,
    end_date        DATE          NOT NULL,
    last_updated    DATETIME2     NOT NULL,
    CONSTRAINT PK_policy_master
        PRIMARY KEY (policy_id)
);
GO

-- Index for faster joins on customer_id
CREATE INDEX IX_policy_master_customer_id
    ON dbo.policy_master(customer_id);
GO

-- Index for faster filtering on policy_status
CREATE INDEX IX_policy_master_policy_status
    ON dbo.policy_master(policy_status);
GO

-- ── Step 4: Create customer_master ───────────────────────────
CREATE TABLE dbo.customer_master (
    customer_id   VARCHAR(20)  NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    email         VARCHAR(100) NOT NULL,
    phone         VARCHAR(15)  NOT NULL,
    city          VARCHAR(50)  NOT NULL,
    state         VARCHAR(50)  NOT NULL,
    dob           DATE         NOT NULL,
    risk_category VARCHAR(10)  NOT NULL,
    last_updated  DATETIME2    NOT NULL,
    CONSTRAINT PK_customer_master
        PRIMARY KEY (customer_id)
);
GO

-- Index for faster filtering on risk_category
CREATE INDEX IX_customer_master_risk_category
    ON dbo.customer_master(risk_category);
GO

-- Index for faster filtering on state
CREATE INDEX IX_customer_master_state
    ON dbo.customer_master(state);
GO

-- ── Step 5: Verify tables created ────────────────────────────
SELECT
    t.name        AS table_name,
    c.name        AS column_name,
    tp.name       AS data_type,
    c.max_length,
    c.is_nullable
FROM sys.tables t
JOIN sys.columns c
    ON t.object_id = c.object_id
JOIN sys.types tp
    ON c.user_type_id = tp.user_type_id
WHERE t.name IN ('policy_master', 'customer_master')
ORDER BY t.name, c.column_id;
GO

-- ── Step 6: Verify row counts after CSV import ───────────────
-- Run this AFTER importing CSVs using Import Wizard
SELECT 'policy_master'  AS table_name,
       COUNT(*)          AS row_count
FROM dbo.policy_master
UNION ALL
SELECT 'customer_master',
       COUNT(*)
FROM dbo.customer_master;
GO
-- Expected: 1500 and 1000
