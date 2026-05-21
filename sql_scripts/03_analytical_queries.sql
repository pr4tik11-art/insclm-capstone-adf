-- =============================================================
-- 03_analytical_queries.sql
-- 3 Required Analytical Queries for Capstone 06
--
-- Run these in Azure Data Studio or SSMS after NB_09 loads
-- all reporting tables.
--
-- Query 1: Approval and Rejection Rate by Policy Type
-- Query 2: Top 10 High-Risk States by Suspicious Claims
-- Query 3: Top 10 Customers by Suspicious Claim Amount
-- =============================================================

USE sqldb-insclm-capstone;
GO

-- =============================================================
-- QUERY 1
-- Approval and Rejection Rate by Policy Type
--
-- Business purpose:
--   Shows which policy types have the highest approval rates
--   and which have the most rejections. Helps underwriting
--   and product teams decide where to tighten risk controls.
--
-- Demonstrates:
--   GROUP BY + aggregation + window functions +
--   business KPI calculations
-- =============================================================

SELECT
    policy_type,
    SUM(total_claims)               AS total_claims,
    SUM(approved_claims)            AS total_approved,
    SUM(rejected_claims)            AS total_rejected,
    SUM(pending_claims)             AS total_pending,
    SUM(investigation_claims)       AS total_investigation,
    ROUND(SUM(total_claim_amount),2) AS total_amount,
    ROUND(AVG(avg_claim_amount), 2)  AS avg_claim_amount,
    ROUND(
        AVG(approval_rate_pct), 2
    )                               AS avg_approval_rate_pct,
    ROUND(
        AVG(rejection_rate_pct), 2
    )                               AS avg_rejection_rate_pct,
    RANK() OVER (
        ORDER BY SUM(approved_claims) DESC
    )                               AS approval_rank
FROM reporting.fact_claim_summary
GROUP BY policy_type
ORDER BY total_claims DESC;
GO

-- =============================================================
-- QUERY 2
-- Top 10 High-Risk States by Suspicious Claims
--
-- Business purpose:
--   Identifies which states have the highest concentration
--   of suspicious claims. Helps investigation teams
--   prioritise resources by geography.
--
-- Demonstrates:
--   TOP N + multi-level aggregation + CASE expressions +
--   risk segmentation
-- =============================================================

SELECT TOP 10
    state,
    COUNT(*)                        AS total_suspicious_claims,
    COUNT(DISTINCT customer_id)     AS unique_customers,
    ROUND(SUM(claim_amount), 2)     AS total_claim_amount,
    ROUND(AVG(claim_amount), 2)     AS avg_claim_amount,
    ROUND(AVG(
        CAST(suspicious_score AS FLOAT)
    ), 2)                           AS avg_suspicious_score,
    SUM(
        CASE WHEN suspicion_level = 'HIGH'
             THEN 1 ELSE 0 END
    )                               AS high_risk_count,
    SUM(
        CASE WHEN suspicion_level = 'MEDIUM'
             THEN 1 ELSE 0 END
    )                               AS medium_risk_count,
    SUM(
        CASE WHEN suspicion_level = 'LOW'
             THEN 1 ELSE 0 END
    )                               AS low_risk_count,
    SUM(
        CASE WHEN policy_status_at_claim
                  IN ('Lapsed','Cancelled','Expired')
             THEN 1 ELSE 0 END
    )                               AS inactive_policy_claims,
    SUM(
        CASE WHEN document_status = 'Incomplete'
             THEN 1 ELSE 0 END
    )                               AS incomplete_doc_claims
FROM reporting.fact_suspicious_claims
GROUP BY state
ORDER BY total_suspicious_claims DESC;
GO

-- =============================================================
-- QUERY 3
-- Top 10 Customers by Suspicious Claim Amount with Ranking
--
-- Business purpose:
--   Identifies the customers generating the most suspicious
--   claim value. These are the highest priority cases for
--   the fraud investigation team.
--
-- Demonstrates:
--   CTE + window functions + RANK() + multi-table logic +
--   risk scoring
-- =============================================================

WITH customer_totals AS (
    SELECT
        customer_id,
        customer_name,
        state,
        risk_category,
        COUNT(DISTINCT claim_id)        AS total_suspicious_claims,
        ROUND(SUM(claim_amount), 2)     AS total_suspicious_amount,
        ROUND(AVG(claim_amount), 2)     AS avg_claim_amount,
        MAX(customer_claim_count)       AS overall_claim_count,
        MAX(suspicious_score)           AS max_suspicious_score,
        SUM(
            CASE WHEN suspicion_level = 'HIGH'
                 THEN 1 ELSE 0 END
        )                               AS high_risk_claims,
        SUM(
            CASE WHEN suspicion_level = 'MEDIUM'
                 THEN 1 ELSE 0 END
        )                               AS medium_risk_claims,
        SUM(
            CASE WHEN policy_status_at_claim
                      IN ('Lapsed','Cancelled','Expired')
                 THEN 1 ELSE 0 END
        )                               AS inactive_policy_claims,
        SUM(
            CASE WHEN document_status = 'Incomplete'
                 THEN 1 ELSE 0 END
        )                               AS incomplete_doc_claims,
        STRING_AGG(
            DISTINCT claim_reason, ', '
        )                               AS claim_reasons
    FROM reporting.fact_suspicious_claims
    GROUP BY
        customer_id,
        customer_name,
        state,
        risk_category
),
ranked_customers AS (
    SELECT
        customer_id,
        customer_name,
        state,
        risk_category,
        total_suspicious_claims,
        total_suspicious_amount,
        avg_claim_amount,
        overall_claim_count,
        max_suspicious_score,
        high_risk_claims,
        medium_risk_claims,
        inactive_policy_claims,
        incomplete_doc_claims,
        claim_reasons,
        RANK() OVER (
            ORDER BY total_suspicious_amount DESC
        )                               AS suspicious_rank,
        RANK() OVER (
            ORDER BY total_suspicious_claims DESC
        )                               AS frequency_rank
    FROM customer_totals
)
SELECT TOP 10
    suspicious_rank,
    customer_id,
    customer_name,
    state,
    risk_category,
    total_suspicious_claims,
    total_suspicious_amount,
    avg_claim_amount,
    overall_claim_count,
    max_suspicious_score,
    high_risk_claims,
    inactive_policy_claims,
    incomplete_doc_claims,
    frequency_rank,
    claim_reasons
FROM ranked_customers
ORDER BY suspicious_rank;
GO

-- =============================================================
-- BONUS: Quick validation queries
-- Run these to verify data loaded correctly
-- =============================================================

-- Check row counts
SELECT 'fact_claim_summary'    AS table_name,
        COUNT(*)                AS rows
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

-- Check SCD Type 2 integrity
-- Must return 0 — only 1 current row per policy
SELECT
    policy_id,
    COUNT(*) AS current_versions
FROM reporting.dim_policy_history
WHERE is_current = 1
GROUP BY policy_id
HAVING COUNT(*) > 1;
GO
-- Expected: 0 rows

-- Check suspicious score distribution
SELECT
    suspicious_score,
    suspicion_level,
    COUNT(*)            AS claim_count,
    ROUND(AVG(claim_amount), 2) AS avg_amount
FROM reporting.fact_suspicious_claims
GROUP BY suspicious_score, suspicion_level
ORDER BY suspicious_score;
GO
