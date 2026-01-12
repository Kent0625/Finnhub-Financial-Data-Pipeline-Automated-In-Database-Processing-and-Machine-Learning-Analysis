-- Phase 3: Critical Refactoring (Statistical Corrections)

-- 1. DATA CLEANING & IMPUTATION
-- Fix: Replaced "Zero Imputation" with "Local Moving Average Imputation"
-- If a value is missing, we infer it from the surrounding 4 quarters (Local Mean).
DROP VIEW IF EXISTS v_preprocessed_data;
CREATE VIEW v_preprocessed_data AS
SELECT 
    f.symbol,
    f.period,
    
    -- Imputation Strategy: Backward-looking Moving Average (4 quarters preceding)
    -- Fix: Removed "2 FOLLOWING" to prevent data leakage from the future.
    COALESCE(f.net_margin, AVG(f.net_margin) OVER (
        PARTITION BY symbol ORDER BY period ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )) as net_margin,
    
    COALESCE(f.total_debt_to_equity, AVG(f.total_debt_to_equity) OVER (
        PARTITION BY symbol ORDER BY period ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    )) as total_debt_to_equity,
    
    f.sales_per_share,
    f.eps
FROM financial_metrics f
WHERE f.period_type = 'quarterly';

-- 2. STATIONARITY & TIME ALIGNMENT
-- Fix: Ensure LAGs respect time intervals (approx 90 days)
DROP VIEW IF EXISTS v_model_features;
CREATE VIEW v_model_features AS
SELECT 
    symbol,
    period,
    
    -- Raw values for reconstruction later
    eps as current_eps,
    
    -- TARGET TRANSFORMATION: Predict Growth Rate, NOT raw Price
    -- Formula: (Next EPS - Curr EPS) / Curr EPS
    -- This makes the target STATIONARY.
    (LEAD(eps, 1) OVER (PARTITION BY symbol ORDER BY period) - eps) / NULLIF(eps, 0) as target_eps_growth,

    -- Feature 1: Sales Growth YoY (Stationary)
    -- Logic: Ensure lag is actually 4 quarters ago (approx 360-370 days)
    CASE 
        WHEN (julianday(period) - julianday(LAG(period, 4) OVER (PARTITION BY symbol ORDER BY period))) BETWEEN 350 AND 380 
        THEN (sales_per_share - LAG(sales_per_share, 4) OVER (PARTITION BY symbol ORDER BY period)) / 
             NULLIF(LAG(sales_per_share, 4) OVER (PARTITION BY symbol ORDER BY period), 0)
        ELSE NULL 
    END as sales_growth_yoy,

    -- Feature 2: EPS Momentum (QoQ Growth) (Stationary)
    -- Logic: Ensure lag is 1 quarter ago (approx 80-100 days)
    CASE 
        WHEN (julianday(period) - julianday(LAG(period, 1) OVER (PARTITION BY symbol ORDER BY period))) BETWEEN 80 AND 100
        THEN (eps - LAG(eps, 1) OVER (PARTITION BY symbol ORDER BY period)) / 
             NULLIF(LAG(eps, 1) OVER (PARTITION BY symbol ORDER BY period), 0)
        ELSE NULL
    END as eps_momentum_qoq,

    net_margin,
    total_debt_to_equity

FROM v_preprocessed_data;

-- 3. FINAL SELECTION
-- Drop rows where Lags failed (Time gaps) or Future Target is missing (Last row)
SELECT * 
FROM v_model_features 
WHERE sales_growth_yoy IS NOT NULL 
AND eps_momentum_qoq IS NOT NULL
AND target_eps_growth IS NOT NULL
ORDER BY period ASC;
