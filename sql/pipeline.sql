-- Phase 3: In-Database Pipelining & Preprocessing
-- Requirements: Merging, Imputation, Normalization, Feature Engineering

-- 1. DATASET MERGING & IMPUTATION
-- Joins the 'companies' dimension with 'financial_metrics' fact table.
-- Handles NULL values using COALESCE (Imputation).
DROP VIEW IF EXISTS v_preprocessed_data;
CREATE VIEW v_preprocessed_data AS
SELECT 
    f.symbol,
    c.sector,
    c.industry,
    f.period,
    
    -- Impute Missing Values
    -- If Net Margin is NULL, assume 0.
    COALESCE(f.net_margin, 0) as net_margin,
    
    -- If Debt/Equity is NULL, assume 0 (conservative approach).
    COALESCE(f.total_debt_to_equity, 0) as total_debt_to_equity,
    
    -- Pass through other critical columns
    f.sales_per_share,
    f.eps
FROM financial_metrics f
JOIN companies c ON f.symbol = c.symbol
WHERE f.period_type = 'quarterly';

-- 2. NORMALIZATION & FEATURE ENGINEERING
-- Calculates relative metrics (Growth) and smoothed trends (Moving Averages).
DROP VIEW IF EXISTS v_model_features;
CREATE VIEW v_model_features AS
SELECT 
    symbol,
    period,
    sector,
    sales_per_share,
    eps,
    
    -- Target: Next Quarter's EPS
    LEAD(eps, 1) OVER (PARTITION BY symbol ORDER BY period) as target_next_eps,

    -- Feature 1: Sales Growth YoY (Normalized Metric)
    -- Calculating percentage change normalizes the scale differences between companies/eras.
    (sales_per_share - LAG(sales_per_share, 4) OVER (PARTITION BY symbol ORDER BY period)) / 
    NULLIF(LAG(sales_per_share, 4) OVER (PARTITION BY symbol ORDER BY period), 0) 
    as sales_growth_yoy,

    -- Feature 2: 4-Quarter Moving Average of EPS (Smoothing)
    AVG(eps) OVER (
        PARTITION BY symbol 
        ORDER BY period 
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) as eps_ma_4q,
    
    -- Feature 3: Min-Max Scaled EPS (within the window)
    -- Showing 'Normalization' concept: Where does current EPS sit relative to the last 4 quarters?
    (eps - MIN(eps) OVER (PARTITION BY symbol ORDER BY period ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)) /
    NULLIF(
        (MAX(eps) OVER (PARTITION BY symbol ORDER BY period ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) - 
         MIN(eps) OVER (PARTITION BY symbol ORDER BY period ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)), 
        0
    ) as eps_relative_strength,

    net_margin,
    total_debt_to_equity

FROM v_preprocessed_data;

-- 3. FINAL DATA SELECTION
-- Filter ready for ML (removing artifacts from Window Functions)
SELECT * 
FROM v_model_features 
WHERE sales_growth_yoy IS NOT NULL 
ORDER BY period DESC
LIMIT 10;