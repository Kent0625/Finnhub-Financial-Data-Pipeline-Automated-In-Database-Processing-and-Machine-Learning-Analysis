# Phase 2: Database Schema Design

## 1. ER Diagram Description

The database follows a **Star Schema** (or strictly, a normalized relational design) suitable for time-series analysis.

*   **`companies` (Dimension Table)**:
    *   Stores static/slowly-changing attributes of an entity.
    *   **PK**: `symbol` (e.g., 'MSFT')
    *   Attributes: Name, Sector, Industry, Market Cap.

*   **`financial_metrics` (Fact Table)**:
    *   Stores quantitative, historical data points linked to the company.
    *   **PK**: `id` (Auto-increment)
    *   **FK**: `symbol` (Links to `companies.symbol`)
    *   Attributes: Period (Date), Type (Annual/Quarterly), Revenue, EPS, Net Margin, Debt/Equity.

## 2. Normalization Justification
*   **1NF**: All columns contain atomic values.
*   **2NF**: All non-key attributes are dependent on the primary key.
*   **3NF**: No transitive dependencies (e.g., Company Sector depends only on Symbol, not on the Financial Report ID).

## 3. Data Dictionary

| Table | Column | Type | Description |
|-------|--------|------|-------------|
| companies | symbol | VARCHAR | Stock Ticker (PK) |
| companies | sector | VARCHAR | Economic Sector |
| financial_metrics | period | DATE | End date of the reporting quarter |
| financial_metrics | revenue | FLOAT | Total Revenue for the period |
| financial_metrics | eps | FLOAT | Earnings Per Share |
