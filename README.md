# Finnhub Financial Data Pipeline: Automated In-Database Processing and Machine Learning Analysis

## Project Overview

This project implements an automated end-to-end data pipeline designed to extract, process, and analyze financial market data for Microsoft Corporation (MSFT). By leveraging the Finnhub API, the system harvests quarterly financial fundamentals (such as Sales Per Share, Earnings Per Share, and Net Margins) and stores them in a normalized relational database.

The core value of this system lies in its usage of "In-Database Pipelining." Rather than processing data in external scripts, data cleaning, normalization, and feature engineering are handled directly within the database using SQL Views and Window Functions.

Finally, the processed data flows into a Machine Learning module that utilizes Linear Regression to forecast future **EPS Growth Rates**, rigorously evaluated against a Naive Baseline model.

## Key Features

1.  **Automated Data Extraction**: A robust Python script that connects to the Finnhub API, handles rate limiting, and extracts over 30 years of historical financial data.
2.  **Normalized Database Design**: A Star Schema architecture that separates company metadata from quantitative financial metrics, ensuring data integrity.
3.  **In-Database Processing**: Advanced SQL logic (Window Functions, Imputation, and Normalization) allows for the automated calculation of complex metrics like Year-over-Year Growth and Moving Averages without external code.
4.  **Rigorous Evaluation**: A statistically sound ML workflow that predicts stationary growth rates (not raw prices) and compares performance against a Persistence Baseline using MAPE (Mean Absolute Percentage Error).

## Technical Architecture

*   **Language**: Python 3.x
*   **Database**: SQLite (SQLAlchemy ORM)
*   **Analysis Libraries**: Pandas, Scikit-Learn, Matplotlib
*   **Data Source**: Finnhub API (Standard/Free Tier compatible)

## Project Structure

*   **extract.py**: The main entry point for the ETL (Extract, Transform, Load) process. It fetches data and populates the database.
*   **finnhub_data.db**: The SQLite database file containing the raw and processed data.
*   **sql/pipeline.sql**: SQL scripts defining the Views for data cleaning, feature engineering, and normalization.
*   **ml_analysis.py**: A Python script that connects to the database, performs Exploratory Data Analysis (EDA), and trains the Machine Learning models.
*   **Phase3_Analysis.ipynb**: A comprehensive Jupyter Notebook documenting the analysis workflow, visualizations, and model evaluation.
*   **schema_design.md**: Technical documentation of the database schema and normalization logic.

## Setup Instructions

### Prerequisites
Ensure you have Python installed on your system. It is recommended to use a virtual environment.

### Installation

1.  Clone the repository to your local machine.
2.  Install the required Python packages:
    ```bash
    pip install pandas sqlalchemy requests scikit-learn matplotlib seaborn
    ```

## Usage Instructions

### 1. Run the Data Pipeline
To initialize the database and fetch the latest financial data, execute the extraction script:

```bash
python extract.py
```
This will create (or update) the `finnhub_data.db` file and populate it with the latest quarterly metrics for MSFT.

### 2. Verify SQL Processing
To verify that the database views and feature engineering steps are working correctly, you can run the pipeline verification script:

```bash
python run_sql_pipeline.py
```
This will output the first few rows of the processed training data, including calculated fields like Sales Growth and Moving Averages.

### 3. Run Analysis and Modeling
To perform the statistical analysis and train the predictive models, run:

```bash
python ml_analysis.py
```
This script will:
*   Generate trend visualization charts (saved as images in the project folder).
*   Train the Linear Regression model and output the MAPE (Mean Absolute Percentage Error).
*   Compare performance against a Naive Baseline model.

Alternatively, you can open `Phase3_Analysis.ipynb` in Jupyter Notebook for an interactive walkthrough of the analysis.