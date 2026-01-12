import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'finnhub_data.db')
SYMBOL = 'MSFT'

def mean_absolute_percentage_error(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    # Avoid division by zero
    mask = y_true != 0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

def analyze():
    print(f"--- Starting Analysis for {SYMBOL} ---")
    
    # 1. Load Data
    conn = sqlite3.connect(DB_PATH)
    # We need 'current_eps' to reconstruct the actual prices for MAPE
    query = """
    SELECT period, current_eps, target_eps_growth, sales_growth_yoy, eps_momentum_qoq, net_margin, total_debt_to_equity
    FROM v_model_features
    WHERE symbol = ?
    ORDER BY period ASC
    """
    df = pd.read_sql_query(query, conn, params=(SYMBOL,))
    conn.close()
    
    df['period'] = pd.to_datetime(df['period'])
    
    # 2. Preprocessing
    print(f"Total Records: {len(df)}")
    df_clean = df.dropna()
    print(f"Clean Records for ML: {len(df_clean)}")
    
    if len(df_clean) < 10:
        print("Not enough data.")
        return

    # 3. EDA: Stationarity Check
    plt.figure(figsize=(10, 6))
    plt.plot(df_clean['period'], df_clean['target_eps_growth'], label='EPS Growth Rate')
    plt.axhline(0, color='red', linestyle='--')
    plt.title('Stationarity Check: EPS Growth Rate over Time')
    plt.ylabel('Growth Rate')
    plt.savefig(os.path.join(BASE_DIR, 'eda_stationarity.png'))
    print("EDA Plot saved.")

    # 4. Machine Learning
    features = ['sales_growth_yoy', 'eps_momentum_qoq', 'net_margin', 'total_debt_to_equity']
    target = 'target_eps_growth'
    
    X = df_clean[features]
    y = df_clean[target]
    
    # Time-Series Split
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    
    # Metadata for reconstruction
    test_dates = df_clean['period'].iloc[split_idx:]
    current_eps_test = df_clean['current_eps'].iloc[split_idx:]
    
    # A. Naive Baseline (Predict 0% growth aka Persistence Model)
    y_pred_naive = np.zeros(len(y_test))
    
    # B. Linear Regression
    model = LinearRegression()
    model.fit(X_train, y_train)
    y_pred_lr = model.predict(X_test)
    
    # 5. Evaluation (Reconstructed)
    # Actual Future EPS = Current EPS * (1 + Actual Growth)
    actual_future_eps = current_eps_test * (1 + y_test)
    
    # Predicted Future EPS (LR) = Current EPS * (1 + Predicted Growth)
    pred_future_eps_lr = current_eps_test * (1 + y_pred_lr)
    
    # Predicted Future EPS (Naive) = Current EPS * (1 + 0)
    pred_future_eps_naive = current_eps_test
    
    # Metrics
    rmse_lr = np.sqrt(mean_squared_error(actual_future_eps, pred_future_eps_lr))
    rmse_naive = np.sqrt(mean_squared_error(actual_future_eps, pred_future_eps_naive))
    
    mape_lr = mean_absolute_percentage_error(actual_future_eps, pred_future_eps_lr)
    mape_naive = mean_absolute_percentage_error(actual_future_eps, pred_future_eps_naive)
    
    print("\n--- Model Evaluation (Reconstructed EPS) ---")
    print(f"Test Set Size: {len(y_test)} quarters")
    print(f"Naive Baseline RMSE: ${rmse_naive:.4f}")
    print(f"Linear Regression RMSE: ${rmse_lr:.4f}")
    
    print(f"Naive Baseline MAPE: {mape_naive:.2f}%")
    print(f"Linear Regression MAPE: {mape_lr:.2f}%")
    
    if rmse_lr < rmse_naive:
        print("RESULT: Linear Regression BEATS the Baseline.")
    else:
        print("RESULT: Linear Regression FAILS to beat the Baseline.")

    # 6. Residual Analysis
    residuals = actual_future_eps - pred_future_eps_lr
    plt.figure(figsize=(10, 6))
    plt.scatter(test_dates, residuals)
    plt.axhline(0, color='red', linestyle='--')
    plt.title('Residual Analysis (Heteroscedasticity Check)')
    plt.ylabel('Error ($)')
    plt.savefig(os.path.join(BASE_DIR, 'ml_residuals.png'))
    print("Residual Plot saved.")

    # 7. Prediction Plot
    plt.figure(figsize=(10, 6))
    plt.plot(test_dates, actual_future_eps, label='Actual Future EPS', marker='o')
    plt.plot(test_dates, pred_future_eps_lr, label='Predicted (LR)', marker='x', linestyle='--')
    plt.plot(test_dates, pred_future_eps_naive, label='Naive Baseline', linestyle=':', alpha=0.5)
    plt.title('Forecast: Actual vs Predicted EPS')
    plt.legend()
    plt.savefig(os.path.join(BASE_DIR, 'ml_forecast.png'))
    print("Forecast Plot saved.")

if __name__ == "__main__":
    analyze()