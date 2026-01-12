import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

# Configuration
DB_PATH = 'finnhub_project/finnhub_data.db'
SYMBOL = 'MSFT'

def analyze():
    print(f"--- Starting Analysis for {SYMBOL} ---")
    
    # 1. Load Data
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT period, eps, target_next_eps, sales_growth_yoy, eps_ma_4q, net_margin, total_debt_to_equity
    FROM v_model_features
    WHERE symbol = ?
    ORDER BY period ASC
    """
    df = pd.read_sql_query(query, conn, params=(SYMBOL,))
    conn.close()
    
    # Convert period to datetime
    df['period'] = pd.to_datetime(df['period'])
    
    # 2. Preprocessing
    print(f"Total Records: {len(df)}")
    # Drop rows with NaNs (created by Lag/Lead/Rolling functions)
    df_clean = df.dropna()
    print(f"Clean Records for ML: {len(df_clean)}")
    
    if len(df_clean) < 10:
        print("Not enough data for ML.")
        return

    # 3. EDA Visualization
    plt.figure(figsize=(10, 6))
    plt.plot(df['period'], df['eps'], label='Actual EPS', marker='o')
    plt.plot(df['period'], df['eps_ma_4q'], label='4-Qtr Moving Avg', linestyle='--')
    plt.title(f'{SYMBOL} Quarterly EPS & Trend')
    plt.xlabel('Date')
    plt.ylabel('Earnings Per Share ($)')
    plt.legend()
    plt.grid(True)
    plt.savefig('finnhub_project/eda_eps_trend.png')
    print("EDA Plot saved to finnhub_project/eda_eps_trend.png")

    # 4. Machine Learning (Regression)
    # Goal: Predict Next Quarter's EPS
    features = ['sales_growth_yoy', 'net_margin', 'total_debt_to_equity', 'eps_ma_4q']
    target = 'target_next_eps'
    
    X = df_clean[features]
    y = df_clean[target]
    
    # Time-based split (Train on past, Test on recent)
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Predictions
    y_pred = model.predict(X_test)
    
    # 5. Evaluation
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    
    print("\n--- Model Evaluation ---")
    print(f"Model: Linear Regression")
    print(f"Features: {features}")
    print(f"Test Set Size: {len(y_test)} quarters")
    print(f"RMSE: ${rmse:.4f}")
    print(f"R² Score: {r2:.4f}")
    
    print("\n--- Coefficients ---")
    for feat, coef in zip(features, model.coef_):
        print(f"{feat}: {coef:.4f}")
        
    # ... (Previous Regression Code)
    # Visualization of Prediction
    plt.figure(figsize=(10, 6))
    plt.plot(y_test.index, y_test.values, label='Actual Next EPS', marker='o')
    plt.plot(y_test.index, y_pred, label='Predicted Next EPS', marker='x', linestyle='--')
    plt.title('Actual vs Predicted EPS (Test Set)')
    plt.legend()
    plt.savefig('finnhub_project/ml_prediction.png')
    print("Prediction Plot saved to finnhub_project/ml_prediction.png")

    # ---------------------------------------------------------
    # PART 2: Classification (Predicting Growth vs. Decline)
    # ---------------------------------------------------------
    print("\n--- PART 2: Classification (Random Forest) ---")
    
    # Create Binary Target: 1 if Next EPS > Current EPS, else 0
    df_clean['target_direction'] = (df_clean['target_next_eps'] > df_clean['eps']).astype(int)
    
    # Check class balance
    print(f"Class Distribution (1=Growth, 0=Decline):\n{df_clean['target_direction'].value_counts()}")
    
    y_class = df_clean['target_direction']
    
    # Split Data
    X_train_c, X_test_c = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train_c, y_test_c = y_class.iloc[:split_idx], y_class.iloc[split_idx:]
    
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
    
    # Train Random Forest
    rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_model.fit(X_train_c, y_train_c)
    
    # Predict
    y_pred_c = rf_model.predict(X_test_c)
    
    # Evaluate
    acc = accuracy_score(y_test_c, y_pred_c)
    cm = confusion_matrix(y_test_c, y_pred_c)
    
    print(f"\nModel: Random Forest Classifier")
    print(f"Accuracy: {acc:.4f}")
    print("Confusion Matrix:")
    print(cm)
    print("\nClassification Report:")
    print(classification_report(y_test_c, y_pred_c))
    
    # Feature Importance
    importances = rf_model.feature_importances_
    indices = np.argsort(importances)[::-1]
    print("\nFeature Ranking:")
    for f in range(X.shape[1]):
        print(f"{f+1}. {features[indices[f]]} ({importances[indices[f]]:.4f})")

if __name__ == "__main__":
    analyze()
