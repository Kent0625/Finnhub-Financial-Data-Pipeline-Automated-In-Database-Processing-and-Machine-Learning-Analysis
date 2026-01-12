import sqlite3
import pandas as pd

DB_PATH = 'finnhub_data.db'
SQL_SCRIPT = 'sql/pipeline.sql'

def execute_pipeline():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("--- Executing SQL Pipeline ---")
    
    with open(SQL_SCRIPT, 'r') as f:
        script = f.read()
    
    try:
        cursor.executescript(script)
        print("SQL Views created successfully.")
        
        # Verify the Output
        print("\n--- Feature Engineering Output (First 5 Rows) ---")
        query = """
        SELECT symbol, period, sales_per_share, sales_growth_yoy, eps_ma_4q
        FROM v_model_features 
        WHERE sales_growth_yoy IS NOT NULL 
        ORDER BY period DESC 
        LIMIT 5
        """
        
        df = pd.read_sql_query(query, conn)
        print(df.to_string())
        
    except Exception as e:
        print(f"Error executing SQL: {e}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    execute_pipeline()