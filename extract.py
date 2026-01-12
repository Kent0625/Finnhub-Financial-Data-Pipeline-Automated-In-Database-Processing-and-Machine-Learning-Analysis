import time
import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime

# --- Configuration ---
API_KEY = "d5i4ejpr01qmmfjekj7gd5i4ejpr01qmmfjekj80"
BASE_URL = "https://finnhub.io/api/v1"
SYMBOLS = ["MSFT"] 

# Database Connection
DB_CONNECTION = 'sqlite:///finnhub_data.db' 

Base = declarative_base()

# --- Database Schema ---
class Company(Base):
    __tablename__ = 'companies'
    
    symbol = Column(String, primary_key=True)
    name = Column(String)
    sector = Column(String)
    industry = Column(String)
    market_cap = Column(Float)
    
    financials = relationship("FinancialMetric", back_populates="company")

class FinancialMetric(Base):
    __tablename__ = 'financial_metrics'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, ForeignKey('companies.symbol'))
    period = Column(Date) # e.g., 2023-12-31
    period_type = Column(String) # 'quarterly' or 'annual'
    
    # Key Metrics (we will extract these from the 'series' data)
    sales_per_share = Column(Float) # Using Sales Per Share as Revenue proxy
    eps = Column(Float) # Earnings Per Share
    net_margin = Column(Float)
    total_debt_to_equity = Column(Float)
    
    company = relationship("Company", back_populates="financials")

# --- API Functions ---
def get_company_profile(symbol):
    url = f"{BASE_URL}/stock/profile2"
    params = {'symbol': symbol, 'token': API_KEY}
    response = requests.get(url, params=params)
    
    if response.status_code == 429:
        print("Rate limit hit (429). Sleeping for 60 seconds...")
        time.sleep(60)
        return get_company_profile(symbol)
        
    if response.status_code != 200:
        print(f"Error fetching profile for {symbol}: {response.status_code} - {response.text}")
        return None

    return response.json()

def get_basic_financials(symbol):
    """
    Fetches basic financials including 'series' data which contains 
    historical quarterly/annual metrics.
    """
    url = f"{BASE_URL}/stock/metric"
    params = {'symbol': symbol, 'metric': 'all', 'token': API_KEY}
    
    response = requests.get(url, params=params)
    
    if response.status_code == 429:
        time.sleep(60)
        return get_basic_financials(symbol)
    
    if response.status_code != 200:
        print(f"Error fetching financials for {symbol}: {response.status_code}")
        return None
        
    return response.json()

# --- ETL Process ---
def run_pipeline():
    # 1. Setup Database
    engine = create_engine(DB_CONNECTION)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    print(f"Starting extraction for symbols: {SYMBOLS}")

    for symbol in SYMBOLS:
        print(f"\nProcessing {symbol}...")
        time.sleep(1) 

        # 2. Extract & Load Company Profile
        profile_data = get_company_profile(symbol)
        if profile_data and 'name' in profile_data:
            company = session.query(Company).filter_by(symbol=symbol).first()
            if not company:
                company = Company(
                    symbol=symbol,
                    name=profile_data.get('name'),
                    sector=profile_data.get('finnhubIndustry'),
                    industry=profile_data.get('finnhubIndustry'),
                    market_cap=profile_data.get('marketCapitalization')
                )
                session.add(company)
                print(f"  Added company info for {symbol}")
            else:
                print(f"  Company info for {symbol} already exists")
            session.commit()
        else:
            print(f"  Could not fetch valid profile for {symbol}")

        # 3. Extract & Load Financial Metrics (Time Series)
        fin_data = get_basic_financials(symbol)
        
        if fin_data and 'series' in fin_data:
            series_data = fin_data['series']
            
            # We will focus on 'quarterly' data as it provides more points for analysis
            if 'quarterly' in series_data:
                quarterly = series_data['quarterly']
                
                # The data structure is { 'metric_name': [ {'period': '2023-12-31', 'v': 100}, ... ] }
                # We need to pivot this to: Period | Revenue | EPS | ...
                
                # 1. Collect all unique periods
                periods = set()
                # Check a common metric to get periods
                if 'eps' in quarterly:
                    for entry in quarterly['eps']:
                        periods.add(entry['period'])
                
                sorted_periods = sorted(list(periods))
                print(f"  Found {len(sorted_periods)} quarterly periods.")
                
                new_records = 0
                for p_date_str in sorted_periods:
                    # Helper to safely get value for a specific period
                    def get_val(metric_key):
                        if metric_key in quarterly:
                            for item in quarterly[metric_key]:
                                if item['period'] == p_date_str:
                                    return item['v']
                        return None

                    p_date = datetime.strptime(p_date_str, "%Y-%m-%d").date()
                    
                    # Check if record exists
                    existing = session.query(FinancialMetric).filter_by(symbol=symbol, period=p_date, period_type='quarterly').first()
                    
                    if not existing:
                        record = FinancialMetric(
                            symbol=symbol,
                            period=p_date,
                            period_type='quarterly',
                            sales_per_share=get_val('salesPerShare'), # Changed from revenue
                            eps=get_val('eps'),     # EPS
                            net_margin=get_val('netMargin'), # Net Margin
                            total_debt_to_equity=get_val('totalDebtToEquity') # Debt/Equity
                        )
                        session.add(record)
                        new_records += 1
                
                session.commit()
                print(f"  Staged {new_records} new quarterly records.")
                
            else:
                print("  No quarterly series data found.")
        else:
            print("  No financial series data returned.")

    session.close()
    print("\nPipeline completed.")

if __name__ == "__main__":
    run_pipeline()
