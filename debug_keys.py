import requests

API_KEY = "d5i4ejpr01qmmfjekj7gd5i4ejpr01qmmfjekj80"
BASE_URL = "https://finnhub.io/api/v1"
SYMBOL = "MSFT"

def check_keys():
    url = f"{BASE_URL}/stock/metric"
    params = {'symbol': SYMBOL, 'metric': 'all', 'token': API_KEY}
    response = requests.get(url, params=params)
    data = response.json()
    
    if 'series' in data and 'quarterly' in data['series']:
        print("Available Quarterly Metrics:")
        print(list(data['series']['quarterly'].keys()))

check_keys()
