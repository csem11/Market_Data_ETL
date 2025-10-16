import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import urllib3
import ssl

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)





def get_sp500_from_wikipedia() -> pd.DataFrame:
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    
    # Set proper headers including User-Agent to comply with Wikipedia's robot policy
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Use requests with SSL verification disabled and proper headers
    response = requests.get(url, verify=False, headers=headers)
    
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch Wikipedia page. Status code: {response.status_code}")
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the S&P 500 constituents table by its ID
    table = soup.find('table', {'id': 'constituents'})
    
    if table is None:
        raise ValueError("Could not find S&P 500 constituents table with id 'constituents'")
    
    # Use StringIO to wrap the HTML string as recommended
    df = pd.read_html(StringIO(str(table)))[0]
    return df