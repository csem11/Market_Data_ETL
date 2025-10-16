#!/usr/bin/env python3
"""
Script to update S&P 500 data from Wikipedia
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.scrapers import get_sp500_from_wikipedia


def main():
    """Main function to update S&P 500 data"""
    print("Fetching S&P 500 data from Wikipedia...")
    
    try:
        df = get_sp500_from_wikipedia()
        print(f"Successfully fetched {len(df)} companies")
        print(df.head())
        
        # Save to data directory relative to project root
        data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
        os.makedirs(data_dir, exist_ok=True)
        csv_path = os.path.join(data_dir, 'sp500_companies.csv')
        df.to_csv(csv_path, index=False)
        print(f"Data saved to: {csv_path}")
        
    except Exception as e:
        print(f"Error fetching S&P 500 data: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
