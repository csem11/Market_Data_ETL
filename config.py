"""
Configuration file for market data ETL project
"""

import os
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
OPTIONS_DATA_DIR = DATA_DIR / "options"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

# Database configuration
DATABASE_PATH = OPTIONS_DATA_DIR / "market_data.db"

# API configuration
RATE_LIMIT_DELAY = 0.1  # Seconds between API calls
MAX_RETRIES = 3
TIMEOUT = 30

# Options data configuration
DEFAULT_MAX_EXPIRATION_DATES = 30
# DEFAULT_DATA_RETENTION_DAYS = 30  # Disabled - keeping all data

# S&P 500 data
SP500_CSV_PATH = DATA_DIR / "sp500_companies.csv"

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Ensure directories exist
OPTIONS_DATA_DIR.mkdir(parents=True, exist_ok=True)
