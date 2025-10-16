# Market Data ETL

A Python-based ETL system for collecting and storing options chain data from the S&P 500 and major index ETFs using async processing for improved performance.

## Purpose

This project provides a fast, efficient way to collect comprehensive options data for:
- All S&P 500 companies
- Major index ETFs (SPY, QQQ, IWM, DIA)
- Multiple expiration dates per symbol
- Both stock information and options chain data

## Key Features

- **Async Processing**: 1.8x faster than traditional sequential methods
- **Comprehensive Coverage**: Up to 30 expiration dates per symbol
- **ETF Support**: Includes major index ETFs alongside S&P 500 stocks
- **SQLite Database**: Local storage with efficient querying
- **Rate Limiting**: Respectful API usage to avoid service limits

## Quick Start

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Basic Usage

**Fetch S&P 500 + ETFs with default settings:**
```bash
python main.py --include-etfs
```

**Fetch only index ETFs:**
```bash
python main.py --etfs-only
```

**Fetch specific symbols:**
```bash
python main.py --symbols AAPL MSFT SPY QQQ
```

**Conservative settings (recommended for full runs):**
```bash
python main.py --include-etfs --max-expiration-dates 10 --rate-limit 0.5 --max-concurrent 2
```

## Files

- `main.py` - Main script for data collection
- `src/scrapers/` - Async options data scrapers
- `src/database.py` - Database operations
- `src/models.py` - Data models
- `data/` - CSV files and SQLite database
- `scripts/` - Utility scripts for querying and updates

## Configuration

Key parameters:
- `--max-expiration-dates`: Number of expiration dates per symbol (default: 30)
- `--rate-limit`: Delay between requests in seconds (default: 0.05)
- `--max-concurrent`: Maximum concurrent requests (default: 15)
- `--include-etfs`: Add index ETFs to S&P 500 processing
- `--stats`: Show database statistics

## Database

Uses SQLite for local storage with tables for:
- `stock_info` - Company information
- `options_chain` - Options contract data

Query the data using the provided scripts in the `scripts/` directory.
