"""
Script for updating market data
"""
from apps.market_data.storage.file_manager import MarketDataStorage
from apps.market_data.yahoo.client import YahooFinanceClient
from libs.database.connection import DatabaseConnection
import time
import pandas as pd
import argparse

FREQ = "1m"

def main(refresh_current_day: bool = False):
    db = DatabaseConnection("data/db/news.db")
    # Get all unique symbols from news_raw table
    db_symbols = db.get_all_symbols()
    print(f"Symbols in DB {len(db_symbols)}")
    
    # Initialize storage
    storage = MarketDataStorage(base_path="data/market_data/yahoo")
    storage.set_frequency(FREQ)
    storage.client = YahooFinanceClient()

    tic = time.time()

    print(f"Updating {FREQ} data for {len(db_symbols)} tickers...")
    if refresh_current_day:
        print("Refresh current day mode: will update even if today's file exists")
    else:
        print("Skip mode: will skip symbols if today's file already exists")
    
    for i_symbol, symbol in enumerate(db_symbols, start=1):
        print(f"\n[{i_symbol}/{len(db_symbols)}|{(i_symbol)/len(db_symbols)*100:.2f}%] Processing {symbol}...")
        if symbol.startswith("$"):
            print(f"Skipping {symbol} ...")
            continue
        try:
            storage.update_symbol_1m(symbol, refresh_current_day=refresh_current_day)
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
        toc = time.time()
        approx_time_left = (toc-tic)/i_symbol*(len(db_symbols)-i_symbol)
        approx_time_left_str = f"{int(approx_time_left // 60):02d}:{int(approx_time_left % 60):02d}"
        print(f"Approximate time left: {approx_time_left_str}")
    print("\nUpdate completed!")

    # Read data for a specific date (just test)
    df = storage.get_stored_data("AAPL", "2025-08-15", "2025-08-16")
    print(df.head())
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update market data from Yahoo Finance",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-r", "--refresh-current-day",
        action="store_true",
        default=False,
        help="Refresh data even if today's parquet file already exists (default: skip if exists)"
    )
    args = parser.parse_args()
    
    main(refresh_current_day=args.refresh_current_day)
