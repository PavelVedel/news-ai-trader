"""
Script for updating market data
"""
from apps.market_data.storage.file_manager import MarketDataStorage
from apps.market_data.yahoo.client import YahooFinanceClient
from libs.database.connection import DatabaseConnection

import pandas as pd

FREQ = "1m"

def main():
    db = DatabaseConnection("data/db/news.db")
    # Get all unique symbols from news_raw table
    db_symbols = db.get_all_symbols()
    print(f"Symbols in DB {len(db_symbols)}")
    
    # Initialize storage
    storage = MarketDataStorage(base_path="data/market_data/yahoo")
    storage.set_frequency(FREQ)
    storage.client = YahooFinanceClient()

    print(f"Updating {FREQ} data for {len(db_symbols)} tickers...")
    
    for i_symbol, symbol in enumerate(db_symbols):
        print(f"\n[{i_symbol+1}/{len(db_symbols)}] Processing {symbol}...")
        try:
            storage.update_symbol_1m(symbol)
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
    
    print("\nUpdate completed!")

    # Read data for a specific date (just test)
    df = storage.get_stored_data("AAPL", "2025-08-15", "2025-08-16")
    print(df.head())
    
if __name__ == "__main__":
    main()
