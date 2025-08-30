"""
Скрипт для обновления рыночных данных
"""
from apps.market_data.storage.file_manager import MarketDataStorage
from apps.market_data.yahoo.client import YahooFinanceClient
from libs.database.connection import DatabaseConnection

import pandas as pd

FREQ = "1m"
# SYMBOLS = ["AAPL", "IBKR", "NVDA"]

def main():
    db = DatabaseConnection("data/db/news.db")
    # Получаем все уникальные symbols из таблицы news_raw
    db_symbols = db.get_all_symbols()
    print(f"Symbols from DB: {db_symbols}")
    
    # Инициализируем хранилище
    storage = MarketDataStorage(base_path="data/market_data/yahoo")
    storage.set_frequency(FREQ)
    storage.client = YahooFinanceClient()

    print(f"Обновление {FREQ} данных для {len(db_symbols)} тикеров...")
    
    for i_symbol, symbol in enumerate(db_symbols):
        print(f"\n[{i_symbol+1}/{len(db_symbols)}] Обработка {symbol}...")
        try:
            storage.update_symbol_1m(symbol)
        except Exception as e:
            print(f"Ошибка при обработке {symbol}: {e}")
    
    print("\nОбновление завершено!")

    # Читаем данные за конкретную дату
    df = storage.get_stored_data("AAPL", "2025-08-15", "2025-08-16")
    print(df.head())
    
if __name__ == "__main__":
    main()
