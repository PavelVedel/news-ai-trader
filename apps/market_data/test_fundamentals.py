"""
Тестовый скрипт для проверки загрузки фундаментальных данных
"""

import yfinance as yf
import json
from pprint import pprint

def test_yahoo_finance():
    """Тестируем загрузку данных с Yahoo Finance"""
    
    # Тестируем на популярных символах
    test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
    
    for symbol in test_symbols:
        print(f"\n{'='*60}")
        print(f"ТЕСТИРУЮ {symbol}")
        print(f"{'='*60}")
        
        try:
            # Создаем объект Ticker
            ticker = yf.Ticker(symbol)
            
            # Получаем основную информацию
            info = ticker.info
            
            if info:
                print(f"✓ Данные получены для {symbol}")
                
                # Показываем ключевые поля
                key_fields = {
                    'marketCap': 'Рыночная капитализация',
                    'trailingPE': 'P/E коэффициент',
                    'sector': 'Сектор',
                    'industry': 'Отрасль',
                    'country': 'Страна',
                    'beta': 'Бета',
                    'dividendYield': 'Дивидендная доходность'
                }
                
                for field, description in key_fields.items():
                    value = info.get(field, 'N/A')
                    print(f"{description}: {value}")
                
                # Показываем все доступные поля (первые 20)
                print(f"\nВсего доступных полей: {len(info)}")
                print("Первые 20 полей:")
                for i, (key, value) in enumerate(list(info.items())[:20]):
                    print(f"  {key}: {value}")
                    
            else:
                print(f"✗ Нет данных для {symbol}")
                
        except Exception as e:
            print(f"✗ Ошибка при загрузке {symbol}: {e}")
    
    print(f"\n{'='*60}")
    print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print(f"{'='*60}")

if __name__ == "__main__":
    test_yahoo_finance()
