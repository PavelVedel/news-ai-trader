"""
Скрипт для извлечения списка символов с данными на 7 и более дней
"""

import os
import csv
from pathlib import Path
from datetime import datetime, timedelta

def count_trading_days(symbol_path: Path) -> int:
    """
    Подсчитать количество торговых дней для символа
    """
    try:
        # Получаем список всех parquet файлов
        parquet_files = list(symbol_path.glob("*.parquet"))
        
        if not parquet_files:
            return 0
        
        # Сортируем файлы по дате
        parquet_files.sort()
        
        # Подсчитываем уникальные даты
        trading_days = set()
        for file_path in parquet_files:
            # Извлекаем дату из имени файла (формат: YYYY-MM-DD.parquet)
            date_str = file_path.stem  # убираем расширение .parquet
            try:
                # Проверяем, что это валидная дата
                datetime.strptime(date_str, '%Y-%m-%d')
                trading_days.add(date_str)
            except ValueError:
                # Пропускаем файлы с неправильным форматом даты
                continue
        
        return len(trading_days)
        
    except Exception as e:
        print(f"Ошибка при подсчете дней для {symbol_path.name}: {e}")
        return 0

def extract_symbols_with_data(min_days: int = 7) -> list:
    """
    Извлечь символы с данными на указанное количество дней и более
    """
    # Получаем путь к родительской директории (data) и поднимаемся на уровень выше
    market_data_path = Path(__file__).parent / "market_data/yahoo/1m"
    
    if not market_data_path.exists():
        print(f"Папка {market_data_path} не найдена!")
        return []
    
    symbols_with_data = []
    total_symbols = 0
    
    print(f"Анализирую символы в {market_data_path}...")
    print(f"Минимальное количество дней: {min_days}")
    print("-" * 60)
    
    # Проходим по всем папкам символов
    for symbol_dir in market_data_path.iterdir():
        if symbol_dir.is_dir():
            total_symbols += 1
            symbol_name = symbol_dir.name
            
            # Подсчитываем количество торговых дней
            trading_days = count_trading_days(symbol_dir)
            
            if trading_days >= min_days:
                symbols_with_data.append({
                    'symbol': symbol_name,
                    'trading_days': trading_days,
                    'data_path': str(symbol_dir)
                })
                
                # Выводим прогресс для символов с достаточным количеством данных
                print(f"✓ {symbol_name:8s} - {trading_days:2d} дней")
            
            # Показываем прогресс каждые 100 символов
            if total_symbols % 100 == 0:
                print(f"Обработано символов: {total_symbols}")
    
    print("-" * 60)
    print(f"Всего символов: {total_symbols}")
    print(f"Символов с данными ≥{min_days} дней: {len(symbols_with_data)}")
    
    return symbols_with_data

def save_to_csv(symbols_data: list, output_file: str = "extracted_kline_list.csv"):
    """
    Сохранить результаты в CSV файл
    """
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['symbol', 'trading_days', 'data_path']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Записываем заголовки
            writer.writeheader()
            
            # Записываем данные
            for symbol_data in symbols_data:
                writer.writerow(symbol_data)
        
        print(f"\nРезультаты сохранены в {output_file}")
        
    except Exception as e:
        print(f"Ошибка при сохранении в CSV: {e}")

def main():
    """
    Основная функция
    """
    print("=" * 60)
    print("ИЗВЛЕЧЕНИЕ СПИСКА СИМВОЛОВ С ДАННЫМИ НА 10+ ДНЕЙ")
    print("=" * 60)
    
    # Извлекаем символы с данными на 7 и более дней
    symbols_data = extract_symbols_with_data(min_days=10)
    
    if symbols_data:
        # Сортируем по количеству торговых дней (по убыванию)
        symbols_data.sort(key=lambda x: x['trading_days'], reverse=True)
        
        # Показываем топ-10 символов с наибольшим количеством данных
        print(f"\nТОП-10 СИМВОЛОВ С НАИБОЛЬШИМ КОЛИЧЕСТВОМ ДАННЫХ:")
        print("-" * 50)
        for i, symbol_data in enumerate(symbols_data[:10], 1):
            print(f"{i:2d}. {symbol_data['symbol']:8s} - {symbol_data['trading_days']:2d} дней")
        
        # Сохраняем в CSV
        save_to_csv(symbols_data)
        
        # Показываем статистику
        total_days = sum(s['trading_days'] for s in symbols_data)
        avg_days = total_days / len(symbols_data) if symbols_data else 0
        
        print(f"\nСТАТИСТИКА:")
        print(f"Среднее количество дней на символ: {avg_days:.1f}")
        print(f"Общее количество торговых дней: {total_days}")
        
    else:
        print("Не найдено символов с достаточным количеством данных!")

if __name__ == "__main__":
    main()
