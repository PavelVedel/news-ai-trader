import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# Загружаем данные
with open('anomaly_analysis_results.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Берем первую новость с символами
first_news = data[0]
print(f"Анализируем новость: {first_news['headline'][:50]}")
print(f"Время новости: {first_news['created_at_utc']}")

# Берем первый символ
first_symbol = first_news['symbols_analysis'][0]
symbol_name = first_symbol['symbol']
print(f"Символ: {symbol_name}")

# Проверяем, есть ли свечи для этого символа
market_data_path = Path("data/market_data/yahoo/1m")
symbol_path = market_data_path / symbol_name

if symbol_path.exists():
    print(f"Папка символа существует: {symbol_path}")
    
    # Получаем дату новости
    news_date = datetime.fromisoformat(first_news['created_at_utc'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
    print(f"Дата новости: {news_date}")
    
    # Ищем файл с данными
    file_path = symbol_path / f"{news_date}.parquet"
    if file_path.exists():
        print(f"Файл данных найден: {file_path}")
        
        # Читаем parquet файл
        try:
            df = pd.read_parquet(file_path)
            print(f"Размер данных: {df.shape}")
            print(f"Колонки: {df.columns.tolist()}")
            print(f"Индекс: {df.index.name}")
            
            if not df.empty:
                print(f"Первые 5 строк:")
                print(df.head())
                
                # Проверяем временной диапазон
                print(f"Временной диапазон: {df.index.min()} - {df.index.max()}")
                
                # Парсим время новости
                news_time = first_news['created_at_utc']
                news_dt = datetime.fromisoformat(news_time.replace('Z', '+00:00'))
                start_time = news_dt
                end_time = news_dt + timedelta(hours=3)
                
                print(f"Время новости: {news_dt}")
                print(f"Диапазон анализа: {start_time} - {end_time}")
                
                # Фильтруем свечи
                mask = (df.index >= start_time) & (df.index <= end_time)
                relevant_candles = df[mask]
                
                print(f"Найдено свечей в диапазоне: {len(relevant_candles)}")
                
                if not relevant_candles.empty:
                    print(f"Первая свеча: {relevant_candles.iloc[0]}")
                    print(f"Последняя свеча: {relevant_candles.iloc[-1]}")
                    
                    # Анализируем движение цены
                    price_at_news = relevant_candles.iloc[0]['close']
                    max_high = relevant_candles['high'].max()
                    min_low = relevant_candles['low'].min()
                    
                    max_up_pct = ((max_high - price_at_news) / price_at_news) * 100
                    max_down_pct = ((min_low - price_at_news) / price_at_news) * 100
                    max_movement_pct = max(abs(max_up_pct), abs(max_down_pct))
                    
                    print(f"Цена в момент новости: {price_at_news}")
                    print(f"Максимум: {max_high}")
                    print(f"Минимум: {min_low}")
                    print(f"Максимальное движение: {max_movement_pct:.2f}%")
                    print(f"Движение вверх: {max_up_pct:.2f}%")
                    print(f"Движение вниз: {max_down_pct:.2f}%")
                    
                    # Проверяем аномалию
                    is_anomaly = max_movement_pct >= 0.5
                    print(f"Аномалия (≥0.5%): {is_anomaly}")
                else:
                    print("Нет свечей в нужном временном диапазоне")
                    
        except Exception as e:
            print(f"Ошибка при чтении файла: {e}")
    else:
        print(f"Файл данных не найден: {file_path}")
        # Показываем доступные файлы
        available_files = list(symbol_path.glob("*.parquet"))
        print(f"Доступные файлы: {[f.name for f in available_files]}")
else:
    print(f"Папка символа не существует: {symbol_path}")
