#!/usr/bin/env python3
"""
Пример использования DatabaseConnection для работы с новостями
"""

from connection import DatabaseConnection
import json

def example_add_single_news():
    """Пример добавления одной новости"""
    
    # Создаем подключение к БД
    db = DatabaseConnection("example_news.db")
    
    # Создаем таблицы
    db.create_database()
    
    # Пример новости (как в вашем примере)
    news_data = {
        "author": "Chris Katje",
        "content": "",
        "created_at": "2025-08-15T19:59:29Z",
        "headline": "David Tepper's Hedge Fund Bets On Intel, UnitedHealth; Cuts Position In Four Mag 7 Stocks",
        "id": 47167369,
        "images": [
            {
                "size": "large",
                "url": "https://cdn.benzinga.com/files/imagecache/2048x1536xUP/images/story/2025/08/15/Business--Man--computer_0.jpeg"
            }
        ],
        "source": "benzinga",
        "summary": "David Tepper sold casino stocks and bought airline stocks in the second quarter. Here's a look at the changes made to the Appaloosa hedge fund.",
        "symbols": ["AAPL", "AMZN", "INTC", "MSFT", "NVDA"],
        "updated_at": "2025-08-15T19:59:29Z",
        "url": "https://www.benzinga.com/trading-ideas/long-ideas/25/08/47167369/david-teppers-hedge-fund-bets-on-intel-unitedhealth-cuts-position-in-four-mag-7-stocks"
    }
    
    # Добавляем новость
    news_id = db.add_raw_news(news_data)
    
    if news_id:
        print(f"✅ Новость успешно добавлена с ID: {news_id}")
        
        # Получаем новости по символу
        intel_news = db.get_news_by_symbol("INTC", limit=10)
        print(f"📰 Найдено {len(intel_news)} новостей по INTC")
        
        # Получаем новости за период
        recent_news = db.get_news_by_date_range(
            "2025-08-15T00:00:00Z", 
            "2025-08-15T23:59:59Z", 
            limit=50
        )
        print(f"📅 Найдено {len(recent_news)} новостей за 15 августа 2025")
        
    else:
        print("❌ Ошибка при добавлении новости")
    
    db.close()

def example_add_batch_news():
    """Пример добавления нескольких новостей пакетом"""
    
    db = DatabaseConnection("example_news.db")
    
    # Создаем таблицы если их нет
    db.create_database()
    
    # Несколько новостей для примера
    news_list = [
        {
            "source": "benzinga",
            "created_at": "2025-08-15T20:00:00Z",
            "headline": "Apple Reports Strong Q3 Earnings",
            "summary": "Apple Inc. reported better-than-expected quarterly results",
            "symbols": ["AAPL"],
            "url": "https://example.com/apple-earnings"
        },
        {
            "source": "reuters",
            "created_at": "2025-08-15T20:15:00Z",
            "headline": "Tesla Announces New Model",
            "summary": "Tesla unveiled its latest electric vehicle model",
            "symbols": ["TSLA"],
            "url": "https://example.com/tesla-new-model"
        },
        {
            "source": "bloomberg",
            "created_at": "2025-08-15T20:30:00Z",
            "headline": "Microsoft Cloud Revenue Soars",
            "summary": "Microsoft's cloud business continues strong growth",
            "symbols": ["MSFT", "AZURE"],
            "url": "https://example.com/microsoft-cloud"
        }
    ]
    
    # Добавляем пакетом
    added_ids = db.add_raw_news_batch(news_list)
    print(f"✅ Добавлено {len(added_ids)} новостей: {added_ids}")
    
    db.close()

def example_search_and_analysis():
    """Пример поиска и анализа новостей"""
    
    db = DatabaseConnection("example_news.db")
    
    # Поиск по символу
    print("\n🔍 Поиск новостей по AAPL:")
    aapl_news = db.get_news_by_symbol("AAPL", limit=5)
    for news in aapl_news:
        print(f"  - {news['headline']} ({news['source']})")
    
    # Поиск по периоду
    print("\n📅 Новости за 15 августа 2025:")
    august_news = db.get_news_by_date_range(
        "2025-08-15T00:00:00Z", 
        "2025-08-15T23:59:59Z", 
        limit=10
    )
    for news in august_news:
        symbols = json.loads(news['symbols_json'])
        print(f"  - {news['headline']} | Символы: {', '.join(symbols)}")
    
    db.close()

if __name__ == "__main__":
    print("🚀 Примеры работы с базой данных новостей\n")
    
    print("1️⃣ Добавление одной новости:")
    example_add_single_news()
    
    print("\n2️⃣ Добавление новостей пакетом:")
    example_add_batch_news()
    
    print("\n3️⃣ Поиск и анализ:")
    example_search_and_analysis()
    
    print("\n✨ Все примеры выполнены!")