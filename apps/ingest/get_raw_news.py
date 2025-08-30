from libs.database.connection import DatabaseConnection

def main():
    db = DatabaseConnection("data/db/news.db")
    db.create_database()
    symbol = "NVDA"
    limit = 5
    news_list = db.get_news_by_symbol(symbol, limit)
    if not news_list:
        print("Новости для NVDA не найдены")
    
    print(f"Найдено {len(news_list)} новостей для {symbol}:")
    print("-" * 80)
    
    # Выводим информацию о каждой новости
    for i, news in enumerate(news_list, 1):
        print(f"{i}. {news['headline']}")
        print(f"   Источник: {news['source']}")
        print(f"   Дата: {news['created_at_utc']}")
        print(f"   Символы: {news['symbols_json']}")
        if news['summary']:
            print(f"   Краткое содержание: {news['summary'][:100]}...")
        if news['url']:
            print(f"   URL: {news['url']}")
        print()

    pass

    
    

if __name__ == "__main__":
    main()