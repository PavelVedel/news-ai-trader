# Database Connection для работы с новостями

Этот модуль предоставляет улучшенные функции для работы с базой данных новостей, включая добавление, поиск и анализ сырых новостей.

## Основные возможности

### 1. Добавление новостей

#### Одиночная новость
```python
from connection import DatabaseConnection

db = DatabaseConnection("news_trader.db")
db.create_database()

news_data = {
    "source": "benzinga",
    "created_at": "2025-08-15T19:59:29Z",
    "headline": "David Tepper's Hedge Fund Bets On Intel",
    "summary": "David Tepper sold casino stocks and bought airline stocks",
    "symbols": ["AAPL", "AMZN", "INTC", "MSFT", "NVDA"],
    "url": "https://example.com/news"
}

news_id = db.add_raw_news(news_data)
if news_id:
    print(f"Новость добавлена с ID: {news_id}")
```

#### Пакетное добавление
```python
news_list = [
    {
        "source": "benzinga",
        "created_at": "2025-08-15T20:00:00Z",
        "headline": "Apple Reports Strong Q3 Earnings",
        "symbols": ["AAPL"]
    },
    {
        "source": "reuters",
        "created_at": "2025-08-15T20:15:00Z",
        "headline": "Tesla Announces New Model",
        "symbols": ["TSLA"]
    }
]

added_ids = db.add_raw_news_batch(news_list)
print(f"Добавлено {len(added_ids)} новостей")
```

### 2. Поиск новостей

#### По символу (тикеру)
```python
# Получить все новости по AAPL
aapl_news = db.get_news_by_symbol("AAPL", limit=100)

for news in aapl_news:
    print(f"- {news['headline']} ({news['source']})")
    symbols = json.loads(news['symbols_json'])
    print(f"  Символы: {', '.join(symbols)}")
```

#### По диапазону дат
```python
# Новости за 15 августа 2025
news_15 = db.get_news_by_date_range(
    "2025-08-15T00:00:00Z",
    "2025-08-15T23:59:59Z",
    limit=1000
)

for news in news_15:
    print(f"- {news['headline']} | {news['created_at_utc']}")
```

### 3. Особенности реализации

#### Автоматическая дедупликация
- Создается MD5 hash на основе `source|headline|floor_minute`
- `floor_minute` - время, округленное до минуты для группировки похожих новостей
- Предотвращает добавление дублирующихся новостей

#### Валидация данных
- Проверяются обязательные поля: `headline`, `created_at`
- Автоматически добавляется `received_at_utc` - время получения новости
- Символы сохраняются как JSON в поле `symbols_json`

#### Обработка ошибок
- Graceful handling ошибок парсинга времени
- Логирование ошибок с подробным описанием
- Возврат `None` при неудачном добавлении

### 4. Структура таблицы

```sql
CREATE TABLE news_raw (
  news_id         INTEGER PRIMARY KEY,
  source          TEXT    NOT NULL,
  created_at_utc  TEXT    NOT NULL,
  received_at_utc TEXT    NOT NULL,
  headline        TEXT    NOT NULL,
  summary         TEXT,
  symbols_json    TEXT    NOT NULL,
  url             TEXT,
  hash_dedupe     TEXT    NOT NULL UNIQUE
);
```

### 5. Примеры использования

#### Полный цикл работы
```python
from connection import DatabaseConnection
import json

def process_news_stream():
    db = DatabaseConnection("news_trader.db")
    db.create_database()
    
    try:
        # Добавляем новость
        news_data = {
            "source": "benzinga",
            "created_at": "2025-08-15T19:59:29Z",
            "headline": "Market Update: Tech Stocks Rally",
            "summary": "Technology stocks showed strong performance today",
            "symbols": ["AAPL", "MSFT", "GOOGL", "NVDA"],
            "url": "https://example.com/market-update"
        }
        
        news_id = db.add_raw_news(news_data)
        
        if news_id:
            # Анализируем влияние на разные символы
            for symbol in ["AAPL", "MSFT", "GOOGL", "NVDA"]:
                symbol_news = db.get_news_by_symbol(symbol, limit=10)
                print(f"Найдено {len(symbol_news)} новостей по {symbol}")
                
                # Можно добавить логику анализа влияния новости
                # на цену акции в определенный период
                
    finally:
        db.close()

if __name__ == "__main__":
    process_news_stream()
```

### 6. Тестирование

Запуск тестов:
```bash
cd libs/database
python test_news_functions.py
```

Тесты покрывают:
- Успешное добавление новостей
- Валидацию обязательных полей
- Предотвращение дублирования
- Пакетное добавление
- Поиск по символу и дате
- Целостность данных

### 7. Производительность

- Используется WAL режим SQLite для конкурентного доступа
- Индексы на `created_at_utc` и `hash_dedupe`
- Пакетные операции для массового добавления
- Эффективный поиск по JSON полям

### 8. Расширение функциональности

Для добавления новых возможностей можно расширить класс:

```python
def get_news_statistics(self, start_date: str, end_date: str) -> dict:
    """Получить статистику по новостям за период"""
    with self.get_cursor() as cursor:
        cursor.execute("""
            SELECT 
                source,
                COUNT(*) as count,
                COUNT(DISTINCT symbols_json) as unique_symbols
            FROM news_raw 
            WHERE created_at_utc BETWEEN ? AND ?
            GROUP BY source
        """, (start_date, end_date))
        
        return {row['source']: {'count': row['count'], 'unique_symbols': row['unique_symbols']} 
                for row in cursor.fetchall()}
```

Этот модуль предоставляет надежную основу для работы с новостными данными в торговых системах.
