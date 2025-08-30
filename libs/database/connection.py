import sqlite3
from pathlib import Path
from typing import Optional
from contextlib import contextmanager
import hashlib
import json
from datetime import datetime, timezone

class DatabaseConnection:
    def __init__(self, db_path: str = "news_trader.db"):
        self.db_path = db_path
        self._connection: Optional[sqlite3.Connection] = None
        
    def get_connection(self) -> sqlite3.Connection:
        """Получить подключение к БД"""
        if self._connection is None:
            # Создаем папку для БД если её нет
            db_dir = Path(self.db_path).parent
            db_dir.mkdir(parents=True, exist_ok=True)
            
            self._connection = sqlite3.connect(self.db_path)
            self._connection.row_factory = sqlite3.Row  # Для удобного доступа к колонкам
            
        return self._connection
    
    @contextmanager
    def get_cursor(self):
        """Контекстный менеджер для работы с курсором"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    
    def close(self):
        """Закрыть подключение"""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


    def create_database(self, schema_file: str = None) -> bool:
        """Создать базу данных и все таблицы из схемы"""
        if schema_file is None:
            # По умолчанию ищем schema.sql в той же папке
            schema_file = Path(__file__).parent / "schema.sql"
        
        try:
            # Читаем SQL схему
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Выполняем SQL команды
            with self.get_cursor() as cursor:
                cursor.executescript(schema_sql)   # ключевой момент
                print("База данных создана успешно!")
                return True
                
        except Exception as e:
            print(f"Ошибка при создании базы данных: {e}")
            return False

    def add_raw_news(self, news_data: dict) -> Optional[int]:
        """
        Добавить сырую новость в базу данных
        
        Args:
            news_data: Словарь с данными новости (например от Benziga)
            
        Returns:
            news_id: ID добавленной новости (inserted_id) или None при ошибке 
        """
        try:
            # Подготавливаем данные для вставки
            provider_id = news_data.get('id')
            source = news_data.get('source', 'unknown')
            created_at_utc = news_data.get('created_at')
            headline = news_data.get('headline', '')
            summary = news_data.get('summary')
            symbols = news_data.get('symbols', [])
            url = news_data.get('url')
            
            # Проверяем обязательные поля
            if not headline or not created_at_utc:
                print("Ошибка: отсутствуют обязательные поля headline или created_at")
                return None
            
            # Парсим время и округляем до минуты для дедупликации
            try:
                dt = datetime.fromisoformat(created_at_utc)
                floor_minute = dt.replace(second=0, microsecond=0).isoformat()
            except ValueError:
                print(f"Ошибка парсинга времени: {created_at_utc}")
                return None
            
            # Создаем hash для дедупликации
            dedupe_string = f"{source}|{provider_id}|{headline}|{floor_minute}"
            hash_dedupe = hashlib.md5(dedupe_string.encode('utf-8')).hexdigest()
            
            # Конвертируем символы в JSON
            symbols_json = json.dumps(symbols, ensure_ascii=False)
            
            # Текущее время получения
            received_at_utc = datetime.now(timezone.utc).isoformat()
            
            with self.get_cursor() as cursor:
                # Проверяем, нет ли уже такой новости
                cursor.execute("""
                    SELECT news_id FROM news_raw WHERE hash_dedupe = ?
                """, (hash_dedupe,))
                
                if cursor.fetchone():
                    print(f"Новость уже существует (hash: {hash_dedupe})")
                    return None
                
                # Вставляем новость
                cursor.execute("""
                    INSERT INTO news_raw (
                        provider_id, source, created_at_utc, received_at_utc,
                        headline, summary, symbols_json, url, hash_dedupe
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    provider_id, source, created_at_utc, received_at_utc,
                    headline, summary, symbols_json, url, hash_dedupe
                ))
                
                # Получаем ID вставленной записи
                inserted_id = cursor.lastrowid
                print(f"Новость добавлена с ID: {inserted_id}")
                return inserted_id
                
        except Exception as e:
            print(f"Ошибка при добавлении новости: {e}")
            return None
    
    def add_raw_news_batch(self, news_list: list) -> list[int]:
        """
        Добавить несколько новостей пакетом
        
        Args:
            news_list: Список словарей с данными новостей
            
        Returns:
            list: Список ID добавленных новостей
        """
        added_ids = []
        
        for news_data in news_list:
            news_id = self.add_raw_news(news_data)
            if news_id:
                added_ids.append(news_id)
        
        return added_ids
    
    def get_news_by_symbol(self, symbol: str, limit: int = 100) -> list:
        """
        Получить новости по символу
        
        Args:
            symbol: Тикер акции
            limit: Максимальное количество новостей
            
        Returns:
            list: Список новостей
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM news_raw 
                    WHERE symbols_json LIKE ?
                    ORDER BY created_at_utc DESC
                    LIMIT ?
                """, (f'%"{symbol}"%', limit))
                
                return cursor.fetchall()
                
        except Exception as e:
            print(f"Ошибка при получении новостей по символу {symbol}: {e}")
            return []
    
    def get_news_by_date_range(self, start_date: str, end_date: str, limit: int = 1000) -> list:
        """
        Получить новости за период
        
        Args:
            start_date: Начальная дата в формате ISO8601
            end_date: Конечная дата в формате ISO8601
            limit: Максимальное количество новостей
            
        Returns:
            list: Список новостей
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM news_raw 
                    WHERE created_at_utc BETWEEN ? AND ?
                    ORDER BY created_at_utc DESC
                    LIMIT ?
                """, (start_date, end_date, limit))
                
                return cursor.fetchall()
                
        except Exception as e:
            print(f"Ошибка при получении новостей за период: {e}")
            return []

    def get_all_symbols(self) -> list[str]:
        """
        Получить все уникальные символы из базы данных
        
        Returns:
            list[str]: Список уникальных символов (тикеров)
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT symbols_json FROM news_raw 
                    WHERE symbols_json IS NOT NULL AND symbols_json != ''
                """)
                
                all_symbols = set()
                for row in cursor.fetchall():
                    try:
                        symbols = json.loads(row['symbols_json'])
                        if isinstance(symbols, list):
                            all_symbols.update(symbols)
                    except (json.JSONDecodeError, TypeError):
                        # Пропускаем некорректные JSON
                        continue
                
                # Сортируем символы для удобства
                return sorted(list(all_symbols))
                
        except Exception as e:
            print(f"Ошибка при получении символов: {e}")
            return []