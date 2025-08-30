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
                
                # Дополнительно создаем таблицу fundamentals если её нет
                self.ensure_fundamentals_table()
                
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
    
    def ensure_fundamentals_table(self) -> bool:
        """
        Убедиться что таблица fundamentals существует
        
        Returns:
            bool: True если таблица создана/существует, False при ошибке
        """
        try:
            with self.get_cursor() as cursor:
                # Проверяем, существует ли таблица
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='fundamentals'
                """)
                
                if not cursor.fetchone():
                    print("Создаю таблицу fundamentals...")
                    
                    # Создаем таблицу
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS fundamentals (
                            symbol              TEXT PRIMARY KEY,
                            market_cap          REAL,
                            enterprise_value    REAL,
                            pe_ratio            REAL,
                            forward_pe          REAL,
                            peg_ratio           REAL,
                            price_to_book       REAL,
                            price_to_sales      REAL,
                            enterprise_to_revenue REAL,
                            enterprise_to_ebitda REAL,
                            return_on_equity    REAL,
                            return_on_assets    REAL,
                            return_on_capital   REAL,
                            current_ratio       REAL,
                            quick_ratio         REAL,
                            debt_to_equity      REAL,
                            dividend_yield      REAL,
                            dividend_rate       REAL,
                            payout_ratio        REAL,
                            beta                REAL,
                            fifty_two_week_high REAL,
                            fifty_two_week_low  REAL,
                            fifty_day_average   REAL,
                            two_hundred_day_average REAL,
                            sector              TEXT,
                            industry            TEXT,
                            country             TEXT,
                            currency            TEXT,
                            last_updated        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            data_source         TEXT NOT NULL DEFAULT 'yahoo_finance'
                        )
                    """)
                    
                    # Создаем индексы
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fundamentals_sector ON fundamentals(sector)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fundamentals_industry ON fundamentals(industry)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fundamentals_market_cap ON fundamentals(market_cap)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fundamentals_pe_ratio ON fundamentals(pe_ratio)")
                    
                    print("[OK] Таблица fundamentals создана успешно")
                else:
                    print("[OK] Таблица fundamentals уже существует")
                
                return True
                    
        except Exception as e:
            print(f"Ошибка при создании таблицы fundamentals: {e}")
            return False
    
    def save_fundamentals(self, fundamentals: dict) -> bool:
        """
        Сохранить фундаментальные данные в базу данных
        
        Args:
            fundamentals: Словарь с фундаментальными данными
            
        Returns:
            bool: True если успешно, False при ошибке
        """
        try:
            with self.get_cursor() as cursor:
                # Подготавливаем SQL запрос для вставки/обновления
                sql = """
                    INSERT OR REPLACE INTO fundamentals (
                        symbol, market_cap, enterprise_value, pe_ratio, forward_pe,
                        peg_ratio, price_to_book, price_to_sales, enterprise_to_revenue,
                        enterprise_to_ebitda, return_on_equity, return_on_assets,
                        return_on_capital, current_ratio, quick_ratio, debt_to_equity,
                        dividend_yield, dividend_rate, payout_ratio, beta,
                        fifty_two_week_high, fifty_two_week_low, fifty_day_average,
                        two_hundred_day_average, sector, industry, country, currency,
                        last_updated, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                # Подготавливаем значения для вставки
                values = (
                    fundamentals['symbol'],
                    fundamentals['market_cap'],
                    fundamentals['enterprise_value'],
                    fundamentals['pe_ratio'],
                    fundamentals['forward_pe'],
                    fundamentals['peg_ratio'],
                    fundamentals['price_to_book'],
                    fundamentals['price_to_sales'],
                    fundamentals['enterprise_to_revenue'],
                    fundamentals['enterprise_to_ebitda'],
                    fundamentals['return_on_equity'],
                    fundamentals['return_on_assets'],
                    fundamentals['return_on_capital'],
                    fundamentals['current_ratio'],
                    fundamentals['quick_ratio'],
                    fundamentals['debt_to_equity'],
                    fundamentals['dividend_yield'],
                    fundamentals['dividend_rate'],
                    fundamentals['payout_ratio'],
                    fundamentals['beta'],
                    fundamentals['fifty_two_week_high'],
                    fundamentals['fifty_two_week_low'],
                    fundamentals['fifty_day_average'],
                    fundamentals['two_hundred_day_average'],
                    fundamentals['sector'],
                    fundamentals['industry'],
                    fundamentals['country'],
                    fundamentals['currency'],
                    fundamentals['last_updated'],
                    fundamentals['data_source']
                )
                
                cursor.execute(sql, values)
                return True
                
        except Exception as e:
            print(f"Ошибка при сохранении данных для {fundamentals['symbol']}: {e}")
            return False
    
    def get_fundamentals(self, symbol: str) -> Optional[dict]:
        """
        Получить фундаментальные данные для символа
        
        Args:
            symbol: Тикер акции
            
        Returns:
            dict: Словарь с фундаментальными данными или None если не найдено
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM fundamentals WHERE symbol = ?
                """, (symbol,))
                
                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            print(f"Ошибка при получении данных для {symbol}: {e}")
            return None
    
    def get_all_fundamentals(self) -> list[dict]:
        """
        Получить все фундаментальные данные
        
        Returns:
            list[dict]: Список всех записей fundamentals
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM fundamentals ORDER BY symbol
                """)
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            print(f"Ошибка при получении всех данных: {e}")
            return []
    
    def delete_fundamentals(self, symbol: str) -> bool:
        """
        Удалить фундаментальные данные для символа
        
        Args:
            symbol: Тикер акции
            
        Returns:
            bool: True если успешно удалено, False при ошибке
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    DELETE FROM fundamentals WHERE symbol = ?
                """, (symbol,))
                
                return cursor.rowcount > 0
                
        except Exception as e:
            print(f"Ошибка при удалении данных для {symbol}: {e}")
            return False
    
    def get_fundamentals_symbols_needing_update(self, max_age_months: int = 3) -> list[str]:
        """
        Получить символы, которые требуют обновления fundamentals
        
        Args:
            max_age_months: Максимальный возраст данных в месяцах
            
        Returns:
            list[str]: Список символов для обновления
        """
        try:
            with self.get_cursor() as cursor:
                # Получаем все символы из news_raw
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
                        continue
                
                # Получаем символы из fundamentals с датой последнего обновления
                cursor.execute("""
                    SELECT symbol, last_updated FROM fundamentals
                """)
                
                existing_fundamentals = {}
                for row in cursor.fetchall():
                    existing_fundamentals[row['symbol']] = row['last_updated']
                
                # Определяем символы, требующие обновления
                symbols_to_update = []
                cutoff_date = datetime.now().replace(day=1)  # Текущий месяц
                
                for i in range(max_age_months):
                    cutoff_date = cutoff_date.replace(month=cutoff_date.month - 1)
                
                cutoff_date_str = cutoff_date.isoformat()
                
                for symbol in all_symbols:
                    clean_symbol = symbol.replace('$', '')
                    if not clean_symbol or len(clean_symbol) > 10:
                        continue
                        
                    if clean_symbol not in existing_fundamentals:
                        # Новый символ - добавляем в список обновления
                        symbols_to_update.append(clean_symbol)
                    else:
                        # Проверяем возраст существующих данных
                        last_updated = existing_fundamentals[clean_symbol]
                        if last_updated < cutoff_date_str:
                            symbols_to_update.append(clean_symbol)
                
                return sorted(symbols_to_update)
                
        except Exception as e:
            print(f"Ошибка при получении символов для обновления: {e}")
            return []
    
    def get_fundamentals_stats(self) -> dict:
        """
        Получить статистику по таблице fundamentals
        
        Returns:
            dict: Словарь со статистикой
        """
        try:
            with self.get_cursor() as cursor:
                # Общее количество символов
                cursor.execute("SELECT COUNT(*) FROM fundamentals")
                total_symbols = cursor.fetchone()[0]
                
                # Количество символов с сектором
                cursor.execute("SELECT COUNT(*) FROM fundamentals WHERE sector IS NOT NULL")
                symbols_with_sector = cursor.fetchone()[0]
                
                # Количество символов с P/E
                cursor.execute("SELECT COUNT(*) FROM fundamentals WHERE pe_ratio IS NOT NULL")
                symbols_with_pe = cursor.fetchone()[0]
                
                # Количество символов с рыночной капитализацией
                cursor.execute("SELECT COUNT(*) FROM fundamentals WHERE market_cap IS NOT NULL")
                symbols_with_market_cap = cursor.fetchone()[0]
                
                # Последнее обновление
                cursor.execute("SELECT MAX(last_updated) FROM fundamentals")
                last_update = cursor.fetchone()[0]
                
                return {
                    'total_symbols': total_symbols,
                    'symbols_with_sector': symbols_with_sector,
                    'symbols_with_pe': symbols_with_pe,
                    'symbols_with_market_cap': symbols_with_market_cap,
                    'last_update': last_update
                }
                
        except Exception as e:
            print(f"Ошибка при получении статистики: {e}")
            return {}