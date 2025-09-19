import sqlite3
from pathlib import Path
from typing import Optional
from contextlib import contextmanager
import hashlib
import json
from datetime import datetime, timezone

class DatabaseConnection:
    def __init__(self, db_path: str = "data/db/news.db"):
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
                # and info table
                self.ensure_infos_table()
                
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
    
    def get_news_by_id(self, news_id: int) -> dict:
        """
        Получить новость по ID
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM news_raw WHERE news_id = ?
                """, (news_id,))
                
                return cursor.fetchone()
                
        except Exception as e:
            print(f"Ошибка при получении новости по ID {news_id}: {e}")
            return None
    
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
        Убедиться что таблица fundamentals существует и имеет все необходимые поля
        
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
                    
                    # Создаем таблицу с полной схемой
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS fundamentals (
                            symbol              TEXT PRIMARY KEY,
                            
                            -- Основные финансовые показатели
                            market_cap          REAL,
                            enterprise_value    REAL,
                            pe_ratio            REAL,
                            forward_pe          REAL,
                            peg_ratio           REAL,
                            price_to_book       REAL,
                            price_to_sales      REAL,
                            enterprise_to_revenue REAL,
                            enterprise_to_ebitda REAL,
                            
                            -- Показатели доходности
                            return_on_equity    REAL,
                            return_on_assets    REAL,
                            return_on_capital   REAL,
                            
                            -- Показатели ликвидности
                            current_ratio       REAL,
                            quick_ratio         REAL,
                            debt_to_equity      REAL,
                            
                            -- Дивиденды
                            dividend_yield      REAL,
                            dividend_rate       REAL,
                            payout_ratio        REAL,
                            five_year_avg_dividend_yield REAL,
                            trailing_annual_dividend_rate REAL,
                            trailing_annual_dividend_yield REAL,
                            
                            -- Технические показатели
                            beta                REAL,
                            fifty_two_week_high REAL,
                            fifty_two_week_low  REAL,
                            fifty_day_average   REAL,
                            two_hundred_day_average REAL,
                            fifty_two_week_change_percent REAL,
                            fifty_day_average_change REAL,
                            fifty_day_average_change_percent REAL,
                            two_hundred_day_average_change REAL,
                            two_hundred_day_average_change_percent REAL,
                            
                            -- Дополнительные финансовые показатели
                            book_value          REAL,
                            total_cash          REAL,
                            total_cash_per_share REAL,
                            total_debt          REAL,
                            total_revenue       REAL,
                            revenue_per_share   REAL,
                            gross_profits       REAL,
                            free_cashflow       REAL,
                            operating_cashflow  REAL,
                            ebitda              REAL,
                            net_income_to_common REAL,
                            
                            -- Показатели роста
                            earnings_growth     REAL,
                            revenue_growth      REAL,
                            earnings_quarterly_growth REAL,
                            
                            -- Маржинальность
                            gross_margins       REAL,
                            ebitda_margins      REAL,
                            operating_margins   REAL,
                            profit_margins      REAL,
                            
                            -- Акции и доля
                            shares_outstanding  REAL,
                            float_shares        REAL,
                            shares_short        REAL,
                            shares_short_prior_month REAL,
                            shares_percent_shares_out REAL,
                            held_percent_insiders REAL,
                            held_percent_institutions REAL,
                            short_ratio         REAL,
                            short_percent_of_float REAL,
                            
                            -- Аналитические оценки
                            target_high_price   REAL,
                            target_low_price    REAL,
                            target_mean_price   REAL,
                            target_median_price REAL,
                            recommendation_mean REAL,
                            recommendation_key  TEXT,
                            number_of_analyst_opinions INTEGER,
                            average_analyst_rating TEXT,
                            
                            -- Риски ESG
                            audit_risk          INTEGER,
                            board_risk          INTEGER,
                            compensation_risk   INTEGER,
                            share_holder_rights_risk INTEGER,
                            overall_risk        INTEGER,
                            
                            -- Временные метки
                            last_fiscal_year_end REAL,
                            next_fiscal_year_end REAL,
                            most_recent_quarter REAL,
                            ex_dividend_date    REAL,
                            dividend_date       REAL,
                            last_dividend_date  REAL,
                            earnings_timestamp  REAL,
                            earnings_timestamp_start REAL,
                            earnings_timestamp_end REAL,
                            
                            -- Разделение акций
                            last_split_factor   TEXT,
                            last_split_date     REAL,
                            
                            -- Метаданные
                            sector              TEXT,
                            industry            TEXT,
                            country             TEXT,
                            currency            TEXT,
                            exchange            TEXT,
                            quote_type          TEXT,
                            market_state        TEXT,
                            
                            -- Временные метки
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
                    
                    # Проверяем и добавляем новые колонки если их нет
                    self._upgrade_fundamentals_table()
                
                return True
                    
        except Exception as e:
            print(f"Ошибка при создании таблицы fundamentals: {e}")
            return False
            
    def _upgrade_fundamentals_table(self) -> bool:
        """
        Обновить структуру таблицы fundamentals, добавив новые поля если их нет
        
        Returns:
            bool: True если успешно, False при ошибке
        """
        try:
            print("Проверяю структуру таблицы fundamentals и добавляю недостающие поля...")
            
            # Список новых полей для проверки и добавления
            new_fields = [
                ("five_year_avg_dividend_yield", "REAL"),
                ("trailing_annual_dividend_rate", "REAL"),
                ("trailing_annual_dividend_yield", "REAL"),
                ("fifty_two_week_change_percent", "REAL"),
                ("fifty_day_average_change", "REAL"),
                ("fifty_day_average_change_percent", "REAL"),
                ("two_hundred_day_average_change", "REAL"),
                ("two_hundred_day_average_change_percent", "REAL"),
                ("book_value", "REAL"),
                ("total_cash", "REAL"),
                ("total_cash_per_share", "REAL"),
                ("total_debt", "REAL"),
                ("total_revenue", "REAL"),
                ("revenue_per_share", "REAL"),
                ("gross_profits", "REAL"),
                ("free_cashflow", "REAL"),
                ("operating_cashflow", "REAL"),
                ("ebitda", "REAL"),
                ("net_income_to_common", "REAL"),
                ("earnings_growth", "REAL"),
                ("revenue_growth", "REAL"),
                ("earnings_quarterly_growth", "REAL"),
                ("gross_margins", "REAL"),
                ("ebitda_margins", "REAL"),
                ("operating_margins", "REAL"),
                ("profit_margins", "REAL"),
                ("shares_outstanding", "REAL"),
                ("float_shares", "REAL"),
                ("shares_short", "REAL"),
                ("shares_short_prior_month", "REAL"),
                ("shares_percent_shares_out", "REAL"),
                ("held_percent_insiders", "REAL"),
                ("held_percent_institutions", "REAL"),
                ("short_ratio", "REAL"),
                ("short_percent_of_float", "REAL"),
                ("target_high_price", "REAL"),
                ("target_low_price", "REAL"),
                ("target_mean_price", "REAL"),
                ("target_median_price", "REAL"),
                ("recommendation_mean", "REAL"),
                ("recommendation_key", "TEXT"),
                ("number_of_analyst_opinions", "INTEGER"),
                ("average_analyst_rating", "TEXT"),
                ("audit_risk", "INTEGER"),
                ("board_risk", "INTEGER"),
                ("compensation_risk", "INTEGER"),
                ("share_holder_rights_risk", "INTEGER"),
                ("overall_risk", "INTEGER"),
                ("last_fiscal_year_end", "REAL"),
                ("next_fiscal_year_end", "REAL"),
                ("most_recent_quarter", "REAL"),
                ("ex_dividend_date", "REAL"),
                ("dividend_date", "REAL"),
                ("last_dividend_date", "REAL"),
                ("earnings_timestamp", "REAL"),
                ("earnings_timestamp_start", "REAL"),
                ("earnings_timestamp_end", "REAL"),
                ("last_split_factor", "TEXT"),
                ("last_split_date", "REAL"),
                ("exchange", "TEXT"),
                ("quote_type", "TEXT"),
                ("market_state", "TEXT")
            ]
            
            with self.get_cursor() as cursor:
                # Получаем текущие колонки
                cursor.execute("PRAGMA table_info(fundamentals)")
                existing_columns = {row[1] for row in cursor.fetchall()}
                
                # Добавляем отсутствующие колонки
                added_columns = 0
                for field_name, field_type in new_fields:
                    if field_name not in existing_columns:
                        sql = f"ALTER TABLE fundamentals ADD COLUMN {field_name} {field_type}"
                        cursor.execute(sql)
                        added_columns += 1
                        print(f"Добавлена колонка: {field_name} ({field_type})")
                
                if added_columns > 0:
                    print(f"[OK] Добавлено {added_columns} новых колонок в таблицу fundamentals")
                else:
                    print("[OK] Таблица fundamentals уже содержит все необходимые колонки")
                
                return True
                
        except Exception as e:
            print(f"Ошибка при обновлении структуры таблицы fundamentals: {e}")
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
                # Создаем список полей и значений
                fields = [
                    "symbol",
                    
                    # Основные финансовые показатели
                    "market_cap", "enterprise_value", "pe_ratio", "forward_pe",
                    "peg_ratio", "price_to_book", "price_to_sales", "enterprise_to_revenue",
                    "enterprise_to_ebitda",
                    
                    # Показатели доходности
                    "return_on_equity", "return_on_assets", "return_on_capital",
                    
                    # Показатели ликвидности
                    "current_ratio", "quick_ratio", "debt_to_equity",
                    
                    # Дивиденды
                    "dividend_yield", "dividend_rate", "payout_ratio", "five_year_avg_dividend_yield",
                    "trailing_annual_dividend_rate", "trailing_annual_dividend_yield",
                    
                    # Технические показатели
                    "beta", "fifty_two_week_high", "fifty_two_week_low", "fifty_day_average",
                    "two_hundred_day_average", "fifty_two_week_change_percent",
                    "fifty_day_average_change", "fifty_day_average_change_percent",
                    "two_hundred_day_average_change", "two_hundred_day_average_change_percent",
                    
                    # Дополнительные финансовые показатели
                    "book_value", "total_cash", "total_cash_per_share", "total_debt",
                    "total_revenue", "revenue_per_share", "gross_profits",
                    "free_cashflow", "operating_cashflow", "ebitda", "net_income_to_common",
                    
                    # Показатели роста
                    "earnings_growth", "revenue_growth", "earnings_quarterly_growth",
                    
                    # Маржинальность
                    "gross_margins", "ebitda_margins", "operating_margins", "profit_margins",
                    
                    # Акции и доля
                    "shares_outstanding", "float_shares", "shares_short", "shares_short_prior_month",
                    "shares_percent_shares_out", "held_percent_insiders", "held_percent_institutions",
                    "short_ratio", "short_percent_of_float",
                    
                    # Аналитические оценки
                    "target_high_price", "target_low_price", "target_mean_price", "target_median_price",
                    "recommendation_mean", "recommendation_key", "number_of_analyst_opinions",
                    "average_analyst_rating",
                    
                    # Риски ESG
                    "audit_risk", "board_risk", "compensation_risk", "share_holder_rights_risk",
                    "overall_risk",
                    
                    # Временные метки
                    "last_fiscal_year_end", "next_fiscal_year_end", "most_recent_quarter",
                    "ex_dividend_date", "dividend_date", "last_dividend_date",
                    "earnings_timestamp", "earnings_timestamp_start", "earnings_timestamp_end",
                    
                    # Разделение акций
                    "last_split_factor", "last_split_date",
                    
                    # Метаданные
                    "sector", "industry", "country", "currency", "exchange", "quote_type", "market_state",
                    
                    # Временные метки
                    "last_updated", "data_source"
                ]
                
                # Создаем SQL запрос с правильным количеством параметров
                fields_str = ", ".join(fields)
                placeholders = ", ".join(["?"] * len(fields))
                
                sql = f"""
                    INSERT OR REPLACE INTO fundamentals (
                        {fields_str}
                    ) VALUES ({placeholders})
                """
                
                # Подготавливаем значения для вставки
                values = []
                for field in fields:
                    if field == 'data_source':
                        values.append(fundamentals.get(field, 'yahoo_finance'))
                    else:
                        values.append(fundamentals.get(field))
                
                # Преобразуем список в кортеж для передачи в execute
                values = tuple(values)
                
                cursor.execute(sql, values)
                return True
                
        except Exception as e:
            print(f"Ошибка при сохранении данных для {fundamentals.get('symbol', 'unknown')}: {e}")
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
    
    def get_fundamentals_symbols_needing_update(self, max_age_days: int = 90) -> list[str]:
        """
        Получить символы, которые требуют обновления fundamentals
        
        Args:
            max_age_days: Максимальный возраст данных в днях
            
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
                
                # Вычисляем дату отсечения на основе max_age_days
                from datetime import datetime, timedelta
                cutoff_date = datetime.now() - timedelta(days=max_age_days)
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
                
                # Новые статистики
                cursor.execute("SELECT COUNT(*) FROM fundamentals WHERE dividend_yield IS NOT NULL")
                symbols_with_dividend = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM fundamentals WHERE recommendation_key IS NOT NULL")
                symbols_with_recommendations = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM fundamentals WHERE exchange IS NOT NULL")
                symbols_with_exchange = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM fundamentals WHERE total_revenue IS NOT NULL")
                symbols_with_revenue = cursor.fetchone()[0]
                
                # Последнее обновление
                cursor.execute("SELECT MAX(last_updated) FROM fundamentals")
                last_update = cursor.fetchone()[0]
                
                return {
                    'total_symbols': total_symbols,
                    'symbols_with_sector': symbols_with_sector,
                    'symbols_with_pe': symbols_with_pe,
                    'symbols_with_market_cap': symbols_with_market_cap,
                    'symbols_with_dividend': symbols_with_dividend,
                    'symbols_with_recommendations': symbols_with_recommendations,
                    'symbols_with_exchange': symbols_with_exchange,
                    'symbols_with_revenue': symbols_with_revenue,
                    'last_update': last_update
                }
                
        except Exception as e:
            print(f"Ошибка при получении статистики: {e}")
            return {}

    # ======= infos (ticker.info) =======
    def ensure_infos_table(self) -> bool:
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS infos (
                      symbol                 TEXT PRIMARY KEY,
                      long_name              TEXT,
                      short_name             TEXT,
                      display_name           TEXT,
                      website                TEXT,
                      ir_website             TEXT,
                      phone                  TEXT,
                      address1               TEXT,
                      city                   TEXT,
                      state                  TEXT,
                      zip                    TEXT,
                      country                TEXT,
                      sector                 TEXT,
                      industry               TEXT,
                      full_time_employees    INTEGER,
                      long_business_summary  TEXT,
                      exchange               TEXT,
                      currency               TEXT,
                      officers_json          TEXT,
                      raw_info_json          TEXT,
                      last_updated           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      data_source            TEXT NOT NULL DEFAULT 'yahoo_finance'
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_infos_sector ON infos(sector)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_infos_industry ON infos(industry)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_infos_country ON infos(country)")
            return True
        except Exception as e:
            print(f"Ошибка при создании таблицы infos: {e}")
            return False

    def save_infos(self, payload: dict) -> bool:
        try:
            with self.get_cursor() as cursor:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO infos (
                      symbol, long_name, short_name, display_name,
                      website, ir_website, phone,
                      address1, city, state, zip, country,
                      sector, industry,
                      full_time_employees, long_business_summary,
                      exchange, currency,
                      officers_json, raw_info_json,
                      last_updated, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload.get('symbol'),
                        payload.get('long_name'),
                        payload.get('short_name'),
                        payload.get('display_name'),
                        payload.get('website'),
                        payload.get('ir_website'),
                        payload.get('phone'),
                        payload.get('address1'),
                        payload.get('city'),
                        payload.get('state'),
                        payload.get('zip'),
                        payload.get('country'),
                        payload.get('sector'),
                        payload.get('industry'),
                        payload.get('full_time_employees'),
                        payload.get('long_business_summary'),
                        payload.get('exchange'),
                        payload.get('currency'),
                        payload.get('officers_json'),
                        payload.get('raw_info_json'),
                        payload.get('last_updated'),
                        payload.get('data_source', 'yahoo_finance'),
                    ),
                )
            return True
        except Exception as e:
            print(f"Ошибка при сохранении infos для {payload.get('symbol')}: {e}")
            return False

    def get_infos(self, symbol: str) -> Optional[dict]:
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT * FROM infos WHERE symbol = ?", (symbol,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            print(f"Ошибка при получении infos для {symbol}: {e}")
            return None

    def get_all_infos(self) -> list[dict]:
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT * FROM infos ORDER BY symbol")
                return [dict(r) for r in cursor.fetchall()]
        except Exception as e:
            print(f"Ошибка при получении всех infos: {e}")
            return []

    def delete_infos(self, symbol: str) -> bool:
        try:
            with self.get_cursor() as cursor:
                cursor.execute("DELETE FROM infos WHERE symbol = ?", (symbol,))
                return cursor.rowcount > 0
        except Exception as e:
            print(f"Ошибка при удалении infos для {symbol}: {e}")
            return False

    def get_infos_symbols_needing_update(self, candidates: list[str], max_age_days: int = 30) -> list[str]:
        """Вернуть подмножество candidates, которые отсутствуют в infos или устарели."""
        try:
            if not candidates:
                return []
            with self.get_cursor() as cursor:
                cursor.execute("SELECT symbol, last_updated FROM infos")
                existing = {row['symbol']: row['last_updated'] for row in cursor.fetchall()}

            cutoff = datetime.now().timestamp() - max_age_days * 86400

            def needs(sym: str) -> bool:
                lu = existing.get(sym)
                if not lu:
                    return True
                # Пытаемся парсить ISO8601
                try:
                    from datetime import datetime
                    ts = datetime.fromisoformat(lu).timestamp()
                    return ts < cutoff
                except Exception:
                    return True

            return [s for s in candidates if needs(s)]
        except Exception as e:
            print(f"Ошибка при вычислении символов для обновления infos: {e}")
            return candidates

    def get_infos_stats(self) -> dict:
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM infos")
                total = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM infos WHERE sector IS NOT NULL AND sector != ''")
                with_sector = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM infos WHERE industry IS NOT NULL AND industry != ''")
                with_industry = cursor.fetchone()[0]
                cursor.execute("SELECT MAX(last_updated) FROM infos")
                last_update = cursor.fetchone()[0]
            return {
                'total': total,
                'with_sector': with_sector,
                'with_industry': with_industry,
                'last_update': last_update,
            }
        except Exception as e:
            return {'error': str(e)}

    def get_news_and_infos_for_ai(self, news_id: int) -> dict:
        """
        Получить новость и информацию о символах для AI
        """
        news = self.get_news_by_id(news_id)
        # news_list = db.get_news_by_symbol(symbol="AAPL", limit=1)
        out_dict: dict[str, dict] = {}
        news_dict = dict(news)
        out_dict['news'] = news_dict

        symbols = json.loads(news['symbols_json'])
        symbol_info_dict: dict[str, dict] = {}
        for symbol in symbols:
            symbol_info_dict[symbol] = self.get_infos(symbol)
        out_dict['symbol_info'] = symbol_info_dict
        return out_dict