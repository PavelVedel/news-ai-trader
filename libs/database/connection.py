import sqlite3
from pathlib import Path
from typing import Optional, Literal, List, Dict, Any, get_origin
from contextlib import contextmanager
import hashlib
import json
from datetime import datetime, timezone, timedelta

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
            
            self._connection = sqlite3.connect(self.db_path, timeout=30)
            self._connection.row_factory = sqlite3.Row  # Для удобного доступа к колонкам
            
            # self._connection.execute("PRAGMA journal_mode=WAL;")
            # self._connection.execute("PRAGMA synchronous=NORMAL;")
            # self._connection.execute("PRAGMA busy_timeout=30000;")  # 30 секунд
            # self._connection.execute("PRAGMA foreign_keys=ON;")
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
                # and entities tables
                self.ensure_entities_tables()
                
                return True
                
        except Exception as e:
            print(f"Ошибка при создании базы данных: {e}")
            return False

    def add_raw_news(self, news_data: dict, verbose: bool = True) -> Optional[int]:
        """
        Добавить сырую новость в базу данных
        
        Args:
            news_data: Словарь с данными новости (например от Benziga)
            verbose: Если False, не выводит сообщения в консоль

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
                if verbose:
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
                    if verbose:
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
                if verbose:
                    print(f"Новость добавлена с ID: {inserted_id}")
                return inserted_id
                
        except Exception as e:
            print(f"Ошибка при добавлении новости: {e}")
            return None
    
    def add_raw_news_batch(self, news_list: list, verbose: bool = True) -> list[int]:
        """
        Добавить несколько новостей пакетом
        
        Args:
            news_list: Список словарей с данными новостей
            verbose: Если False, не выводит сообщения в консоль

        Returns:
            list: Список ID добавленных новостей
        """
        added_ids = []
        
        for news_data in news_list:
            news_id = self.add_raw_news(news_data, verbose)
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

    def get_all_symbols(self, filter_strange: bool = False) -> list[str]:
        """
        Get list of sorted symbols.
        
        Returns:
            list[str]: Sorted list of unique symbols (tickers)
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
                sorted_list = sorted(list(all_symbols))

                if filter_strange:
                        sorted_list = [
                            s for s in sorted_list 
                            if s and not (s.startswith('$') or ':' in s or '/' in s)
                        ]
                return sorted_list
                
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
    
    def get_fundamentals(self, symbol: str, remove_none_fields: bool = False) -> Optional[dict]:
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
                    if remove_none_fields:
                        return  {key: value for key, value in dict(row).items() if value is not None}

                    else:
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
                    if not clean_symbol:
                        continue
                    if ':' in clean_symbol or '/' in clean_symbol:
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

    def ensure_entities_tables(self) -> bool:
        """Create entities, aliases, and affiliations tables from entities.sql schema"""
        try:
            # Read entities.sql schema
            entities_schema_file = Path(__file__).parent / "entities.sql"
            if not entities_schema_file.exists():
                print(f"Файл схемы entities.sql не найден: {entities_schema_file}")
                return False
                
            with open(entities_schema_file, 'r', encoding='utf-8') as f:
                entities_sql = f.read()
            
            # Execute entities schema
            with self.get_cursor() as cursor:
                cursor.executescript(entities_sql)
                print("Таблицы entities созданы успешно!")
                
                # Ensure UNIQUE constraint exists for affiliations (for existing databases)
                try:
                    cursor.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS uq_affiliation_unique 
                        ON affiliations(person_id, org_id, role_title)
                    """)
                except Exception as e:
                    # Index might already exist, ignore error
                    pass
                
            return True
        except Exception as e:
            print(f"Ошибка при создании таблиц entities: {e}")
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
        
    def ensure_news_analysis_a_table(self) -> bool:
        """
        Создать таблицу news_analysis_a если она не существует
        
        Returns:
            bool: True если успешно, False при ошибке
        """
        try:
            with self.get_cursor() as cursor:
                # Читаем SQL схему для news_analysis_a
                schema_file = Path(__file__).parent / "news_analysis_a.sql"
                
                if schema_file.exists():
                    with open(schema_file, 'r', encoding='utf-8') as f:
                        schema_sql = f.read()
                    cursor.executescript(schema_sql)
                else:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS news_analysis_a (
                          news_id                     INTEGER PRIMARY KEY REFERENCES news_raw(news_id) ON DELETE CASCADE,
                          created_at_utc                TEXT,
                          headline                    TEXT NOT NULL,
                          symbols_input               TEXT NOT NULL,  -- JSON array
                          actors                      TEXT,           -- JSON array of actor objects
                          event                       TEXT,           -- JSON object with event details
                          symbol_mentions_in_text     TEXT,           -- JSON array
                          symbol_not_mentioned_in_text TEXT,          -- JSON array
                          unresolved_entities         TEXT,           -- JSON array
                          is_news_grounded            INTEGER DEFAULT 0,  -- Boolean: 0=false, 1=true
                          analyzed_at                 TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_news_analysis_a_grounded 
                        ON news_analysis_a(is_news_grounded)
                    """)
                
                print("[OK] Таблица news_analysis_a создана или уже существует")
                return True
                
        except Exception as e:
            print(f"Ошибка при создании таблицы news_analysis_a: {e}")
            return False
    
    def save_news_analysis_a(self, analysis_data: dict) -> bool:
        """
        Сохранить результаты анализа новости в базу данных
        
        Args:
            analysis_data: Словарь с результатами анализа
            
        Returns:
            bool: True если успешно, False при ошибке
        """
        try:
            with self.get_cursor() as cursor:
                # Конвертируем вложенные словари/списки в JSON строки
                data = {
                    'news_id': analysis_data.get('news_id'),
                    'created_at_utc': analysis_data.get('created_at_utc'),
                    'headline': analysis_data.get('headline'),
                    'symbols_input': json.dumps(analysis_data.get('symbols_input', []), ensure_ascii=False),
                    'actors': json.dumps(analysis_data.get('actors', []), ensure_ascii=False),
                    'event': json.dumps(analysis_data.get('event', {}), ensure_ascii=False),
                    'symbol_mentions_in_text': json.dumps(analysis_data.get('symbol_mentions_in_text', []), ensure_ascii=False),
                    'symbol_not_mentioned_in_text': json.dumps(analysis_data.get('symbol_not_mentioned_in_text', []), ensure_ascii=False),
                    'unresolved_entities': json.dumps(analysis_data.get('unresolved_entities', []), ensure_ascii=False)
                }
                
                cursor.execute("""
                    INSERT OR REPLACE INTO news_analysis_a (
                        news_id, created_at_utc, headline, symbols_input,
                        actors, event, symbol_mentions_in_text,
                        symbol_not_mentioned_in_text, unresolved_entities
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    data['news_id'], data['created_at_utc'], data['headline'], data['symbols_input'],
                    data['actors'], data['event'], data['symbol_mentions_in_text'],
                    data['symbol_not_mentioned_in_text'], data['unresolved_entities']
                ))
                
                print(f"[OK] Анализ для новости {data['news_id']} сохранен")
                return True
                
        except Exception as e:
            print(f"Ошибка при сохранении анализа новости {analysis_data.get('news_id')}: {e}")
            return False
    
    def get_news_analysis_a(self, news_id: int) -> Optional[dict]:
        """
        Получить результаты анализа новости
        
        Args:
            news_id: ID новости
            
        Returns:
            dict: Словарь с результатами анализа или None если не найдено
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM news_analysis_a WHERE news_id = ?
                """, (news_id,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                # Конвертируем JSON строки обратно в объекты Python
                result = dict(row)
                for field in ['symbols_input', 'actors', 'event', 'symbol_mentions_in_text', 
                             'symbol_not_mentioned_in_text', 'unresolved_entities']:
                    if result.get(field):
                        result[field] = json.loads(result[field])
                
                return result
                
        except Exception as e:
            print(f"Ошибка при получении анализа новости {news_id}: {e}")
            return None

    @staticmethod
    def parse_news_analysis_a_row(row: dict[str, Any]) -> dict[str, Any]:
        """
        Parses a row from news_analysis_a into typed Python objects
        """
        parsed = {}
        
        # Simple fields
        parsed['news_id'] = int(row['news_id'])
        parsed['headline'] = str(row['headline'])
        parsed['is_news_grounded'] = bool(row['is_news_grounded'])
        
        # JSON fields with typing
        json_fields = {
            'symbols_input': list[str],
            'actors': list[dict[str, Any]],
            'event': dict[str, Any],
            'symbol_mentions_in_text': list[dict[str, Any]],
            'symbol_not_mentioned_in_text': list[str],
            'unresolved_entities': list[dict[str, Any]]
        }
        
        for field, expected_type in json_fields.items():
            if row.get(field):
                try:
                    parsed[field] = json.loads(row[field])
                    # Get base type from parameterized type
                    base_type = get_origin(expected_type) or expected_type
                    if not isinstance(parsed[field], base_type):
                        print(f"Warning: {field} expected {base_type}, got {type(parsed[field])}")
                except json.JSONDecodeError as e:
                    print(f"JSON decode error for {field}: {e}")
                    parsed[field] = None
            else:
                parsed[field] = [] if expected_type == list else {}
        
        # Datetime fields
        for field in ['created_at_utc', 'analyzed_at']:
            if row.get(field):
                parsed[field] = DatabaseConnection.parse_datetime(row[field])
            else:
                parsed[field] = None
        return parsed

    @staticmethod
    def parse_datetime(date_str: str) -> Optional[datetime]:
        """Parses various date formats"""
        if not date_str:
            return None
            
        try:
            # ISO format with Z
            if date_str.endswith('Z'):
                return datetime.fromisoformat(date_str[:-1] + '+00:00')
            # ISO format without timezone
            elif 'T' in date_str:
                return datetime.fromisoformat(date_str)
            # SQLite datetime format
            else:
                return datetime.fromisoformat(date_str)
        except ValueError:
            return None

    def iterate_news_analysis_a(self, skip_grounded: bool = True):
        """
        Generator for lazy iteration over parsed data
        
        Args:
            skip_grounded: If True, skip news where is_news_grounded = 1 (default: True)
        """
        with self.get_cursor() as cursor:
            if skip_grounded:
                cursor.execute("""
                    SELECT * FROM news_analysis_a 
                    WHERE is_news_grounded = 0 
                    ORDER BY analyzed_at DESC
                """)
            else:
                cursor.execute("SELECT * FROM news_analysis_a ORDER BY analyzed_at DESC")
            for row in cursor:
                yield DatabaseConnection.parse_news_analysis_a_row(dict(row))

    def get_total_news_analysis_a(self) -> int:
        """Получить общее количество новостей в news_analysis_a"""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM news_analysis_a")
            return cursor.fetchone()[0]

    def update_news_grounding(self, news_id: int, is_grounded: bool = True) -> bool:
        """
        Обновить статус заземления (grounding) для новости
        
        Args:
            news_id: ID новости
            is_grounded: True если новость заземлена, False иначе
            
        Returns:
            bool: True если успешно, False при ошибке
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    UPDATE news_analysis_a 
                    SET is_news_grounded = ? 
                    WHERE news_id = ?
                """, (1 if is_grounded else 0, news_id))
                
                return cursor.rowcount > 0
                
        except Exception as e:
            print(f"Ошибка при обновлении статуса заземления для новости {news_id}: {e}")
            return False

    # =========================================================
    # ENTITY MANAGEMENT METHODS
    # =========================================================
    
    def insert_entity(self, entity_type: str, **fields) -> int:
        """
        Insert entity into entities table
        
        Args:
            entity_type: 'org' or 'person'
            **fields: Entity fields (canonical_full, given, family, etc.)
            
        Returns:
            int: entity_id of inserted entity
            
        Raises:
            Exception: If duplicate found or validation fails
        """
        try:
            with self.get_cursor() as cursor:
                # Validate required fields
                if entity_type == 'org':
                    if 'canonical_full' not in fields:
                        raise Exception("canonical_full is required for org entities")
                elif entity_type == 'person':
                    if 'given' not in fields or 'family' not in fields:
                        raise Exception("given and family are required for person entities")
                else:
                    raise Exception(f"Invalid entity_type: {entity_type}")
                
                # Check for duplicates
                if entity_type == 'org':
                    existing = self.get_entity_by_canonical('org', canonical_full=fields['canonical_full'])
                    if existing:
                        raise Exception(f"Organization already exists: {fields['canonical_full']}")
                elif entity_type == 'person':
                    existing = self.get_entity_by_canonical('person', given=fields['given'], family=fields['family'])
                    if existing:
                        raise Exception(f"Person already exists: {fields['given']} {fields['family']}")
                
                # Build INSERT query dynamically
                field_names = list(fields.keys())
                placeholders = ', '.join(['?' for _ in field_names])
                field_list = ', '.join(field_names)
                
                query = f"""
                    INSERT INTO entities (entity_type, {field_list})
                    VALUES (?, {placeholders})
                """
                
                values = [entity_type] + [fields[f] for f in field_names]
                cursor.execute(query, values)
                
                entity_id = cursor.lastrowid
                print(f"Entity inserted with ID: {entity_id}")
                return entity_id
                
        except Exception as e:
            print(f"Ошибка при вставке entity: {e}")
            raise
    
    def insert_alias(self, entity_id: int, alias_text: str, alias_type: str, 
                    normalized: str, **optional) -> int:
        """
        Insert alias into aliases table
        
        Args:
            entity_id: ID of the entity this alias refers to
            alias_text: Raw alias text (e.g., "AAPL", "Apple Inc.")
            alias_type: Type of alias ('symbol', 'long_name', 'short_name', etc.)
            normalized: Normalized version for matching
            **optional: Optional fields (lang, script, source, confidence, primary_exchange, is_primary)
            
        Returns:
            int: alias_id of inserted alias
        """
        try:
            with self.get_cursor() as cursor:
                # Default values
                defaults = {
                    'lang': None,
                    'script': None,
                    'source': 'yahoo_finance',
                    'confidence': 1.0,
                    'primary_exchange': None,
                    'is_primary': 0
                }
                defaults.update(optional)
                
                cursor.execute("""
                    INSERT INTO aliases (
                        entity_id, alias_text, alias_type, normalized,
                        lang, script, source, confidence, primary_exchange, is_primary
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    entity_id, alias_text, alias_type, normalized,
                    defaults['lang'], defaults['script'], defaults['source'],
                    defaults['confidence'], defaults['primary_exchange'], defaults['is_primary']
                ))
                
                alias_id = cursor.lastrowid
                return alias_id
                
        except Exception as e:
            # Don't print error for UNIQUE constraint violations (duplicate aliases)
            # This is expected behavior when trying to insert existing aliases
            error_str = str(e)
            if 'UNIQUE constraint' not in error_str and 'constraint failed' not in error_str:
                print(f"Ошибка при вставке alias: {e}")
            raise
    
    def insert_aliases(self, aliases_list: List[tuple]) -> int:
        """
        Batch insert aliases into aliases table.
        
        Args:
            aliases_list: List of tuples, each containing:
                (entity_id, alias_text, alias_type, normalized, lang, script, source, confidence, primary_exchange, is_primary)
                Optional fields can be None.
                
        Returns:
            int: Number of aliases successfully inserted (excluding duplicates)
        """
        if not aliases_list:
            return 0
        
        try:
            with self.get_cursor() as cursor:
                # Use INSERT OR IGNORE to skip duplicates silently
                cursor.executemany("""
                    INSERT OR IGNORE INTO aliases (
                        entity_id, alias_text, alias_type, normalized,
                        lang, script, source, confidence, primary_exchange, is_primary
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, aliases_list)
                
                return cursor.rowcount
                
        except Exception as e:
            error_str = str(e)
            if 'UNIQUE constraint' not in error_str and 'constraint failed' not in error_str:
                print(f"Error in batch insert aliases: {e}")
            return 0
    
    def insert_affiliation(self, person_id: int, org_id: int, role_title: str, **optional) -> Optional[int]:
        """
        Insert affiliation linking person to organization.
        If affiliation already exists, returns existing affiliation_id without creating duplicate.
        
        Args:
            person_id: ID of person entity
            org_id: ID of organization entity
            role_title: Role/title (e.g., "CEO", "SVP & CFO")
            **optional: Optional fields (symbol_alias_id, valid_from, valid_to, source, confidence)
            
        Returns:
            int: affiliation_id of inserted or existing affiliation, None if error
        """
        try:
            # Check if affiliation already exists
            existing = self.get_affiliation(person_id, org_id, role_title)
            if existing:
                return existing['affiliation_id']
            
            with self.get_cursor() as cursor:
                # Default values
                defaults = {
                    'symbol_alias_id': None,
                    'valid_from': None,
                    'valid_to': None,
                    'source': 'yahoo_finance',
                    'confidence': 1.0
                }
                defaults.update(optional)
                
                cursor.execute("""
                    INSERT INTO affiliations (
                        person_id, org_id, role_title, symbol_alias_id,
                        valid_from, valid_to, source, confidence
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    person_id, org_id, role_title, defaults['symbol_alias_id'],
                    defaults['valid_from'], defaults['valid_to'],
                    defaults['source'], defaults['confidence']
                ))
                
                affiliation_id = cursor.lastrowid
                return affiliation_id
                
        except Exception as e:
            # Don't print error for UNIQUE constraint violations (duplicate affiliations)
            error_str = str(e)
            if 'UNIQUE constraint' not in error_str and 'constraint failed' not in error_str:
                print(f"Ошибка при вставке affiliation: {e}")
            return None
    
    def get_affiliation(self, person_id: int, org_id: int, role_title: str) -> Optional[dict]:
        """
        Check if affiliation already exists
        
        Args:
            person_id: ID of person entity
            org_id: ID of organization entity
            role_title: Role/title
            
        Returns:
            dict: Affiliation row as dict or None if not found
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM affiliations 
                    WHERE person_id = ? AND org_id = ? AND role_title = ?
                """, (person_id, org_id, role_title))
                
                row = cursor.fetchone()
                return dict(row) if row else None
                
        except Exception as e:
            print(f"Ошибка при поиске affiliation: {e}")
            return None
    
    def get_entity_by_canonical(self, entity_type: Literal['org', 'person'], canonical_full: Optional[str] = None, 
                               given: Optional[str] = None, family: Optional[str] = None) -> Optional[dict]:
        """
        Check if entity already exists
        
        Args:
            entity_type: 'org' or 'person'
            canonical_full: For org entities
            given: For person entities
            family: For person entities
            
        Returns:
            dict: Full entity row as dict or None
        """
        try:
            with self.get_cursor() as cursor:
                if entity_type == 'org':
                    if not canonical_full:
                        return None
                    cursor.execute("""
                        SELECT * FROM entities 
                        WHERE entity_type = 'org' AND canonical_full = ?
                    """, (canonical_full,))
                elif entity_type == 'person':
                    if not given or not family:
                        return None
                    cursor.execute("""
                        SELECT * FROM entities 
                        WHERE entity_type = 'person' AND given = ? AND family = ?
                    """, (given, family))
                else:
                    return None
                
                row = cursor.fetchone()
                return dict(row) if row else None
                
        except Exception as e:
            print(f"Ошибка при поиске entity: {e}")
            return None
    
    def get_all_entities_by_type(self, entity_type: Literal['org', 'person']) -> List[dict]:
        """
        Get all entities of a specific type for caching purposes
        
        Args:
            entity_type: 'org' or 'person'
            
        Returns:
            List[dict]: List of all entities of the specified type
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM entities 
                    WHERE entity_type = ?
                """, (entity_type,))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            print(f"Ошибка при получении всех entities типа {entity_type}: {e}")
            return []

    # =========================================================
    # SEARCH METHODS FOR NEWS ANALYSIS
    # =========================================================
    
    def find_entity_by_symbol(self, symbol: str) -> Optional[dict]:
        """
        Search for organization by stock symbol
        
        Args:
            symbol: Stock symbol (e.g., "AAPL")
            
        Returns:
            dict: Entity dict or None
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT e.* FROM entities e 
                    JOIN aliases a ON e.entity_id = a.entity_id 
                    WHERE a.alias_type = 'symbol' AND a.normalized = ?
                """, (symbol.lower(),))
                
                row = cursor.fetchone()
                return dict(row) if row else None
                
        except Exception as e:
            print(f"Ошибка при поиске entity по символу {symbol}: {e}")
            return None


    def find_entity_by_id(self, entity_id: int) -> Optional[dict]:
        """
        Find an entity by its unique entity_id.

        Args:
            entity_id: The unique ID of the entity.

        Returns:
            dict: The entity as a dictionary, or None if not found.
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM entities WHERE entity_id = ?
                """, (entity_id,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            print(f"Ошибка при поиске entity по entity_id {entity_id}: {e}")
            return None

            
    def find_entity_by_alias(self, alias_text: str, fuzzy: bool = False) -> list[dict]:
        """
        Search entities by any alias text
        
        Args:
            alias_text: Text to search for
            fuzzy: If True, use FTS5 for partial matching
            
        Returns:
            list: List of dicts with entity, alias_type, confidence, alias_text
        """
        try:
            with self.get_cursor() as cursor:
                if fuzzy:
                    # Use FTS5 for fuzzy matching
                    # Escape special FTS5 characters to avoid syntax errors
                    escaped_text = self._escape_fts5_query(alias_text)
                    cursor.execute("""
                        SELECT DISTINCT e.*, a.alias_type, a.confidence, a.alias_text
                        FROM entities e 
                        JOIN aliases a ON e.entity_id = a.entity_id
                        JOIN alias_fts fts ON a.alias_id = fts.rowid
                        WHERE alias_fts MATCH ?
                        ORDER BY a.confidence DESC, a.is_primary DESC
                    """, (escaped_text,))
                else:
                    # Exact match on normalized field
                    normalized = self._normalize_text(alias_text)
                    cursor.execute("""
                        SELECT e.*, a.alias_type, a.confidence, a.alias_text
                        FROM entities e 
                        JOIN aliases a ON e.entity_id = a.entity_id
                        WHERE a.normalized = ?
                        ORDER BY a.confidence DESC
                    """, (normalized,))
                
                results = []
                for row in cursor.fetchall():
                    results.append({
                        'entity': dict(row),
                        'alias_type': row['alias_type'],
                        'confidence': row['confidence'],
                        'alias_text': row['alias_text']
                    })
                
                return results
                
        except Exception as e:
            print(f"Ошибка при поиске entity по alias {alias_text}: {e}")
            return []
    
    def find_person_by_name(self, family: str, given: str = None, given_prefix: str = None) -> list[dict]:
        """
        Search for persons by name components
        
        Args:
            family: Family name (required)
            given: Given name (optional)
            given_prefix: Given name prefix (optional)
            
        Returns:
            list: List of matching person entity dicts
        """
        try:
            with self.get_cursor() as cursor:
                family_norm = self._normalize_text(family)
                
                # Build WHERE clause dynamically
                where_conditions = ["entity_type = 'person'", "family_norm = ?"]
                params = [family_norm]
                
                if given:
                    given_norm = self._normalize_text(given)
                    where_conditions.append("given_norm = ?")
                    params.append(given_norm)
                elif given_prefix:
                    given_prefix_norm = self._normalize_text(given_prefix)
                    # Если префикс короткий (1-2 символа), ищем по given_initial
                    if len(given_prefix_norm) <= 2:
                        where_conditions.append("given_initial = ?")
                        params.append(given_prefix_norm)
                    else:
                        # Если префикс длинный (3+ символов), ищем по given_prefix3
                        where_conditions.append("given_prefix3 = ?")
                        params.append(given_prefix_norm)
                
                query = f"""
                    SELECT * FROM entities 
                    WHERE {' AND '.join(where_conditions)}
                """
                
                cursor.execute(query, params)
                
                results = []
                for row in cursor.fetchall():
                    results.append(dict(row))
                
                return results
                
        except Exception as e:
            print(f"Ошибка при поиске person по имени {family}: {e}")
            return []
    
    def find_person_affiliations(self, person_entity_id: int, active_only: bool = True) -> list[dict]:
        """
        Get all organizations linked to a person
        
        Args:
            person_entity_id: ID of person entity
            active_only: If True, filter WHERE valid_to IS NULL
            
        Returns:
            list: List of dicts with org, symbol, role_title, etc.
        """
        try:
            with self.get_cursor() as cursor:
                where_clause = "WHERE a.person_id = ?"
                params = [person_entity_id]
                
                if active_only:
                    where_clause += " AND a.valid_to IS NULL"
                
                cursor.execute(f"""
                    SELECT 
                        e.*,
                        s.alias_text as symbol,
                        a.role_title,
                        a.valid_from,
                        a.valid_to,
                        a.confidence
                    FROM affiliations a
                    JOIN entities e ON a.org_id = e.entity_id
                    LEFT JOIN aliases s ON a.symbol_alias_id = s.alias_id
                    {where_clause}
                    ORDER BY a.confidence DESC
                """, params)
                
                results = []
                for row in cursor.fetchall():
                    # Создаем словарь для организации из всех полей entity
                    org_data = {}
                    for key, value in dict(row).items():
                        if key not in ['symbol', 'role_title', 'valid_from', 'valid_to', 'confidence']:
                            org_data[key] = value
                    
                    results.append({
                        'org': org_data,
                        'symbol': row['symbol'],
                        'role_title': row['role_title'],
                        'valid_from': row['valid_from'],
                        'valid_to': row['valid_to'],
                        'confidence': row['confidence']
                    })
                
                return results
                
        except Exception as e:
            print(f"Ошибка при поиске affiliations для person {person_entity_id}: {e}")
            return []
    
    def get_entity_context(self, entity_id: int) -> dict:
        """
        Get comprehensive context for an entity
        
        Args:
            entity_id: ID of entity
            
        Returns:
            dict: Structured context with entity, aliases, affiliations
        """
        try:
            with self.get_cursor() as cursor:
                # Get entity
                cursor.execute("SELECT * FROM entities WHERE entity_id = ?", (entity_id,))
                entity_row = cursor.fetchone()
                if not entity_row:
                    return {}
                
                entity = dict(entity_row)
                
                # Get aliases
                cursor.execute("""
                    SELECT alias_text, alias_type, is_primary 
                    FROM aliases 
                    WHERE entity_id = ? 
                    ORDER BY is_primary DESC, alias_type
                """, (entity_id,))
                
                aliases = []
                for row in cursor.fetchall():
                    aliases.append({
                        'alias_text': row['alias_text'],
                        'alias_type': row['alias_type'],
                        'is_primary': row['is_primary']
                    })
                
                # Get affiliations
                affiliations = []
                if entity['entity_type'] == 'org':
                    # Get affiliated persons
                    cursor.execute("""
                        SELECT 
                            e.*,
                            a.role_title,
                            a.valid_from,
                            a.valid_to,
                            a.confidence
                        FROM affiliations a
                        JOIN entities e ON a.person_id = e.entity_id
                        WHERE a.org_id = ?
                        ORDER BY a.confidence DESC
                    """, (entity_id,))
                    
                    for row in cursor.fetchall():
                        # Создаем словарь для персоны из всех полей entity
                        person_data = {}
                        for key, value in dict(row).items():
                            if key not in ['role_title', 'valid_from', 'valid_to', 'confidence']:
                                person_data[key] = value
                        
                        affiliations.append({
                            'person': person_data,
                            'role_title': row['role_title'],
                            'valid_from': row['valid_from'],
                            'valid_to': row['valid_to'],
                            'confidence': row['confidence']
                        })
                        
                elif entity['entity_type'] == 'person':
                    # Get affiliated organizations
                    cursor.execute("""
                        SELECT 
                            e.*,
                            s.alias_text as symbol,
                            a.role_title,
                            a.valid_from,
                            a.valid_to,
                            a.confidence
                        FROM affiliations a
                        JOIN entities e ON a.org_id = e.entity_id
                        LEFT JOIN aliases s ON a.symbol_alias_id = s.alias_id
                        WHERE a.person_id = ?
                        ORDER BY a.confidence DESC
                    """, (entity_id,))
                    
                    for row in cursor.fetchall():
                        # Создаем словарь для организации из всех полей entity
                        org_data = {}
                        for key, value in dict(row).items():
                            if key not in ['symbol', 'role_title', 'valid_from', 'valid_to', 'confidence']:
                                org_data[key] = value
                        
                        affiliations.append({
                            'org': org_data,
                            'symbol': row['symbol'],
                            'role_title': row['role_title'],
                            'valid_from': row['valid_from'],
                            'valid_to': row['valid_to'],
                            'confidence': row['confidence']
                        })
                
                return {
                    'entity': entity,
                    'aliases': aliases,
                    'affiliations': affiliations
                }
                
        except Exception as e:
            print(f"Ошибка при получении context для entity {entity_id}: {e}")
            return {}
    
    def _normalize_text(self, text: str) -> str:
        """
        Normalize text using NFKD decomposition, remove diacritics, and convert to lowercase.
        Helper method for search functionality.
        """
        if not text:
            return ""
        
        import unicodedata
        
        # NFKD normalization
        normalized = unicodedata.normalize('NFKD', text)
        
        # Remove diacritics (combining characters)
        without_diacritics = ''.join(
            char for char in normalized 
            if not unicodedata.combining(char)
        )
        
        # Convert to lowercase and remove extra whitespace
        result = without_diacritics.lower().strip()
        
        return result
    
    def _escape_fts5_query(self, query: str) -> str:
        """
        Escape special FTS5 characters to avoid syntax errors.
        FTS5 special characters: " ' * + - : ^ ~
        """
        if not query:
            return ""
        
        # Escape special FTS5 characters
        escaped = query.replace('"', '""')  # Double quotes for literal quotes
        escaped = escaped.replace("'", "''")  # Single quotes for literal quotes
        escaped = escaped.replace('*', '')   # Remove asterisks
        escaped = escaped.replace('+', '')   # Remove plus signs
        escaped = escaped.replace('-', '')   # Remove minus signs
        escaped = escaped.replace(':', '')   # Remove colons
        escaped = escaped.replace('^', '')   # Remove carets
        escaped = escaped.replace('~', '')   # Remove tildes
        
        # Wrap in quotes for phrase matching
        return f'"{escaped}"'

    # ========== Web Search Cache Functions ==========
    
    def ensure_web_search_tables(self) -> bool:
        """Create web_search_cache table from web_search.sql schema"""
        try:
            # Read web_search.sql schema
            web_search_schema_file = Path(__file__).parent / "web_search.sql"
            if not web_search_schema_file.exists():
                print(f"Файл схемы web_search.sql не найден: {web_search_schema_file}")
                return False
                
            with open(web_search_schema_file, 'r', encoding='utf-8') as f:
                web_search_sql = f.read()
            
            # Execute web_search schema
            with self.get_cursor() as cursor:
                cursor.executescript(web_search_sql)
                print("Таблицы web_search_cache созданы успешно!")
                
            return True
        except Exception as e:
            print(f"Ошибка при создании таблиц web_search: {e}")
            return False

    def get_cached_search(self, normalized_query: str, provider: Optional[str] = None, fuzzy: bool = False, 
                         filter_empty: bool = False) -> Optional[dict]:
        """
        Retrieve cached search result by normalized query
        If fuzzy=True, uses FTS5 search for flexible matching
        
        Args:
            normalized_query: Normalized query string
            provider: Optional provider filter ('wikipedia', 'wikidata', etc.)
            fuzzy: If True, use FTS search for partial matching
            filter_empty: If True, filter out results with status 'empty', 'error', or 'ratelimited'
            
        Returns:
            Dict with cached result or None if not found (or filtered out)
        """
        try:
            with self.get_cursor() as cursor:
                if fuzzy:
                    # Use FTS5 for flexible search
                    if provider:
                        cursor.execute("""
                            SELECT c.* FROM web_search_cache c
                            INNER JOIN web_search_cache_fts fts ON c.id = fts.rowid
                            WHERE web_search_cache_fts MATCH ? AND c.provider = ?
                            ORDER BY 
                                CASE WHEN c.status = 'ok' AND c.results_json != '[]' THEN 0 ELSE 1 END,
                                c.fetched_at_utc DESC
                            LIMIT 1
                        """, (normalized_query, provider))
                    else:
                        cursor.execute("""
                            SELECT c.* FROM web_search_cache c
                            INNER JOIN web_search_cache_fts fts ON c.id = fts.rowid
                            WHERE web_search_cache_fts MATCH ?
                            ORDER BY 
                                CASE WHEN c.status = 'ok' AND c.results_json != '[]' THEN 0 ELSE 1 END,
                                c.fetched_at_utc DESC
                            LIMIT 1
                        """, (normalized_query,))
                else:
                    # Exact match - prioritize results with status='ok' and non-empty results
                    if provider:
                        cursor.execute("""
                            SELECT * FROM web_search_cache 
                            WHERE normalized_query = ? AND provider = ?
                            ORDER BY 
                                CASE WHEN status = 'ok' AND results_json != '[]' THEN 0 ELSE 1 END,
                                fetched_at_utc DESC
                            LIMIT 1
                        """, (normalized_query, provider))
                    else:
                        cursor.execute("""
                            SELECT * FROM web_search_cache 
                            WHERE normalized_query = ?
                            ORDER BY 
                                CASE WHEN status = 'ok' AND results_json != '[]' THEN 0 ELSE 1 END,
                                fetched_at_utc DESC
                            LIMIT 1
                        """, (normalized_query,))
                
                row = cursor.fetchone()
                if row:
                    result = dict(row)
                    result['results'] = json.loads(result['results_json'])
                    
                    # Filter out empty/invalid results if requested
                    if filter_empty and result.get('status') in ('empty', 'error', 'ratelimited'):
                        return None
                    
                    return result
                return None
        except Exception as e:
            print(f"Ошибка при получении кэша для '{normalized_query}': {e}")
            return None

    def get_all_cached_searches(self, normalized_query: str, fuzzy: bool = False, 
                               filter_empty: bool = True) -> List[Dict[str, Any]]:
        """
        Retrieve all cached search results for normalized query from all providers
        
        Args:
            normalized_query: Normalized query string
            fuzzy: If True, use FTS search for partial matching
            filter_empty: If True, filter out empty/error results
            
        Returns:
            List of cached results, ordered by priority (ok with results > empty > error)
            Each result is a dict with 'provider', 'results', 'status', etc.
        """
        try:
            with self.get_cursor() as cursor:
                if fuzzy:
                    cursor.execute("""
                        SELECT c.* FROM web_search_cache c
                        INNER JOIN web_search_cache_fts fts ON c.id = fts.rowid
                        WHERE web_search_cache_fts MATCH ?
                        ORDER BY 
                            CASE 
                                WHEN c.status = 'ok' AND c.results_json != '[]' THEN 0
                                WHEN c.status = 'empty' THEN 1
                                ELSE 2
                            END,
                            c.fetched_at_utc DESC
                    """, (normalized_query,))
                else:
                    cursor.execute("""
                        SELECT * FROM web_search_cache 
                        WHERE normalized_query = ?
                        ORDER BY 
                            CASE 
                                WHEN status = 'ok' AND results_json != '[]' THEN 0
                                WHEN status = 'empty' THEN 1
                                ELSE 2
                            END,
                            fetched_at_utc DESC
                    """, (normalized_query,))
                
                results = []
                for row in cursor.fetchall():
                    result = dict(row)
                    result['results'] = json.loads(result['results_json'])
                    
                    if filter_empty and result.get('status') in ('empty', 'error', 'ratelimited'):
                        continue
                    
                    results.append(result)
                
                return results
        except Exception as e:
            print(f"Ошибка при получении всех кэшей для '{normalized_query}': {e}")
            return []

    def save_search_result(self, provider: str, normalized_query: str, results_json: list, status: str, 
                          http_code: Optional[int] = None, error: Optional[str] = None, 
                          backoff_until_utc: Optional[str] = None) -> bool:
        """
        Save search result to cache
        
        Args:
            provider: Search provider name ('wikipedia', 'wikidata', etc.)
            normalized_query: Normalized query string
            results_json: List of result dicts
            status: 'ok' | 'empty' | 'error' | 'ratelimited'
            http_code: HTTP response code if applicable
            error: Error message if applicable
            backoff_until_utc: ISO8601 timestamp when to retry after backoff
        """
        try:
            now = datetime.now(timezone.utc).isoformat()
            
            with self.get_cursor() as cursor:
                cursor.execute("""
                    INSERT OR REPLACE INTO web_search_cache 
                    (provider, normalized_query, results_json, status, http_code, error, 
                     fetched_at_utc, attempts, backoff_until_utc)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
                """, (
                    provider,
                    normalized_query,
                    json.dumps(results_json),
                    status,
                    http_code,
                    error,
                    now,
                    backoff_until_utc
                ))
            return True
        except Exception as e:
            print(f"Ошибка при сохранении результата поиска для '{normalized_query}': {e}")
            return False

    def is_provider_in_backoff(self, provider: str) -> bool:
        """Check if provider is currently in backoff period"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT backoff_until_utc FROM web_search_cache 
                    WHERE provider = ? AND backoff_until_utc IS NOT NULL AND backoff_until_utc > ?
                    LIMIT 1
                """, (provider, datetime.now(timezone.utc).isoformat()))
                row = cursor.fetchone()
                return row is not None
        except Exception as e:
            print(f"Ошибка при проверке backoff для '{provider}': {e}")
            return False

    def update_search_attempts(self, provider: str, normalized_query: str) -> int:
        """Increment attempt counter for search and return new value"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    UPDATE web_search_cache 
                    SET attempts = attempts + 1 
                    WHERE provider = ? AND normalized_query = ?
                """, (provider, normalized_query))
                cursor.execute("""
                    SELECT attempts FROM web_search_cache 
                    WHERE provider = ? AND normalized_query = ?
                """, (provider, normalized_query))
                row = cursor.fetchone()
                return row['attempts'] if row else 1
        except Exception as e:
            print(f"Ошибка при обновлении attempts для '{normalized_query}': {e}")
            return 1

    def get_provider_daily_usage(self, provider: str) -> int:
        """Get count of searches made by provider today (UTC)"""
        try:
            today_start = datetime.now(timezone.utc).date().isoformat()
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM web_search_cache 
                    WHERE provider = ? 
                    AND fetched_at_utc >= ?
                    AND status IN ('ok', 'empty')
                """, (provider, today_start))
                row = cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            print(f"Ошибка при получении daily usage для '{provider}': {e}")
            return 0
    
    def get_recent_empty_count(self, provider: str, minutes: int = 30) -> int:
        """
        Count how many empty responses this provider has returned in recent time
        
        Args:
            provider: Provider name
            minutes: Time window in minutes to look back
        
        Returns:
            Number of consecutive empty responses
        """
        try:
            cutoff_time = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM web_search_cache 
                    WHERE provider = ? 
                    AND fetched_at_utc >= ?
                    AND status = 'empty'
                    ORDER BY fetched_at_utc DESC
                """, (provider, cutoff_time))
                row = cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            print(f"Ошибка при подсчете пустых ответов для '{provider}': {e}")
            return 0