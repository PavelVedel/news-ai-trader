"""
Скрипт для загрузки фундаментальных данных с Yahoo Finance
Загружает данные для всех символов из news.db и сохраняет в таблицу fundamentals
"""

import yfinance as yf
import time
import logging
from typing import List, Dict, Optional
from datetime import datetime
from libs.database.connection import DatabaseConnection

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fundamentals_update.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FundamentalsUpdater:
    def __init__(self, db_path: str = "data/db/news.db"):
        self.db_path = db_path
        self.db_connection = DatabaseConnection(db_path)
        
    def get_all_symbols(self) -> List[str]:
        """Получить все уникальные символы из базы данных"""
        try:
            symbols = self.db_connection.get_all_symbols()
            logger.info(f"Найдено {len(symbols)} уникальных символов в базе данных")
            return symbols
        except Exception as e:
            logger.error(f"Ошибка при получении символов: {e}")
            return []
    
    def get_fundamentals_for_symbol(self, symbol: str) -> Optional[Dict]:
        """
        Получить фундаментальные данные для символа с Yahoo Finance
        
        Args:
            symbol: Тикер акции (например, 'AAPL')
            
        Returns:
            Dict с фундаментальными данными или None при ошибке
        """
        try:
            logger.info(f"Загружаю данные для {symbol}...")
            
            # Создаем объект Ticker
            ticker = yf.Ticker(symbol)
            
            # Получаем основную информацию
            info = ticker.info
            
            if not info:
                logger.warning(f"Нет данных для {symbol}")
                return None
            
            # Извлекаем нужные поля
            fundamentals = {
                'symbol': symbol,
                
                # Основные финансовые показатели
                'market_cap': self._safe_get(info, 'marketCap'),
                'enterprise_value': self._safe_get(info, 'enterpriseValue'),
                'pe_ratio': self._safe_get(info, 'trailingPE'),
                'forward_pe': self._safe_get(info, 'forwardPE'),
                'peg_ratio': self._safe_get(info, 'pegRatio'),
                'price_to_book': self._safe_get(info, 'priceToBook'),
                'price_to_sales': self._safe_get(info, 'priceToSalesTrailing12Months'),
                'enterprise_to_revenue': self._safe_get(info, 'enterpriseToRevenue'),
                'enterprise_to_ebitda': self._safe_get(info, 'enterpriseToEbitda'),
                
                # Показатели доходности
                'return_on_equity': self._safe_get(info, 'returnOnEquity'),
                'return_on_assets': self._safe_get(info, 'returnOnAssets'),
                'return_on_capital': self._safe_get(info, 'returnOnCapital'),
                
                # Показатели ликвидности
                'current_ratio': self._safe_get(info, 'currentRatio'),
                'quick_ratio': self._safe_get(info, 'quickRatio'),
                'debt_to_equity': self._safe_get(info, 'debtToEquity'),
                
                # Дивиденды
                'dividend_yield': self._safe_get(info, 'dividendYield'),
                'dividend_rate': self._safe_get(info, 'dividendRate'),
                'payout_ratio': self._safe_get(info, 'payoutRatio'),
                
                # Технические показатели
                'beta': self._safe_get(info, 'beta'),
                'fifty_two_week_high': self._safe_get(info, 'fiftyTwoWeekHigh'),
                'fifty_two_week_low': self._safe_get(info, 'fiftyTwoWeekLow'),
                'fifty_day_average': self._safe_get(info, 'fiftyDayAverage'),
                'two_hundred_day_average': self._safe_get(info, 'twoHundredDayAverage'),
                
                # Метаданные
                'sector': self._safe_get(info, 'sector'),
                'industry': self._safe_get(info, 'industry'),
                'country': self._safe_get(info, 'country'),
                'currency': self._safe_get(info, 'currency'),
                
                # Временные метки
                'last_updated': datetime.now().isoformat(),
                'data_source': 'yahoo_finance'
            }
            
            logger.info(f"[OK] Успешно загружены данные для {symbol}")
            return fundamentals
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных для {symbol}: {e}")
            return None
    
    def _safe_get(self, data: Dict, key: str, default=None):
        """Безопасно получить значение из словаря"""
        try:
            value = data.get(key, default)
            # Проверяем, что значение не является строкой "N/A" или похожей
            if isinstance(value, str) and value.upper() in ['N/A', 'NAN', 'INF', '-INF']:
                return default
            return value
        except:
            return default
    
    def save_fundamentals_to_db(self, fundamentals: Dict) -> bool:
        """
        Сохранить фундаментальные данные в базу данных
        
        Args:
            fundamentals: Словарь с фундаментальными данными
            
        Returns:
            bool: True если успешно, False при ошибке
        """
        try:
            with self.db_connection.get_cursor() as cursor:
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
            logger.error(f"Ошибка при сохранении данных для {fundamentals['symbol']}: {e}")
            return False
    
    def create_fundamentals_table(self):
        """Создать таблицу fundamentals если её нет"""
        try:
            with self.db_connection.get_cursor() as cursor:
                # Проверяем, существует ли таблица
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='fundamentals'
                """)
                
                if not cursor.fetchone():
                    logger.info("Создаю таблицу fundamentals...")
                    
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
                    
                    logger.info("[OK] Таблица fundamentals создана успешно")
                else:
                    logger.info("[OK] Таблица fundamentals уже существует")
                    
        except Exception as e:
            logger.error(f"Ошибка при создании таблицы: {e}")
    
    def update_all_fundamentals(self, delay_seconds: float = 0.1, max_symbols: int = None, max_age_months: int = 3):
        """
        Обновить фундаментальные данные для символов, требующих обновления
        
        Args:
            delay_seconds: Задержка между запросами (для избежания блокировки)
            max_symbols: Максимальное количество символов для обработки
            max_age_months: Максимальный возраст данных в месяцах
        """
        try:
            # Убеждаемся что таблица fundamentals существует
            if not self.db_connection.ensure_fundamentals_table():
                logger.error("Не удалось создать таблицу fundamentals")
                return
            
            # Получаем символы, требующие обновления
            symbols_to_update = self.db_connection.get_fundamentals_symbols_needing_update(max_age_months)
            
            if not symbols_to_update:
                logger.info("Все символы имеют актуальные данные (не старше 3 месяцев)")
                return
            
            # Ограничиваем количество символов если указан лимит
            if max_symbols is not None:
                symbols_to_update = symbols_to_update[:max_symbols]
            
            logger.info(f"Найдено {len(symbols_to_update)} символов, требующих обновления (возраст > {max_age_months} месяцев)")
            logger.info(f"Обрабатываю {len(symbols_to_update)} символов (лимит: {max_symbols})")
            
            successful_updates = 0
            failed_updates = 0
            
            for i, symbol in enumerate(symbols_to_update, 1):
                try:
                    logger.info(f"[{i}/{len(symbols_to_update)}] Обрабатываю {symbol}")
                    
                    # Получаем фундаментальные данные
                    fundamentals = self.get_fundamentals_for_symbol(symbol)
                    
                    if fundamentals:
                        # Сохраняем в базу данных используя метод из connection.py
                        if self.db_connection.save_fundamentals(fundamentals):
                            successful_updates += 1
                            logger.info(f"[OK] {symbol} обновлен успешно")
                        else:
                            failed_updates += 1
                            logger.error(f"[ERROR] {symbol} - ошибка сохранения")
                    else:
                        failed_updates += 1
                        logger.warning(f"[NO DATA] {symbol} - нет данных")
                    
                    # Задержка между запросами
                    if i < len(symbols_to_update):  # Не ждем после последнего символа
                        time.sleep(delay_seconds)
                        
                except Exception as e:
                    failed_updates += 1
                    logger.error(f"[CRITICAL] {symbol} - критическая ошибка: {e}")
                    continue
            
            # Итоговая статистика
            logger.info("=" * 60)
            logger.info("ОБНОВЛЕНИЕ ЗАВЕРШЕНО")
            logger.info("=" * 60)
            logger.info(f"Всего символов: {len(symbols_to_update)}")
            logger.info(f"Успешно обновлено: {successful_updates}")
            logger.info(f"Ошибок: {failed_updates}")
            logger.info(f"Процент успеха: {(successful_updates/len(symbols_to_update)*100):.1f}%")
            
            # Показываем статистику из базы данных
            stats = self.db_connection.get_fundamentals_stats()
            if stats:
                logger.info(f"\nСТАТИСТИКА БАЗЫ ДАННЫХ:")
                logger.info(f"Всего записей в fundamentals: {stats.get('total_symbols', 0)}")
                logger.info(f"С сектором: {stats.get('symbols_with_sector', 0)}")
                logger.info(f"С P/E: {stats.get('symbols_with_pe', 0)}")
                logger.info(f"С рыночной капитализацией: {stats.get('symbols_with_market_cap', 0)}")
                logger.info(f"Последнее обновление: {stats.get('last_update', 'N/A')}")
                
            # Показываем информацию об обновлении
            logger.info(f"\nИНФОРМАЦИЯ ОБ ОБНОВЛЕНИИ:")
            logger.info(f"Обновлялись только символы старше {max_age_months} месяцев")
            logger.info(f"Следующий запуск обновит символы, которые станут старше {max_age_months} месяцев")
            
        except Exception as e:
            logger.error(f"Критическая ошибка при обновлении: {e}")
        finally:
            self.db_connection.close()


def main():
    """Основная функция"""
    print("=" * 60)
    print("ОБНОВЛЕНИЕ ФУНДАМЕНТАЛЬНЫХ ДАННЫХ С YAHOO FINANCE")
    print("=" * 60)
    
    try:
        # Создаем обновлятель
        updater = FundamentalsUpdater()
        
        # Запускаем обновление (только символы старше 3 месяцев, лимит 100)
        updater.update_all_fundamentals(delay_seconds=0, max_symbols=None, max_age_months=3)
        
    except KeyboardInterrupt:
        print("\nОбновление прервано пользователем")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        logger.error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    main()
