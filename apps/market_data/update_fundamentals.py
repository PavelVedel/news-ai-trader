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
        logging.FileHandler('fundamentals_update.log'),
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
            
            logger.info(f"✓ Успешно загружены данные для {symbol}")
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
                    
                    logger.info("✓ Таблица fundamentals создана успешно")
                else:
                    logger.info("✓ Таблица fundamentals уже существует")
                    
        except Exception as e:
            logger.error(f"Ошибка при создании таблицы: {e}")
    
    def update_all_fundamentals(self, delay_seconds: float = 0.1):
        """
        Обновить фундаментальные данные для всех символов
        
        Args:
            delay_seconds: Задержка между запросами (для избежания блокировки)
        """
        try:
            # Создаем таблицу если её нет
            self.create_fundamentals_table()
            
            # Получаем все символы
            symbols = self.get_all_symbols()
            
            if not symbols:
                logger.error("Не удалось получить символы из базы данных")
                return
            
            logger.info(f"Начинаю обновление фундаментальных данных для {len(symbols)} символов")
            
            successful_updates = 0
            failed_updates = 0
            
            for i, symbol in enumerate(symbols, 1):
                try:
                    logger.info(f"[{i}/{len(symbols)}] Обрабатываю {symbol}")
                    
                    # Получаем фундаментальные данные
                    fundamentals = self.get_fundamentals_for_symbol(symbol)
                    
                    if fundamentals:
                        # Сохраняем в базу данных
                        if self.save_fundamentals_to_db(fundamentals):
                            successful_updates += 1
                            logger.info(f"✓ {symbol} обновлен успешно")
                        else:
                            failed_updates += 1
                            logger.error(f"✗ {symbol} - ошибка сохранения")
                    else:
                        failed_updates += 1
                        logger.warning(f"✗ {symbol} - нет данных")
                    
                    # Задержка между запросами
                    if i < len(symbols):  # Не ждем после последнего символа
                        time.sleep(delay_seconds)
                        
                except Exception as e:
                    failed_updates += 1
                    logger.error(f"✗ {symbol} - критическая ошибка: {e}")
                    continue
            
            # Итоговая статистика
            logger.info("=" * 60)
            logger.info("ОБНОВЛЕНИЕ ЗАВЕРШЕНО")
            logger.info("=" * 60)
            logger.info(f"Всего символов: {len(symbols)}")
            logger.info(f"Успешно обновлено: {successful_updates}")
            logger.info(f"Ошибок: {failed_updates}")
            logger.info(f"Процент успеха: {(successful_updates/len(symbols)*100):.1f}%")
            
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
        
        # Запускаем обновление
        updater.update_all_fundamentals()
        
    except KeyboardInterrupt:
        print("\nОбновление прервано пользователем")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        logger.error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    main()
