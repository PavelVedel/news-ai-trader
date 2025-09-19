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
            
            # Извлекаем все доступные поля из Yahoo Finance
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
                'five_year_avg_dividend_yield': self._safe_get(info, 'fiveYearAvgDividendYield'),
                'trailing_annual_dividend_rate': self._safe_get(info, 'trailingAnnualDividendRate'),
                'trailing_annual_dividend_yield': self._safe_get(info, 'trailingAnnualDividendYield'),
                
                # Технические показатели
                'beta': self._safe_get(info, 'beta'),
                'fifty_two_week_high': self._safe_get(info, 'fiftyTwoWeekHigh'),
                'fifty_two_week_low': self._safe_get(info, 'fiftyTwoWeekLow'),
                'fifty_day_average': self._safe_get(info, 'fiftyDayAverage'),
                'two_hundred_day_average': self._safe_get(info, 'twoHundredDayAverage'),
                'fifty_two_week_change_percent': self._safe_get(info, 'fiftyTwoWeekChangePercent'),
                'fifty_day_average_change': self._safe_get(info, 'fiftyDayAverageChange'),
                'fifty_day_average_change_percent': self._safe_get(info, 'fiftyDayAverageChangePercent'),
                'two_hundred_day_average_change': self._safe_get(info, 'twoHundredDayAverageChange'),
                'two_hundred_day_average_change_percent': self._safe_get(info, 'twoHundredDayAverageChangePercent'),
                
                # Дополнительные финансовые показатели
                'book_value': self._safe_get(info, 'bookValue'),
                'total_cash': self._safe_get(info, 'totalCash'),
                'total_cash_per_share': self._safe_get(info, 'totalCashPerShare'),
                'total_debt': self._safe_get(info, 'totalDebt'),
                'total_revenue': self._safe_get(info, 'totalRevenue'),
                'revenue_per_share': self._safe_get(info, 'revenuePerShare'),
                'gross_profits': self._safe_get(info, 'grossProfits'),
                'free_cashflow': self._safe_get(info, 'freeCashflow'),
                'operating_cashflow': self._safe_get(info, 'operatingCashflow'),
                'ebitda': self._safe_get(info, 'ebitda'),
                'net_income_to_common': self._safe_get(info, 'netIncomeToCommon'),
                
                # Показатели роста
                'earnings_growth': self._safe_get(info, 'earningsGrowth'),
                'revenue_growth': self._safe_get(info, 'revenueGrowth'),
                'earnings_quarterly_growth': self._safe_get(info, 'earningsQuarterlyGrowth'),
                
                # Маржинальность
                'gross_margins': self._safe_get(info, 'grossMargins'),
                'ebitda_margins': self._safe_get(info, 'ebitdaMargins'),
                'operating_margins': self._safe_get(info, 'operatingMargins'),
                'profit_margins': self._safe_get(info, 'profitMargins'),
                
                # Акции и доля
                'shares_outstanding': self._safe_get(info, 'sharesOutstanding'),
                'float_shares': self._safe_get(info, 'floatShares'),
                'shares_short': self._safe_get(info, 'sharesShort'),
                'shares_short_prior_month': self._safe_get(info, 'sharesShortPriorMonth'),
                'shares_percent_shares_out': self._safe_get(info, 'sharesPercentSharesOut'),
                'held_percent_insiders': self._safe_get(info, 'heldPercentInsiders'),
                'held_percent_institutions': self._safe_get(info, 'heldPercentInstitutions'),
                'short_ratio': self._safe_get(info, 'shortRatio'),
                'short_percent_of_float': self._safe_get(info, 'shortPercentOfFloat'),
                
                # Аналитические оценки
                'target_high_price': self._safe_get(info, 'targetHighPrice'),
                'target_low_price': self._safe_get(info, 'targetLowPrice'),
                'target_mean_price': self._safe_get(info, 'targetMeanPrice'),
                'target_median_price': self._safe_get(info, 'targetMedianPrice'),
                'recommendation_mean': self._safe_get(info, 'recommendationMean'),
                'recommendation_key': self._safe_get(info, 'recommendationKey'),
                'number_of_analyst_opinions': self._safe_get(info, 'numberOfAnalystOpinions'),
                'average_analyst_rating': self._safe_get(info, 'averageAnalystRating'),
                
                # Риски ESG
                'audit_risk': self._safe_get(info, 'auditRisk'),
                'board_risk': self._safe_get(info, 'boardRisk'),
                'compensation_risk': self._safe_get(info, 'compensationRisk'),
                'share_holder_rights_risk': self._safe_get(info, 'shareHolderRightsRisk'),
                'overall_risk': self._safe_get(info, 'overallRisk'),
                
                # Временные метки
                'last_fiscal_year_end': self._safe_get(info, 'lastFiscalYearEnd'),
                'next_fiscal_year_end': self._safe_get(info, 'nextFiscalYearEnd'),
                'most_recent_quarter': self._safe_get(info, 'mostRecentQuarter'),
                'ex_dividend_date': self._safe_get(info, 'exDividendDate'),
                'dividend_date': self._safe_get(info, 'dividendDate'),
                'last_dividend_date': self._safe_get(info, 'lastDividendDate'),
                'earnings_timestamp': self._safe_get(info, 'earningsTimestamp'),
                'earnings_timestamp_start': self._safe_get(info, 'earningsTimestampStart'),
                'earnings_timestamp_end': self._safe_get(info, 'earningsTimestampEnd'),
                
                # Разделение акций
                'last_split_factor': self._safe_get(info, 'lastSplitFactor'),
                'last_split_date': self._safe_get(info, 'lastSplitDate'),
                
                # Метаданные
                'sector': self._safe_get(info, 'sector'),
                'industry': self._safe_get(info, 'industry'),
                'country': self._safe_get(info, 'country'),
                'currency': self._safe_get(info, 'currency'),
                'exchange': self._safe_get(info, 'exchange'),
                'quote_type': self._safe_get(info, 'quoteType'),
                'market_state': self._safe_get(info, 'marketState'),
                
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
            # Используем метод из connection.py для сохранения
            return self.db_connection.save_fundamentals(fundamentals)
                
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных для {fundamentals['symbol']}: {e}")
            return False
    
    
    def update_all_fundamentals(self, delay_seconds: float = 0.1, max_symbols: int = None, max_age_days: int = 90):
        """
        Обновить фундаментальные данные для символов, требующих обновления
        
        Args:
            delay_seconds: Задержка между запросами (для избежания блокировки)
            max_symbols: Максимальное количество символов для обработки
            max_age_days: Максимальный возраст данных в днях
        """
        try:
            # Убеждаемся что таблица fundamentals существует
            if not self.db_connection.ensure_fundamentals_table():
                logger.error("Не удалось создать таблицу fundamentals")
                return
            
            # Получаем символы, требующие обновления
            symbols_to_update = self.db_connection.get_fundamentals_symbols_needing_update(max_age_days)
            
            if not symbols_to_update:
                logger.info(f"Все символы имеют актуальные данные (не старше {max_age_days} дней)")
                return
            
            # Ограничиваем количество символов если указан лимит
            if max_symbols is not None:
                symbols_to_update = symbols_to_update[:max_symbols]
            
            logger.info(f"Найдено {len(symbols_to_update)} символов, требующих обновления (возраст > {max_age_days} дней)")
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
            logger.info(f"Обновлялись только символы старше {max_age_days} дней")
            logger.info(f"Следующий запуск обновит символы, которые станут старше {max_age_days} дней")
            
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
        
        # Принудительно обновляем структуру таблицы, добавляя новые поля если их нет
        print("Обновляем структуру таблицы fundamentals...")
        updater.db_connection.ensure_fundamentals_table()
        
        # Тестовый запрос для одного символа
        f = updater.get_fundamentals_for_symbol("AAPL")
        
        # Запускаем обновление (только символы старше 1 дня, без лимита)
        updater.update_all_fundamentals(delay_seconds=0, max_symbols=None, max_age_days=1)
        
    except KeyboardInterrupt:
        print("\nОбновление прервано пользователем")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        logger.error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    main()
