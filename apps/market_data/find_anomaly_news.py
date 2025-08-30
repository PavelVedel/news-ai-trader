"""
Скрипт для поиска аномальных новостей на основе изменения цены акций
"""

import json
import sqlite3
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, time
from typing import List, Dict, Tuple, Optional
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AnomalyNewsFinder:
    def __init__(self, db_path: str = "data/db/news.db", market_data_path: str = "data/market_data/yahoo/1m"):
        self.db_path = db_path
        self.market_data_path = Path(market_data_path)
        
        # Часовые пояса для определения торговых сессий
        self.market_open = time(9, 30)  # 9:30 AM ET
        self.market_close = time(16, 0)  # 4:00 PM ET
        
    def get_all_news_with_symbols(self, limit: int = None) -> List[Dict]:
        """Получить все новости с их символами из базы данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            
            cursor = conn.cursor()
            if limit:
                cursor.execute("""
                    SELECT news_id, created_at_utc, symbols_json, headline, source
                    FROM news_raw
                    ORDER BY created_at_utc DESC
                    LIMIT ?
                """, (limit,))
            else:
                cursor.execute("""
                    SELECT news_id, created_at_utc, symbols_json, headline, source
                    FROM news_raw
                    ORDER BY created_at_utc
                """)
            
            news_list = []
            for row in cursor.fetchall():
                try:
                    symbols = json.loads(row['symbols_json'])
                    if isinstance(symbols, list) and symbols:
                        news_list.append({
                            'news_id': row['news_id'],
                            'created_at_utc': row['created_at_utc'],
                            'symbols': symbols,
                            'headline': row['headline'],
                            'source': row['source']
                        })
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Ошибка парсинга JSON для новости {row['news_id']}")
                    continue
            
            conn.close()
            logger.info(f"Найдено {len(news_list)} новостей с символами")
            return news_list
            
        except Exception as e:
            logger.error(f"Ошибка при получении новостей: {e}")
            return []
    
    def is_market_open(self, utc_time: str) -> bool:
        """Определить, был ли открыт рынок в момент выхода новости"""
        try:
            # Парсим UTC время
            dt = datetime.fromisoformat(utc_time.replace('Z', '+00:00'))
            
            # Конвертируем в ET (Eastern Time)
            # Для простоты используем UTC-5 (EST) или UTC-4 (EDT)
            # В реальном проекте лучше использовать pytz или zoneinfo
            et_time = dt - timedelta(hours=5)  # UTC-5
            
            # Проверяем, что это рабочий день (понедельник-пятница)
            if et_time.weekday() >= 5:  # 5=суббота, 6=воскресенье
                return False
            
            # Проверяем время торговой сессии
            current_time = et_time.time()
            
            # Расширяем торговые часы для учета пре-маркета и после-маркета
            # Pre-market: 4:00 AM - 9:30 AM ET
            # Regular hours: 9:30 AM - 4:00 PM ET  
            # After-hours: 4:00 PM - 8:00 PM ET
            pre_market_start = time(4, 0)
            after_market_end = time(20, 0)
            
            return (pre_market_start <= current_time <= after_market_end)
            
        except Exception as e:
            logger.error(f"Ошибка при определении торговой сессии: {e}")
            return False
    
    def get_market_session_info(self, utc_time: str) -> Tuple[bool, str]:
        """Определить статус рынка и тип торговой сессии"""
        try:
            # Парсим UTC время
            dt = datetime.fromisoformat(utc_time.replace('Z', '+00:00'))
            
            # Конвертируем в ET (Eastern Time)
            et_time = dt - timedelta(hours=5)  # UTC-5
            
            # Проверяем, что это рабочий день (понедельник-пятница)
            if et_time.weekday() >= 5:  # 5=суббота, 6=воскресенье
                return False, "weekend"
            
            current_time = et_time.time()
            
            # Определяем тип торговой сессии
            if time(4, 0) <= current_time < time(9, 30):
                return True, "pre_market"
            elif time(9, 30) <= current_time <= time(16, 0):
                return True, "regular_hours"
            elif time(16, 0) < current_time <= time(20, 0):
                return True, "after_hours"
            else:
                return False, "closed"
                
        except Exception as e:
            logger.error(f"Ошибка при определении торговой сессии: {e}")
            return False, "unknown"
    
    def get_candles_for_symbol_date(self, symbol: str, date_str: str) -> Optional[pd.DataFrame]:
        """Получить свечи для символа на конкретную дату"""
        try:
            # Формируем путь к файлу
            symbol_path = self.market_data_path / symbol
            if not symbol_path.exists():
                return None
            
            file_path = symbol_path / f"{date_str}.parquet"
            if not file_path.exists():
                return None
            
            # Читаем parquet файл
            df = pd.read_parquet(file_path)
            
            # Убеждаемся, что есть колонка времени
            if 'timestamp' not in df.columns and df.index.name == 'timestamp':
                df = df.reset_index()
            
            # Конвертируем время в datetime если нужно
            if 'timestamp' in df.columns:
                if df['timestamp'].dtype == 'object':
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.set_index('timestamp')
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка при чтении свечей для {symbol} на {date_str}: {e}")
            return None

    @staticmethod
    def _typical_price(df: pd.DataFrame) -> pd.Series:
        """Типичная цена свечи: (H+L+C)/3. Требуются колонки high, low, close."""
        if not {'high', 'low', 'close'}.issubset(df.columns):
            raise ValueError("Ожидаются колонки: high, low, close")
        tp = (df['high'] + df['low'] + df['close']) / 3.0
        tp.name = 'typical_price'
        return tp

    @staticmethod
    def _price_at_or_before(series: pd.Series, t: datetime) -> Optional[float]:
        """Цена на последней свече не позже t (<= t)."""
        s = series.loc[series.index <= pd.Timestamp(t)]
        return None if s.empty else float(s.iloc[-1])

    @staticmethod
    def _price_at_or_after(series: pd.Series, t: datetime) -> Optional[float]:
        """Цена на первой свече не раньше t (>= t)."""
        s = series.loc[series.index >= pd.Timestamp(t)]
        return None if s.empty else float(s.iloc[0])

    @staticmethod
    def _log_returns(series: pd.Series) -> pd.Series:
        """Лог-доходности из последовательности цен."""
        return np.log(series).diff()

    @staticmethod
    def _cum_change_pct(p_start: Optional[float], p_end: Optional[float]) -> Optional[float]:
        """Кумулятивное изменение в процентах (из лог-формы) между двумя ценами."""
        if p_start is None or p_end is None or p_start <= 0 or p_end <= 0:
            return None
        return float((np.exp(np.log(p_end) - np.log(p_start)) - 1.0) * 100.0)

    @staticmethod
    def _realized_vol_pct(log_ret: pd.Series) -> Optional[float]:
        """Реализованная волатильность на окне: sqrt(sum r_t^2), в процентах."""
        if log_ret.empty:
            return None
        return float(np.sqrt((log_ret ** 2).sum()) * 100.0)

    def find_price_changes(self, symbol: str, news_time: str, candles_df: pd.DataFrame) -> Optional[Dict]:
        """Найти движения цены вокруг новости + добавить pre/post CAR и RV без бенчмарков."""
        try:
            news_dt = datetime.fromisoformat(news_time.replace('Z', '+00:00')).replace(tzinfo=None)

            # Диапазон данных
            data_start = candles_df.index.min().replace(tzinfo=None)
            data_end = candles_df.index.max().replace(tzinfo=None)

            # Типичная цена на всех свечах дня
            tp = self._typical_price(candles_df)
            log_r = self._log_returns(tp)

            # --- расчет базового 3-часового окна после новости (как у вас было) ---
            if news_dt > data_end:
                start_time = data_end - timedelta(hours=3)
                end_time = data_end
                logger.info("Новость после закрытия рынка, анализирую последние 3 часа данных")
            else:
                start_time = news_dt
                end_time = news_dt + timedelta(hours=3)

            start_time_pd = pd.Timestamp(start_time)
            end_time_pd = pd.Timestamp(end_time)

            mask_3h = (candles_df.index >= start_time_pd) & (candles_df.index <= end_time_pd)
            relevant_candles = candles_df[mask_3h]

            if relevant_candles.empty:
                return None

            # Цена в момент новости (берем первую свечу >= news_dt, типичная цена)
            price_at_news = self._price_at_or_after(tp, news_dt)
            if price_at_news is None:
                # fallback: если нет свечей после, возьмем последнюю перед событием
                price_at_news = self._price_at_or_before(tp, news_dt)
            if price_at_news is None:
                return None

            # --- Старые метрики (сохраняем совместимость) ---
            max_high = relevant_candles['high'].max()
            min_low = relevant_candles['low'].min()
            max_up_pct = ((max_high - price_at_news) / price_at_news) * 100.0
            max_down_pct = ((min_low - price_at_news) / price_at_news) * 100.0
            max_movement_pct = max(abs(max_up_pct), abs(max_down_pct))
            is_anomaly = max_movement_pct >= 0.5
            if abs(max_up_pct) > abs(max_down_pct):
                movement_direction = "up"
                movement_pct = max_up_pct
            else:
                movement_direction = "down"
                movement_pct = max_down_pct

            # --- NEW: окна для pre/post CAR (в процентах) ---
            # Определения окон:
            pre_start = news_dt - timedelta(minutes=60)
            pre_end   = news_dt - timedelta(minutes=5)
            post_15   = news_dt + timedelta(minutes=15)
            post_60   = news_dt + timedelta(minutes=60)
            post_180  = news_dt + timedelta(minutes=180)

            # Точки цен для окон (концы берем "не позже", начало pre тоже "не позже")
            p_pre_start = self._price_at_or_before(tp, pre_start)
            p_pre_end   = self._price_at_or_before(tp, pre_end)

            p0_post = self._price_at_or_after(tp, news_dt)  # price_at_news уже подобран аналогично
            p_15 = self._price_at_or_before(tp, post_15)
            p_60 = self._price_at_or_before(tp, post_60)
            p_180 = self._price_at_or_before(tp, post_180)

            pre_car_60m_pct = self._cum_change_pct(p_pre_start, p_pre_end)
            post_car_15m_pct = self._cum_change_pct(p0_post, p_15)
            post_car_60m_pct = self._cum_change_pct(p0_post, p_60)
            post_car_180m_pct = self._cum_change_pct(p0_post, p_180)

            # --- NEW: реализованная волатильность на окнах (опционально) ---
            # Предокно: (pre_start, pre_end]
            pre_mask = (log_r.index > pd.Timestamp(pre_start)) & (log_r.index <= pd.Timestamp(pre_end))
            pre_rv_60m = self._realized_vol_pct(log_r.loc[pre_mask])

            # Постокно: (news_dt, news_dt+60m]
            post_mask_60 = (log_r.index > pd.Timestamp(news_dt)) & (log_r.index <= pd.Timestamp(post_60))
            post_rv_60m = self._realized_vol_pct(log_r.loc[post_mask_60])

            return {
                'symbol': symbol,
                'price_at_news': float(price_at_news),
                'max_high': float(max_high),
                'min_low': float(min_low),
                'max_up_pct': float(max_up_pct),
                'max_down_pct': float(max_down_pct),
                'max_movement_pct': float(max_movement_pct),
                'movement_direction': movement_direction,
                'movement_pct': float(movement_pct),
                'is_anomaly': is_anomaly,
                'candles_count': int(len(relevant_candles)),

                # NEW: «чистые» pre/post CAR без бенчмарков (в процентах)
                'pre_car_60m_pct': pre_car_60m_pct,
                'post_car_15m_pct': post_car_15m_pct,
                'post_car_60m_pct': post_car_60m_pct,
                'post_car_180m_pct': post_car_180m_pct,

                # NEW (опционально полезно для фильтрации шума)
                'pre_rv_60m': pre_rv_60m,
                'post_rv_60m': post_rv_60m,
            }

        except Exception as e:
            logger.error(f"Ошибка при анализе изменений цены для {symbol}: {e}")
            return None

    
    def analyze_news_impact(self, limit: int = None) -> List[Dict]:
        """Основной метод анализа влияния новостей на цены"""
        logger.info("Начинаю анализ влияния новостей на цены...")
        
        # Получаем новости (с лимитом если указан)
        news_list = self.get_all_news_with_symbols(limit)
        if not news_list:
            logger.warning("Новости не найдены")
            return []
        
        results = []
        
        for news in news_list:
            logger.info(f"Анализирую новость {news['news_id']}: {news['headline'][:50]}...")
            
            # Определяем, был ли открыт рынок и тип торговой сессии
            market_open, session_type = self.get_market_session_info(news['created_at_utc'])
            
            # Получаем дату для поиска свечей
            news_date = datetime.fromisoformat(news['created_at_utc'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
            
            news_result = {
                'news_id': news['news_id'],
                'headline': news['headline'],
                'source': news['source'],
                'created_at_utc': news['created_at_utc'],
                'market_open': market_open,
                'session_type': session_type,
                'symbols_analysis': []
            }
            
            for symbol in news['symbols']:
                logger.info(f"  Анализирую символ: {symbol}")
                
                # Получаем свечи для символа
                candles_df = self.get_candles_for_symbol_date(symbol, news_date)
                
                if candles_df is None or candles_df.empty:
                    logger.info(f"    Свечи для {symbol} не найдены, пропускаю")
                    continue
                
                # Анализируем изменения цены
                price_analysis = self.find_price_changes(symbol, news['created_at_utc'], candles_df)
                
                if price_analysis:
                    symbol_result = {
                        'symbol': symbol,
                        'has_candles': True,
                        'price_analysis': price_analysis
                    }
                else:
                    symbol_result = {
                        'symbol': symbol,
                        'has_candles': True,
                        'price_analysis': None
                    }
                
                news_result['symbols_analysis'].append(symbol_result)
            
            results.append(news_result)
        
        return results
    
    def find_anomalies(self, results: List[Dict]) -> List[Dict]:
        """Найти аномальные новости (с изменением цены >= 1%)"""
        anomalies = []
        
        for news_result in results:
            for symbol_analysis in news_result['symbols_analysis']:
                if (symbol_analysis['has_candles'] and 
                    symbol_analysis['price_analysis'] and 
                    symbol_analysis['price_analysis']['is_anomaly']):
                    
                    anomaly = {
                        'news_id': news_result['news_id'],
                        'headline': news_result['headline'],
                        'symbol': symbol_analysis['symbol'],
                        'created_at_utc': news_result['created_at_utc'],
                        'market_open': news_result['market_open'],
                        'session_type': news_result.get('session_type', 'unknown'),
                        'max_movement_pct': symbol_analysis['price_analysis']['max_movement_pct'],
                        'movement_direction': symbol_analysis['price_analysis']['movement_direction'],
                        'movement_pct': symbol_analysis['price_analysis']['movement_pct'],
                        'price_at_news': symbol_analysis['price_analysis']['price_at_news'],
                        'max_high': symbol_analysis['price_analysis']['max_high'],
                        'min_low': symbol_analysis['price_analysis']['min_low']
                    }
                    anomalies.append(anomaly)
        
        return anomalies
    
    def save_results(self, results: List[Dict], output_file: str = "anomaly_analysis_results.json"):
        """Сохранить результаты анализа в JSON файл"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"Результаты сохранены в {output_file}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении результатов: {e}")
    
    def print_summary(self, results: List[Dict], anomalies: List[Dict]):
        """Вывести сводку по анализу"""
        print("\n" + "="*80)
        print("СВОДКА АНАЛИЗА АНОМАЛЬНЫХ НОВОСТЕЙ")
        print("="*80)
        
        total_news = len(results)
        total_symbols = sum(len(news['symbols_analysis']) for news in results)
        symbols_with_candles = sum(
            sum(1 for sa in news['symbols_analysis'] if sa['has_candles'])
            for news in results
        )
        market_open_news = sum(1 for news in results if news['market_open'])
        total_anomalies = len(anomalies)
        
        # Статистика по торговым сессиям
        session_stats = {}
        for news in results:
            session = news.get('session_type', 'unknown')
            session_stats[session] = session_stats.get(session, 0) + 1
        
        print(f"Всего новостей: {total_news}")
        print(f"Всего символов: {total_symbols}")
        print(f"Символов с доступными свечами: {symbols_with_candles}")
        print(f"Новостей во время торговой сессии: {market_open_news}")
        print(f"Найдено аномалий (≥0.5%): {total_anomalies}")
        
        print(f"\nРАСПРЕДЕЛЕНИЕ ПО ТОРГОВЫМ СЕССИЯМ:")
        print("-" * 40)
        for session, count in sorted(session_stats.items()):
            session_name = {
                'pre_market': 'Пре-маркет (4:00-9:30 AM ET)',
                'regular_hours': 'Регулярные часы (9:30 AM-4:00 PM ET)',
                'after_hours': 'После-маркет (4:00-8:00 PM ET)',
                'closed': 'Рынок закрыт',
                'weekend': 'Выходные',
                'unknown': 'Неизвестно'
            }.get(session, session)
            print(f"{session_name}: {count}")
        
        if anomalies:
            print(f"\nТОП-10 АНОМАЛЬНЫХ ДВИЖЕНИЙ:")
            print("-" * 80)
            
            # Сортируем по максимальному движению цены
            sorted_anomalies = sorted(anomalies, key=lambda x: x['max_movement_pct'], reverse=True)
             
            for i, anomaly in enumerate(sorted_anomalies[:10], 1):
                 direction = "↗️" if anomaly['movement_direction'] == "up" else "↘️"
                 market_status = "🟢 РЫНОК ОТКРЫТ" if anomaly['market_open'] else "🔴 РЫНОК ЗАКРЫТ"
                 session_info = anomaly.get('session_type', 'unknown')
                 
                 # Форматируем время новости для удобства
                 news_time = datetime.fromisoformat(anomaly['created_at_utc'].replace('Z', '+00:00'))
                 formatted_time = news_time.strftime('%Y-%m-%d %H:%M:%S UTC')
                 
                 print(f"{i:2d}. {anomaly['symbol']:6s} {direction} {anomaly['max_movement_pct']:6.2f}% "
                       f"| {anomaly['headline'][:50]:50s} | {formatted_time} | {market_status} | {session_info}")
        
        print("="*80)


def main():
    """Основная функция"""
    try:
        # Создаем анализатор
        finder = AnomalyNewsFinder()
        
        # Проверяем существование базы данных
        if not Path(finder.db_path).exists():
            logger.error(f"База данных не найдена: {finder.db_path}")
            return
        
        # Проверяем существование папки с данными рынка
        if not finder.market_data_path.exists():
            logger.error(f"Папка с данными рынка не найдена: {finder.market_data_path}")
            return
        
        # Выполняем анализ (начинаем с небольшого количества для тестирования)
        logger.info("Запускаю анализ аномальных новостей...")
        results = finder.analyze_news_impact(limit=1000)  # Анализируем только последние 100 новостей
        
        if not results:
            logger.warning("Результаты анализа пусты")
            return
        
        # Находим аномалии
        anomalies = finder.find_anomalies(results)
        
        # Выводим сводку
        finder.print_summary(results, anomalies)
        
        # Сохраняем результаты
        finder.save_results(results)
        
        logger.info("Анализ завершен успешно!")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
