#!/usr/bin/env python3
"""
Stage A: Анализ новостей с помощью LLM
Извлекает структурированную информацию из новостей и сохраняет в БД
"""
import os
import json
import glob
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Any, List, Tuple
from libs.database.connection import DatabaseConnection
from libs.utils.logging_setup import get_logger
from apps.ai.pipelines.news_analyzer_2 import analyze_one
import time

# Константы для настройки работы скрипта
# Установите NEWS_ID для анализа одной конкретной новости, или None для обработки всех
NEWS_ID = None  # Пример: 10
# Ограничение на количество новостей для обработки (None = без ограничений)
LIMIT = None
# Сохранять ли результаты в базу данных
SAVE_TO_DB = True
# Путь к данным свечей
MARKET_DATA_PATH = "data/market_data/yahoo/1m"
# Фильтровать новости по наличию свечей
FILTER_BY_CANDLES = True
# Фильтровать старые записи по времени analyzed_at (CEST)
# Если включено, скрипт сначала обновит все записи в news_analysis_a,
# у которых analyzed_at раньше указанного времени
FILTER_OLD_ANALYSIS = False
FILTER_ANALYSIS_BEFORE = "2025-09-28 12:00:00"  # CEST время

logger = get_logger("news.ai.stage_a")

# Кеш для самой ранней даты свечей (чтобы не вычислять каждый раз)
_earliest_candle_date = None

def get_earliest_candle_date() -> Optional[date]:
    """
    Находит самую раннюю дату, для которой у нас есть данные свечей.
    Результат кешируется для повторного использования.
    
    Returns:
        date: Самая ранняя дата или None, если данные не найдены
    """
    global _earliest_candle_date
    
    # Используем кеш, если он уже заполнен
    if _earliest_candle_date is not None:
        return _earliest_candle_date
    
    try:
        # Ищем все файлы .parquet во всех поддиректориях
        parquet_files = []
        market_data_path = Path(MARKET_DATA_PATH)
        
        # Проверяем, что директория существует
        if not market_data_path.exists():
            logger.error(f"Директория с данными свечей не найдена: {MARKET_DATA_PATH}")
            return None
        
        # Ищем все поддиректории (символы)
        symbol_dirs = [d for d in market_data_path.iterdir() if d.is_dir()]
        
        if not symbol_dirs:
            logger.error(f"В директории {MARKET_DATA_PATH} не найдено поддиректорий с символами")
            return None
        
        # Ищем все файлы .parquet во всех поддиректориях символов
        for symbol_dir in symbol_dirs:
            parquet_files.extend(list(symbol_dir.glob("*.parquet")))
        
        if not parquet_files:
            logger.error(f"Не найдено файлов .parquet в директориях символов")
            return None
        
        # Извлекаем даты из имен файлов
        dates = []
        for file_path in parquet_files:
            try:
                file_date_str = file_path.stem  # Имя файла без расширения (YYYY-MM-DD)
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
                dates.append(file_date)
            except ValueError:
                # Пропускаем файлы с некорректным форматом имени
                continue
        
        if not dates:
            logger.error("Не удалось извлечь даты из имен файлов .parquet")
            return None
        
        # Находим самую раннюю дату
        earliest_date = min(dates)
        logger.info(f"Самая ранняя дата свечей: {earliest_date}")
        
        # Сохраняем в кеш
        _earliest_candle_date = earliest_date
        return earliest_date
        
    except Exception as e:
        logger.error(f"Ошибка при поиске самой ранней даты свечей: {str(e)}")
        return None

def has_candles_for_news(news_dict: dict) -> bool:
    """
    Проверяет, есть ли свечи хотя бы для одного символа из новости
    на дату публикации новости.
    
    Args:
        news_dict: Словарь с данными новости
        
    Returns:
        bool: True если есть свечи хотя бы для одного символа, False иначе
    """
    try:
        # Получаем дату публикации новости
        created_at = news_dict.get('created_at_utc')
        if not created_at:
            logger.warning(f"Новость {news_dict.get('news_id')} не имеет даты публикации")
            return False
        
        # Преобразуем в объект datetime и получаем дату в формате YYYY-MM-DD
        news_date = datetime.fromisoformat(created_at.replace('Z', '+00:00')).strftime('%Y-%m-%d')
        
        # Получаем символы из новости
        symbols_json = news_dict.get('symbols_json', '[]')
        symbols = json.loads(symbols_json)
        
        if not symbols:
            logger.warning(f"Новость {news_dict.get('news_id')} не имеет символов")
            return False
        
        # Проверяем наличие свечей для каждого символа
        for symbol in symbols:
            # Путь к файлу свечей
            candle_file = Path(MARKET_DATA_PATH) / symbol / f"{news_date}.parquet"
            
            if candle_file.exists():
                logger.info(f"Найдены свечи для символа {symbol} на дату {news_date}")
                return True
        
        logger.warning(f"Не найдены свечи ни для одного символа из новости {news_dict.get('news_id')} на дату {news_date}")
        return False
        
    except Exception as e:
        logger.error(f"Ошибка при проверке свечей для новости {news_dict.get('news_id')}: {str(e)}")
        return False

def update_old_analysis_records(db: DatabaseConnection, filter_time_cest: str) -> int:
    """
    Обновляет старые записи в таблице news_analysis_a, у которых analyzed_at 
    раньше указанного времени в CEST.
    
    Args:
        db: Подключение к базе данных
        filter_time_cest: Время в формате "YYYY-MM-DD HH:MM:SS" в CEST
        
    Returns:
        int: Количество обновленных записей
    """
    try:
        # Преобразуем CEST время в UTC для сравнения
        # CEST = UTC + 2 часа, поэтому вычитаем 2 часа
        filter_dt = datetime.strptime(filter_time_cest, "%Y-%m-%d %H:%M:%S")
        filter_dt_utc = filter_dt.replace(tzinfo=None) - timedelta(hours=2)
        filter_time_utc = filter_dt_utc.strftime("%Y-%m-%d %H:%M:%S")
        
        logger.info(f"Обновляю записи с analyzed_at до {filter_time_cest} CEST ({filter_time_utc} UTC)")
        
        with db.get_cursor() as cursor:
            # Сначала получаем количество записей для обновления
            cursor.execute("""
                SELECT COUNT(*) FROM news_analysis_a 
                WHERE analyzed_at < ?
            """, (filter_time_utc,))
            
            count = cursor.fetchone()[0]
            logger.info(f"Найдено {count} записей для обновления")
            
            if count == 0:
                return 0
            
            # Получаем все новости, которые нужно переанализировать
            cursor.execute("""
                SELECT n.* FROM news_raw n
                INNER JOIN news_analysis_a a ON n.news_id = a.news_id
                WHERE a.analyzed_at < ?
                ORDER BY n.created_at_utc
            """, (filter_time_utc,))
            
            news_items = cursor.fetchall()
            logger.info(f"Получено {len(news_items)} новостей для переанализа")
            
            updated_count = 0
            processing_times = []
            start_time = time.time()
            
            for i_item, item in enumerate(news_items):
                news_dict = dict(item)
                news_id = news_dict['news_id']
                
                logger.info(f"Переанализирую новость {news_id} ({i_item+1}/{len(news_items)}): {news_dict['headline'][:50]}...")
                
                # Анализируем новость заново
                tic = time.time()
                analysis_result = analyze_one(news_dict)
                toc = time.time()
                
                # Время обработки этой новости
                processing_time = toc - tic
                processing_times.append(processing_time)
                
                # Рассчитываем среднее время и оценку оставшегося времени
                avg_time = sum(processing_times) / len(processing_times)
                remaining_items = len(news_items) - (i_item + 1)
                estimated_remaining_time = avg_time * remaining_items
                
                # Форматируем оставшееся время
                remaining_hours = int(estimated_remaining_time // 3600)
                remaining_minutes = int((estimated_remaining_time % 3600) // 60)
                remaining_seconds = int(estimated_remaining_time % 60)
                
                # Выводим информацию
                logger.info(f"Переанализ новости {news_id} занял {processing_time:.2f} секунд")
                logger.info(f"Среднее время: {avg_time:.2f} сек/новость, осталось: {remaining_items} новостей " +
                           f"(~{remaining_hours}ч {remaining_minutes}м {remaining_seconds}с)")
                
                if analysis_result:
                    # Сохраняем обновленный результат
                    db.save_news_analysis_a(analysis_result)
                    updated_count += 1
                    logger.info(f"Новость {news_id} успешно переанализирована")
                else:
                    logger.warning(f"Не удалось переанализировать новость {news_id}")
            
            # Рассчитываем общее время выполнения
            total_time = time.time() - start_time
            hours = int(total_time // 3600)
            minutes = int((total_time % 3600) // 60)
            seconds = int(total_time % 60)
            
            # Выводим статистику выполнения
            if processing_times:
                avg_time = sum(processing_times) / len(processing_times)
                logger.info(f"Обновлено {updated_count} записей из {count}")
                logger.info(f"Общее время выполнения: {hours}ч {minutes}м {seconds}с")
                logger.info(f"Среднее время на новость: {avg_time:.2f} секунд")
            else:
                logger.info(f"Обновлено {updated_count} записей из {count}")
            
            return updated_count
            
    except Exception as e:
        logger.error(f"Ошибка при обновлении старых записей: {str(e)}")
        return 0

def process_all_news_stage_a(limit: int = None, save_to_db: bool = True) -> list:
    """
    Stage A: Обработать все новости из базы данных, которые еще не были проанализированы
    
    Args:
        limit: Максимальное количество новостей для обработки
        save_to_db: Сохранять ли результаты в базу данных
        
    Returns:
        list: Список результатов анализа
    """
    db = DatabaseConnection("data/db/news.db")
    
    # Создаем таблицу для результатов анализа если её нет
    if save_to_db:
        db.ensure_news_analysis_a_table()
    
    # Обновляем старые записи если включена соответствующая опция
    if FILTER_OLD_ANALYSIS and save_to_db:
        logger.info("Обновляю старые записи анализа...")
        updated_count = update_old_analysis_records(db, FILTER_ANALYSIS_BEFORE)
        logger.info(f"Обновлено {updated_count} старых записей")
    
    # Получаем все новости, которые еще не были проанализированы
    with db.get_cursor() as cursor:
        cursor.execute("""
            SELECT n.* FROM news_raw n
            LEFT JOIN news_analysis_a a ON n.news_id = a.news_id
            WHERE a.news_id IS NULL
            ORDER BY n.created_at_utc
        """)
        
        if limit:
            news_items = cursor.fetchmany(limit)
        else:
            news_items = cursor.fetchall()
    
    logger.info(f"Stage A: Найдено {len(news_items)} неанализированных новостей")
    
    # Фильтруем новости по наличию свечей
    print(f"FILTER_BY_CANDLES: {FILTER_BY_CANDLES} ...")
    if FILTER_BY_CANDLES:
        # Получаем самую раннюю дату свечей для предварительной фильтрации
        earliest_candle_date = get_earliest_candle_date()
        
        if earliest_candle_date:
            logger.info(f"Stage A: Предварительная фильтрация новостей по дате (не ранее {earliest_candle_date})")
            
            # Предварительная фильтрация по дате
            pre_filtered_items = []
            skipped_by_date = 0
            
            for item in news_items:
                news_dict = dict(item)
                created_at = news_dict.get('created_at_utc')
                
                if not created_at:
                    continue
                
                try:
                    # Преобразуем в объект datetime и получаем дату
                    news_date = datetime.fromisoformat(created_at.replace('Z', '+00:00')).date()
                    
                    # Если новость раньше самой ранней даты свечей - пропускаем
                    if news_date < earliest_candle_date:
                        skipped_by_date += 1
                        continue
                    
                    pre_filtered_items.append(item)
                except ValueError:
                    # Пропускаем новости с некорректной датой
                    continue
            
            logger.info(f"Stage A: Пропущено {skipped_by_date} новостей по дате (ранее {earliest_candle_date})")
            logger.info(f"Stage A: После предварительной фильтрации осталось {len(pre_filtered_items)} новостей из {len(news_items)}")
            
            # Основная фильтрация по наличию свечей
            filtered_news_items = []
            for item in pre_filtered_items:
                news_dict = dict(item)
                if has_candles_for_news(news_dict):
                    filtered_news_items.append(item)
            
            logger.info(f"Stage A: После финальной фильтрации осталось {len(filtered_news_items)} новостей из {len(pre_filtered_items)}")
            news_items = filtered_news_items
        else:
            # Если не удалось определить самую раннюю дату, используем обычную фильтрацию
            filtered_news_items = []
            for item in news_items:
                news_dict = dict(item)
                if has_candles_for_news(news_dict):
                    filtered_news_items.append(item)
            
            logger.info(f"Stage A: После фильтрации осталось {len(filtered_news_items)} новостей из {len(news_items)}")
            news_items = filtered_news_items
    
    logger.info(f"Stage A: Найдено {len(news_items)} новостей для анализа")
    
    results = []
    processing_times = []
    start_time = time.time()
    
    for i_item, item in enumerate(news_items):
        news_dict = dict(item)
        logger.info(f"Stage A: Анализирую новость {news_dict['news_id']} ({i_item+1}/{len(news_items)}): {news_dict['headline'][:50]}...")
        
        # Анализируем новость
        tic = time.time()
        analysis_result = analyze_one(news_dict)
        toc = time.time()
        
        # Время обработки этой новости
        processing_time = toc - tic
        processing_times.append(processing_time)
        
        # Рассчитываем среднее время и оценку оставшегося времени
        avg_time = sum(processing_times) / len(processing_times)
        remaining_items = len(news_items) - (i_item + 1)
        estimated_remaining_time = avg_time * remaining_items
        
        # Форматируем оставшееся время
        remaining_hours = int(estimated_remaining_time // 3600)
        remaining_minutes = int((estimated_remaining_time % 3600) // 60)
        remaining_seconds = int(estimated_remaining_time % 60)
        
        # Выводим информацию
        logger.info(f"Stage A: Анализ новости {news_dict['news_id']} занял {processing_time:.2f} секунд")
        logger.info(f"Stage A: Среднее время: {avg_time:.2f} сек/новость, осталось: {remaining_items} новостей " +
                   f"(~{remaining_hours}ч {remaining_minutes}м {remaining_seconds}с)")
        
        if analysis_result:
            results.append(analysis_result)
            
            # Сохраняем результат в базу данных
            if save_to_db:
                db.save_news_analysis_a(analysis_result)
                logger.info(f"Stage A: Результат анализа для новости {news_dict['news_id']} сохранен в БД")
        else:
            logger.warning(f"Stage A: Не удалось проанализировать новость {news_dict['news_id']}")
    
    # Рассчитываем общее время выполнения
    total_time = time.time() - start_time
    hours = int(total_time // 3600)
    minutes = int((total_time % 3600) // 60)
    seconds = int(total_time % 60)
    
    # Выводим статистику выполнения
    if processing_times:
        avg_time = sum(processing_times) / len(processing_times)
        logger.info(f"Stage A: Обработано {len(results)} новостей из {len(news_items)}")
        logger.info(f"Stage A: Общее время выполнения: {hours}ч {minutes}м {seconds}с")
        logger.info(f"Stage A: Среднее время на новость: {avg_time:.2f} секунд")
        
        # Показываем прогноз времени для оставшихся новостей (если есть)
        remaining_items = len(news_items) - len(results)
        if remaining_items > 0:
            estimated_remaining_time = avg_time * remaining_items
            remaining_hours = int(estimated_remaining_time // 3600)
            remaining_minutes = int((estimated_remaining_time % 3600) // 60)
            remaining_seconds = int(estimated_remaining_time % 60)
            logger.info(f"Stage A: Осталось обработать {remaining_items} новостей " +
                       f"(примерно {remaining_hours}ч {remaining_minutes}м {remaining_seconds}с)")
    else:
        logger.info(f"Stage A: Новости не были обработаны")
    
    return results

def process_one_news_stage_a(news_id: int, save_to_db: bool = True) -> Optional[dict[str, Any]]:
    """
    Stage A: Обработать одну новость по её ID
    
    Args:
        news_id: ID новости
        save_to_db: Сохранять ли результат в базу данных
        
    Returns:
        dict: Результат анализа или None при ошибке
    """
    db = DatabaseConnection("data/db/news.db")
    
    # Создаем таблицу для результатов анализа если её нет
    if save_to_db:
        db.ensure_news_analysis_a_table()
    
    # Получаем новость
    news_dict = dict(db.get_news_by_id(news_id))
    if not news_dict:
        logger.error(f"Stage A: Новость с ID {news_id} не найдена")
        return None
    
    # Проверяем наличие свечей для новости
    if FILTER_BY_CANDLES:
        # Предварительная проверка по дате
        created_at = news_dict.get('created_at_utc')
        if created_at:
            try:
                # Получаем самую раннюю дату свечей
                earliest_candle_date = get_earliest_candle_date()
                if earliest_candle_date:
                    # Преобразуем в объект datetime и получаем дату
                    news_date = datetime.fromisoformat(created_at.replace('Z', '+00:00')).date()
                    
                    # Если новость раньше самой ранней даты свечей - пропускаем
                    if news_date < earliest_candle_date:
                        logger.warning(f"Stage A: Пропускаю новость {news_id} - дата ({news_date}) раньше самой ранней даты свечей ({earliest_candle_date})")
                        return None
            except ValueError:
                # Пропускаем новость с некорректной датой
                logger.warning(f"Stage A: Пропускаю новость {news_id} - некорректный формат даты")
                return None
        
        # Основная проверка наличия свечей
        if not has_candles_for_news(news_dict):
            logger.warning(f"Stage A: Пропускаю новость {news_id} - нет свечей")
            return None
    
    logger.info(f"Stage A: Анализирую новость {news_id}: {news_dict['headline'][:50]}...")
    
    # Анализируем новость
    analysis_result = analyze_one(news_dict)
    
    if analysis_result and save_to_db:
        db.save_news_analysis_a(analysis_result)
        logger.info(f"Stage A: Результат анализа для новости {news_id} сохранен в БД")
    
    return analysis_result

def main():
    """
    Основная функция для запуска анализа новостей.
    Используйте константы в начале файла для настройки:
    - NEWS_ID: ID конкретной новости или None для обработки всех
    - LIMIT: ограничение количества новостей или None без ограничений
    - SAVE_TO_DB: True для сохранения в БД, False только для тестирования
    """
    if NEWS_ID:
        # Обрабатываем одну конкретную новость
        logger.info(f"Stage A: Анализирую новость с ID {NEWS_ID}")
        result = process_one_news_stage_a(NEWS_ID, save_to_db=SAVE_TO_DB)
        if result:
            logger.info(f"Stage A: Анализ новости {NEWS_ID} завершен успешно")
        else:
            logger.error(f"Stage A: Не удалось проанализировать новость {NEWS_ID}")
    else:
        # Обрабатываем все неанализированные новости
        logger.info(f"Stage A: Запускаю анализ всех неанализированных новостей (лимит: {LIMIT or 'не установлен'})")
        results = process_all_news_stage_a(limit=LIMIT, save_to_db=SAVE_TO_DB)
        logger.info(f"Stage A: Анализ завершен. Обработано {len(results)} новостей")

if __name__ == "__main__":
    main()
