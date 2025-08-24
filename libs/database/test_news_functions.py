#!/usr/bin/env python3
"""
Тесты для функций работы с новостями в DatabaseConnection
"""

import unittest
import tempfile
import os
from connection import DatabaseConnection
import json

class TestNewsFunctions(unittest.TestCase):
    
    def setUp(self):
        """Настройка перед каждым тестом"""
        # Создаем временную БД для тестов
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db = DatabaseConnection(self.temp_db.name)
        self.db.create_database()
    
    def tearDown(self):
        """Очистка после каждого теста"""
        self.db.close()
        os.unlink(self.temp_db.name)
    
    def test_add_raw_news_success(self):
        """Тест успешного добавления новости"""
        news_data = {
            "id": 12345,  # ID от провайдера
            "source": "test_source",
            "created_at": "2025-08-15T19:59:29Z",
            "headline": "Test Headline",
            "summary": "Test summary",
            "symbols": ["AAPL", "MSFT"],
            "url": "https://example.com"
        }
        
        news_id = self.db.add_raw_news(news_data)
        
        self.assertIsNotNone(news_id)
        self.assertIsInstance(news_id, int)
    
    def test_add_raw_news_missing_required_fields(self):
        """Тест добавления новости с отсутствующими обязательными полями"""
        # Без headline
        news_data = {
            "source": "test_source",
            "created_at": "2025-08-15T19:59:29Z",
            "summary": "Test summary",
            "symbols": ["AAPL"]
        }
        
        news_id = self.db.add_raw_news(news_data)
        self.assertIsNone(news_id)
        
        # Без created_at
        news_data = {
            "source": "test_source",
            "headline": "Test Headline",
            "summary": "Test summary",
            "symbols": ["AAPL"]
        }
        
        news_id = self.db.add_raw_news(news_data)
        self.assertIsNone(news_id)
    
    def test_add_raw_news_duplicate_prevention(self):
        """Тест предотвращения дублирования новостей"""
        news_data = {
            "id": 12345,  # Добавляем ID от провайдера
            "source": "test_source",
            "created_at": "2025-08-15T19:59:29Z",
            "headline": "Test Headline",
            "summary": "Test summary",
            "symbols": ["AAPL"]
        }
        
        # Добавляем первую новость
        news_id1 = self.db.add_raw_news(news_data)
        self.assertIsNotNone(news_id1)
        
        # Пытаемся добавить ту же новость снова
        news_id2 = self.db.add_raw_news(news_data)
        self.assertIsNone(news_id2)  # Должна быть отклонена
        
        # Пытаемся добавить новость с тем же ID от того же источника, но другим заголовком
        news_data_different = news_data.copy()
        news_data_different["headline"] = "Different Headline"
        news_id3 = self.db.add_raw_news(news_data_different)
        self.assertIsNone(news_id3)  # Должна быть отклонена по source+news_id
        
        # Но можно добавить новость с тем же ID от другого источника
        news_data_different_source = news_data.copy()
        news_data_different_source["source"] = "different_source"
        news_id4 = self.db.add_raw_news(news_data_different_source)
        self.assertIsNotNone(news_id4)  # Должна быть добавлена
    
    def test_add_raw_news_batch(self):
        """Тест пакетного добавления новостей"""
        news_list = [
            {
                "source": "source1",
                "created_at": "2025-08-15T20:00:00Z",
                "headline": "News 1",
                "symbols": ["AAPL"]
            },
            {
                "source": "source2",
                "created_at": "2025-08-15T20:01:00Z",
                "headline": "News 2",
                "symbols": ["MSFT"]
            }
        ]
        
        added_ids = self.db.add_raw_news_batch(news_list)
        
        self.assertEqual(len(added_ids), 2)
        self.assertIsInstance(added_ids[0], int)
        self.assertIsInstance(added_ids[1], int)
    
    def test_get_news_by_symbol(self):
        """Тест поиска новостей по символу"""
        # Добавляем тестовые новости
        news_data1 = {
            "source": "test_source",
            "created_at": "2025-08-15T20:00:00Z",
            "headline": "Apple News",
            "symbols": ["AAPL", "MSFT"]
        }
        news_data2 = {
            "source": "test_source",
            "created_at": "2025-08-15T20:01:00Z",
            "headline": "Microsoft News",
            "symbols": ["MSFT", "GOOGL"]
        }
        
        self.db.add_raw_news(news_data1)
        self.db.add_raw_news(news_data2)
        
        # Ищем по AAPL
        aapl_news = self.db.get_news_by_symbol("AAPL")
        self.assertEqual(len(aapl_news), 1)
        self.assertIn("Apple News", aapl_news[0]['headline'])
        
        # Ищем по MSFT
        msft_news = self.db.get_news_by_symbol("MSFT")
        self.assertEqual(len(msft_news), 2)
    
    def test_get_news_by_date_range(self):
        """Тест поиска новостей по диапазону дат"""
        # Добавляем тестовые новости с разными датами
        news_data1 = {
            "source": "test_source",
            "created_at": "2025-08-15T20:00:00Z",
            "headline": "News 1",
            "symbols": ["AAPL"]
        }
        news_data2 = {
            "source": "test_source",
            "created_at": "2025-08-16T20:00:00Z",
            "headline": "News 2",
            "symbols": ["MSFT"]
        }
        
        self.db.add_raw_news(news_data1)
        self.db.add_raw_news(news_data2)
        
        # Ищем за 15 августа
        news_15 = self.db.get_news_by_date_range(
            "2025-08-15T00:00:00Z",
            "2025-08-15T23:59:59Z"
        )
        self.assertEqual(len(news_15), 1)
        
        # Ищем за 16 августа
        news_16 = self.db.get_news_by_date_range(
            "2025-08-16T00:00:00Z",
            "2025-08-16T23:59:59Z"
        )
        self.assertEqual(len(news_16), 1)
    
    def test_news_data_integrity(self):
        """Тест целостности данных новости"""
        news_data = {
            "source": "test_source",
            "created_at": "2025-08-15T19:59:29Z",
            "headline": "Test Headline with Special Chars: éñç",
            "summary": "Test summary with quotes: 'single' and \"double\"",
            "symbols": ["AAPL", "MSFT", "GOOGL"],
            "url": "https://example.com/path?param=value"
        }
        
        news_id = self.db.add_raw_news(news_data)
        self.assertIsNotNone(news_id)
        
        # Проверяем, что данные корректно сохранились
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT * FROM news_raw WHERE news_id = ?", (news_id,))
            saved_news = cursor.fetchone()
        
        self.assertIsNotNone(saved_news)
        self.assertEqual(saved_news['headline'], news_data['headline'])
        self.assertEqual(saved_news['summary'], news_data['summary'])
        self.assertEqual(saved_news['source'], news_data['source'])
        
        # Проверяем JSON символы
        saved_symbols = json.loads(saved_news['symbols_json'])
        self.assertEqual(saved_symbols, news_data['symbols'])
    
    def test_same_id_different_sources(self):
        """Тест: одинаковые ID от разных источников должны быть разрешены"""
        news_data1 = {
            "id": 99999,
            "source": "source1",
            "created_at": "2025-08-15T19:59:29Z",
            "headline": "News from source 1",
            "summary": "Summary 1",
            "symbols": ["AAPL"]
        }
        
        news_data2 = {
            "id": 99999,  # Тот же ID
            "source": "source2",  # Но другой источник
            "created_at": "2025-08-15T19:59:29Z",
            "headline": "News from source 2",
            "summary": "Summary 2",
            "symbols": ["MSFT"]
        }
        
        # Обе новости должны быть добавлены
        news_id1 = self.db.add_raw_news(news_data1)
        news_id2 = self.db.add_raw_news(news_data2)
        
        self.assertIsNotNone(news_id1)
        self.assertIsNotNone(news_id2)
        self.assertNotEqual(news_id1, news_id2)  # Разные внутренние ID

if __name__ == '__main__':
    unittest.main()
