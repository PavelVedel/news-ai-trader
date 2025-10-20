"""
Простой playground для экспериментов с FTS поиском по алиасам
Использует готовые методы из libs/database/connection.py
"""

import sys
from pathlib import Path

# Добавляем корневую папку проекта в путь
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from libs.database.connection import DatabaseConnection


def test_basic_search():
    """Базовый тест поиска по алиасам"""
    print("=" * 50)
    print("БАЗОВЫЙ ТЕСТ ПОИСКА ПО АЛИАСАМ")
    print("=" * 50)
    
    with DatabaseConnection() as db:
        test_queries = ["AAPL", "Apple", "apple", "MSFT", "Microsoft", "Tesla", "Daily MSFT Bear Direxion 1X"]
        
        for query in test_queries:
            print(f"\n--- Поиск: '{query}' ---")
            
            # Точный поиск
            exact_results = db.find_entity_by_alias(query, fuzzy=False)
            print(f"Точный поиск: {len(exact_results)} результатов")
            
            # FTS поиск
            fts_results = db.find_entity_by_alias(query, fuzzy=True)
            print(f"FTS поиск: {len(fts_results)} результатов")
            
            if fts_results:
                for result in fts_results[:5]:  # Показываем первые 5
                    entity = result['entity']
                    print(f"  - {entity['display_name']} ({entity['entity_type']})")
                    print(f"    Alias: '{result['alias_text']}' ({result['alias_type']})")


def test_fts_operators():
    """Тест FTS операторов"""
    print("\n" + "=" * 50)
    print("ТЕСТ FTS ОПЕРАТОРОВ")
    print("=" * 50)
    
    with DatabaseConnection() as db:
        # Тестируем различные варианты поиска
        test_cases = [
            "apple",
            "microsoft", 
            "tesla",
            "AAPL",
            "MSFT",
            "TSLA"
        ]
        
        for query in test_cases:
            print(f"\n--- FTS поиск: '{query}' ---")
            results = db.find_entity_by_alias(query, fuzzy=True)
            
            if results:
                print(f"Найдено {len(results)} результатов:")
                for result in results:
                    entity = result['entity']
                    print(f"  ✓ {entity['display_name']} - '{result['alias_text']}' ({result['alias_type']})")
            else:
                print("  ✗ Результаты не найдены")


def compare_search_methods():
    """Сравнение точного и FTS поиска"""
    print("\n" + "=" * 50)
    print("СРАВНЕНИЕ МЕТОДОВ ПОИСКА")
    print("=" * 50)
    
    with DatabaseConnection() as db:
        test_cases = [
            "Apple",
            "apple",  # строчные
            "APPLE",  # заглавные
            "AAPL",   # тикер
            "Microsoft",
            "microsoft",
            "MSFT"
        ]
        
        for query in test_cases:
            print(f"\n--- Тест: '{query}' ---")
            
            # Точный поиск
            exact_results = db.find_entity_by_alias(query, fuzzy=False)
            print(f"Точный поиск: {len(exact_results)} результатов")
            
            # FTS поиск
            fts_results = db.find_entity_by_alias(query, fuzzy=True)
            print(f"FTS поиск: {len(fts_results)} результатов")
            
            # Показываем разницу
            if len(exact_results) != len(fts_results):
                print(f"  ⚠ Разница в количестве результатов!")
                if fts_results and not exact_results:
                    print(f"  → FTS нашел то, что точный поиск пропустил")
                elif exact_results and not fts_results:
                    print(f"  → Точный поиск нашел то, что FTS пропустил")


def test_person_search():
    """Тест поиска персон по имени"""
    print("\n" + "=" * 50)
    print("ТЕСТ ПОИСКА ПЕРСОН ПО ИМЕНИ")
    print("=" * 50)
    
    with DatabaseConnection() as db:
        # Тестируем различные варианты поиска персон
        test_cases = [
            # (family, given, given_prefix)
            ("Cook", None, None),           # Только фамилия
            ("Cook", "Timothy", None),      # Фамилия + имя
            ("Cook", None, "T"),            # Фамилия + префикс имени (1 символ)
            ("Cook", None, "Ti"),           # Фамилия + префикс имени (2 символа)
            ("Cook", None, "Tim"),          # Фамилия + префикс имени (3 символа)
            ("Smith", None, None),          # Популярная фамилия
            ("Gates", None, None),          # Другая известная фамилия
            ("Jobs", None, None),           # Еще одна известная фамилия
            ("Nadella", None, None),        # Современная фамилия
        ]
        
        for family, given, given_prefix in test_cases:
            print(f"\n--- Поиск персоны: family='{family}', given='{given}', prefix='{given_prefix}' ---")
            
            try:
                results = db.find_person_by_name(family, given, given_prefix)
                
                if results:
                    print(f"Найдено {len(results)} персон:")
                    # Показываем только первые 5 результатов
                    for i, person in enumerate(results[:5], 1):
                        print(f"  {i}. {person['given']} {person['family']}")
                        print(f"     Display: {person['display_name']} (id: {person['entity_id']})")
                        if person.get('given_norm'):
                            print(f"     Given norm: {person['given_norm']}")
                        if person.get('family_norm'):
                            print(f"     Family norm: {person['family_norm']}")
                    
                    if len(results) > 5:
                        print(f"  ... и еще {len(results) - 5} персон")
                else:
                    print("  ✗ Персоны не найдены")
                    
            except Exception as e:
                print(f"  ❌ Ошибка при поиске: {e}")


def test_person_search_variations():
    """Тест поиска персон с различными вариантами написания"""
    print("\n" + "=" * 50)
    print("ТЕСТ ПОИСКА ПЕРСОН - ВАРИАНТЫ НАПИСАНИЯ")
    print("=" * 50)
    
    with DatabaseConnection() as db:
        # Тестируем нормализацию имен
        test_cases = [
            ("cook", None, None),           # строчные
            ("COOK", None, None),           # заглавные
            ("Cook", None, None),           # правильный регистр
            ("cOoK", None, None),           # смешанный регистр
            ("Cook", "timothy", None),      # строчное имя
            ("Cook", "TIMOTHY", None),      # заглавное имя
            ("Cook", None, "t"),            # строчная буква (1 символ)
            ("Cook", None, "T"),            # заглавная буква (1 символ)
            ("Cook", None, "ti"),           # строчные 2 символа
            ("Cook", None, "TI"),           # заглавные 2 символа
            ("Cook", None, "tim"),          # строчные 3 символа
            ("Cook", None, "TIM"),          # заглавные 3 символа
        ]
        
        for family, given, given_prefix in test_cases:
            print(f"\n--- Поиск: family='{family}', given='{given}', prefix='{given_prefix}' ---")
            
            try:
                results = db.find_person_by_name(family, given, given_prefix)
                
                if results:
                    print(f"✓ Найдено {len(results)} персон:")
                    # Показываем только первые 3 результата
                    for person in results[:3]:
                        print(f"    - {person['given']} {person['family']}")
                    if len(results) > 3:
                        print(f"    ... и еще {len(results) - 3} персон")
                else:
                    print("  ✗ Персоны не найдены")
                    
            except Exception as e:
                print(f"  ❌ Ошибка: {e}")


def test_person_affiliations():
    """Тест поиска аффилиаций персон"""
    print("\n" + "=" * 50)
    print("ТЕСТ АФФИЛИАЦИЙ ПЕРСОН")
    print("=" * 50)
    
    with DatabaseConnection() as db:
        # Сначала найдем несколько персон
        test_families = ["Cook", "Smith", "Gates", "Jobs"]
        
        for family in test_families:
            print(f"\n--- Поиск аффилиаций для фамилии '{family}' ---")
            
            try:
                persons = db.find_person_by_name(family)
                
                if persons:
                    # Берем первую найденную персону
                    person = persons[0]
                    person_id = person['entity_id']
                    
                    print(f"Персона: {person['given']} {person['family']}")
                    
                    # Ищем аффилиации
                    affiliations = db.find_person_affiliations(person_id, active_only=True)
                    
                    if affiliations:
                        print(f"Найдено {len(affiliations)} аффилиаций:")
                        for aff in affiliations:
                            org = aff['org']
                            print(f"  - {aff['role_title']} в {org['display_name']}")
                            if aff['symbol']:
                                print(f"    Символ: {aff['symbol']}")
                            if aff['valid_from']:
                                print(f"    С: {aff['valid_from']}")
                            if aff['valid_to']:
                                print(f"    По: {aff['valid_to']}")
                    else:
                        print("  ✗ Аффилиации не найдены")
                else:
                    print(f"  ✗ Персоны с фамилией '{family}' не найдены")
                    
            except Exception as e:
                print(f"  ❌ Ошибка при поиске аффилиаций: {e}")


def test_person_context():
    """Тест получения полного контекста персоны"""
    print("\n" + "=" * 50)
    print("ТЕСТ КОНТЕКСТА ПЕРСОН")
    print("=" * 50)
    
    with DatabaseConnection() as db:
        # Найдем несколько персон для тестирования
        test_families = ["Cook", "Smith"]
        
        for family in test_families:
            print(f"\n--- Контекст для фамилии '{family}' ---")
            
            try:
                persons = db.find_person_by_name(family)
                
                if persons:
                    # Берем первую найденную персону
                    person = persons[0]
                    person_id = person['entity_id']
                    
                    print(f"Персона: {person['given']} {person['family']}")
                    
                    # Получаем полный контекст
                    context = db.get_entity_context(person_id)
                    
                    if context:
                        entity = context['entity']
                        aliases = context['aliases']
                        affiliations = context['affiliations']
                        
                        print(f"Контекст:")
                        print(f"  Entity ID: {entity['entity_id']}")
                        print(f"  Type: {entity['entity_type']}")
                        print(f"  Canonical: {entity['canonical_full']}")
                        print(f"  Display: {entity['display_name']}")
                        
                        if aliases:
                            print(f"  Алиасы ({len(aliases)}):")
                            for alias in aliases:
                                primary_mark = "⭐" if alias['is_primary'] else "  "
                                print(f"    {primary_mark} {alias['alias_type']}: {alias['alias_text']}")
                        
                        if affiliations:
                            print(f"  Аффилиации ({len(affiliations)}):")
                            for aff in affiliations:
                                print(f"    - {aff['role_title']} в {aff['org']['display_name']}")
                                if aff['symbol']:
                                    print(f"      Символ: {aff['symbol']}")
                    else:
                        print("  ✗ Контекст не найден")
                else:
                    print(f"  ✗ Персоны с фамилией '{family}' не найдены")
                    
            except Exception as e:
                print(f"  ❌ Ошибка при получении контекста: {e}")


def test_organization_context():
    """Тест получения полного контекста организаций"""
    print("\n" + "=" * 50)
    print("ТЕСТ КОНТЕКСТА ОРГАНИЗАЦИЙ")
    print("=" * 50)
    
    with DatabaseConnection() as db:
        # Тестируем с известными организациями
        test_orgs = [
            {"name": "Apple", "entity_id": 183},
            {"name": "Microsoft", "entity_id": None},  # Найдем по имени
            {"name": "Tesla", "entity_id": None}       # Найдем по имени
        ]
        
        for org_info in test_orgs:
            print(f"\n--- Контекст для '{org_info['name']}' ---")
            
            try:
                entity_id = org_info['entity_id']
                
                # Если entity_id не указан, ищем по имени
                if not entity_id:
                    # Ищем организацию по имени через алиасы
                    with db.get_cursor() as cursor:
                        cursor.execute("""
                            SELECT DISTINCT e.entity_id, e.display_name, e.canonical_full
                            FROM entities e
                            JOIN aliases a ON e.entity_id = a.entity_id
                            WHERE e.entity_type = 'org' 
                            AND (LOWER(a.alias_text) LIKE LOWER(?) OR LOWER(e.display_name) LIKE LOWER(?))
                            LIMIT 1
                        """, (f"%{org_info['name']}%", f"%{org_info['name']}%"))
                        
                        result = cursor.fetchone()
                        if result:
                            entity_id = result['entity_id']
                            print(f"Найдена организация: {result['display_name']} (ID: {entity_id})")
                        else:
                            print(f"  ✗ Организация '{org_info['name']}' не найдена")
                            continue
                
                if entity_id:
                    # Получаем полный контекст
                    context = db.get_entity_context(entity_id)
                    
                    if context:
                        entity = context['entity']
                        aliases = context['aliases']
                        affiliations = context['affiliations']
                        
                        print(f"Контекст:")
                        print(f"  Entity ID: {entity['entity_id']}")
                        print(f"  Type: {entity['entity_type']}")
                        print(f"  Canonical: {entity['canonical_full']}")
                        print(f"  Display: {entity['display_name']}")
                        
                        # Проверяем long_business_summary
                        if entity['long_business_summary']:
                            print(f"  Business Summary:")
                            print(f"    {entity['long_business_summary']}")
                        else:
                            print(f"  Business Summary: НЕТ")
                        
                        # Дополнительная информация для организаций
                        if entity['sector']:
                            print(f"  Sector: {entity['sector']}")
                        if entity['industry']:
                            print(f"  Industry: {entity['industry']}")
                        if entity['full_time_employees']:
                            print(f"  Employees: {entity['full_time_employees']:,}")
                        if entity['website']:
                            print(f"  Website: {entity['website']}")
                        
                        if aliases:
                            print(f"  Алиасы ({len(aliases)}):")
                            for alias in aliases[:5]:  # Показываем только первые 5
                                primary_mark = "⭐" if alias['is_primary'] else "  "
                                print(f"    {primary_mark} {alias['alias_type']}: {alias['alias_text']}")
                            if len(aliases) > 5:
                                print(f"    ... и еще {len(aliases) - 5} алиасов")
                        
                        if affiliations:
                            print(f"  Связанные персоны ({len(affiliations)}):")
                            for aff in affiliations[:3]:  # Показываем только первые 3
                                print(f"    - {aff['role_title']}: {aff['person']['given']} {aff['person']['family']}")
                            if len(affiliations) > 3:
                                print(f"    ... и еще {len(affiliations) - 3} персон")
                    else:
                        print("  ✗ Контекст не найден")
                        
            except Exception as e:
                print(f"  ❌ Ошибка при получении контекста: {e}")


def interactive_person_search():
    """Интерактивный поиск персон"""
    print("\n" + "=" * 50)
    print("ИНТЕРАКТИВНЫЙ ПОИСК ПЕРСОН")
    print("=" * 50)
    print("Введите фамилию для поиска (или 'quit' для выхода)")
    print("Формат: фамилия [имя] [префикс]")
    print("Примеры: 'Cook', 'Cook Timothy', 'Cook T'")
    
    with DatabaseConnection() as db:
        while True:
            try:
                query = input("\n> ").strip()
                
                if query.lower() == 'quit':
                    break
                
                if not query:
                    continue
                
                # Парсим ввод
                parts = query.split()
                family = parts[0] if parts else None
                given = parts[1] if len(parts) > 1 else None
                given_prefix = None
                
                # Если второе слово короткое, считаем его префиксом
                if given and len(given) <= 3:
                    given_prefix = given
                    given = None
                
                print(f"\nПоиск: family='{family}', given='{given}', prefix='{given_prefix}'")
                print("-" * 50)
                
                results = db.find_person_by_name(family, given, given_prefix)
                
                if results:
                    print(f"Найдено {len(results)} персон:")
                    for i, person in enumerate(results, 1):
                        print(f"\n{i}. {person['given']} {person['family']}")
                        print(f"   Display: {person['display_name']}")
                        print(f"   Canonical: {person['canonical_full']}")
                        
                        # Показываем аффилиации
                        affiliations = db.find_person_affiliations(person['entity_id'], active_only=True)
                        if affiliations:
                            print(f"   Аффилиации:")
                            for aff in affiliations[:3]:  # Показываем первые 3
                                print(f"     - {aff['role_title']} в {aff['org']['display_name']}")
                                if aff['symbol']:
                                    print(f"       Символ: {aff['symbol']}")
                else:
                    print("Персоны не найдены")
                
            except KeyboardInterrupt:
                print("\nВыход...")
                break
            except Exception as e:
                print(f"Ошибка: {e}")




def interactive_search():
    """Интерактивный поиск"""
    print("\n" + "=" * 50)
    print("ИНТЕРАКТИВНЫЙ ПОИСК")
    print("=" * 50)
    print("Введите поисковый запрос (или 'quit' для выхода)")
    
    with DatabaseConnection() as db:
        while True:
            try:
                query = input("\n> ").strip()
                
                if query.lower() == 'quit':
                    break
                
                if not query:
                    continue
                
                print(f"\nПоиск: '{query}'")
                print("-" * 30)
                
                # Точный поиск
                exact_results = db.find_entity_by_alias(query, fuzzy=False)
                print(f"Точный поиск: {len(exact_results)} результатов")
                
                # FTS поиск
                fts_results = db.find_entity_by_alias(query, fuzzy=True)
                print(f"FTS поиск: {len(fts_results)} результатов")
                
                if fts_results:
                    print("\nНайденные сущности:")
                    for i, result in enumerate(fts_results, 1):
                        entity = result['entity']
                        print(f"  {i}. {entity['display_name']} ({entity['entity_type']})")
                        print(f"     Alias: '{result['alias_text']}' ({result['alias_type']})")
                        print(f"     Confidence: {result['confidence']}")
                
            except KeyboardInterrupt:
                print("\nВыход...")
                break
            except Exception as e:
                print(f"Ошибка: {e}")


def show_database_info():
    """Показать информацию о базе данных"""
    print("\n" + "=" * 50)
    print("ИНФОРМАЦИЯ О БАЗЕ ДАННЫХ")
    print("=" * 50)
    
    with DatabaseConnection() as db:
        with db.get_cursor() as cursor:
            # Проверяем FTS таблицу
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='alias_fts'")
            if cursor.fetchone():
                print("✓ FTS таблица 'alias_fts' существует")
                
                # Количество записей в FTS
                cursor.execute("SELECT COUNT(*) as count FROM alias_fts")
                fts_count = cursor.fetchone()['count']
                print(f"✓ Записей в FTS: {fts_count}")
                
                # Количество записей в aliases
                cursor.execute("SELECT COUNT(*) as count FROM aliases")
                aliases_count = cursor.fetchone()['count']
                print(f"✓ Записей в aliases: {aliases_count}")
                
                # Количество записей в entities
                cursor.execute("SELECT COUNT(*) as count FROM entities")
                entities_count = cursor.fetchone()['count']
                print(f"✓ Записей в entities: {entities_count}")
                
                # Количество персон
                cursor.execute("SELECT COUNT(*) as count FROM entities WHERE entity_type = 'person'")
                persons_count = cursor.fetchone()['count']
                print(f"✓ Записей персон: {persons_count}")
                
                # Примеры алиасов
                cursor.execute("SELECT alias_text, alias_type FROM aliases LIMIT 10")
                print("\nПримеры алиасов:")
                for row in cursor.fetchall():
                    print(f"  - '{row['alias_text']}' ({row['alias_type']})")
                
                # Примеры персон
                if persons_count > 0:
                    cursor.execute("SELECT given, family, display_name FROM entities WHERE entity_type = 'person' LIMIT 5")
                    print("\nПримеры персон:")
                    for row in cursor.fetchall():
                        print(f"  - {row['given']} {row['family']} (display: {row['display_name']})")
                    
            else:
                print("✗ FTS таблица 'alias_fts' не найдена")


def main():
    """Главная функция"""
    print("FTS ALIAS SEARCH PLAYGROUND")
    print("Простой playground для экспериментов с поиском по алиасам и персонам")
    
    try:
        # Показываем информацию о БД
        show_database_info()
        
        # Базовые тесты для алиасов
        test_basic_search()
        test_fts_operators()
        compare_search_methods()
        
        # Тесты для персон (компактные)
        test_person_search()
        test_person_search_variations()
        
        # Пропускаем длинные тесты по умолчанию
        print("\n" + "=" * 60)
        print("ДОПОЛНИТЕЛЬНЫЕ ТЕСТЫ")
        print("=" * 60)
        print("Хотите запустить дополнительные тесты?")
        print("1. Тесты аффилиаций персон")
        print("2. Тесты контекста персон")
        print("3. Тесты контекста организаций")
        print("4. Пропустить дополнительные тесты")
        
        choice = input("\nВыберите опцию (1-4): ").strip()
        
        if choice == '1':
            test_person_affiliations()
        elif choice == '2':
            test_person_context()
        elif choice == '3':
            test_organization_context()
        elif choice == '4':
            print("Пропускаем дополнительные тесты")
        else:
            print("Неверный выбор, пропускаем дополнительные тесты")
        
        # Интерактивные режимы
        print("\n" + "=" * 60)
        print("ИНТЕРАКТИВНЫЕ РЕЖИМЫ")
        print("=" * 60)
        print("1. Поиск по алиасам (компании, символы)")
        print("2. Поиск по персонам")
        print("3. Выход")
        
        while True:
            try:
                choice = input("\nВыберите режим (1-3): ").strip()
                
                if choice == '1':
                    interactive_search()
                elif choice == '2':
                    interactive_person_search()
                elif choice == '3':
                    print("До свидания!")
                    break
                else:
                    print("Неверный выбор. Введите 1, 2 или 3.")
                    
            except KeyboardInterrupt:
                print("\nВыход...")
                break
            except Exception as e:
                print(f"Ошибка: {e}")
        
    except Exception as e:
        print(f"\nОшибка: {e}")
        print("\nВозможные причины:")
        print("1. База данных не существует")
        print("2. Схема БД не создана")
        print("3. Нет данных в таблице aliases")
        print("4. Нет данных в таблице entities")


if __name__ == "__main__":
    main()


