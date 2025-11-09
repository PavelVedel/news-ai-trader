"""
apps\ingest\web_search\populate_cache.py
apps\market_data\statistic_from_stage_a_b.py
"""


from libs.database.connection import DatabaseConnection
from pathlib import Path
import json
from typing import Any, Optional, get_origin
from datetime import datetime, timezone
from apps.ai.perform_stage_b_entity_alias_formation import normalize_name
import time
import pandas as pd
from apps.ingest.web_search.search_manager import WebSearchManager


def main():
    db = DatabaseConnection("data/db/news.db")
    
    # Ensure the news_analysis_a table exists
    assert db.ensure_news_analysis_a_table(), "Failed to create news_analysis_a table"

    # Initialize manager
    manager = WebSearchManager(db)
    # Check backoff status
    manager.check_backoff_status()

    # Global statistics (accumulated across all news)
    found_symbols: dict[str, dict] = dict()
    not_found_symbols: set[str] = set()
    found_entities: dict[str, dict] = dict()
    not_found_entities: set[str] = dict()
    
    # Get count of ungrounded news only
    with db.get_cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM news_analysis_a WHERE is_news_grounded = 0")
        total_news = cursor.fetchone()[0]

    print("Starting analysis...")
    print(f"Total ungrounded news to process: {total_news}")
    tic = time.time()
    
    for i_news, parsed_row in enumerate(db.iterate_news_analysis_a(skip_grounded=True), start=1):
        if i_news % 100 == 0:
            print(f"Done {i_news} news ...")
        
        news_id = parsed_row['news_id']
        
        # Track results for this specific news item
        news_found_symbols = {}
        news_not_found_symbols = set()
        news_found_entities = {}
        news_not_found_entities = {}
        
        # Find symbols in the database
        for symbol in parsed_row['symbols_input']:
            human_readable_local_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            res = db.find_entity_by_alias(symbol, fuzzy=False)
            if res:
                news_found_symbols[symbol] = {key: value for key, value in res[0]['entity'].items() if value is not None and key != 'entity_id'}
                found_symbols[symbol] = news_found_symbols[symbol]  # Add to global stats
            else:
                res = manager.search(query=symbol, fuzzy=False, entity_type='symbol')
                if res.get('status', None) in {'ok'} and res.get('results', None):
                    if not res['cached']:
                        print(f"[{i_news}/{total_news}][{human_readable_local_time}] Performed web-search of symbol '{symbol}'")
                    news_found_symbols[symbol] = {i: result for i, result in enumerate(res['results'])}
                    news_found_symbols[symbol]['source_type'] = 'web_search'
                    found_symbols[symbol] = news_found_symbols[symbol]  # Add to global stats
                else:
                    news_not_found_symbols.add(symbol)
                    not_found_symbols.add(symbol)  # Add to global stats
        
        # Try search in aliases
        for entity in parsed_row['actors']:
            human_readable_local_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            # Fuzzy OFF
            res = db.find_entity_by_alias(entity['name'], fuzzy=False)
            if res:
                news_found_entities[entity['name']] = {key: value for key, value in res[0]['entity'].items() if value is not None and key != 'entity_id'}
                news_found_entities[entity['name']]['source_type'] = 'infos'
                found_entities[entity['name']] = news_found_entities[entity['name']]  # Add to global stats
                continue
            res = db.find_entity_by_alias(entity['name'], fuzzy=True)
            # Fuzzy ON
            if res:
                news_found_entities[entity['name']] = {key: value for key, value in res[0]['entity'].items() if value is not None and key != 'entity_id'}
                news_found_entities[entity['name']]['source_type'] = 'infos'
                found_entities[entity['name']] = news_found_entities[entity['name']]  # Add to global stats
                continue
            # Person
            if entity['type'] in {'person'}:
                normalize_named = normalize_name(entity['name'])
                res = db.find_person_by_name(normalize_named.family_norm, normalize_named.given_norm, normalize_named.given_prefix3)
                if res:
                    news_found_entities[entity['name']] = {key: value for key, value in res[0].items() if value is not None and key != 'entity_id'}
                    news_found_entities[entity['name']]['source_type'] = 'infos'
                    found_entities[entity['name']] = news_found_entities[entity['name']]  # Add to global stats
                    continue
            # Internet (cashed) search
            res = manager.search(query=entity['name'], fuzzy=False, entity_type=entity['type'])
            if res.get('status', None) in {'ok'} and res.get('results', None):
                if not res['cached']:
                    print(f"[{i_news}/{total_news}][{human_readable_local_time}] Performed web-search of '{entity['name']}'")
                news_found_entities[entity['name']] = {i: result for i, result in enumerate(res['results'])}
                news_found_entities[entity['name']]['source_type'] = 'web_search'
                found_entities[entity['name']] = news_found_entities[entity['name']]  # Add to global stats
                continue

            # If not found, add to not found entities
            news_not_found_entities[entity['name']] = {"type":entity["type"], "role":entity["role"]}
            not_found_entities[entity['name']] = news_not_found_entities[entity['name']]  # Add to global stats
        
        # Check if all elements were processed (found or determined as not_found)
        # Use set comparison to handle duplicates correctly
        # All symbols processed: all unique symbols from input are in found or not_found
        processed_symbols = set(news_found_symbols.keys()) | set(news_not_found_symbols)
        expected_symbols = set(parsed_row['symbols_input'])
        all_symbols_processed = processed_symbols == expected_symbols
        
        # All entities processed: all unique entity names from actors are in found or not_found
        processed_entities = set(news_found_entities.keys()) | set(news_not_found_entities.keys())
        expected_entities = {d['name'] for d in parsed_row['actors']}
        all_entities_processed = processed_entities == expected_entities
        
        # Mark news as grounded only if all elements were processed
        if all_symbols_processed and all_entities_processed:
            db.update_news_grounding(news_id, is_grounded=True)
        else:
            print(f"Warning: News {news_id} not fully processed. Symbols: {len(processed_symbols)}/{len(expected_symbols)}, Entities: {len(processed_entities)}/{len(expected_entities)}")
    
    toc = time.time()
    execution_time = toc - tic


    # Save not found symbols and entities (with type and role) to not_found_entities.xlsx
    not_found_symbols_list = [{"name": symbol, "type": "symbol", "role": ""} for symbol in not_found_symbols]
    not_found_entities_list = [{"name": name, "type": info.get("type", ""), "role": info.get("role", "")} for name, info in not_found_entities.items()]
    df_not_found = pd.DataFrame(not_found_symbols_list + not_found_entities_list)
    if not df_not_found.empty:
        df_not_found.to_excel("apps/market_data/not_found_entities.xlsx", index=False)

    # ==== BEAUTIFUL STATISTICS ====
    print("\n" + "=" * 60)
    print(" STAGE A+B RESULTS: FOUND SYMBOLS AND ENTITIES ".center(60, "="))
    print("=" * 60)

    total_symbols = len(found_symbols) + len(not_found_symbols)
    total_entities = len(found_entities) + len(not_found_entities)
    found_symbols_count = len(found_symbols)
    not_found_symbols_count = len(not_found_symbols)
    found_entities_count = len(found_entities)
    not_found_entities_count = len(not_found_entities)

    def percent(part, total):
        return (part / total * 100) if total else 0

    print(f"\nSecurities symbols:")
    print(f"  Found     : {found_symbols_count:4} / {total_symbols:4}  ({percent(found_symbols_count, total_symbols):6.2f}%)")
    print(f"  Not found : {not_found_symbols_count:4} / {total_symbols:4}  ({percent(not_found_symbols_count, total_symbols):6.2f}%)")

    print(f"\nEntities (actors):")
    print(f"  Found     : {found_entities_count:4} / {total_entities:4}  ({percent(found_entities_count, total_entities):6.2f}%)")
    print(f"  Not found : {not_found_entities_count:4} / {total_entities:4}  ({percent(not_found_entities_count, total_entities):6.2f}%)")
    
    print(f"\nExecution time: {execution_time:.2f} seconds")
    if execution_time == 0:
        print(f"Processing speed: {(total_symbols + total_entities) / execution_time:.1f} items/second") 
    
    print("\nExamples of not found symbols:", ", ".join(list(not_found_symbols)[:10]) or "None")
    print("Examples of not found entities:", ", ".join(list(not_found_entities)[:10]) or "None")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()