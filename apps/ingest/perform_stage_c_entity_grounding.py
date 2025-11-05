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

    found_symbols: dict[str, dict] = dict()
    not_found_symbols: set[str] = set()
    found_entities: dict[str, dict] = dict()
    not_found_entities: set[str] = dict()
    
    total_news = db.get_total_news_analysis_a()

    print("Starting analysis...")
    tic = time.time()
    
    for i_news, parsed_row in enumerate(db.iterate_news_analysis_a(), start=1):
        if i_news % 100 == 0:
            print(f"Done {i_news} news ...")
        # print(parsed_row)
        # Find symbols in the database
        for symbol in parsed_row['symbols_input']:
            human_readable_local_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            res = db.find_entity_by_alias(symbol, fuzzy=False)
            if res:
                found_symbols[symbol] = {key: value for key, value in res[0]['entity'].items() if value is not None and key != 'entity_id'}
            else:
                res = manager.search(query=symbol, fuzzy=False, entity_type='symbol')
                if res.get('status', None) in {'ok'} and res.get('results', None):
                    if not res['cached']:
                        print(f"[{i_news}/{total_news}][{human_readable_local_time}] Performed web-search of symbol '{symbol}'")
                    found_symbols[symbol] = {i: result for i, result in enumerate(res['results'])}
                    found_symbols[symbol]['source_type'] = 'web_search'
                    pass
                else:
                    not_found_symbols.add(symbol)
        # Try search in aliases
        for entity in parsed_row['actors']:
            human_readable_local_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            # Fuzzy OFF
            res = db.find_entity_by_alias(entity['name'], fuzzy=False)
            if res:
                found_entities[entity['name']] = {key: value for key, value in res[0]['entity'].items() if value is not None and key != 'entity_id'}
                found_entities[entity['name']]['source_type'] = 'infos'
                continue
            res = db.find_entity_by_alias(entity['name'], fuzzy=True)
            # Fuzzy ON
            if res:
                found_entities[entity['name']] = {key: value for key, value in res[0]['entity'].items() if value is not None and key != 'entity_id'}
                found_entities[entity['name']]['source_type'] = 'infos'
                continue
            # Person
            if entity['type'] in {'person'}:
                normalize_named = normalize_name(entity['name'])
                res = db.find_person_by_name(normalize_named.family_norm, normalize_named.given_norm, normalize_named.given_prefix3)
                if res:
                    found_entities[entity['name']] = {key: value for key, value in res[0].items() if value is not None and key != 'entity_id'}
                    found_entities[entity['name']]['source_type'] = 'infos'
                    continue
            # Internet (cashed) search
            res = manager.search(query=entity['name'], fuzzy=False, entity_type=entity['type'])
            if res.get('status', None) in {'ok'} and res.get('results', None):
                if not res['cached']:
                    print(f"[{i_news}/{total_news}][{human_readable_local_time}] Performed web-search of '{entity['name']}'")
                found_entities[entity['name']] = {i: result for i, result in enumerate(res['results'])}
                found_entities[entity['name']]['source_type'] = 'web_search'
                continue


            # If not found, add to not found entities
            not_found_entities[entity['name']] = {"type":entity["type"], "role":entity["role"]}
    
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
    print(f"Processing speed: {(total_symbols + total_entities) / execution_time:.1f} items/second")
    
    print("\nExamples of not found symbols:", ", ".join(list(not_found_symbols)[:10]) or "None")
    print("Examples of not found entities:", ", ".join(list(not_found_entities)[:10]) or "None")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()