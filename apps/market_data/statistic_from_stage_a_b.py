from libs.database.connection import DatabaseConnection
from pathlib import Path
import json
from typing import Any, Optional, get_origin
from datetime import datetime
from apps.ai.perform_stage_b_entity_alias_formation import normalize_name
import time
import pandas as pd

def main():
    db = DatabaseConnection("data/db/news.db")
    
    # Ensure the news_analysis_a table exists
    assert db.ensure_news_analysis_a_table(), "Failed to create news_analysis_a table"

    found_symbols: dict[str, dict] = dict()
    not_found_symbols: set[str] = set()
    found_entities: dict[str, dict] = dict()
    not_found_entities: set[str] = dict()
    
    print("Starting analysis...")
    tic = time.time()
    
    for i_news, parsed_row in enumerate(iterate_parsed_news_analysis(db), start=1):
        if i_news % 1000 == 0:
            print(f"Done {i_news} rows ...")
        # print(parsed_row)
        # Find symbols in the database
        for symbol in parsed_row['symbols_input']:
            res = db.find_entity_by_alias(symbol, fuzzy=False)
            if res:
                found_symbols[symbol] = {key: value for key, value in res[0]['entity'].items() if value is not None and key != 'entity_id'}
            else:
                not_found_symbols.add(symbol)
        # Try search in aliases
        for entity in parsed_row['actors']:
            # Fuzzy OFF
            res = db.find_entity_by_alias(entity['name'], fuzzy=False)
            if res:
                found_entities[entity['name']] = {key: value for key, value in res[0]['entity'].items() if value is not None and key != 'entity_id'}
                continue
            res = db.find_entity_by_alias(entity['name'], fuzzy=True)
            # Fuzzy ON
            if res:
                found_entities[entity['name']] = {key: value for key, value in res[0]['entity'].items() if value is not None and key != 'entity_id'}
                continue
            if entity['type'] in {'person'}:
                normalize_named = normalize_name(entity['name'])
                res = db.find_person_by_name(normalize_named.family_norm, normalize_named.given_norm, normalize_named.given_prefix3)
                if res:
                    found_entities[entity['name']] = {key: value for key, value in res[0].items() if value is not None and key != 'entity_id'}
            # Else
            not_found_entities[entity['name']] = {"type":entity["type"], "role":entity["role"]}
    
    toc = time.time()
    execution_time = toc - tic


    # Save not found symbols and entities (with type and role) to not_found_entities.xlsx
    not_found_symbols_list = [{"name": symbol, "type": "symbol", "role": ""} for symbol in not_found_symbols]
    not_found_entities_list = [{"name": name, "type": info.get("type", ""), "role": info.get("role", "")} for name, info in not_found_entities.items()]
    df_not_found = pd.DataFrame(not_found_symbols_list + not_found_entities_list)
    if not df_not_found.empty:
        df_not_found.to_excel("not_found_entities.xlsx", index=False)

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
    
    print("\nExamples of not found symbols:", ", ".join(list(not_found_symbols)[:5]) or "None")
    print("Examples of not found entities:", ", ".join(list(not_found_entities)[:5]) or "None")

    print("=" * 60 + "\n")

def iterate_parsed_news_analysis(db: DatabaseConnection):
    """
    Generator for lazy iteration over parsed data
    """
    with db.get_cursor() as cursor:
        cursor.execute("SELECT * FROM news_analysis_a ORDER BY analyzed_at DESC")
        
        for row in cursor:
            yield parse_news_analysis_row(dict(row))


def parse_news_analysis_row(row: dict[str, Any]) -> dict[str, Any]:
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
            parsed[field] = parse_datetime(row[field])
        else:
            parsed[field] = None
    
    return parsed

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

if __name__ == "__main__":
    main()