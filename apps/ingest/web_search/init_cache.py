"""Initialize web search cache from not_found_entities.xlsx"""

import sys
import argparse
from pathlib import Path
from typing import Optional
import pandas as pd

# # Add project root to path
# project_root = Path(__file__).parent.parent.parent.parent
# sys.path.insert(0, str(project_root))

from libs.database.connection import DatabaseConnection
from apps.ingest.web_search.normalizer import normalize_query
from apps.ingest.web_search.search_manager import WebSearchManager


def load_entities_from_excel(excel_path: str) -> list[dict]:
    """
    Load entities from not_found_entities.xlsx
    
    Returns list of dicts with 'name', 'type', 'role'
    """
    try:
        df = pd.read_excel(excel_path)
        entities = df.to_dict('records')
        print(f"Loaded {len(entities)} entities from {excel_path}")
        return entities
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        return []


def init_pending_cache(entities: list[dict], db: DatabaseConnection):
    """Insert placeholder cache entries with status='pending'"""
    inserted = 0
    skipped = 0
    
    for entity in entities:
        name = entity.get('name', '').strip()
        if not name:
            continue
        
        normalized = normalize_query(name)
        
        # Check if already exists
        existing = db.get_cached_search(normalized)
        if existing:
            skipped += 1
            continue
        
        # Insert pending entry
        try:
            db.save_search_result(
                provider='pending',
                normalized_query=normalized,
                results_json=[],
                status='pending',
                error=None,
                backoff_until_utc=None
            )
            inserted += 1
        except Exception as e:
            print(f"Error inserting {normalized}: {e}")
    
    print(f"\nInitialization complete:")
    print(f"  Inserted: {inserted}")
    print(f"  Skipped (already exists): {skipped}")
    return inserted


def populate_cache(entities: list[dict], manager: WebSearchManager, max_searches: Optional[int] = None):
    """Actually perform searches for pending entities"""
    total = len(entities)
    max_searches = max_searches or total
    
    print(f"\nStarting search for up to {max_searches} entities...")
    
    for i, entity in enumerate(entities[:max_searches], 1):
        name = entity.get('name', '').strip()
        if not name:
            continue
        
        print(f"\n[{i}/{max_searches}] Searching: {name}")
        
        try:
            result = manager.search(name)
            print(f"  Provider: {result['provider']}")
            print(f"  Status: {result['status']}")
            print(f"  Results: {len(result['results'])}")
            print(f"  Cached: {result['cached']}")
            
            if result['results']:
                print(f"  First result: {result['results'][0]['title']}")
        except Exception as e:
            print(f"  Error: {e}")


def main():
    parser = argparse.ArgumentParser(description='Initialize web search cache')
    parser.add_argument('--excel', type=str, default='apps/market_data/not_found_entities.xlsx',
                       help='Path to not_found_entities.xlsx')
    parser.add_argument('--init-only', action='store_true',
                       help='Only initialize pending cache entries, do not search')
    parser.add_argument('--populate', action='store_true',
                       help='Actually perform searches for pending entities')
    parser.add_argument('--max-searches', type=int, default=None,
                       help='Maximum number of searches to perform')
    
    args = parser.parse_args()
    
    # Initialize database connection
    db = DatabaseConnection()
    
    # Ensure web_search tables exist
    print("Ensuring web_search tables exist...")
    db.ensure_web_search_tables()
    
    # Load entities from Excel
    entities = load_entities_from_excel(args.excel)
    if not entities:
        print("No entities to process")
        return
    
    # Initialize pending cache entries
    print("\nInitializing pending cache entries...")
    init_pending_cache(entities, db)
    
    # Perform searches if requested
    if args.populate:
        manager = WebSearchManager(db)
        populate_cache(entities, manager, args.max_searches)
    
    print("\nDone!")


if __name__ == '__main__':
    main()

