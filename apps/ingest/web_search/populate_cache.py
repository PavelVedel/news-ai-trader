"""Populate web search cache with entities from not_found_entities.xlsx
Only searches entities that are not already in cache
"""

import os
import argparse
import traceback
import pandas as pd
from libs.database.connection import DatabaseConnection
from apps.ingest.web_search.search_manager import WebSearchManager
from apps.ingest.web_search.normalizer import normalize_query


def get_entities_from_excel(excel_path: str) -> list[dict]:
    """Load entities from not_found_entities.xlsx"""
    try:
        if not os.path.exists(excel_path):
            print(f"ERROR: Excel file not found: {excel_path}")
            print(f"Current working directory: {os.getcwd()}")
            print(f"Looking for file at: {os.path.abspath(excel_path)}")
            return []
        
        df = pd.read_excel(excel_path)
        entities = df.to_dict('records')
        print(f"Loaded {len(entities)} entities from {excel_path}")
        return entities
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        traceback.print_exc()
        return []


def filter_not_in_cache(entities: list[dict], db: DatabaseConnection, statuses_to_skip: list[str] = None) -> list[dict]:
    """Filter out entities that are already in cache
    
    By default, skips entities with any cached result.
    You can specify which statuses should be retried by passing statuses_to_skip.
    
    Args:
        entities: List of entities to filter
        db: Database connection
        statuses_to_skip: List of statuses to retry (default: []). 
                         Common statuses: 'ok', 'empty', 'error', 'pending', 'ratelimited'
    
    Returns:
        List of entities that need to be searched
    """
    if statuses_to_skip is None:
        statuses_to_skip = []  # By default, skip all cached entities
    
    not_cached = []
    cached_count = 0
    
    for entity in entities:
        name = entity.get('name', '').strip()
        if not name:
            continue
        
        normalized = normalize_query(name)
        
        # Check cache for any result
        cached_result = db.get_cached_search(normalized)
        
        # If entity is in cache and should be skipped
        if cached_result and cached_result['status'] not in statuses_to_skip:
            # Already cached, skip it
            cached_count += 1
        else:
            # Not cached or should be retried - need to search
            not_cached.append(entity)
    
    print(f"\nCache status:")
    print(f"  Already cached: {cached_count}")
    print(f"  Need to search: {len(not_cached)}")
    
    return not_cached


def main():
    """Main function to populate web search cache from Excel file"""
    parser = argparse.ArgumentParser(description='Populate web search cache from Excel')
    parser.add_argument('--excel', type=str, 
                       default='apps/market_data/not_found_entities.xlsx',
                       help='Path to not_found_entities.xlsx')
    parser.add_argument('--max-searches', type=int, default=None,
                       help='Maximum number of searches to perform')
    parser.add_argument('--start-from', type=int, default=0,
                       help='Start from this index (useful for resuming)')
    parser.add_argument('--skip-filter', action='store_true',
                       help='Skip cache filtering, search all entities')
    parser.add_argument('--retry-statuses', type=str, nargs='+',
                       default=[],
                       help='Retry entities with these cached statuses (default: skip all cached)')
    parser.add_argument('--check-backoff', action='store_true',
                       help='Check backoff status of providers and exit')
    
    args = parser.parse_args()
    
    # Initialize database
    db = DatabaseConnection()
    db.ensure_web_search_tables()
    
    # Initialize manager
    manager = WebSearchManager(db)
    
    # Check backoff status if requested
    if args.check_backoff:
        manager.check_backoff_status()
        return
    
    # Check backoff status before starting
    manager.check_backoff_status()
    
    # Load entities
    entities = get_entities_from_excel(args.excel)
    if not entities:
        print("No entities to process")
        return
    
    # Filter out already cached entities (unless skip_filter)
    if not args.skip_filter:
        entities = filter_not_in_cache(entities, db, args.retry_statuses)
        if not entities:
            print("\nAll entities are already in cache!")
            return
    else:
        print(f"\nSkipping cache filter, will search all {len(entities)} entities")
    
    # Perform batch search
    manager.search_batch(entities, max_searches=args.max_searches, start_from=args.start_from)
    
    print("\nDone!")


if __name__ == '__main__':
    main()

