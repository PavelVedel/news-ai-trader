"""Populate web search cache with entities from not_found_entities.xlsx
Only searches entities that are not already in cache
"""

import pandas as pd
from libs.database.connection import DatabaseConnection
from apps.ingest.web_search.search_manager import WebSearchManager
from apps.ingest.web_search.normalizer import normalize_query


def get_entities_from_excel(excel_path: str) -> list[dict]:
    """Load entities from not_found_entities.xlsx"""
    import os
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
        import traceback
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


def search_entities(entities: list[dict], manager: WebSearchManager, db: DatabaseConnection,
                   max_searches: int = None, start_from: int = 0):
    """Search entities that are not in cache"""
    import time
    
    total = len(entities)
    max_searches = max_searches or total
    
    print(f"\nStarting search for {min(max_searches, total)} entities...")
    print(f"Starting from index {start_from}")
    
    # Track statistics
    stats = {
        'total': 0,
        'success': 0,
        'empty': 0,
        'error': 0,
        'by_provider': {},
        'failed_searches': []
    }
    
    # Time tracking
    start_time = time.time()
    task_times = []  # Track time for each task
    
    end_index = min(start_from + max_searches, total)
    entities_to_search = entities[start_from:end_index]
    
    for i, entity in enumerate(entities_to_search, start=start_from):
        task_start = time.time()
        
        name = entity.get('name', '').strip()
        if not name:
            continue
        
        stats['total'] += 1
        
        # Calculate elapsed time and ETA
        elapsed_time = time.time() - start_time
        remaining = total - stats['total'] + 1
        
        if task_times:
            fastest_time = min(task_times)
            slowest_time = max(task_times)
            
            eta_best = fastest_time * remaining
            eta_worst = slowest_time * remaining
        else:
            eta_best = eta_worst = 0
        
        # Format time
        def format_time(seconds):
            if seconds < 60:
                return f"{int(seconds)}s"
            elif seconds < 3600:
                return f"{int(seconds / 60)}m {int(seconds % 60)}s"
            else:
                return f"{int(seconds / 3600)}h {int((seconds % 3600) / 60)}m"
        
        print(f"\n[{i+1}/{total}] Searching: {name}")
        print(f"  Type: {entity.get('type', 'unknown')}, Role: {entity.get('role', 'unknown')}")
        print(f"  Elapsed: {format_time(elapsed_time)} | Remaining: {remaining} tasks")
        if task_times:
            print(f"  ETA (best/worst): {format_time(eta_best)} / {format_time(eta_worst)}")
        
        try:
            entity_type = entity.get('type', None)
            
            # Check if all providers are in backoff for this entity type
            if entity_type != 'symbol':
                # Non-symbols can use all providers
                providers_to_check = ['wikipedia', 'wikidata', 'duckduckgo']
            else:
                # Symbols skip wiki providers
                providers_to_check = ['duckduckgo']
            
            if manager.google_cse:
                providers_to_check.append('google_cse')
            
            all_blocked = True
            for provider in providers_to_check:
                # If ANY provider is NOT in backoff, we can try
                if not db.is_provider_in_backoff(provider):
                    all_blocked = False
                    break
            
            if all_blocked:
                print(f"  ⚠️  All available providers are in backoff, skipping for now")
                stats['error'] += 1
                continue
            
            result = manager.search(name, force_refresh=False, entity_type=entity_type)
            
            # Track task time
            task_time = time.time() - task_start
            task_times.append(task_time)
            
            # Update statistics
            stats['by_provider'][result['provider']] = stats['by_provider'].get(result['provider'], 0) + 1
            
            status = result['status']
            if status == 'ok':
                stats['success'] += 1
            elif status == 'empty':
                stats['empty'] += 1
            else:
                stats['error'] += 1
                stats['failed_searches'].append({
                    'name': name,
                    'provider': result['provider'],
                    'status': status,
                    'results_count': len(result['results'])
                })
            
            print(f"  Status: {status}")
            print(f"  Provider: {result['provider']}")
            print(f"  Results found: {len(result['results'])}")
            
            # Show failed providers if any
            if 'failed_providers' in result and result['failed_providers']:
                failed = result['failed_providers']
                failed_names = [f"{f['name']} ({f['reason']})" for f in failed]
                print(f"  Failed providers: {', '.join(failed_names)}")
            
            if result['results']:
                first = result['results'][0]
                print(f"  Top result: {first['title'][:80]}")
                if first.get('snippet'):
                    print(f"  Top snippet: {first['snippet'][:200]}...")
            else:
                print(f"  No results found from {result['provider']}")
        except Exception as e:
            stats['error'] += 1
            stats['failed_searches'].append({
                'name': name,
                'error': str(e)
            })
            print(f"  Error: {e}")
    
    # Print statistics summary
    total_time = time.time() - start_time
    
    def format_time(seconds):
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds / 60)}m {int(seconds % 60)}s"
        else:
            return f"{int(seconds / 3600)}h {int((seconds % 3600) / 60)}m"
    
    print("\n" + "=" * 60)
    print("SEARCH STATISTICS SUMMARY")
    print("=" * 60)
    print(f"Total searches: {stats['total']}")
    print(f"Total time: {format_time(total_time)}")
    print(f"Average time per search: {format_time(total_time / stats['total']) if stats['total'] > 0 else 'N/A'}")
    print(f"Successful (with results): {stats['success']}")
    print(f"Empty (no results): {stats['empty']}")
    print(f"Errors: {stats['error']}")
    print(f"\nBy provider:")
    for provider, count in sorted(stats['by_provider'].items(), key=lambda x: -x[1]):
        print(f"  {provider:15} {count:4} searches")
    
    if stats['failed_searches']:
        print(f"\nFailed searches ({len(stats['failed_searches'])}):")
        for failed in stats['failed_searches'][:10]:  # Show first 10
            if 'error' in failed:
                print(f"  - {failed['name']}: {failed['error']}")
            else:
                print(f"  - {failed['name']}: {failed['provider']} returned {failed['status']}")
        if len(stats['failed_searches']) > 10:
            print(f"  ... and {len(stats['failed_searches']) - 10} more")
    
    print("=" * 60)


def check_backoff_status(db: DatabaseConnection):
    """Check which providers are in backoff"""
    from datetime import datetime, timezone
    
    providers = ['wikipedia', 'wikidata', 'duckduckgo', 'google_cse']
    in_backoff = []
    
    for provider in providers:
        if db.is_provider_in_backoff(provider):
            # Get backoff until time
            with db.get_cursor() as cursor:
                cursor.execute("""
                    SELECT backoff_until_utc FROM web_search_cache 
                    WHERE provider = ? AND backoff_until_utc IS NOT NULL
                    LIMIT 1
                """, (provider,))
                row = cursor.fetchone()
                if row:
                    backoff_until = row[0]
                    try:
                        backoff_dt = datetime.fromisoformat(backoff_until)
                        now = datetime.now(timezone.utc).replace(tzinfo=None)
                        if backoff_dt > now:
                            remaining = (backoff_dt - now).total_seconds() / 60  # minutes
                            in_backoff.append((provider, backoff_until, remaining))
                    except:
                        pass
    
    if in_backoff:
        print("\n⚠️  Providers in backoff:")
        for provider, until, remaining_mins in in_backoff:
            print(f"  - {provider}: until {until} ({remaining_mins:.1f} min remaining)")
    else:
        print("\n✓ No providers in backoff")
    
    return in_backoff


def main():
    import argparse
    
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
    
    # Check backoff status if requested
    if args.check_backoff:
        check_backoff_status(db)
        return
    
    # Check backoff status before starting
    backoff_status = check_backoff_status(db)
    
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
    
    # Perform searches
    manager = WebSearchManager(db)
    search_entities(entities, manager, db, args.max_searches, args.start_from)
    
    print("\nDone!")


if __name__ == '__main__':
    main()

