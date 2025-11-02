"""Web search manager with cache integration and provider cascade"""

import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone, timedelta

from libs.database.connection import DatabaseConnection
from apps.ingest.web_search.normalizer import normalize_query
from apps.ingest.web_search.rate_limiter import RateLimiter
from apps.ingest.web_search.providers import (
    WikipediaProvider,
    WikidataProvider,
    DuckDuckGoProvider,
    GoogleCSEProvider
)
from apps.ingest.web_search.config import (
    BACKOFF_BASE_DELAY_MINUTES,
    BACKOFF_MAX_DELAY_MINUTES,
)


class WebSearchManager:
    """
    Main orchestrator for web search with caching and provider cascade
    
    Search flow:
    1. Normalize query
    2. Check cache (with optional fuzzy matching)
    3. If miss: cascade through providers (Wikipedia → Wikidata → DuckDuckGo → Google CSE)
    4. Save result to cache
    5. Return results
    """
    
    def __init__(self, db: DatabaseConnection):
        self.db = db
        self.rate_limiter = RateLimiter()
        
        # Initialize providers
        self.wikipedia = WikipediaProvider(self.rate_limiter)
        self.wikidata = WikidataProvider(self.rate_limiter)
        self.duckduckgo = DuckDuckGoProvider(self.rate_limiter)
        
        # Google CSE is optional - pass db for persistent quota tracking
        try:
            self.google_cse = GoogleCSEProvider(self.rate_limiter, db=self.db)
        except ValueError:
            self.google_cse = None
    
    def search(self, query: str, force_refresh: bool = False, fuzzy: bool = False, 
               entity_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Search for entity with caching. Overwise perform search through providers.
        
        Args:
            query: Search query string
            force_refresh: If True, bypass cache and search fresh
            fuzzy: If True, use fuzzy matching when checking cache
            entity_type: Type of entity (e.g., 'symbol', 'person', 'company')
            
        Returns:
            {
                'query': original query,
                'normalized_query': normalized query,
                'provider': provider name,
                'results': [result dicts],
                'status': 'ok'|'empty'|'error'|'ratelimited',
                'cached': bool
            }
        """
        normalized = normalize_query(query)
        
        # Check cache first (unless force_refresh)
        # Only return cached if it's a valid result (filter out empty/error/ratelimited)
        if not force_refresh:
            cached = self.db.get_cached_search(normalized, fuzzy=fuzzy, filter_empty=True)
            if cached:
                # Valid cached result - return it
                return {
                    'query': query,
                    'normalized_query': normalized,
                    'provider': cached['provider'],
                    'results': cached['results'],
                    'status': cached['status'],
                    'cached': True
                }
        
        # Cache miss or force_refresh - perform search
        result = self._search_through_providers(normalized, entity_type=entity_type)
        
        return {
            'query': query,
            'normalized_query': normalized,
            'provider': result['provider'],
            'results': result['results'],
            'status': result['status'],
            'cached': False
        }
    
    def _search_through_providers(self, normalized_query: str, entity_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Cascade through search providers until we get results
        
        Provider order: DuckDuckGo → Google CSE → Wikidata → Wikipedia
        Financial/business queries work better with DuckDuckGo and Google CSE
        
        Args:
            normalized_query: Normalized search query
            entity_type: Type of entity to determine which providers to use
        """
        # Provider order: DuckDuckGo, Google CSE, Wikipedia, Wikidata
        # Wikipedia has better snippets (5 sentences), Wikidata has short descriptions
        providers = [
            (self.duckduckgo, 'duckduckgo'),
        ]
        
        # Add Google CSE if available (second priority)
        if self.google_cse:
            providers.append((self.google_cse, 'google_cse'))
        
        # Add Wikipedia and Wikidata as fallback
        # Wikipedia prioritized over Wikidata because of longer snippets
        providers.extend([
            (self.wikipedia, 'wikipedia'),
            (self.wikidata, 'wikidata'),
        ])
        
        # Skip wiki providers for symbols (they don't work well for stock symbols)
        if entity_type == 'symbol':
            providers = [(p, n) for p, n in providers if n not in ('wikipedia', 'wikidata')]
        
        # Track failed providers
        failed_providers = []
        
        for provider, name in providers:
            # Skip if provider is in backoff
            if self.db.is_provider_in_backoff(name):
                failed_providers.append({'name': name, 'reason': 'backoff'})
                continue
            
            # Perform search
            results, http_code, error = provider.search(normalized_query)
            
            # Handle rate limiting
            if http_code == 429:
                failed_providers.append({'name': name, 'reason': 'ratelimited'})
                # Set backoff
                self._set_backoff(name)
                self.db.save_search_result(
                    provider=name,
                    normalized_query=normalized_query,
                    results_json=[],
                    status='ratelimited',
                    http_code=429,
                    error=error
                )
                continue
            
            # Handle server errors (5xx)
            if http_code and 500 <= http_code < 600:
                # Exponential backoff
                attempts = self.db.update_search_attempts(name, normalized_query)
                if attempts < 5:  # Don't backoff forever
                    self._set_backoff(name, exponential=True, attempts=attempts)
                    self.db.save_search_result(
                        provider=name,
                        normalized_query=normalized_query,
                        results_json=[],
                        status='error',
                        http_code=http_code,
                        error=error
                    )
                continue
            
            # Determine status
            if error:
                status = 'error'
                failed_providers.append({'name': name, 'reason': 'error', 'error': error})
            elif not results:
                status = 'empty'
                failed_providers.append({'name': name, 'reason': 'empty'})
                
                # Check for consecutive empty responses from DuckDuckGo
                # If multiple empty responses in recent time, it might be temporarily blocked
                if name == 'duckduckgo':
                    recent_empty = self.db.get_recent_empty_count('duckduckgo', minutes=30)
                    if recent_empty >= 3:
                        # Multiple empty responses - likely temporary block
                        # Set short backoff (5 minutes) to let it recover
                        print(f"  DuckDuckGo returned {recent_empty} empty responses in last 30 min, setting short backoff")
                        self._set_backoff(name, delay_minutes=5)
                        backoff_until = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
                        self.db.save_search_result(
                            provider=name,
                            normalized_query=normalized_query,
                            results_json=[],
                            status='empty',
                            http_code=http_code,
                            error=None,
                            backoff_until_utc=backoff_until
                        )
                        continue  # Skip to next provider
            else:
                status = 'ok'
            
            # Save to cache
            self.db.save_search_result(
                provider=name,
                normalized_query=normalized_query,
                results_json=results,
                status=status,
                http_code=http_code,
                error=error
            )
            
            # Return first non-empty result with failed providers info
            if status == 'ok':
                return {
                    'provider': name,
                    'results': results,
                    'status': status,
                    'failed_providers': failed_providers
                }
        
        # All providers exhausted
        return {
            'provider': 'none',
            'results': [],
            'status': 'empty',
            'failed_providers': failed_providers
        }
    
    def _set_backoff(self, provider: str, exponential: bool = False, attempts: int = 1, delay_minutes: Optional[int] = None):
        """
        Set backoff period for provider
        
        Args:
            provider: Provider name
            exponential: Use exponential backoff
            attempts: Number of attempts (for exponential backoff)
            delay_minutes: Custom delay in minutes (overrides other options)
        """
        if delay_minutes is not None:
            delay = delay_minutes
        elif exponential:
            # Exponential backoff: 2^attempts * base_delay
            delay = min(
                (2 ** attempts) * BACKOFF_BASE_DELAY_MINUTES,
                BACKOFF_MAX_DELAY_MINUTES
            )
        else:
            delay = BACKOFF_BASE_DELAY_MINUTES
        
        self.rate_limiter.set_backoff(provider, delay)
        
        # Also update in database
        backoff_until = (datetime.now(timezone.utc) + timedelta(minutes=delay)).isoformat()
        with self.db.get_cursor() as cursor:
            cursor.execute("""
                UPDATE web_search_cache 
                SET backoff_until_utc = ? 
                WHERE provider = ? AND backoff_until_utc IS NULL
            """, (backoff_until, provider))
    
    def check_backoff_status(self):
        """Check which providers are in backoff"""
        providers = ['wikipedia', 'wikidata', 'duckduckgo', 'google_cse']
        in_backoff = []
        
        for provider in providers:
            if self.db.is_provider_in_backoff(provider):
                # Get backoff until time
                with self.db.get_cursor() as cursor:
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
    
    def search_batch(self, entities: List[Dict[str, Any]], max_searches: Optional[int] = None, start_from: int = 0):
        """
        Search multiple entities in batch with progress tracking and statistics
        
        Args:
            entities: List of entity dictionaries with 'name', 'type', 'role' keys
            max_searches: Maximum number of searches to perform (None = all)
            start_from: Start from this index (useful for resuming)
            
        Returns:
            Dictionary with statistics about the batch search
        """
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
        
        def format_time(seconds):
            """Format time in human-readable format"""
            if seconds < 60:
                return f"{int(seconds)}s"
            elif seconds < 3600:
                return f"{int(seconds / 60)}m {int(seconds % 60)}s"
            else:
                return f"{int(seconds / 3600)}h {int((seconds % 3600) / 60)}m"
        
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
                
                if self.google_cse:
                    providers_to_check.append('google_cse')
                
                all_blocked = True
                for provider in providers_to_check:
                    # If ANY provider is NOT in backoff, we can try
                    if not self.db.is_provider_in_backoff(provider):
                        all_blocked = False
                        break
                
                if all_blocked:
                    print(f"  ⚠️  All available providers are in backoff, skipping for now")
                    stats['error'] += 1
                    continue
                
                result = self.search(name, force_refresh=False, entity_type=entity_type)
                
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
        
        return stats

