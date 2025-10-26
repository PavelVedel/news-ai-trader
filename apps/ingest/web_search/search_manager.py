"""Web search manager with cache integration and provider cascade"""

import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone, timedelta

from libs.database.connection import DatabaseConnection
from apps.ingest.web_search.normalizer import normalize_query
from apps.ingest.web_search.rate_limiter import RateLimiter, RateLimitError
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
        # Only return cached if it's a valid result (not pending/error)
        if not force_refresh:
            cached = self.db.get_cached_search(normalized, fuzzy=fuzzy)
            if cached and cached['status'] not in ('pending', 'error'):
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
        
        Order: Wikipedia → Wikidata → DuckDuckGo → Google CSE (rare)
        
        Args:
            normalized_query: Normalized search query
            entity_type: Type of entity to determine which providers to use
        """
        providers = [
            (self.wikipedia, 'wikipedia'),
            (self.wikidata, 'wikidata'),
            (self.duckduckgo, 'duckduckgo'),
        ]
        
        # Add Google CSE if available
        if self.google_cse:
            providers.append((self.google_cse, 'google_cse'))
        
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
    
    def _set_backoff(self, provider: str, exponential: bool = False, attempts: int = 1):
        """Set backoff period for provider"""
        if exponential:
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

