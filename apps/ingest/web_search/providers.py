"""Web search providers (Wikipedia, Wikidata, DuckDuckGo, Google CSE)"""

import requests
import time
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone
from bs4 import BeautifulSoup

from apps.ingest.web_search.config import (
    GOOGLE_CSE_API_KEY,
    GOOGLE_CSE_ID,
    PROVIDER_RATE_LIMITS,
    BACKOFF_BASE_DELAY_MINUTES,
    BACKOFF_MAX_DELAY_MINUTES,
    BACKOFF_MAX_ATTEMPTS,
)
from apps.ingest.web_search.rate_limiter import RateLimiter, RateLimitError


class SearchProvider:
    """Base class for search providers"""
    
    def __init__(self, name: str, rate_limiter: RateLimiter):
        self.name = name
        self.rate_limiter = rate_limiter
        self.rps = PROVIDER_RATE_LIMITS.get(name, 0.5)
    
    def search(self, query: str) -> tuple[List[Dict[str, Any]], Optional[int], Optional[str]]:
        """
        Search using provider
        
        Returns:
            (results, http_code, error_message)
        """
        raise NotImplementedError
    
    def _make_result(self, title: str, url: str, snippet: str, 
                     relevance_score: float = 1.0, **metadata) -> Dict[str, Any]:
        """Create standardized result dict"""
        return {
            "title": title,
            "url": url,
            "snippet": snippet,
            "relevance_score": relevance_score,
            "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
            "source_metadata": {
                "provider": self.name,
                **metadata
            }
        }


class WikipediaProvider(SearchProvider):
    """Wikipedia API search provider"""
    
    def __init__(self, rate_limiter: RateLimiter):
        super().__init__('wikipedia', rate_limiter)
        self.base_url = 'https://en.wikipedia.org/w/api.php'
    
    def search(self, query: str) -> tuple[List[Dict], Optional[int], Optional[str]]:
        """
        Search Wikipedia using Main Search API with snippets
        
        Returns up to 10 results with snippets containing search context
        """
        try:
            self.rate_limiter.wait_if_needed(self.name, self.rps)
            
            # Step 1: Search for pages
            search_params = {
                'action': 'query',
                'list': 'search',
                'srsearch': query,
                'srlimit': 10,
                'srnamespace': 0,
                'format': 'json',
            }
            
            headers = {
                'User-Agent': 'NewsAI-Trader/1.0 (research project; +https://github.com)'
            }
            
            response = requests.get(self.base_url, params=search_params, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if 'query' not in data or 'search' not in data['query']:
                return [], None, None
            
            search_results = data['query']['search']
            if not search_results:
                return [], response.status_code, None
            
            # Step 2: Get extracts for top results
            # Get extracts for first 5 results only (to minimize API calls)
            top_pages = search_results[:5]
            page_titles = [page['title'] for page in top_pages]
            
            extract_params = {
                'action': 'query',
                'prop': 'extracts',
                'exintro': '1',  # Get only intro section
                'exsentences': '5',  # Get first 5 sentences
                'explaintext': '1',  # Return plain text instead of HTML
                'titles': '|'.join(page_titles),
                'format': 'json',
            }
            
            response = requests.get(self.base_url, params=extract_params, headers=headers, timeout=10)
            response.raise_for_status()
            extract_data = response.json()
            
            # Build results
            results = []
            for i, page in enumerate(search_results):
                title = page['title']
                snippet = page.get('snippet', '')
                
                # Clean snippet HTML
                snippet = snippet.replace('<span class="searchmatch">', '').replace('</span>', '').strip()
                
                # Try to get extract if available (for top results)
                if i < len(page_titles) and 'pages' in extract_data.get('query', {}):
                    pages = extract_data['query']['pages']
                    for pid, page_data in pages.items():
                        if page_data.get('title') == title and 'extract' in page_data:
                            snippet = page_data['extract'].strip()
                            # explaintext already returns plain text, just normalize whitespace
                            snippet = ' '.join(snippet.split())
                            break
                
                # Construct URL from title
                url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
                
                score = 1.0 - (i * 0.1)
                result = self._make_result(
                    title=title,
                    url=url,
                    snippet=snippet,
                    relevance_score=max(0.1, score),
                    wiki_id=None
                )
                results.append(result)
            
            # Check for empty results
            if not results:
                return [], response.status_code, None
            
            return results, response.status_code, None
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                # Rate limited
                return [], 429, str(e)
            return [], e.response.status_code if e.response else 500, str(e)
        except Exception as e:
            return [], None, str(e)


class WikidataProvider(SearchProvider):
    """Wikidata SPARQL search provider"""
    
    def __init__(self, rate_limiter: RateLimiter):
        super().__init__('wikidata', rate_limiter)
        self.endpoint = 'https://query.wikidata.org/sparql'
    
    def search(self, query: str) -> tuple[List[Dict], Optional[int], Optional[str]]:
        """
        Search Wikidata using SPARQL
        """
        try:
            self.rate_limiter.wait_if_needed(self.name, self.rps)
            
            # SPARQL query to find entities matching the search term
            # This searches in labels and aliases
            sparql = f"""
            SELECT DISTINCT ?item ?itemLabel ?itemDescription ?article WHERE {{
              {{
                ?item rdfs:label "{query}"@en .
              }}
              UNION
              {{
                ?item skos:altLabel "{query}"@en .
              }}
              SERVICE wikibase:label {{ 
                bd:serviceParam wikibase:language "en" .
              }}
              OPTIONAL {{
                ?article schema:about ?item .
                ?article schema:inLanguage "en" .
                ?article schema:isPartOf <https://en.wikipedia.org/> .
              }}
            }}
            LIMIT 10
            """
            
            headers = {
                'User-Agent': 'NewsAI-Trader/1.0 (research project; +https://github.com)',
                'Accept': 'application/sparql-results+json'
            }
            
            response = requests.get(
                self.endpoint,
                params={'query': sparql, 'format': 'json'},
                headers=headers,
                timeout=15
            )
            response.raise_for_status()
            
            data = response.json()
            bindings = data.get('results', {}).get('bindings', [])
            
            results = []
            for i, binding in enumerate(bindings):
                item = binding.get('item', {}).get('value', '')
                label = binding.get('itemLabel', {}).get('value', '')
                desc = binding.get('itemDescription', {}).get('value', '')
                article = binding.get('article', {}).get('value', '')
                
                # Use Wikipedia URL if available, otherwise Wikidata URL
                url = article if article else item
                
                # If no description, use label as fallback
                snippet = desc if desc else f"Wikidata entity: {label or item.split('/')[-1]}"
                
                score = 1.0 - (i * 0.1)
                result = self._make_result(
                    title=label or 'Unknown',
                    url=url,
                    snippet=snippet,
                    relevance_score=max(0.1, score),
                    wikidata_id=item
                )
                results.append(result)
            
            return results, response.status_code, None
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                return [], 429, str(e)
            return [], e.response.status_code if e.response else 500, str(e)
        except Exception as e:
            return [], None, str(e)


class DuckDuckGoProvider(SearchProvider):
    """DuckDuckGo HTML scraping provider (fallback only)"""
    
    def __init__(self, rate_limiter: RateLimiter):
        super().__init__('duckduckgo', rate_limiter)
        self.base_url = 'https://html.duckduckgo.com/html/'
    
    def search(self, query: str) -> tuple[List[Dict], Optional[int], Optional[str]]:
        """
        Search DuckDuckGo via HTML scraping
        """
        try:
            self.rate_limiter.wait_if_needed(self.name, self.rps)
            
            params = {'q': query}
            
            # Use headers to avoid getting blocked
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(self.base_url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # DuckDuckGo HTML structure (may change, so this is fragile)
            results = []
            for i, result in enumerate(soup.select('.result')):
                title_elem = result.select_one('.result__a')
                snippet_elem = result.select_one('.result__snippet')
                
                if not title_elem:
                    continue
                
                title = title_elem.get_text(strip=True)
                url = title_elem.get('href', '')
                snippet = snippet_elem.get_text(strip=True) if snippet_elem else ''
                
                score = 1.0 - (i * 0.15)
                result_dict = self._make_result(
                    title=title,
                    url=url,
                    snippet=snippet,  # Limit snippet length can be done here [:200]
                    relevance_score=max(0.1, score)
                )
                results.append(result_dict)
            
            if not results:
                pass
            return results, response.status_code, None
            
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                return [], 429, str(e)
            return [], e.response.status_code if e.response else 500, str(e)
        except Exception as e:
            return [], None, str(e)


class GoogleCSEProvider(SearchProvider):
    """Google Custom Search Engine provider (very rare, quota limited)"""
    
    def __init__(self, rate_limiter: RateLimiter, db=None):
        super().__init__('google_cse', rate_limiter)
        
        if not GOOGLE_CSE_API_KEY or not GOOGLE_CSE_ID:
            raise ValueError("Google CSE credentials not configured")
        
        self.api_key = GOOGLE_CSE_API_KEY
        self.cse_id = GOOGLE_CSE_ID
        self.base_url = 'https://www.googleapis.com/customsearch/v1'
        self.db = db  # Database connection for persistent quota tracking
    
    def _check_quota(self) -> bool:
        """
        Check if we have quota remaining (persistent via database)
        Counts Google CSE searches made today (UTC)
        """
        if not self.db:
            # Fallback: allow search if no DB
            return True
        
        try:
            # Use database method for consistent quota tracking
            count = self.db.get_provider_daily_usage('google_cse')
            return count < 100  # 100 queries/day free tier
        except Exception as e:
            print(f"Error checking CSE quota: {e}")
            # Allow search if quota check fails
            return True
    
    def search(self, query: str) -> tuple[List[Dict], Optional[int], Optional[str]]:
        """
        Search using Google Custom Search Engine
        
        Returns up to 10 results
        """
        try:
            if not self._check_quota():
                return [], 429, "Daily quota exceeded (100 queries)"
            
            self.rate_limiter.wait_if_needed(self.name, self.rps)
            
            params = {
                'key': self.api_key,
                'cx': self.cse_id,
                'q': query,
                'num': 10
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            items = data.get('items', [])
            
            results = []
            for i, item in enumerate(items):
                title = item.get('title', '')
                url = item.get('link', '')
                snippet = item.get('snippet', '')
                
                score = 1.0 - (i * 0.1)
                result = self._make_result(
                    title=title,
                    url=url,
                    snippet=snippet,
                    relevance_score=max(0.1, score)
                )
                results.append(result)
            
            return results, response.status_code, None
            
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                return [], 429, str(e)
            if e.response and e.response.status_code >= 500:
                return [], e.response.status_code, str(e)
            return [], e.response.status_code if e.response else 500, str(e)
        except Exception as e:
            return [], None, str(e)

