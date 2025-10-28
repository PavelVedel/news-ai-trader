"""Simple example of web search usage"""

import argparse
from libs.database.connection import DatabaseConnection
from apps.ingest.web_search.search_manager import WebSearchManager
from apps.ingest.web_search.providers import (
    WikipediaProvider,
    WikidataProvider,
    DuckDuckGoProvider,
    GoogleCSEProvider
)
from apps.ingest.web_search.rate_limiter import RateLimiter
from apps.ingest.web_search.normalizer import normalize_query

# Configuration: Set to None to test all providers, or specific: 'wikipedia', 'wikidata', 'duckduckgo', 'google_cse'
CHOSEN_PROVIDER = 'duckduckgo'  # Change to test specific provider
TEST_QUERY = "tim walz"  # Default query | Larry Ellison


def test_single_provider(provider_name: str, query: str, db=None):
    """Test a single provider directly"""
    print(f"\n{'=' * 60}")
    print(f"Testing {provider_name.upper()} provider")
    print(f"Query: {query}")
    print(f"{'=' * 60}\n")
    
    rate_limiter = RateLimiter()
    
    # Initialize the requested provider
    if provider_name == 'wikipedia':
        provider = WikipediaProvider(rate_limiter)
    elif provider_name == 'wikidata':
        provider = WikidataProvider(rate_limiter)
    elif provider_name == 'duckduckgo':
        provider = DuckDuckGoProvider(rate_limiter)
    elif provider_name == 'google_cse':
        if not db:
            print("ERROR: Google CSE requires database connection")
            return
        try:
            provider = GoogleCSEProvider(rate_limiter, db=db)
        except ValueError as e:
            print(f"ERROR: {e}")
            return
    else:
        print(f"ERROR: Unknown provider '{provider_name}'")
        print("Available providers: wikipedia, wikidata, duckduckgo, google_cse")
        return
    
    # Normalize query
    normalized = normalize_query(query)
    print(f"Normalized query: {normalized}")
    
    # Perform search
    try:
        results, http_code, error = provider.search(normalized)
        
        print(f"\nHTTP Code: {http_code}")
        print(f"Error: {error if error else 'None'}")
        print(f"Results found: {len(results)}")
        
        if results:
            print(f"\nTop results:")
            for i, result in enumerate(results, 1):
                print(f"\n  {i}. {result['title']}")
                print(f"     URL: {result['url']}")
                print(f"     Snippet: {result['snippet']}")
                print(f"     Relevance: {result['relevance_score']:.2f}")
        else:
            print("\nNo results found")
            
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(description='Test web search providers')
    parser.add_argument('--provider', type=str, default=CHOSEN_PROVIDER,
                       choices=['wikipedia', 'wikidata', 'duckduckgo', 'google_cse'],
                       help='Test specific provider (overrides CHOSEN_PROVIDER)')
    parser.add_argument('--query', type=str, default=TEST_QUERY,
                       help='Query to search')
    
    args = parser.parse_args()
    
    # Initialize database
    db = DatabaseConnection()
    db.ensure_web_search_tables()
    
    # Test specific provider or all
    if args.provider:
        test_single_provider(args.provider, args.query, db)
    elif CHOSEN_PROVIDER:
        test_single_provider(CHOSEN_PROVIDER, args.query, db)
    else:
        # Test all providers
        manager = WebSearchManager(db)
        
        test_queries = [
            "Larry Ellison",
            "Apple Inc",
            "Tesla Motors",
        ]
        
        print("Testing Web Search Manager (All Providers)\n" + "=" * 60)
        
        for query in test_queries:
            print(f"\nQuery: {query}")
            result = manager.search(query, force_refresh=True)
            
            print(f"  Status: {result['status']}")
            print(f"  Provider: {result['provider']}")
            print(f"  Results found: {len(result['results'])}")
            print(f"  From cache: {result['cached']}")
            
            if result['results']:
                first = result['results'][0]
                print(f"  Top result: {first['title']}")
                print(f"  URL: {first['url']}")
                print(f"  Snippet: {first['snippet'][:150]}...")


if __name__ == '__main__':
    main()

