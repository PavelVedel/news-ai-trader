"""Simple example of web search usage"""

from libs.database.connection import DatabaseConnection
from apps.ingest.web_search.search_manager import WebSearchManager

def main():
    # Initialize database
    db = DatabaseConnection()
    db.ensure_web_search_tables()
    
    # Create search manager
    manager = WebSearchManager(db)
    
    # Test searches
    test_queries = [
        "Larry Ellison",
        "Apple Inc",
        "Tesla Motors",
    ]
    
    print("Testing Web Search Manager\n" + "=" * 60)
    
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
            print(f"  Snippet: {first['snippet'][:100]}...")


if __name__ == '__main__':
    main()

