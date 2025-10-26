"""Check quota usage for search providers"""

from libs.database.connection import DatabaseConnection

def main():
    db = DatabaseConnection()
    db.ensure_web_search_tables()
    
    providers = ['wikipedia', 'wikidata', 'duckduckgo', 'google_cse']
    
    print("Provider Daily Usage (UTC)")
    print("=" * 60)
    
    for provider in providers:
        usage = db.get_provider_daily_usage(provider)
        
        # Show quota limits
        if provider == 'google_cse':
            limit = 100
            print(f"{provider:15} {usage:4}/{limit:4} ({usage*100//limit}%)")
        else:
            print(f"{provider:15} {usage:4} searches (no limit)")
    
    print("=" * 60)

if __name__ == '__main__':
    main()

