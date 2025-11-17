"""Short script to show 20 random news from news_raw table"""
from libs.database.connection import DatabaseConnection
import json

def main():
    db = DatabaseConnection("data/db/news.db")
    
    try:
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT headline, symbols_json 
                FROM news_raw 
                ORDER BY RANDOM() 
                LIMIT 100
            """)
            
            rows = cursor.fetchall()
            
            print(f"\n{'='*80}")
            print(f"20 Random News from news_raw")
            print(f"{'='*80}\n")
            
            for i, row in enumerate(rows, 1):
                headline = row['headline'] or "No headline"
                symbols_json = row['symbols_json'] or "[]"
                
                # Try to parse symbols_json if it's a string
                try:
                    if isinstance(symbols_json, str):
                        symbols = json.loads(symbols_json)
                    else:
                        symbols = symbols_json
                    symbols_str = ", ".join(symbols) if symbols else "No symbols"
                except:
                    symbols_str = str(symbols_json)
                
                print(f"{i:2}. {headline} - {symbols_str}")
            
            print(f"\n{'='*80}\n")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()





