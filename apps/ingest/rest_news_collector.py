from libs.database.connection import DatabaseConnection
from apps.ingest.alpaca_client.client import fetch_news, fetch_all_in_interval
from collections import deque
import time
import argparse


NEWS_LIMIT_PER_REQUEST = 50 # 50 is max
MAX_DONE = 500 # number of symbols for which reqest were performed

def requrent_rest_news_connector(max_done:int = MAX_DONE):
    db = DatabaseConnection("data/db/news.db")
    db.create_database()

    # Get initial news
    news_list = fetch_news(symbol=None, limit=NEWS_LIMIT_PER_REQUEST)
    db.add_raw_news_batch(news_list)
    initial_symbols: set[str] = set()
    for news in news_list:
        initial_symbols.update(set(news['symbols']))

    pending = deque(list(initial_symbols))  # initial tickers
    seen: set[str] = set()     # all ever met
    done: set[str] = set()     # successfully processed

    while pending and len(done) < max_done:
        symbol = pending.popleft()
        if symbol in done:
            continue
        seen.add(symbol)

        print(f"[{len(done)+1}/{max_done}|{(len(done)+1)/max_done*100:.2f}%] Working with {symbol} ... ")
        news_list = fetch_news(symbol, NEWS_LIMIT_PER_REQUEST)
        if not news_list:
            print(f"Something is wrong with fetching news for {symbol}. Skipping ...")
            continue

        db.add_raw_news_batch(news_list)
        done.add(symbol)

        # Extract and put new symbols in queue
        extracted = set()
        for news in news_list:
            extracted.update(news.get("symbols", []))

        for s in extracted:
            if s not in seen and s not in done:
                pending.append(s)

    # Get status
    print(f"Pending symbols: {pending}")
    print(f"Seen symbols: {seen}")
    print(f"Done symbols: {done}")
    print(f"len(done) = {len(done)} where max_done = {max_done}")
    # requested_symbols = {s: (s in done) for s in seen}
    
def update_news_for_all_symbols():
    db = DatabaseConnection("data/db/news.db")
    db.create_database()

    # Counters
    total_amount_of_fetched_news = 0
    total_amount_of_new_fetched_news = 0

    # Get initial news
    news_list = fetch_news(symbol=None, limit=NEWS_LIMIT_PER_REQUEST)
    added_news = db.add_raw_news_batch(news_list, verbose=False)
    
    # Add counters
    total_amount_of_fetched_news += len(news_list)
    total_amount_of_new_fetched_news += len(added_news)

    # Get set of all avaliable symbols
    all_symbols = db.get_all_symbols()

    tic = time.time()
    # Fetch news for all symbols
    for i_symbol, symbol in enumerate(all_symbols, start=1):
        # Get news
        news_list = fetch_news(symbol=symbol, limit=NEWS_LIMIT_PER_REQUEST)
        added_news = db.add_raw_news_batch(news_list, verbose=False)

        # Add counters
        total_amount_of_fetched_news += len(news_list)
        total_amount_of_new_fetched_news += len(added_news)

        # Time calc
        toc = time.time()
        approx_time_left = (toc-tic)/i_symbol*(len(all_symbols)-i_symbol)
        approx_time_left_str = f"{int(approx_time_left // 60):02d}:{int(approx_time_left % 60):02d}"

        # Print logs
        print(f"[{i_symbol}/{len(all_symbols)}:{i_symbol/len(all_symbols)*100:.2f}%] {symbol}: \tfetched {len(news_list)};\t new news {len(added_news)};\t time left {approx_time_left_str}.")


def download_the_latest_missed_news(symbol: str = None, from_day: str = None):
    """
    Finds the latest news in the database and requests news updates
    from that news moment to the current time.
    
    Args:
        symbol: Optional symbol to filter news. If None,
                all news are requested.
        from_day: Optional date string in format "YYYY-MM-DD" (e.g., "2025-08-15").
                  If provided, downloads news from this date. If None, uses the latest
                  news date from the database.
    """
    db = DatabaseConnection("data/db/news.db")
    db.create_database()
    
    # If from_day is specified, use it as start_date
    if from_day:
        from datetime import datetime, timezone
        try:
            # Parse the date string and convert to ISO format with 'Z'
            date_obj = datetime.strptime(from_day, "%Y-%m-%d")
            start_date = date_obj.replace(tzinfo=timezone.utc).isoformat().replace('+00:00', 'Z')
            print(f"Using specified start date: {start_date}")
        except ValueError as e:
            print(f"Error parsing from_day '{from_day}': {e}. Expected format: YYYY-MM-DD")
            return
    else:
        # Find the latest news in the database
        try:
            with db.get_cursor() as cursor:
                cursor.execute("""
                    SELECT created_at_utc FROM news_raw 
                    ORDER BY created_at_utc DESC 
                    LIMIT 1
                """)
                row = cursor.fetchone()
                
                if not row:
                    print("No news in the database. Starting from current date.")
                    # If database is empty, use current date minus 1 day as starting point
                    from datetime import datetime, timezone, timedelta
                    start_date = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat().replace('+00:00', 'Z')
                else:
                    start_date = row['created_at_utc']
                    # Ensure date format is correct (should end with 'Z')
                    if not start_date.endswith('Z') and '+' not in start_date:
                        start_date = start_date + 'Z'
                    print(f"Found latest news in database: {start_date}")
        except Exception as e:
            print(f"Error while searching for latest news: {e}")
            return
    
    # Request news from the latest news moment to current time
    print(f"Requesting news from {start_date} to current time...")
    try:
        news_list = fetch_all_in_interval(symbol=symbol, start=start_date, end=None)
        print(f"Received {len(news_list)} news items")
        
        # Save news to database
        if news_list:
            added_news = db.add_raw_news_batch(news_list, verbose=False)
            print(f"Added {len(added_news)} new news items out of {len(news_list)} received")
        else:
            print("No new news found")
    except Exception as e:
        print(f"Error while requesting news: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collect news from Alpaca API",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-m", "--mode",
        choices=["update_all", "recurrent", "download_latest"],
        default="recurrent",
        help="Mode: 'update_all' - update all symbols, 'recurrent' - recurrently update news for the latest symbols, 'download_latest' - download missed news since latest in DB (default: recurrent)"
    )
    parser.add_argument(
        "-md", "--max-done",
        type=int,
        default=MAX_DONE,
        help=f"Maximum number of symbols to process in recurrent mode (default: {MAX_DONE})"
    )
    parser.add_argument(
        "-l", "--limit",
        type=int,
        default=NEWS_LIMIT_PER_REQUEST,
        help=f"News limit per request (default: {NEWS_LIMIT_PER_REQUEST})"
    )
    parser.add_argument(
        "-s", "--symbol",
        type=str,
        default=None,
        help="Symbol to filter news (only for download_latest mode)"
    )
    parser.add_argument(
        "-d", "--from-day",
        type=str,
        default=None,
        help="Start date for downloading news in format YYYY-MM-DD (e.g., 2025-08-15). Only for download_latest mode. If not specified, uses latest news date from database."
    )
    args = parser.parse_args()

    if args.mode == "update_all":
        update_news_for_all_symbols()
    elif args.mode == "recurrent":
        requrent_rest_news_connector(max_done=args.max_done)
    elif args.mode == "download_latest":
        download_the_latest_missed_news(symbol=args.symbol, from_day=args.from_day)