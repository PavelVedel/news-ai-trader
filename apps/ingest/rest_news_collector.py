from libs.database.connection import DatabaseConnection
from apps.ingest.alpaca_client.client import fetch_news
import time
from collections import deque

NEWS_LIMIT_PER_REQUEST = 50 # 50 is max
MAX_DONE = 1000

def rest_news_connector(max_done:int = MAX_DONE):
    db = DatabaseConnection("news.db")
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

        print(f"[{len(done)+1}/{max_done}] Working with {symbol} ... ")
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
    

if __name__ == "__main__":
    rest_news_connector()