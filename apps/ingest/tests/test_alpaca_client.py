import asyncio
from apps.ingest.alpaca_client.client import fetch_news, stream_news, stream_news_iter

def test_fetch_news():
    news = fetch_news("AAPL", limit=3)
    assert isinstance(news, list)
    for item in news:
        print(item["headline"], item["created_at"])

def test_stream_news():
    # asyncio.run(stream_news(["AAPL", "MSFT", 'INTC', 'META'], max_messages=5, timeout_sec=20))
    asyncio.run(stream_news(["*"], max_messages=10, timeout_sec=180))

def test_stream_news_iter():
    async def consume():
        print("Running test_stream_news_iter:consume")
        async for item in stream_news_iter(["*"]):
            print(item["headline"])
            pass
    asyncio.run(consume())


def main():
    test_fetch_news()
    test_stream_news_iter()
    # test_stream_news()
    

if __name__ == "__main__":
    main()