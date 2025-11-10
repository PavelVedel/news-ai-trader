import os
import requests
import json
import asyncio
import websockets
from dotenv import load_dotenv
from typing import Iterable, Any
from libs.utils.logging_setup import get_logger
from typing import Optional

load_dotenv()  # загружает .env в os.environ
logger = get_logger("news.alpaca")

ALPACA_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET = os.getenv("ALPACA_API_SECRET")
# https://docs.alpaca.markets/docs/real-time-stock-pricing-data
REST_URL = "https://data.alpaca.markets/v1beta1/news"
WS_URL = "wss://stream.data.alpaca.markets/v1beta1/news"
MAX_NEWS_PER_REQUEST = 50

def fetch_all_in_interval(symbol:Optional[str]=None, start="2025-11-10T00:00:00Z", end=None):
    headers = {"Apca-Api-Key-Id": ALPACA_KEY, "Apca-Api-Secret-Key": ALPACA_SECRET}
    token = None
    items_all = []
    itteration = 0
    while True:
        itteration += 1
        params = {
            "symbols": symbol,
            "limit": MAX_NEWS_PER_REQUEST,               # max
            "start": start,
            "sort": "asc",
        }
        if end:
            params["end"] = end
        if token:
            params["page_token"] = token

        r = requests.get(REST_URL, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        payload = r.json()
        # print(payload)
        print(f"[{itteration}] Get {len(payload['news'])} news from {payload['news'][0]['created_at']} to {payload['news'][-1]['created_at']}")
        items = payload.get("news", [])
        items_all.extend(items)

        token = payload.get("next_page_token")
        if not token:
            break

    return items_all


def fetch_news(symbol: str = "AAPL", limit: int = MAX_NEWS_PER_REQUEST):
    """Простой REST-запрос новостей по тикеру. Symbol может быть None."""
    headers = {"Apca-Api-Key-Id": ALPACA_KEY, "Apca-Api-Secret-Key": ALPACA_SECRET}
    params = {"limit": limit}
    if symbol is not None:
        params["symbols"] = symbol
    r = requests.get(REST_URL, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    items = r.json().get("news", [])
    for it in items:
        # Запишем в лог (JSONL) уже нормализованное событие
        logger.debug("rest_news", extra={"payload": _normalize_news(it)})
    return items


def _normalize_news(n: dict[str, Any]) -> dict[str, Any]:
    # Простейшая нормализация к единому виду
    return {
        "id": n.get("id"),
        "headline": n.get("headline"),
        "summary": n.get("summary"),
        "symbols": n.get("symbols") or [],
        "source": n.get("source"),
        "created_at": n.get("created_at"),
        "updated_at": n.get("updated_at"),
        "url": n.get("url") or n.get("author_url"),
    }


def _parse_ws_payload(raw: str) -> list[dict[str, Any]]:
    data = json.loads(raw)
    if isinstance(data, list):
        return data
    return [data]


async def stream_news(symbols, max_messages: int | None = None, timeout_sec: float | None = None):
    """Стрим новостей. Если заданы max_messages/timeout_sec, завершится по достижении лимита."""
    async def _run():
        count = 0
        async with websockets.connect(WS_URL) as ws:
            await ws.send(json.dumps({"action":"auth","key":ALPACA_KEY,"secret":ALPACA_SECRET}))
            await ws.send(json.dumps({"action":"subscribe","news":list(symbols)}))
            while True:
                msg = await ws.recv()
                print("WS:", msg)
                if max_messages is not None:
                    count += 1
                    if count >= max_messages:
                        break

    if timeout_sec is None:
        # бесконечный цикл
        await _run()
    else:
        try:
            await asyncio.wait_for(_run(), timeout=timeout_sec)
        except asyncio.TimeoutError:
            # тихо выходим по таймауту
            pass


async def stream_news_iter(symbols: Iterable[str], normalize: bool = True):
    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({"action": "auth", "key": ALPACA_KEY, "secret": ALPACA_SECRET}))
        await ws.send(json.dumps({"action": "subscribe", "news": list(symbols)}))
        print(f"[stream_news_iter] Subscribed on: {list(symbols)}")
        while True:
            for ev in _parse_ws_payload(await ws.recv()):
                if isinstance(ev, dict) and not ev.get("headline") and not ev.get("id"):
                    logger.debug("ws_news_system", extra={"payload": ev})
                    continue
                item = _normalize_news(ev) if normalize else ev
                logger.info("ws_news", extra={"payload": item})  # <— лог в JSONL/консоль
                yield item


if __name__ == "__main__":
    print("Starting fetching news ... ")
    n = fetch_all_in_interval()

    n1 = fetch_news()
    pass