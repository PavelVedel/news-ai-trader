#!/usr/bin/env python3
from __future__ import annotations

import os
import asyncio
import signal
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from libs.utils.logging_setup import get_logger  # ← ваш JSON/консоль логгер
from libs.database.connection import DatabaseConnection
from apps.ingest.alpaca_client.client import fetch_news, stream_news_iter
# fetch_news: sync (requests), symbol: str | None, limit <= 50
# stream_news_iter: async generator; отдаёт события (желательно нормализованные)

# ---------- КОНСТАНТЫ / НАСТРОЙКИ ----------
REST_LIMIT = 50                    # максимум у Alpaca за один REST-запрос
WORKERS = 4                        # количество параллельных REST-воркеров
SYMBOL_TTL_SEC = 300               # не опрашивать один и тот же тикер чаще (сек)
MAX_PENDING = 2000                 # предел очереди тикеров на опрос
WS_SUBSCRIPTION = ["*"]            # все новости (или, например, ["AAPL","MSFT"])
REST_WINDOW_HOURS = 6              # окно истории при доборе REST (если добавите start/end)
MAX_NEW_PER_NEWSBATCH = 10         # ограничить рост очереди на одну новость

# ---------- ВСПОМОГАТЕЛЬНОЕ ----------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def recent_window_iso(hours: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

# ---------- ОСНОВНОЙ КЛАСС ----------
class NewsDaemon:
    def __init__(self, db_path: str = "news.db"):
        # все логи этого инстанса — в канале news.daemon (у вас настроен корень "news")
        self.log = get_logger("news.daemon")
        self.db = DatabaseConnection(db_path)
        self.stop = asyncio.Event()

        # Очередь тикеров для REST-добора
        self.pending: asyncio.Queue[str] = asyncio.Queue(maxsize=MAX_PENDING)
        # последняя попытка опроса по символу (для throttle)
        self.last_fetch_ts: dict[str, float] = {}
        # набор успешно обработанных символов
        self.done_symbols: set[str] = set()

    # ---- сигналы ОС ----
    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stop.set)

    # ---- WebSocket-потребитель ----
    async def ws_consumer(self) -> None:
        self.log.info(
            "WS consumer starting",
            extra={"payload": {"subscription": WS_SUBSCRIPTION}}
        )

        # ваш генератор уже делает auth+subscribe и отдаёт события
        async for item in stream_news_iter(WS_SUBSCRIPTION, normalize=True):
            if self.stop.is_set():
                break

            # 1) Пишем саму новость в БД
            try:
                self.db.add_raw_news_batch([item])
            except Exception as e:
                self.log.error("DB insert failed for WS item",
                               extra={"payload": {"error": str(e), "item": item}})
                continue

            # 2) Извлекаем символы и пополняем очередь (с ограничителем)
            symbols = item.get("symbols") or []
            if not symbols:
                continue

            added = 0
            for sym in symbols:
                if await self._enqueue_symbol(sym):
                    added += 1
                if added >= MAX_NEW_PER_NEWSBATCH:
                    break

    async def _enqueue_symbol(self, symbol: str) -> bool:
        """Добавить символ в очередь с учётом throttle и ёмкости очереди."""
        sym = (symbol or "").strip().upper()
        if not sym or not sym.isalnum():
            return False

        now = time.time()
        last = self.last_fetch_ts.get(sym, 0.0)
        if (now - last) < SYMBOL_TTL_SEC:
            return False  # слишком часто

        if self.pending.full():
            # debug, чтобы не засорять JSONL
            self.log.debug("Pending is full; dropping candidate",
                           extra={"payload": {"symbol": sym}})
            return False

        try:
            await self.pending.put(sym)
            self.last_fetch_ts[sym] = now
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log.error("Failed to enqueue",
                           extra={"payload": {"symbol": sym, "error": str(e)}})
            return False

    # ---- REST-воркеры ----
    async def rest_worker(self, idx: int) -> None:
        """
        Берёт символы из очереди и делает REST fetch (до 50).
        fetch_news синхронный → заворачиваем в to_thread, чтобы не блокировать loop.
        """
        self.log.info("REST worker started", extra={"payload": {"worker": idx}})
        while not self.stop.is_set():
            try:
                symbol = await asyncio.wait_for(self.pending.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            # если добавите поддержку start/end в fetch_news — передавайте start_iso
            start_iso = recent_window_iso(REST_WINDOW_HOURS)

            def _do_fetch() -> list[dict[str, Any]]:
                return fetch_news(symbol=symbol, limit=REST_LIMIT)

            try:
                news_list = await asyncio.to_thread(_do_fetch)
                count = len(news_list) if news_list else 0

                if count:
                    try:
                        self.db.add_raw_news_batch(news_list)
                    except Exception as e:
                        self.log.error("DB batch insert failed",
                                       extra={"payload": {"symbol": symbol, "error": str(e)}})
                    else:
                        self.done_symbols.add(symbol)
                        self.log.info("REST fetched",
                                      extra={"payload": {
                                          "worker": idx, "symbol": symbol,
                                          "items": count, "done_total": len(self.done_symbols),
                                          "start_iso": start_iso
                                      }})
                else:
                    self.log.warning("REST empty",
                                     extra={"payload": {"worker": idx, "symbol": symbol}})

            except Exception as e:
                self.log.error("REST fetch failed",
                               extra={"payload": {"worker": idx, "symbol": symbol, "error": str(e)}})
            finally:
                self.pending.task_done()

    # ---- точка входа ----
    async def run(self) -> None:
        self.install_signal_handlers()
        self.db.create_database()

        # 1) Сид-лист из последних новостей без символа
        try:
            seed = fetch_news(symbol=None, limit=REST_LIMIT)  # sync
            if seed:
                self.db.add_raw_news_batch(seed)
            seed_syms: set[str] = set()
            for n in seed or ():
                for s in (n.get("symbols") or []):
                    if await self._enqueue_symbol(s):
                        seed_syms.add(s)
            self.log.info("Seeded symbols from latest news",
                          extra={"payload": {"count": len(seed_syms), "sample": sorted(list(seed_syms))[:10]}})
        except Exception as e:
            self.log.error("Initial seeding failed", extra={"payload": {"error": str(e)}})

        # 2) Поднимаем задачи: один WS-потребитель и пул REST-воркеров
        tasks: list[asyncio.Task[Any]] = []
        tasks.append(asyncio.create_task(self.ws_consumer(), name="ws_consumer"))
        for i in range(WORKERS):
            tasks.append(asyncio.create_task(self.rest_worker(i + 1), name=f"rest_worker_{i+1}"))

        # 3) Ожидаем сигнал остановки
        await self.stop.wait()
        self.log.info("Stopping...", extra={"payload": {
            "done_symbols": len(self.done_symbols),
            "pending_size": self.pending.qsize()
        }})

        # 4) Мягкая остановка
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        self.db.close()
        self.log.info("Stopped cleanly",
                      extra={"payload": {"done_symbols": len(self.done_symbols), "pending": self.pending.qsize()}})

# --- main ---
def main() -> None:
    # ваш get_logger сам конфигурирует корневой "news" только один раз
    _ = get_logger("news.bootstrap").info("Bootstrapping daemon")
    db_path = os.getenv("NEWS_DB", "news.db")
    daemon = NewsDaemon(db_path=db_path)
    asyncio.run(daemon.run())

if __name__ == "__main__":
    main()
