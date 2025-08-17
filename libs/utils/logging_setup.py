from __future__ import annotations
import json
import logging
import os
import pathlib
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler

# --- JSON-форматтер (1 строка = 1 событие) ---
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = getattr(record, "payload", None)
        data = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if payload is not None:
            data["payload"] = payload
        return json.dumps(data, ensure_ascii=False)

_configured = False  # чтобы не плодить хендлеры при повторных импортax

def _setup_root_logger() -> None:
    global _configured
    if _configured:
        return

    # Настройки из окружения (удобно менять без кода)
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file  = os.getenv("LOG_FILE", "logs/news_ws.jsonl")
    rotate_when = os.getenv("LOG_ROTATE_WHEN", "midnight")   # или 'D', 'H'
    rotate_backups = int(os.getenv("LOG_BACKUPS", "7"))

    root = logging.getLogger("news")  # корневой логгер проекта
    root.setLevel(getattr(logging, log_level, logging.INFO))

    # Консоль — человекочитаемый вывод
    ch = logging.StreamHandler()
    ch.setLevel(root.level)
    ch.setFormatter(logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    ))
    root.addHandler(ch)

    # Файл — JSONL с ротацией по времени
    path = pathlib.Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = TimedRotatingFileHandler(
        filename=str(path),
        when=rotate_when,
        backupCount=rotate_backups,
        encoding="utf-8",
        utc=True,
    )
    fh.setLevel(root.level)
    fh.setFormatter(JsonFormatter())
    root.addHandler(fh)

    _configured = True

def get_logger(name: str = "news") -> logging.Logger:
    """Вернуть логгер проекта. Хендлеры настраиваются один раз."""
    _setup_root_logger()
    return logging.getLogger(name)
