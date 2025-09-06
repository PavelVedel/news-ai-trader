# ===============================
# File: apps/market_data/update_infos.py
# ===============================
"""
Скрипт для загрузки и обновления таблицы "infos" из Yahoo Finance (ticker.info)
— Проходит по всем символам из data/db/news.db (через libs/database/connection.py)
— Извлекает ключевые поля из ticker.info + сохраняет полный JSON в raw_info_json
— Пишет логи в infos_update.log

Запуск:  python -m apps.market_data.update_infos
"""

import json
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime

import yfinance as yf
from libs.database.connection import DatabaseConnection

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('infos_update.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class InfosUpdater:
    def __init__(self, db_path: str = "data/db/news.db"):
        self.db_path = db_path
        self.db = DatabaseConnection(db_path)

    # --------- публичные методы ---------
    def update_all_infos(
        self,
        delay_seconds: float = 0.0,
        max_symbols: Optional[int] = None,
        max_age_days: int = 30,
    ) -> None:
        """Обновить информацию info для символов, требующих обновления.

        Args:
            delay_seconds: Пауза между запросами к Yahoo Finance.
            max_symbols: Необязательный лимит на количество символов.
            max_age_days: Максимальный возраст данных в днях, старше — обновляем.
        """
        try:
            # Убеждаемся, что таблица infos существует
            if not self.db.ensure_infos_table():
                logger.error("Не удалось создать/проверить таблицу infos")
                return

            # Получаем символы из news_raw
            symbols = self.db.get_all_symbols()
            symbols = [self._normalize_symbol(s) for s in symbols]
            symbols = [s for s in symbols if s]
            symbols = sorted(set(symbols))

            if max_symbols is not None:
                symbols = symbols[:max_symbols]

            logger.info(f"Найдено {len(symbols)} символов для проверки/обновления")

            # Получаем актуальность уже сохранённых записей
            need_update = self.db.get_infos_symbols_needing_update(symbols, max_age_days=max_age_days)
            logger.info(f"К обновлению помечено: {len(need_update)} символов (age > {max_age_days} d)")

            ok, fail = 0, 0
            for i, symbol in enumerate(need_update, 1):
                try:
                    logger.info(f"[{i}/{len(need_update)}] Обрабатываю {symbol}")
                    payload = self._fetch_info(symbol)
                    if not payload:
                        fail += 1
                        logger.warning(f"Нет данных для {symbol}")
                    else:
                        saved = self.db.save_infos(payload)
                        if saved:
                            ok += 1
                            logger.info(f"[OK] {symbol} сохранён")
                        else:
                            fail += 1
                            logger.error(f"[ERROR] {symbol}: ошибка при сохранении")
                except Exception as e:
                    fail += 1
                    logger.exception(f"Критическая ошибка при обработке {symbol}: {e}")
                finally:
                    if delay_seconds > 0 and i < len(need_update):
                        time.sleep(delay_seconds)

            logger.info("=" * 60)
            logger.info("ОБНОВЛЕНИЕ infos ЗАВЕРШЕНО")
            logger.info(f"Успехов: {ok}; Ошибок: {fail}")

            stats = self.db.get_infos_stats()
            if stats:
                logger.info("СТАТИСТИКА infos:")
                logger.info(
                    f"Всего записей: {stats.get('total', 0)}, "
                    f"с sector: {stats.get('with_sector', 0)}, "
                    f"с industry: {stats.get('with_industry', 0)}, "
                    f"последнее обновление: {stats.get('last_update', 'N/A')}"
                )
        finally:
            self.db.close()

    # --------- внутренние методы ---------
    def _fetch_info(self, symbol: str) -> Optional[Dict]:
        """Получить и нормализовать ticker.info для сохранения в БД."""
        try:
            t = yf.Ticker(symbol)
            info: Dict = t.info or {}
            if not info:
                return None

            # Подготовка officer-списка (только name/title)
            officers = info.get("companyOfficers") or []
            officers_small = []
            for o in officers:
                if not isinstance(o, dict):
                    continue
                name = o.get("name")
                title = o.get("title")
                if name or title:
                    officers_small.append({"name": name, "title": title})

            payload = {
                # ключ
                "symbol": symbol,

                # имена/наименования
                "long_name": self._safe(info, "longName"),
                "short_name": self._safe(info, "shortName"),
                "display_name": self._safe(info, "displayName"),

                # сайт/IR/контакты
                "website": self._safe(info, "website"),
                "ir_website": self._safe(info, "irWebsite"),
                "phone": self._safe(info, "phone"),

                # адрес
                "address1": self._safe(info, "address1"),
                "city": self._safe(info, "city"),
                "state": self._safe(info, "state"),
                "zip": self._safe(info, "zip"),
                "country": self._safe(info, "country"),

                # отрасль/сектор
                "sector": self._safe(info, "sector"),
                "industry": self._safe(info, "industry"),

                # штат сотрудников и описание
                "full_time_employees": self._safe(info, "fullTimeEmployees"),
                "long_business_summary": self._safe(info, "longBusinessSummary"),

                # биржа/валюта (метаданные)
                "exchange": self._safe(info, "fullExchangeName") or self._safe(info, "exchange"),
                "currency": self._safe(info, "currency"),

                # офицеры
                "officers_json": json.dumps(officers_small, ensure_ascii=False),

                # сырой JSON для дальнейшего парсинга при необходимости
                "raw_info_json": json.dumps(info, ensure_ascii=False),

                # служебные поля
                "last_updated": datetime.now().isoformat(),
                "data_source": "yahoo_finance",
            }
            return payload
        except Exception:
            logger.exception(f"Ошибка получения info для {symbol}")
            return None

    @staticmethod
    def _normalize_symbol(s: str) -> Optional[str]:
        if not s:
            return None
        s = s.strip().upper().replace('$', '')
        # Фильтруем очевидный мусор
        if len(s) == 0 or len(s) > 20:
            return None
        return s

    @staticmethod
    def _safe(d: Dict, key: str, default=None):
        v = d.get(key, default)
        if isinstance(v, str) and v.upper() in {"N/A", "NAN", "INF", "-INF"}:
            return default
        return v


def main():
    print("=" * 60)
    print("ОБНОВЛЕНИЕ ТАБЛИЦЫ infos (Yahoo Finance: ticker.info)")
    print("=" * 60)
    updater = InfosUpdater()
    updater.update_all_infos(delay_seconds=0.0, max_symbols=None, max_age_days=30)


if __name__ == "__main__":
    main()