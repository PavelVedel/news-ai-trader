"""
Менеджер для хранения рыночных данных в файловой системе
"""

import os
from pathlib import Path
import pandas as pd
from typing import Dict, Optional
from datetime import datetime
import pyarrow


class MarketDataStorage:
    def __init__(self, base_path: str = "data_yahoo", freq: str = "1m", client=None):
        self.base_path = Path(base_path)
        self.set_frequency(freq)
        self.client = client  # DI: передавайте YahooFinanceClient() снаружи

    def set_frequency(self, freq: str):
        allowed = {"1m","2m","5m","15m","30m","60m","90m","1h","1d"}
        if freq not in allowed:
            raise ValueError(f"Unsupported freq {freq}")
        self.freq = freq
        (self.base_path / self.freq).mkdir(parents=True, exist_ok=True)

    def get_day_path(self, symbol: str, day: datetime) -> Path:
        """Путь к файлу с минутками за конкретный календарный день (UTC)."""
        ddir = self.base_path / self.freq / symbol.upper()
        ddir.mkdir(parents=True, exist_ok=True)
        return ddir / f"{pd.Timestamp(day).date()}.parquet"

    def split_by_calendar_day(self, df: pd.DataFrame) -> Dict[pd.Timestamp, pd.DataFrame]:
        """Разрезает DataFrame по календарным датам индекса (UTC)."""
        out: Dict[pd.Timestamp, pd.DataFrame] = {}
        if df.empty:
            return out
        # гарантируем DatetimeIndex без tz
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        if df.index.tz is not None:
            df.index = df.index.tz_convert("UTC").tz_localize(None)
        for day, chunk in df.groupby(df.index.normalize()):
            out[pd.Timestamp(day)] = chunk
        return out

    def _safe_write_parquet(self, df: pd.DataFrame, path: Path):
        tmp = path.with_suffix(".parquet.tmp")
        df.to_parquet(tmp, index=True, compression="zstd")  # или snappy по умолчанию
        os.replace(tmp, path)  # атомарная замена (Windows/Linux/Mac)

    def merge_save_day(self, symbol: str, day: datetime, chunk: pd.DataFrame):
        p = self.get_day_path(symbol, day)
        # нормализуем схему/колонки
        expected = ["open","high","low","close","volume"]
        chunk = chunk.reindex(columns=expected)
        if p.exists():
            old = pd.read_parquet(p)
            if not isinstance(old.index, pd.DatetimeIndex):
                old.index = pd.to_datetime(old.index)
            old = old.reindex(columns=expected)
            merged = pd.concat([old, chunk], axis=0)
            merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        else:
            merged = chunk.sort_index()
        self._safe_write_parquet(merged, p)

    def update_symbol_1m(self, symbol: str):
        if self.client is None:
            raise RuntimeError("YahooFinanceClient not provided")
        df = self.client.get_1m_candles(symbol, period="7d")
        if df.empty:
            print(f"Нет данных для {symbol}")
            return
        for day, chunk in self.split_by_calendar_day(df).items():
            self.merge_save_day(symbol, day, chunk)
        print(f"Обновлено {len(self.split_by_calendar_day(df))} дней для {symbol}")

    def get_stored_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        frames = []
        current = start.normalize()
        while current <= end.normalize():
            p = self.get_day_path(symbol, current)
            if p.exists():
                frames.append(pd.read_parquet(p))
            current += pd.Timedelta(days=1)
        if not frames:
            return pd.DataFrame(columns=["open","high","low","close","volume"])
        df = pd.concat(frames).sort_index()
        # фильтр по времени и финальная дедупликация
        df = df.loc[(df.index >= start) & (df.index <= end)]
        return df[~df.index.duplicated(keep="last")]

    def save_fundamentals(self, symbol: str, fund: dict):
        fdir = self.base_path / "fundamentals" / symbol.upper()
        fdir.mkdir(parents=True, exist_ok=True)
        for name, obj in fund.items():
            if isinstance(obj, pd.DataFrame):
                self._safe_write_parquet(obj, fdir / f"{name}.parquet")
            elif isinstance(obj, dict):
                pd.DataFrame([obj]).to_parquet(fdir / f"{name}.parquet", index=False, compression="zstd")


