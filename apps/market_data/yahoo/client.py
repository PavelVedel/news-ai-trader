"""
Клиент для работы с Yahoo Finance API
"""

import yfinance as yf
import pandas as pd
from typing import Optional, List
from datetime import datetime, timedelta

class YahooFinanceClient:
    def __init__(self):
        pass

    def get_1m_candles(self, symbol: str, period: str = "7d") -> pd.DataFrame:
        # 1) Жёсткий clamp периода
        period = "7d"
        ticker = yf.Ticker(symbol)
        df = ticker.history(interval="1m", period=period, auto_adjust=False, actions=False)
        if df.empty:
            return pd.DataFrame(columns=["open","high","low","close","volume"])
        df = df.rename(columns=str.lower)[["open","high","low","close","volume"]]
        if df.index.tz is not None:
            df.index = df.index.tz_convert("UTC").tz_localize(None)
        df = df[~df.index.duplicated(keep="last")].sort_index()
        # 2) Фиксация dtypes
        return df.astype({
            "open":"float64","high":"float64","low":"float64","close":"float64","volume":"float64"
        })

    def get_daily_candles(self, symbol: str, period: str = "max") -> pd.DataFrame:
        ticker = yf.Ticker(symbol)
        df = ticker.history(interval="1d", period=period, auto_adjust=False, actions=True)
        if df.empty:
            return pd.DataFrame(columns=["open","high","low","close","volume","dividends","stock splits"])
        df = df.rename(columns=str.lower)
        if df.index.tz is not None:
            df.index = df.index.tz_convert("UTC").tz_localize(None)
        cols = ["open","high","low","close","volume","dividends","stock splits"]
        for c in cols:
            if c not in df.columns:  # на всякий случай, если actions=False/пусто
                df[c] = 0.0
        return df[cols].sort_index().astype({
            "open":"float64","high":"float64","low":"float64","close":"float64",
            "volume":"float64","dividends":"float64","stock splits":"float64"
        })

    def get_fundamentals(self, symbol: str) -> dict:
        t = yf.Ticker(symbol)
        out = {
            "financials": t.financials,
            "quarterly_financials": t.quarterly_financials,
            "balance_sheet": t.balance_sheet,
            "quarterly_balance_sheet": t.quarterly_balance_sheet,
            "cashflow": t.cashflow,
            "quarterly_cashflow": t.quarterly_cashflow,
            "info": pd.DataFrame([t.info]) if getattr(t, "info", None) else pd.DataFrame()
        }
        for k,v in out.items():
            if isinstance(v, pd.DataFrame):
                v.index = v.index.astype(str); v.columns = v.columns.astype(str)
        return out

