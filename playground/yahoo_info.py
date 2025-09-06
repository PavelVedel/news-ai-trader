import yfinance as yf
ticker = yf.Ticker("AAPL")
print(ticker.info["longName"])       # Apple Inc.
pass