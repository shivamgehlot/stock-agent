import yfinance as yf
import datetime
import os
import json
import pandas as pd
import requests
from io import StringIO
import time

# ticker = yf.Ticker("AAPL")
# data = ticker.history(period="1mo")
# print(data)

# 1. Get S&P 500 tickers
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers)

    tables = pd.read_html(StringIO(response.text))
    df = tables[0]

    tickers = df["Symbol"].tolist()

    # Fix tickers with dots (e.g., BRK.B -> BRK-B)
    tickers = [ticker.replace(".", "-") for ticker in tickers]

    return tickers

# 2. Fetch stock data
def fetch_stock_data(ticker, max_retries=3, delay=2):
    for attempt in range(max_retries):
        try:

            df = yf.download(ticker, period="5y", interval="1d", auto_adjust=True, progress=False)

            if df.empty:
                print(f"No data for {ticker}")
                return None
            
            df.reset_index(inplace=True)

            # flatten MultiIndex columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] for col in df.columns]

            # # use adjusted close if available
            # if "Adj Close" in df.columns:
            #     df["Close"] = df["Adj Close"]

            df["ticker"] = ticker

            return df

        except Exception as e:
            print(f"Error fetching data for {ticker} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                return None

def fetch_macro_data():
    macro_tickers = {
        "vix": "^VIX",
        "tnx": "^TNX",
        "xlf": "XLF",
        "xlk": "XLK",
        "xle": "XLE",
    }

    macro_df = None

    for name, ticker in macro_tickers.items():
        df = yf.download(ticker, period="5y", interval="1d", progress=False)

        if df.empty:
            print(f"No data for {ticker}")
            continue

        df.reset_index(inplace=True)

        # flatten MultiIndex columns
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        df = df[["Date", "Close"]].copy()
        df.rename(columns={"Close": name}, inplace=True)

        if macro_df is None:
            macro_df = df
        else:
            macro_df = macro_df.merge(df, on="Date", how="outer")

    #normalize data column
    if "Date" not in macro_df.columns:
        macro_df.rename(columns={"index": "Date"}, inplace=True)

    return macro_df

def has_missing_trading_days(df):
    if "Date" not in df.columns:
        return True
    # ensure Date is datetime
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.sort_values("Date")

    # expected business days
    expected = pd.date_range(start=df["Date"].min(), end=df["Date"].max(), freq="B")

    actual = pd.to_datetime(df["Date"])

    missing = set(expected.date) - set(actual.dt.date)

    # allow small gaps (holidays)
    if len(missing) > 50:
        return True

    return False

# 3. Validation
def validate_data(df):
    if df is None or df.empty:
        return False, "DataFrame is empty or None"
    
    if df["Date"].isnull().any():
        return False, "Missing dates in data"
    
    if df.duplicated(subset=["Date"]).any():
        return False, "Duplicate dates"
    
    if len(df) < 500:
        return False, "insufficient_data"
    
     # missing trading days
    if has_missing_trading_days(df):
        return False, "missing_trading_days"
    
    # macro validation
    macro_cols = ["vix", "tnx", "xlf", "xlk", "xle"]

    # check columns exist
    if not all(col in df.columns for col in macro_cols):
        return False, "macro_columns_missing"

    if df[macro_cols].isnull().any().any():
        return False, "missing_macro_data"
    
    return True, "ok"

# 4. Save as parquet
def save_data(df, ticker):
    os.makedirs("data/raw", exist_ok=True)
    file_path = f"data/raw/{ticker}.parquet"
    df.to_parquet(file_path, index=False)

# 5. Main pipeline
def run_ingestion_pipeline(limit=None):
    tickers = get_sp500_tickers()

    macro_df = fetch_macro_data()
    if macro_df is None or macro_df.empty:
        raise Exception("Macro data is empty or missing")
    macro_df["Date"] = pd.to_datetime(macro_df["Date"])

    # for testing
    if limit:
        tickers = tickers[:limit]

    success = 0
    failed = []

    total_rows = 0

    for ticker in tickers:
        try:
            print(f"Processing {ticker}...")

            df = fetch_stock_data(ticker)
            if df is None:
                failed.append({"ticker": ticker, "reason": "no_data"})
                continue
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.merge(macro_df, on="Date", how="left")
            df.ffill(inplace=True)
            df.bfill(inplace=True)

            valid, reason = validate_data(df)

            if not valid:
                failed.append({"ticker": ticker, "reason": reason})
                continue


            df["event_timestamp"] = pd.to_datetime(df["Date"], utc=True)
            save_data(df, ticker)

            success += 1
            total_rows += len(df)


        except Exception as e:
            failed.append({"ticker": ticker, "reason": str(e)})

    # 6. Logging

    log = {
        "timestamp": datetime.datetime.now().isoformat(),
        "total_tickers": len(tickers),
        "success" : success,
        "failed": failed,
        "total_rows": total_rows,
    }

    os.makedirs("logs", exist_ok=True)

    log_filename = f"logs/ingestion_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    with open(log_filename, "w") as f:
        json.dump(log, f, indent=2)

    print("Ingestion completed!")
    print(log)

    return log


# Entry point
if __name__ == "__main__":
    run_ingestion_pipeline()