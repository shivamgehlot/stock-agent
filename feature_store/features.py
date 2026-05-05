#feature.py
import pandas as pd
import numpy as np
import os
from glob import glob

RAW_PATH = "data/raw/*.parquet"
OUTPUT_PATH = "data/processed/features.parquet"


def compute_rsi(series, window=14):
    delta = series.diff()

    gain = (delta.where(delta > 0, 0)).rolling(window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window).mean()

    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def compute_features(df):
    df = df.sort_values("Date")

    # RETURNS
    df["return_1d"] = df["Close"].pct_change(1)
    df["return_5d"] = df["Close"].pct_change(5)
    df["return_20d"] = df["Close"].pct_change(20)

    # MOVING AVERAGES
    df["ma20"] = df["Close"].rolling(20).mean()
    df["ma50"] = df["Close"].rolling(50).mean()
    df["ma_ratio"] = df["ma20"] / df["ma50"]

    # VOLATILITY
    df["volatility_20d"] = df["return_1d"].rolling(20).std()

    # RSI
    df["rsi_14"] = compute_rsi(df["Close"], 14)

    # BOLLINGER BAND WIDTH
    rolling_std = df["Close"].rolling(20).std()
    upper = df["ma20"] + 2 * rolling_std
    lower = df["ma20"] - 2 * rolling_std
    df["bb_width"] = (upper - lower) / df["ma20"]

    # VOLUME RATIO
    df["volume_ma20"] = df["Volume"].rolling(20).mean()
    df["volume_ratio"] = df["Volume"] / df["volume_ma20"]

    # DROP NA (important)
    df = df.dropna()

    return df


def run_feature_pipeline():
    files = glob(RAW_PATH)

    all_data = []

    for file in files:
        df = pd.read_parquet(file)

        df = compute_features(df)

        df.drop(columns=["Date"], inplace=True)

        all_data.append(df)

    final_df = pd.concat(all_data)
    final_df["created_timestamp"] = pd.Timestamp.now(tz="UTC")

    os.makedirs("data/processed", exist_ok=True)
    final_df.to_parquet(OUTPUT_PATH, index=False)

    print("Feature pipeline completed!")


if __name__ == "__main__":
    run_feature_pipeline()