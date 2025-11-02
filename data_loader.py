"""
data_loader.py

- Load the data using both pandas and polars.
- Parse into a time-indexed DataFrame with columns: timestamp, symbol, price.
- Demonstrate equivalent parsing logic in both libraries.
- Compare ingestion time and memory usage using profiling tools (will be done in reporting.py)
"""

import pandas as pd
import polars as pl
import time

# timestamp column given as YYYY-MM-DD HH:MM:SS format
FILEPATH = "market_data-1.csv"


def load_data_pandas(file_path=FILEPATH, **kwargs):

    df = pd.read_csv(
        file_path,
        parse_dates=["timestamp"],
        date_format="%Y-%m-%d %H:%M:%S",
        dtype={"symbol": "string", "price": "float64"},
        index_col="timestamp",
        **kwargs,
    )
    return df.sort_values(["timestamp"])


def load_data_polars(file_path=FILEPATH, **kwargs):

    # polars has no index concept, so we just parse the timestamp column
    df = pl.read_csv(
        file_path,
        schema={"timestamp": pl.Datetime("ms"), "symbol": pl.Utf8, "price": pl.Float64},
        try_parse_dates=True,
        **kwargs,
    )
    return df.sort(["timestamp"])


def load_data_to_dict(file_path=FILEPATH, loader="pandas", **kwargs):
    """Load data into a list of dictionaries using specified loader."""
    if loader == "pandas":
        df = load_data_pandas(file_path, **kwargs)
        data_dict = {symbol: group["price"] for symbol, group in df.groupby("symbol")}
    elif loader == "polars":
        df = load_data_polars(file_path, **kwargs)
        data_dict = {
            symbol: df.filter(pl.col("symbol") == symbol)[["timestamp", "price"]]
            for symbol in df["symbol"].unique()
        }
    else:
        raise ValueError("Unsupported loader specified. Use 'pandas' or 'polars'.")
    return data_dict


if __name__ == "__main__":

    import json

    # t1 = time.perf_counter()
    # df_pd = load_data_pandas()
    # t2 = time.perf_counter()
    # df_pl = load_data_polars()
    # t3 = time.perf_counter()
    # print(f"Pandas load time: {t2 - t1:.4f} seconds")
    # print(f"Polars load time: {t3 - t2:.4f} seconds")

    # print("\nPandas DataFrame:")
    # print(df_pd.head())
    # print("\nPolars DataFrame:")
    # print(df_pl.head())

    df = load_data_to_dict(file_path="market_data-2.csv", loader="pandas")
    print(df)
