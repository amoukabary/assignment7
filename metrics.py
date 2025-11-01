from data_loader import load_data_pandas, load_data_polars
import pandas as pd
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import time
import time
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt


def compute_pandas_metrics(df_pandas: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    """Compute 20-period rolling mean, std, and Sharpe ratio in Pandas."""
    start = time.time()

    df_pandas = df_pandas.sort_values(["symbol", "timestamp"])

    df_pandas["ma20"] = df_pandas.groupby("symbol")["price"].transform(
        lambda x: x.rolling(20, min_periods=1).mean()
    )
    df_pandas["std20"] = df_pandas.groupby("symbol")["price"].transform(
        lambda x: x.rolling(20, min_periods=1).std()
    )
    df_pandas["sharpe20"] = df_pandas["ma20"] / df_pandas["std20"]

    elapsed = time.time() - start
    print(f"Pandas rolling metrics time: {elapsed:.4f} sec")

    df_pandas = df_pandas.reset_index()  # ensure 'timestamp' is a column
    return df_pandas, elapsed


def compute_polars_metrics(df_polars: pl.DataFrame) -> tuple[pl.DataFrame, float]:
    """Compute 20-period rolling mean, std, and Sharpe ratio in Polars."""
    start = time.time()

    df_polars = (
        df_polars.sort(["symbol", "timestamp"])
        .group_by("symbol", maintain_order=True)
        .agg(
            [
                pl.col("timestamp"),
                pl.col("price"),
                pl.col("price").rolling_mean(window_size=20).alias("ma20"),
                pl.col("price").rolling_std(window_size=20).alias("std20"),
            ]
        )
        .explode(["timestamp", "price", "ma20", "std20"])
        .with_columns((pl.col("ma20") / pl.col("std20")).alias("sharpe20"))
    )

    elapsed = time.time() - start
    print(f"Polars rolling metrics time: {elapsed:.4f} sec")

    return df_polars, elapsed


def plot_results(
    df_pandas: pd.DataFrame, df_polars: pl.DataFrame, symbol: str = "AAPL"
):
    """Plot 20-period moving averages for a given symbol."""
    df_pandas_symbol = df_pandas[df_pandas["symbol"] == symbol]
    df_polars_symbol = df_polars.filter(pl.col("symbol") == symbol).to_pandas()

    plt.figure(figsize=(12, 6))
    plt.plot(
        df_pandas_symbol["timestamp"],
        df_pandas_symbol["price"],
        label="Price",
        alpha=0.4,
    )
    plt.plot(
        df_pandas_symbol["timestamp"],
        df_pandas_symbol["ma20"],
        label="MA20 (Pandas)",
        linewidth=1.8,
    )
    plt.plot(
        df_polars_symbol["timestamp"],
        df_polars_symbol["ma20"],
        "--",
        label="MA20 (Polars)",
        linewidth=1.8,
    )
    plt.title(f"{symbol}: 20-Period Rolling Metrics Comparison")
    plt.xlabel("Timestamp")
    plt.ylabel("Price / Moving Avg")
    plt.legend()
    plt.show()


if __name__ == "__main__":
    df_pandas = load_data_pandas()
    df_polars = load_data_polars()

    df_pandas, pandas_time = compute_pandas_metrics(df_pandas)
    df_polars, polars_time = compute_polars_metrics(df_polars)

    plot_results(df_pandas, df_polars, symbol="AAPL")

    print("\nPerformance Summary:")
    print(f"Pandas Time: {pandas_time:.4f} sec")
    print(f"Polars Time: {polars_time:.4f} sec")
