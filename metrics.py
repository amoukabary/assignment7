from data_loader import load_data_pandas, load_data_polars
import pandas as pd
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import time

start_pandas = time.time()

df_pandas = load_data_pandas().sort_values(["symbol", "timestamp"])

df_pandas["ma20"] = df_pandas.groupby("symbol")["price"].transform(
    lambda x: x.rolling(20, min_periods=1).mean()
)
df_pandas["std20"] = df_pandas.groupby("symbol")["price"].transform(
    lambda x: x.rolling(20, min_periods=1).std()
)
df_pandas["sharpe20"] = df_pandas["ma20"] / df_pandas["std20"]
end_pandas = time.time()
print(f"Pandas rolling metrics time: {end_pandas - start_pandas:.4f} sec")
df_pandas = df_pandas.reset_index()  # ensure 'timestamp' is a column
df_pandas_aapl = df_pandas[df_pandas["symbol"] == "AAPL"]


start_polars = time.time()

df_polars = (
    load_data_polars()
    .sort(["symbol", "timestamp"])
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


end_polars = time.time()
print(f"Polars rolling metrics time: {end_polars - start_polars:.4f} sec")

# Convert Polars to Pandas for plotting if needed
df_polars_aapl = df_polars.filter(pl.col("symbol") == "AAPL").to_pandas()
plt.figure(figsize=(12, 6))
plt.plot(df_pandas_aapl["timestamp"], df_pandas_aapl["price"], label="Price", alpha=0.4)
plt.plot(
    df_pandas_aapl["timestamp"],
    df_pandas_aapl["ma20"],
    label="MA20 (Pandas)",
    linewidth=1.8,
)
plt.plot(
    df_polars_aapl["timestamp"],
    df_polars_aapl["ma20"],
    "--",
    label="MA20 (Polars)",
    linewidth=1.8,
)
plt.title("AAPL: 20-Period Rolling Metrics Comparison")
plt.xlabel("Timestamp")
plt.ylabel("Price / Moving Avg")
plt.legend()
plt.show()
