# portfolio.py
import json
from collections import defaultdict
from abc import ABC, abstractmethod
from threading import Lock
import polars as pl
import pandas as pd
from data_loader import load_data_pandas
import numpy as np
from enum import Enum


class Annualization(Enum):
    D = 252
    B = 252
    W = 52
    M = 12
    Q = 4
    A = 1
    H = 252 * 6.5
    T = 252 * 6.5 * 60
    S = 252 * 6.5 * 60 * 60


# -------------------------------------------- #
# Compute metrics for positions and portfolios #
# -------------------------------------------- #


def compute_position_metrics(
    position: dict, df: pd.DataFrame, annualization: float = 1.0
) -> dict:
    symbol = position["symbol"]
    quantity = position["quantity"]
    symbol_prices = df[df["symbol"] == symbol]["price"]
    freq = symbol_prices.index.freq or pd.infer_freq(symbol_prices.index)
    last_price = symbol_prices.iloc[-1]
    rets = symbol_prices.pct_change().dropna()
    vol = rets.rolling(window=20).std().mean()
    drawdown = (symbol_prices - symbol_prices.cummax()) / symbol_prices.cummax()
    max_dd = drawdown.min()

    metrics_dict = {
        "symbol": symbol,
        "value": float(last_price * quantity),
        "volatility": float(vol) * np.sqrt(annualization),
        "drawdown": float(max_dd),
    }

    return metrics_dict


def compute_portfolio_metrics_serialized(
    portfolio: dict, df: pd.DataFrame, annualization: float = 1.0
) -> dict:
    name = portfolio.get("name", "")
    owner = portfolio.get("owner", "")
    positions = portfolio.get("positions", [])
    sub_portfolios = portfolio.get("sub_portfolios", [])
    portfolio_metrics = {
        "name": name,
    }
    if owner:
        portfolio_metrics["owner"] = owner

    total_value = 0.0
    total_vol = 0.0

    position_metrics = []
    for pos in positions:
        pm = compute_position_metrics(pos, df, annualization)
        val = pm["value"]
        vol = pm["volatility"]
        total_vol += vol * val
        total_value += val
        position_metrics.append(pm)

    sub_portfolio_metrics = []
    for sp in sub_portfolios:
        sp = compute_portfolio_metrics_serialized(sp, df, annualization)
        val = sp["total_value"]
        vol = sp["aggregate_volatility"]
        total_vol += vol * val
        total_value += val
        sub_portfolio_metrics.append(sp)

    portfolio_metrics["positions"] = position_metrics
    portfolio_metrics["sub_portfolios"] = sub_portfolio_metrics

    portfolio_metrics["total_value"] = total_value
    agg_vol = total_vol / total_value if total_value else 0.0
    portfolio_metrics["aggregate_volatility"] = agg_vol

    return portfolio_metrics


if __name__ == "__main__":

    df = load_data_pandas()
    index = df.index.unique()
    freq = pd.infer_freq(index)
    af = Annualization[freq.upper()].value

    portfolio = json.load(open("portfolio_structure.json"))
    metrics = compute_portfolio_metrics_serialized(portfolio, df)
    print(json.dumps(metrics, indent=2))
