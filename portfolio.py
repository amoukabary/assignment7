# portfolio.py
import json
from collections import defaultdict
from abc import ABC, abstractmethod
from threading import Lock


class PortfolioComponent(ABC):
    @abstractmethod
    def get_value(self, market_price) -> float:
        pass

    @abstractmethod
    def get_positions(self) -> list:
        pass


class Position(PortfolioComponent):
    """Leaf node representing a single financial position."""

    def __init__(self, symbol: str, quantity: float, price: float):
        self._symbol = symbol
        self._quantity = quantity
        self._price = price  # this will be maintained as VWAP

    def get_value(self, market_price: dict[str, float]) -> float:
        return self._quantity * market_price.get(self._symbol, 0.0)

    def get_positions(self) -> list:
        return [self]

    def update(self, quantity: float, price: float):
        self._quantity = quantity
        self._price = price

    def __repr__(self):
        return f"Position(symbol={self._symbol!r}, qty={self._quantity}, price={self._price})"


class Portfolio(PortfolioComponent):
    """Composite node representing a portfolio of positions and/or sub-portfolios."""

    def __init__(self, name: str = "", components: list[PortfolioComponent] = []):
        self._name = name
        self._owner: str = ""
        self._components = components
        self._position_cache: dict[str, Position] = {}

    def _vwap(
        self, old_qty: float, old_vwap: float, trade_qty: float, trade_price: float
    ) -> tuple[float, float]:

        new_qty = old_qty + trade_qty
        if new_qty == 0:
            return 0.0, 0.0

        if old_qty * new_qty < 0:
            # position flip
            return trade_price, trade_price

        new_vwap = (old_qty * old_vwap + trade_qty * trade_price) / new_qty

        return new_qty, new_vwap

    def add_position(self, symbol: str, quantity: float, price: float):

        pos = self._position_cache.get(symbol, None)
        if pos is None:
            pos = Position(symbol, 0.0, 0.0)
            self._position_cache[symbol] = pos
            self._components.append(pos)

        # update VWAP
        # NOTE: this part is redundant for new positions but keeps logic simple
        # may consider removing
        new_qty, new_vwap = self._vwap(pos._quantity, pos._price, quantity, price)
        if new_qty == 0:
            del self._position_cache[symbol]
            self._components.remove(pos)
        else:
            pos.update(new_qty, new_vwap)

    def remove(self, component: PortfolioComponent):
        self._components.remove(component)

    def get_value(self, market_price: dict[str, float]) -> float:
        return sum(component.get_value(market_price) for component in self._components)

    def get_positions(self) -> list:
        positions = []
        for component in self._components:
            positions.extend(component.get_positions())
        return positions

    def __repr__(self):
        return f"Portfolio(name={self._name!r}, owner={self._owner!r}, components={self._components})"


if __name__ == "__main__":

    portfolio = Portfolio("My Portfolio")
    print(portfolio)
