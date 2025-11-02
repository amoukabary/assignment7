from __future__ import annotations
from abc import ABC, abstractmethod
import json


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
        self.symbol = symbol
        self.quantity = quantity
        self.price = price  # this will be maintained as VWAP

    def get_value(self, market_price: dict[str, float]) -> float:
        return self.quantity * market_price.get(self.symbol, 0.0)

    def get_positions(self) -> list:
        return [self]

    def update(self, quantity: float, price: float):
        self.quantity = quantity
        self.price = price

    def __repr__(self):
        return (
            f"Position(symbol={self.symbol!r}, qty={self.quantity}, price={self.price})"
        )

    def _to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "quantity": self.quantity,
            "price": self.price,
        }


class Portfolio(PortfolioComponent):
    """Composite node representing a portfolio of positions and/or sub-portfolios."""

    def __init__(self, name: str = "", owner: str = ""):
        self.name = name
        self.positions = {}
        self.sub_portfolios = []
        self.owner = owner

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

    def add(self, component: PortfolioComponent):
        if isinstance(component, Portfolio):
            self.sub_portfolios.append(component)
        elif isinstance(component, Position):
            symbol = component.symbol
            pos = self.positions.get(symbol)
            if pos:
                # update VWAP
                pos = self.positions[symbol]
                new_qty, new_vwap = self._vwap(
                    pos.quantity, pos.price, component.quantity, component.price
                )
                if new_qty == 0:
                    del self.positions[symbol]
                else:
                    pos.update(new_qty, new_vwap)
            else:
                if component.quantity != 0:
                    self.positions[symbol] = component

    def remove(self, component: PortfolioComponent):
        if isinstance(component, Portfolio):
            self.sub_portfolios.remove(component)
        elif isinstance(component, Position):
            symbol = component.symbol
            if symbol in self.positions:
                del self.positions[symbol]

    def get_positions(self) -> list:
        positions = list(self.positions.values())
        for subportfolio in self.sub_portfolios:
            positions.extend(subportfolio.get_positions())
        return positions

    def get_value(self, market_price: dict[str, float]) -> float:
        total = sum(pos.get_value(market_price) for pos in self.positions.values())
        total += sum(sp.get_value(market_price) for sp in self.sub_portfolios)
        return total

    def __repr__(self):
        if self.owner:
            return f"Portfolio(name={self.name!r}, owner={self.owner!r}, positions={len(self.positions)}, sub_portfolios={len(self.sub_portfolios)})"
        return f"Portfolio(name={self.name!r}, positions={len(self.positions)}, sub_portfolios={len(self.sub_portfolios)})"

    def _to_dict(self) -> dict:
        res: dict[str, object] = {"name": self.name}

        if self.owner:
            res["owner"] = self.owner

        if self.positions:
            res["positions"] = [pos._to_dict() for pos in self.positions.values()]
        if self.sub_portfolios:
            res["sub_portfolios"] = [sp._to_dict() for sp in self.sub_portfolios]

        return res


class PortfolioBuilder:
    """Builder class to construct Portfolio objects"""

    def __init__(self, name: str = ""):
        self._portfolio = Portfolio(name)

    def set_owner(self, owner: str):
        self._portfolio.owner = owner

    def add_position(self, symbol: str, quantity: float, price: float):
        position = Position(symbol, quantity, price)
        self._portfolio.add(position)
        return self

    def add_subportfolio(self, builder: PortfolioBuilder):
        sub = builder.build()
        self._portfolio.add(sub)
        return self

    def build(self):
        built = self._portfolio
        self._portfolio = Portfolio()
        return built

    @classmethod
    def from_dict(cls, data: dict) -> Portfolio:
        def build_recursive(data: dict) -> PortfolioBuilder:
            builder = cls(data.get("name", ""))
            if "owner" in data:
                builder.set_owner(data["owner"])
            for pos_data in data.get("positions", []):
                builder.add_position(
                    symbol=pos_data["symbol"],
                    quantity=pos_data["quantity"],
                    price=pos_data["price"],
                )
            for sp_data in data.get("sub_portfolios", []):
                subbuilder = build_recursive(sp_data)
                builder.add_subportfolio(subbuilder)
            return builder

        builder = build_recursive(data)
        return builder.build()

    @classmethod
    def from_json(cls, filepath: str) -> Portfolio:
        with open(filepath, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)
