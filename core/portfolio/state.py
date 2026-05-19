"""PortfolioState — tracks positions and account equity from fills.

Updated by OrderManager._drain_fills() in the asyncio event loop.
Read by RiskPolicy to check current exposure before sizing new orders.
"""

from __future__ import annotations

import threading

from ..types import AccountSnapshot, Fill, Instrument, Position


class PortfolioState:
    """Thread-safe position and equity tracker."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._positions: dict[Instrument, Position] = {}
        self._strategy_positions: dict[tuple[str, Instrument], Position] = {}
        self._net_liquidation: float = 0.0
        self._account_id: str = ""

    # ------------------------------------------------------------------
    # Fill application
    # ------------------------------------------------------------------

    def apply_fill(self, fill: Fill, strategy_id: str | None = None) -> None:
        """Update position from a broker fill.

        Fills are signed: positive quantity = bought, negative = sold.
        """
        with self._lock:
            buy_sides = {"BOT", "BUY", "B", "LONG"}
            signed_qty = fill.quantity if fill.side.upper() in buy_sides else -fill.quantity
            self._positions[fill.instrument] = _apply_signed_qty(
                self._positions.get(fill.instrument),
                fill.instrument,
                signed_qty,
                fill.price,
            )
            if strategy_id:
                key = (strategy_id, fill.instrument)
                self._strategy_positions[key] = _apply_signed_qty(
                    self._strategy_positions.get(key),
                    fill.instrument,
                    signed_qty,
                    fill.price,
                )

    def adopt_position(self, position: Position, strategy_id: str | None = None) -> None:
        """Seed state from broker positions discovered at startup."""
        with self._lock:
            self._positions[position.instrument] = position
            if strategy_id:
                self._strategy_positions[(strategy_id, position.instrument)] = position

    def adopt_positions(
        self,
        positions: list[Position],
        strategy_map: dict[Instrument, str] | None = None,
    ) -> None:
        strategy_map = strategy_map or {}
        for position in positions:
            self.adopt_position(position, strategy_map.get(position.instrument))

    def update_account(self, snapshot: AccountSnapshot) -> None:
        with self._lock:
            self._net_liquidation = snapshot.net_liquidation
            self._account_id = snapshot.account_id

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------

    def get_position(self, instrument: Instrument) -> Position | None:
        with self._lock:
            pos = self._positions.get(instrument)
            if pos is None or pos.is_flat:
                return None
            return pos

    def get_strategy_position(
        self,
        strategy_id: str,
        instrument: Instrument,
    ) -> Position | None:
        with self._lock:
            pos = self._strategy_positions.get((strategy_id, instrument))
            if pos is None or pos.is_flat:
                return None
            return pos

    def strategy_positions(self) -> list[tuple[str, Position]]:
        with self._lock:
            return [
                (sid, pos)
                for (sid, _), pos in self._strategy_positions.items()
                if not pos.is_flat
            ]

    def positions(self) -> list[Position]:
        with self._lock:
            return [p for p in self._positions.values() if not p.is_flat]

    def net_liquidation(self) -> float:
        with self._lock:
            return self._net_liquidation

    def is_flat(self, instrument: Instrument) -> bool:
        with self._lock:
            pos = self._positions.get(instrument)
            return pos is None or pos.is_flat


def _apply_signed_qty(
    existing: Position | None,
    instrument: Instrument,
    signed_qty: float,
    price: float,
) -> Position:
    if existing is None or existing.is_flat:
        return Position(
            instrument=instrument,
            quantity=signed_qty,
            avg_cost=price,
        )

    old_qty = existing.quantity
    old_cost = existing.avg_cost
    new_qty = old_qty + signed_qty
    if new_qty == 0:
        return Position(instrument, 0.0, 0.0)
    if (old_qty > 0) == (signed_qty > 0):
        new_cost = (old_qty * old_cost + signed_qty * price) / new_qty
        return Position(instrument, new_qty, new_cost)
    return Position(
        instrument,
        new_qty,
        old_cost if new_qty * old_qty > 0 else price,
    )
