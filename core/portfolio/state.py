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
        self._strategy_position_lots: dict[
            tuple[str, Instrument, str],
            Position,
        ] = {}
        self._net_liquidation: float = 0.0
        self._account_id: str = ""

    # ------------------------------------------------------------------
    # Fill application
    # ------------------------------------------------------------------

    def apply_fill(
        self,
        fill: Fill,
        strategy_id: str | None = None,
        trade_id: str | None = None,
    ) -> None:
        """Update position from a broker fill.

        Fills are signed: positive quantity = bought, negative = sold.
        """
        with self._lock:
            buy_sides = {"BOT", "BUY", "B", "LONG"}
            signed_qty = (
                fill.quantity if fill.side.upper() in buy_sides else -fill.quantity
            )
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
                if trade_id:
                    lot_key = (strategy_id, fill.instrument, trade_id)
                    self._strategy_position_lots[lot_key] = _apply_signed_qty(
                        self._strategy_position_lots.get(lot_key),
                        fill.instrument,
                        signed_qty,
                        fill.price,
                        trade_id=trade_id,
                    )

    def adopt_position(self, position: Position, strategy_id: str | None = None) -> None:
        """Seed state from broker positions discovered at startup."""
        with self._lock:
            self._positions[position.instrument] = position
            if strategy_id:
                self._strategy_positions[(strategy_id, position.instrument)] = position
                if position.trade_id:
                    self._strategy_position_lots[
                        (strategy_id, position.instrument, position.trade_id)
                    ] = position

    def adopt_strategy_position(self, strategy_id: str, position: Position) -> None:
        """Seed strategy-owned exposure without changing broker-wide position."""
        with self._lock:
            key = (strategy_id, position.instrument)
            self._strategy_positions[key] = _apply_signed_qty(
                self._strategy_positions.get(key),
                position.instrument,
                position.quantity,
                position.avg_cost,
            )
            if position.trade_id:
                self._strategy_position_lots[
                    (strategy_id, position.instrument, position.trade_id)
                ] = position

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

    def get_strategy_positions(
        self,
        strategy_id: str,
        instrument: Instrument,
    ) -> list[Position]:
        with self._lock:
            lots = [
                pos
                for (sid, inst, _), pos in self._strategy_position_lots.items()
                if sid == strategy_id and inst == instrument and not pos.is_flat
            ]
            if lots:
                return lots
            pos = self._strategy_positions.get((strategy_id, instrument))
            if pos is None or pos.is_flat:
                return []
            return [pos]

    def strategy_positions(self) -> list[tuple[str, Position]]:
        with self._lock:
            return [
                (sid, pos)
                for (sid, _), pos in self._strategy_positions.items()
                if not pos.is_flat
            ]

    def strategy_position_lots(self) -> list[tuple[str, Position]]:
        with self._lock:
            return [
                (sid, pos)
                for (sid, _, _), pos in self._strategy_position_lots.items()
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
    trade_id: str | None = None,
) -> Position:
    if existing is None or existing.is_flat:
        return Position(
            instrument=instrument,
            quantity=signed_qty,
            avg_cost=price,
            trade_id=trade_id,
        )

    old_qty = existing.quantity
    old_cost = existing.avg_cost
    new_qty = old_qty + signed_qty
    if new_qty == 0:
        return Position(instrument, 0.0, 0.0, trade_id=trade_id)
    if (old_qty > 0) == (signed_qty > 0):
        new_cost = (old_qty * old_cost + signed_qty * price) / new_qty
        return Position(instrument, new_qty, new_cost, trade_id=trade_id)
    return Position(
        instrument,
        new_qty,
        old_cost if new_qty * old_qty > 0 else price,
        trade_id,
    )
