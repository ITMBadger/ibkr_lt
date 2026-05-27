"""RiskPolicy — order sizing policy.

Deferred: GuardrailEngine, entry windows, kill switch.
Those are Phase 7+ operational features ported back from legacy/.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..portfolio.state import PortfolioState
from ..types import Signal

SIZING_MODE_FIXED_SHARES = "fixed_shares"
SIZING_MODE_FULL_EQUITY = "full_equity"
SizingMode = Literal["fixed_shares", "full_equity"]


@dataclass
class RiskPolicy:
    """Order sizing. No guardrails at MVP."""

    position_size_shares: int = 1
    max_order_quantity: float | None = 2
    sizing_mode: SizingMode = SIZING_MODE_FIXED_SHARES
    equity_fraction: float = 1.0
    max_order_notional: float | None = None
    buying_power_buffer_pct: float | None = None
    max_intraday_drawdown_pct: float | None = None

    def size_order(
        self,
        signal: Signal,
        portfolio: PortfolioState,
        *,
        allow_existing_position: bool = False,
        reference_price: float | None = None,
        account_equity: float | None = None,
    ) -> float:
        """Return the quantity to trade.

        Returns 0 if the portfolio already has a position in this instrument
        unless the caller explicitly allows adding independent strategy lots.
        Returns 0 if signal is "flat".
        Fixed-share mode caps at max_order_quantity. Full-equity mode only caps
        if max_order_quantity is explicitly set.
        """
        if signal.side == "flat":
            return 0
        if not allow_existing_position and not portfolio.is_flat(signal.instrument):
            return 0

        if self.sizing_mode == SIZING_MODE_FULL_EQUITY:
            if reference_price is None or account_equity is None:
                return 0
            price = float(reference_price)
            equity = float(account_equity)
            multiplier = float(signal.instrument.multiplier or 1.0)
            if price <= 0 or equity <= 0 or multiplier <= 0:
                return 0
            qty = equity * float(self.equity_fraction) / (price * multiplier)
            if self.max_order_quantity is not None:
                qty = min(qty, float(self.max_order_quantity))
            return max(0.0, qty)

        qty = float(self.position_size_shares)
        if self.max_order_quantity is not None:
            qty = min(qty, float(self.max_order_quantity))
        return max(0, qty)
