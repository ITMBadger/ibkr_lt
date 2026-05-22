"""RiskPolicy — MVP fixed-shares sizing with max_order_quantity cap.

Deferred: GuardrailEngine, entry windows, kill switch.
Those are Phase 7+ operational features ported back from legacy/.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..portfolio.state import PortfolioState
from ..types import Signal


@dataclass
class RiskPolicy:
    """Simple fixed-position sizing. No guardrails at MVP."""

    position_size_shares: int = 1
    max_order_quantity: int = 2

    def size_order(
        self,
        signal: Signal,
        portfolio: PortfolioState,
        *,
        allow_existing_position: bool = False,
    ) -> int:
        """Return the quantity to trade.

        Returns 0 if the portfolio already has a position in this instrument
        unless the caller explicitly allows adding independent strategy lots.
        Returns 0 if signal is "flat".
        Caps at max_order_quantity.
        """
        if signal.side == "flat":
            return 0
        if not allow_existing_position and not portfolio.is_flat(signal.instrument):
            return 0
        qty = min(self.position_size_shares, self.max_order_quantity)
        return max(0, qty)
