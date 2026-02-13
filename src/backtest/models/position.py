from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional


class PositionSide(str, Enum):
    """Side of a fill applied to a position."""
    BUY = "buy"
    SELL = "sell"


@dataclass
class Position:
    """
    Represents a position in a single asset (token).

    Tracks quantity, average entry price, and P&L. Uses dataclass for
    mutable state that updates as fills are applied.
    """

    asset_id: str
    quantity: Decimal = Decimal("0")
    avg_entry_price: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    total_fees_paid: Decimal = Decimal("0")

    def apply_fill(
        self,
        side: PositionSide | str,
        price: Decimal,
        quantity: Decimal,
        fees: Decimal
    ) -> Decimal:
        """
        Apply a fill to this position and return realized P&L.

        Args:
            side: PositionSide enum or string ("buy"/"sell")
            price: Fill price
            quantity: Fill quantity (always positive)
            fees: Fees paid on this fill (always positive)

        Returns:
            Realized P&L from this fill (0 if increasing position)
        """
        realized = Decimal("0")
        self.total_fees_paid += fees

        # Normalize side to string for comparison
        side_str = side.value if isinstance(side, Enum) else side

        if side_str == "buy":
            if self.quantity >= 0:
                # Increasing long position or opening from flat
                total_cost = (self.avg_entry_price * self.quantity) + (price * quantity)
                self.quantity += quantity
                if self.quantity > Decimal("0"):
                    self.avg_entry_price = total_cost / self.quantity
                else:
                    self.avg_entry_price = Decimal("0")
            else:
                # Reducing short position
                reduce_qty = min(quantity, abs(self.quantity))
                realized = (self.avg_entry_price - price) * reduce_qty
                self.realized_pnl += realized
                self.quantity += quantity

                if self.quantity > Decimal("0"):
                    self.avg_entry_price = price
                elif self.quantity == Decimal("0"):
                    self.avg_entry_price = Decimal("0")

        elif side_str == "sell":
            if self.quantity > Decimal("0"):
                # Reducing long position
                reduce_qty = min(quantity, self.quantity)
                realized = (price - self.avg_entry_price) * reduce_qty
                self.realized_pnl += realized
                self.quantity -= quantity

                if self.quantity < Decimal("0"):
                    self.avg_entry_price = price
                elif self.quantity == Decimal("0"):
                    self.avg_entry_price = Decimal("0")
            else:
                # Increasing short position or opening from flat
                total_cost = (self.avg_entry_price * abs(self.quantity)) + (price * quantity)
                self.quantity -= quantity
                if self.quantity != Decimal("0"):
                    self.avg_entry_price = total_cost / abs(self.quantity)
                else:
                    self.avg_entry_price = Decimal("0")

        return realized

    def update_unrealized_pnl(self, current_price: Decimal) -> None:
        """
        Update unrealized P&L based on current market price.

        Args:
            current_price: Current mark price for this asset
        """
        if self.quantity > 0:
            # Long position
            self.unrealized_pnl = (current_price - self.avg_entry_price) * self.quantity
        elif self.quantity < 0:
            # Short position
            self.unrealized_pnl = (self.avg_entry_price - current_price) * abs(self.quantity)
        else:
            # Flat
            self.unrealized_pnl = Decimal("0")

    def market_value(self, current_price: Decimal) -> Decimal:
        """
        Calculate current market value of position.

        Args:
            current_price: Current mark price for this asset

        Returns:
            Position value (quantity * price)
        """
        return self.quantity * current_price

    @property
    def total_pnl(self) -> Decimal:
        """Total P&L (realized + unrealized)."""
        return self.realized_pnl + self.unrealized_pnl

    @property
    def is_flat(self) -> bool:
        """True if position is closed (quantity == 0)."""
        return self.quantity == 0


@dataclass
class MarketPosition:
    """
    Aggregates all positions for a single market (condition_id).

    In prediction markets, a single market (e.g., "Will team X win?") may have
    multiple outcome tokens (yes/no or multiple outcomes). This class tracks
    all positions within that market.

    Attributes:
        market_id: Market identifier (condition_id)
        positions: Dictionary of positions keyed by asset_id (token_id)
    """

    market_id: str
    positions: dict[str, Position] = field(default_factory=dict)

    def get_position(self, asset_id: str) -> Optional[Position]:
        """
        Get position for a specific asset.

        Args:
            asset_id: Token ID to look up

        Returns:
            Position if it exists, None otherwise
        """
        return self.positions.get(asset_id)

    def get_or_create_position(self, asset_id: str) -> Position:
        """
        Get existing position or create new one for asset.

        Args:
            asset_id: Token ID to look up or create

        Returns:
            Position (existing or newly created)
        """
        if asset_id not in self.positions:
            self.positions[asset_id] = Position(asset_id=asset_id)
        return self.positions[asset_id]

    def update_unrealized_pnl(self, prices: dict[str, Decimal]) -> None:
        """
        Update unrealized P&L for all positions using current prices.

        Args:
            prices: Dictionary mapping asset_id to current price
        """
        for asset_id, position in self.positions.items():
            if asset_id in prices:
                position.update_unrealized_pnl(prices[asset_id])

    @property
    def total_pnl(self) -> Decimal:
        """Total P&L across all positions in this market."""
        return sum(pos.total_pnl for pos in self.positions.values())

    def net_exposure(self, prices: dict[str, Decimal]) -> Decimal:
        """
        Calculate net market value exposure across all positions.

        Sums the market value (quantity * price) for all positions.

        Args:
            prices: Dictionary mapping asset_id to current price

        Returns:
            Net exposure (sum of all position market values)
        """
        exposure = Decimal("0")
        for asset_id, position in self.positions.items():
            if asset_id in prices:
                exposure += position.market_value(prices[asset_id])
        return exposure
