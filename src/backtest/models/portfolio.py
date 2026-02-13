from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional

from .position import Position, MarketPosition
from .order import Fill
from .market_pair import MarketPairRegistry


class PortfolioView(ABC):
    """
    Read-only interface for portfolio state.

    Strategies receive this interface to query positions and account values
    without being able to mutate the portfolio directly. The backtesting engine
    maintains the actual Portfolio instance and updates it via fills.
    """

    @property
    @abstractmethod
    def cash(self) -> Decimal:
        """Current cash balance."""
        pass

    @property
    @abstractmethod
    def total_value(self) -> Decimal:
        """Total account value (cash + position market values)."""
        pass

    @property
    @abstractmethod
    def buying_power(self) -> Decimal:
        """Available buying power (equals cash for prediction markets)."""
        pass

    @abstractmethod
    def get_position(self, asset_id: str) -> Optional[Position]:
        """
        Get position for a specific asset.

        Args:
            asset_id: Token ID to look up

        Returns:
            Position if it exists, None otherwise
        """
        pass

    @abstractmethod
    def get_all_positions(self) -> dict[str, Position]:
        """
        Get all current positions.

        Returns:
            Dictionary mapping asset_id to Position
        """
        pass

    @abstractmethod
    def get_market_position(self, market_id: str) -> Optional[MarketPosition]:
        """
        Get aggregated position for a market (all tokens in that market).

        Args:
            market_id: Market identifier (condition_id)

        Returns:
            MarketPosition if it exists, None otherwise
        """
        pass


class Portfolio(PortfolioView):
    """
    Mutable portfolio implementation for backtesting.

    Tracks cash, positions, and P&L throughout the backtest. Updates state
    via apply_fill() calls from the execution engine. Maintains both individual
    positions (per token) and market-level aggregates.

    Attributes:
        _cash: Current cash balance
        _initial_cash: Starting cash (for return calculation)
        _positions: All positions keyed by asset_id (token_id)
        _market_positions: Market-level positions keyed by market_id (condition_id)
        _current_prices: Last known prices for mark-to-market calculations
        _fills: Historical record of all fills
        _market_pairs: Optional registry for market pair lookups
    """

    def __init__(
        self,
        initial_cash: Decimal,
        market_pairs: Optional[MarketPairRegistry] = None
    ):
        """
        Initialize portfolio with starting cash.

        Args:
            initial_cash: Starting cash balance
            market_pairs: Optional registry for market pair awareness
        """
        self._cash = initial_cash
        self._initial_cash = initial_cash
        self._positions: dict[str, Position] = {}
        self._market_positions: dict[str, MarketPosition] = {}
        self._current_prices: dict[str, Decimal] = {}
        self._fills: list[Fill] = []
        self._market_pairs = market_pairs

    @property
    def cash(self) -> Decimal:
        """Current cash balance."""
        return self._cash

    @property
    def total_value(self) -> Decimal:
        """Total account value (cash + position market values)."""
        position_value = Decimal("0")
        for asset_id, position in self._positions.items():
            if asset_id in self._current_prices:
                position_value += position.market_value(self._current_prices[asset_id])
        return self._cash + position_value

    @property
    def buying_power(self) -> Decimal:
        """Available buying power (equals cash for prediction markets - no margin)."""
        return self._cash

    def get_position(self, asset_id: str) -> Optional[Position]:
        """
        Get position for a specific asset.

        Args:
            asset_id: Token ID to look up

        Returns:
            Position if it exists, None otherwise
        """
        return self._positions.get(asset_id)

    def get_all_positions(self) -> dict[str, Position]:
        """
        Get all current positions.

        Returns:
            Dictionary mapping asset_id to Position
        """
        return self._positions.copy()

    def get_market_position(self, market_id: str) -> Optional[MarketPosition]:
        """
        Get aggregated position for a market (all tokens in that market).

        Args:
            market_id: Market identifier (condition_id)

        Returns:
            MarketPosition if it exists, None otherwise
        """
        return self._market_positions.get(market_id)

    def apply_fill(self, fill: Fill) -> None:
        """
        Apply a fill to the portfolio.

        Updates cash and position state based on the fill:
        - BUY: Decreases cash by (price * quantity + fees)
        - SELL: Increases cash by (price * quantity - fees)

        Creates position if it doesn't exist. Uses position.apply_fill() to
        handle P&L calculations and position state updates.

        Args:
            fill: Fill object containing execution details
        """
        # Record the fill
        self._fills.append(fill)

        # Get or create position
        if fill.asset_id not in self._positions:
            self._positions[fill.asset_id] = Position(asset_id=fill.asset_id)

        position = self._positions[fill.asset_id]

        # Apply fill to position (handles P&L and position state)
        position.apply_fill(
            side=fill.side.value,
            price=fill.price,
            quantity=fill.quantity,
            fees=fill.fees
        )

        # Update cash based on fill
        if fill.side.value == "buy":
            # Buying costs: price * quantity + fees
            cost = (fill.price * fill.quantity) + fill.fees
            self._cash -= cost
        else:  # sell
            # Selling generates: price * quantity - fees
            proceeds = (fill.price * fill.quantity) - fill.fees
            self._cash += proceeds

        # Update market position if we can determine market_id
        market_id = self._determine_market_id(fill.asset_id)
        if market_id:
            if market_id not in self._market_positions:
                self._market_positions[market_id] = MarketPosition(market_id=market_id)
            # Link position to market position
            self._market_positions[market_id].positions[fill.asset_id] = position

    def update_mark_prices(self, prices: dict[str, Decimal]) -> None:
        """
        Update current market prices and recalculate unrealized P&L.

        Args:
            prices: Dictionary mapping asset_id to current price
        """
        self._current_prices.update(prices)

        # Update unrealized P&L for all positions
        for asset_id, position in self._positions.items():
            if asset_id in self._current_prices:
                position.update_unrealized_pnl(self._current_prices[asset_id])

        # Update market-level unrealized P&L
        for market_position in self._market_positions.values():
            market_position.update_unrealized_pnl(self._current_prices)

    def get_total_pnl(self) -> Decimal:
        """
        Calculate total P&L (realized + unrealized) across all positions.

        Returns:
            Total P&L
        """
        return sum(pos.total_pnl for pos in self._positions.values())

    def get_return(self) -> float:
        """
        Calculate portfolio return as a percentage.

        Returns:
            Return as decimal (e.g., 0.15 for 15% return)
        """
        if self._initial_cash == 0:
            return 0.0
        return float((self.total_value - self._initial_cash) / self._initial_cash)

    def _determine_market_id(self, asset_id: str) -> Optional[str]:
        """
        Determine market_id (condition_id) for a given asset.

        Uses market_pairs registry if available to find the condition_id
        associated with this token.

        Args:
            asset_id: Token ID

        Returns:
            Market ID (condition_id) if found, None otherwise
        """
        if self._market_pairs:
            pair = self._market_pairs.get_pair_for_token(asset_id)
            if pair:
                return pair.condition_id
        return None

    def get_fills(self) -> list[Fill]:
        """
        Get all historical fills.

        Returns:
            List of all fills applied to this portfolio
        """
        return self._fills.copy()

    @property
    def initial_cash(self) -> Decimal:
        """Initial cash balance at portfolio creation."""
        return self._initial_cash

    def get_total_fees_paid(self) -> Decimal:
        """Calculate total fees paid across all fills."""
        return sum((fill.fees for fill in self._fills), Decimal("0"))
