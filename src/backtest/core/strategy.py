"""
Strategy abstract base class for prediction market backtesting.

Strategies receive market data callbacks and submit orders via an injected
execution engine. They have read-only access to portfolio state via PortfolioView.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from models.orderbook import OrderbookSnapshot
from models.trade import Trade
from models.market import Market

from ..models.order import Order, Fill
from ..models.portfolio import PortfolioView
from .interfaces import IExecutionEngine


@dataclass
class BacktestContext:
    """
    Context information passed to strategies at backtest start/end.

    Provides metadata about the backtest run including time range,
    initial capital, platform filter, and all markets being tested.

    Attributes:
        start_time_ms: Start of backtest period in milliseconds
        end_time_ms: End of backtest period in milliseconds
        initial_cash: Starting capital amount
        platform: Optional platform filter (polymarket, kalshi, or None for all)
        markets: All markets included in this backtest (keyed by token_id)
    """
    start_time_ms: int
    end_time_ms: int
    initial_cash: float
    platform: Optional[str]
    markets: dict[str, Market]


class Strategy(ABC):
    """
    Abstract base class for backtesting strategies.

    Strategies implement trading logic by responding to market data callbacks
    (orderbook updates, trades) and submitting orders via the execution engine.

    The BacktestEngine injects dependencies via _inject_dependencies() before
    running the backtest. Strategies should not call this method directly.

    Lifecycle:
        1. Strategy instantiated by user
        2. BacktestEngine calls _inject_dependencies()
        3. BacktestEngine calls on_start()
        4. Market data callbacks (on_orderbook, on_trade) fire during replay
        5. Fill callbacks (on_fill) fire when orders execute
        6. Settlement callbacks (on_market_close) fire when markets resolve
        7. BacktestEngine calls on_end()

    Example:
        class SimpleMarketMaker(Strategy):
            def __init__(self, spread: float):
                super().__init__(name="SimpleMarketMaker")
                self.spread = spread

            def on_orderbook(self, snapshot: OrderbookSnapshot, is_forward_filled: bool):
                if is_forward_filled:
                    return  # Skip synthetic snapshots

                mid = snapshot.mid_price
                if mid:
                    # Quote around mid with spread
                    buy_order = Order(
                        asset_id=snapshot.asset_id,
                        side=OrderSide.BUY,
                        order_type=OrderType.LIMIT,
                        price=Decimal(str(mid - self.spread/2)),
                        quantity=Decimal("10")
                    )
                    self.submit_order(buy_order)
    """

    def __init__(self, name: str):
        """
        Initialize strategy with a name.

        Args:
            name: Human-readable strategy name for logging and reporting
        """
        self.name = name
        self._portfolio_view: Optional[PortfolioView] = None
        self._execution_engine: Optional[IExecutionEngine] = None

    def _inject_dependencies(
        self,
        portfolio_view: PortfolioView,
        execution_engine: IExecutionEngine
    ) -> None:
        """
        Inject dependencies from the backtest engine.

        Called internally by BacktestEngine before starting the backtest.
        Strategies should not call this method directly.

        Args:
            portfolio_view: Read-only view of portfolio state
            execution_engine: Interface for submitting orders
        """
        self._portfolio_view = portfolio_view
        self._execution_engine = execution_engine

    @property
    def portfolio(self) -> PortfolioView:
        """
        Get read-only view of current portfolio state.

        Provides access to cash, positions, and portfolio values.
        Use this to check positions before placing orders.

        Returns:
            PortfolioView interface

        Raises:
            RuntimeError: If accessed before dependency injection
        """
        if self._portfolio_view is None:
            raise RuntimeError(
                "Portfolio not available - strategy must be injected with dependencies "
                "by BacktestEngine before use"
            )
        return self._portfolio_view

    # ============ Lifecycle Hooks ============

    def on_start(self, context: BacktestContext) -> None:
        """
        Called once before the backtest starts.

        Use this to initialize strategy state, load configuration,
        or perform any setup based on the backtest parameters.

        Args:
            context: Backtest metadata (time range, capital, markets)
        """
        pass

    def on_end(self, context: BacktestContext) -> None:
        """
        Called once after the backtest completes.

        Use this to perform cleanup, log final statistics,
        or save strategy state for analysis.

        Args:
            context: Backtest metadata (time range, capital, markets)
        """
        pass

    # ============ Market Data Callbacks (Abstract) ============

    @abstractmethod
    def on_orderbook(
        self,
        snapshot: OrderbookSnapshot,
        is_forward_filled: bool
    ) -> None:
        """
        Called when an orderbook snapshot is received.

        This is the primary signal for most strategies. Fired for both
        real WebSocket events and forward-filled synthetic snapshots.

        Forward-filled snapshots are copies of the last real snapshot,
        emitted at regular intervals (typically 100ms) to maintain a
        continuous data stream. Many strategies skip these to avoid
        reacting to unchanged data.

        Args:
            snapshot: Orderbook snapshot with bids, asks, and computed metrics
            is_forward_filled: True if this is a synthetic copy (no real update)
        """
        pass

    # ============ Optional Callbacks ============

    def on_trade(self, trade: Trade) -> None:
        """
        Called when a trade occurs on the market tape.

        Trades represent actual executions between market participants.
        Use this to track market activity, volume profiles, or trade flow.

        Args:
            trade: Trade event with price, size, and side
        """
        pass

    def on_fill(self, fill: Fill) -> None:
        """
        Called when one of this strategy's orders is filled.

        Fires after the portfolio has been updated with the fill.
        Use this to track execution quality, update internal state,
        or trigger conditional logic based on fills.

        Args:
            fill: Fill details including price, quantity, fees, and maker/taker status
        """
        pass

    def on_market_close(self, market: Market, final_price: float) -> None:
        """
        Called when a market settles and positions are closed.

        At settlement, all positions in the market are automatically
        closed at the final resolution price. Use this to log results
        or update strategy state based on outcomes.

        Args:
            market: Market that settled
            final_price: Settlement price (0.0 or 1.0 for binary markets)
        """
        pass

    # ============ Order Submission Methods ============

    def submit_order(self, order: Order) -> str:
        """
        Submit an order for execution.

        Orders are validated and matched against the current orderbook
        state maintained by the execution engine.

        Args:
            order: Order specification (asset, side, type, price, quantity)

        Returns:
            order_id assigned to this order

        Raises:
            RuntimeError: If execution engine not injected
            ValueError: If order validation fails
        """
        if self._execution_engine is None:
            raise RuntimeError(
                "Execution engine not available - strategy must be injected with "
                "dependencies by BacktestEngine before use"
            )
        return self._execution_engine.submit_order(order)

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel a pending order.

        Args:
            order_id: ID of order to cancel

        Returns:
            True if order was cancelled, False if already filled/cancelled

        Raises:
            RuntimeError: If execution engine not injected
        """
        if self._execution_engine is None:
            raise RuntimeError(
                "Execution engine not available - strategy must be injected with "
                "dependencies by BacktestEngine before use"
            )
        return self._execution_engine.cancel_order(order_id)

    def get_open_orders(self, asset_id: Optional[str] = None) -> list[Order]:
        """
        Get all open (pending) orders.

        Args:
            asset_id: Optional filter to only return orders for specific asset

        Returns:
            List of pending orders

        Raises:
            RuntimeError: If execution engine not injected
        """
        if self._execution_engine is None:
            raise RuntimeError(
                "Execution engine not available - strategy must be injected with "
                "dependencies by BacktestEngine before use"
            )
        return self._execution_engine.get_open_orders(asset_id)
