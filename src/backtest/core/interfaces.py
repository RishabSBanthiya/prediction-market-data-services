"""
Abstract interfaces for the backtesting framework.

Defines contracts for data loading and order execution that can be
swapped for testing or alternative implementations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterator, Optional, Union

from models.orderbook import OrderbookSnapshot
from models.trade import Trade
from models.market import Market

from ..models.order import Order, Fill, OrderStatus
from ..models.config import BacktestConfig


# ============ Event Types ============

@dataclass
class BacktestEvent:
    """Base class for backtest events."""
    timestamp_ms: int
    event_index: int  # Position in the event stream


@dataclass
class OrderbookBacktestEvent(BacktestEvent):
    """An orderbook snapshot event."""
    snapshot: OrderbookSnapshot


@dataclass
class TradeBacktestEvent(BacktestEvent):
    """A trade event from the market tape."""
    trade: Trade


# ============ Data Loading ============

@dataclass
class BacktestDataset:
    """Container for loaded backtest data."""
    orderbooks: list[OrderbookSnapshot]
    trades: list[Trade]
    markets: dict[str, Market]  # token_id -> Market
    start_time_ms: int
    end_time_ms: int

    def get_event_iterator(self) -> Iterator[BacktestEvent]:
        """Returns time-ordered iterator merging orderbooks and trades.

        When timestamps are equal, trades come first (they caused the
        orderbook change).
        """
        ob_idx, trade_idx = 0, 0
        event_index = 0

        while ob_idx < len(self.orderbooks) or trade_idx < len(self.trades):
            if ob_idx >= len(self.orderbooks):
                yield TradeBacktestEvent(
                    timestamp_ms=self.trades[trade_idx].timestamp,
                    event_index=event_index,
                    trade=self.trades[trade_idx],
                )
                trade_idx += 1
            elif trade_idx >= len(self.trades):
                yield OrderbookBacktestEvent(
                    timestamp_ms=self.orderbooks[ob_idx].timestamp,
                    event_index=event_index,
                    snapshot=self.orderbooks[ob_idx],
                )
                ob_idx += 1
            else:
                ob_ts = self.orderbooks[ob_idx].timestamp
                trade_ts = self.trades[trade_idx].timestamp

                if trade_ts <= ob_ts:
                    # Trades first at equal timestamps
                    yield TradeBacktestEvent(
                        timestamp_ms=trade_ts,
                        event_index=event_index,
                        trade=self.trades[trade_idx],
                    )
                    trade_idx += 1
                else:
                    yield OrderbookBacktestEvent(
                        timestamp_ms=ob_ts,
                        event_index=event_index,
                        snapshot=self.orderbooks[ob_idx],
                    )
                    ob_idx += 1

            event_index += 1


class IDataLoader(ABC):
    """Loads historical market data for backtesting."""

    @abstractmethod
    async def load(
        self,
        config: BacktestConfig,
    ) -> BacktestDataset:
        """Load orderbooks and trades for the configured time range and assets.

        Args:
            config: Backtest configuration with time range, assets, platform filters

        Returns:
            BacktestDataset with orderbooks, trades, and market metadata
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources (connection pools, etc.)."""
        pass


# ============ Execution ============

class IExecutionEngine(ABC):
    """Simulates order execution against historical orderbook data."""

    @abstractmethod
    def submit_order(self, order: Order) -> str:
        """Submit an order for execution.

        Args:
            order: Order specification

        Returns:
            order_id assigned to this order
        """
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order.

        Args:
            order_id: ID of order to cancel

        Returns:
            True if cancelled, False if already filled/cancelled
        """
        pass

    @abstractmethod
    def get_open_orders(self, asset_id: Optional[str] = None) -> list[Order]:
        """Get all pending orders, optionally filtered by asset.

        Args:
            asset_id: If provided, only return orders for this asset

        Returns:
            List of pending orders
        """
        pass

    @abstractmethod
    def get_order_status(self, order_id: str) -> OrderStatus:
        """Check status of an order."""
        pass

    @abstractmethod
    def process_orderbook_update(self, snapshot: OrderbookSnapshot) -> list[Fill]:
        """Update internal orderbook state and check for fills.

        Args:
            snapshot: New orderbook snapshot

        Returns:
            List of fills generated by this update
        """
        pass

    @abstractmethod
    def process_trade(self, trade: Trade) -> list[Fill]:
        """Process a market trade and update queue positions.

        Args:
            trade: Trade from the market tape

        Returns:
            List of fills if trade matched pending orders
        """
        pass
