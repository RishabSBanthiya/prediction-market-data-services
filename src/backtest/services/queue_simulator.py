"""
Queue position simulator for resting limit orders.

Simulates realistic order queue position tracking for limit orders that rest
in the order book. Tracks each order's position in the queue and advances
position as trades occur at that price level.
"""

import random
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional
import structlog

from ..models.order import Order, OrderSide
from models.orderbook import OrderbookSnapshot
from models.trade import Trade


logger = structlog.get_logger(__name__)


@dataclass
class QueueEntry:
    """
    Tracks a single order's position in the queue at a price level.

    When a limit order is placed that doesn't immediately cross the spread,
    it rests in the order book behind existing orders at that price. This
    entry tracks:
    - Initial queue position (size_ahead)
    - Cumulative volume that has traded through at this price
    - Fill eligibility when enough volume has passed
    """
    order_id: str
    asset_id: str
    side: OrderSide
    price: Decimal
    quantity: Decimal
    size_ahead: Decimal  # Estimated size ahead in queue when order was placed
    cumulative_volume_at_price: Decimal = Decimal("0")  # Volume traded at this price since order placed


@dataclass
class QueueState:
    """Current queue state for tracking."""
    entries: dict[str, QueueEntry] = field(default_factory=dict)  # order_id -> QueueEntry


class QueueSimulator:
    """
    Simulates order queue position for resting limit orders.

    When a limit order is placed that doesn't immediately cross the spread,
    it rests in the order book. In a real exchange, it sits behind existing
    orders at that price level. This simulator:

    1. Estimates initial queue position (size_ahead) from the orderbook
    2. Advances queue position as trades occur at the order's price
    3. Triggers fills when enough volume has traded through

    Uses configurable fill_probability (default 1.0 = deterministic) to
    optionally add randomness for more realistic simulation.

    Example:
        If you place a BUY limit at 0.55 and there's already 100 shares
        at 0.55 on the bid side, your order is behind those 100 shares.
        As trades occur at 0.55, the cumulative volume increases. Once
        100+ shares have traded, your order is at the front and eligible
        to fill.
    """

    def __init__(self, fill_probability: float = 1.0):
        """
        Initialize queue simulator.

        Args:
            fill_probability: Probability that an order fills when queue
                position reached (0.0-1.0). Default 1.0 = deterministic.
        """
        if not 0.0 <= fill_probability <= 1.0:
            raise ValueError(f"fill_probability must be in [0, 1], got {fill_probability}")

        self._queue_state = QueueState()
        self._fill_probability = fill_probability
        logger.info("queue_simulator_initialized", fill_probability=fill_probability)

    def add_order(self, order: Order, snapshot: OrderbookSnapshot) -> None:
        """
        Add a resting limit order to the queue tracker.

        Estimates size_ahead by looking at the orderbook:
        - For BUY limit: sum sizes of all bid levels at order.price or better
        - For SELL limit: sum sizes of all ask levels at order.price or better

        This gives a conservative estimate of how much volume needs to trade
        before this order reaches the front of the queue.

        Args:
            order: The limit order being added to queue
            snapshot: Current orderbook snapshot
        """
        if order.order_id in self._queue_state.entries:
            logger.warning(
                "order_already_in_queue",
                order_id=order.order_id,
            )
            return

        # Estimate size ahead based on orderbook levels
        size_ahead = Decimal("0")

        if order.side == OrderSide.BUY:
            # For buy orders, sum all bid levels at our price or better
            for bid in snapshot.bids:
                bid_price = Decimal(str(bid.price))
                bid_size = Decimal(str(bid.size))

                if bid_price >= order.price:
                    size_ahead += bid_size
        else:  # SELL
            # For sell orders, sum all ask levels at our price or better
            for ask in snapshot.asks:
                ask_price = Decimal(str(ask.price))
                ask_size = Decimal(str(ask.size))

                if ask_price <= order.price:
                    size_ahead += ask_size

        # Create queue entry
        entry = QueueEntry(
            order_id=order.order_id,
            asset_id=order.asset_id,
            side=order.side,
            price=order.price,
            quantity=order.quantity,
            size_ahead=size_ahead,
            cumulative_volume_at_price=Decimal("0"),
        )

        self._queue_state.entries[order.order_id] = entry

        logger.info(
            "order_added_to_queue",
            order_id=order.order_id,
            asset_id=order.asset_id,
            side=order.side.value,
            price=str(order.price),
            quantity=str(order.quantity),
            size_ahead=str(size_ahead),
        )

    def remove_order(self, order_id: str) -> None:
        """
        Remove order from queue (cancelled or filled elsewhere).

        Args:
            order_id: ID of order to remove
        """
        if order_id in self._queue_state.entries:
            entry = self._queue_state.entries.pop(order_id)
            logger.info(
                "order_removed_from_queue",
                order_id=order_id,
                cumulative_volume=str(entry.cumulative_volume_at_price),
                size_ahead=str(entry.size_ahead),
            )

    def process_trade(self, trade: Trade) -> list[str]:
        """
        Process a trade and advance queue positions.

        For each queued order at the trade's price:
        - Add trade.size to cumulative_volume_at_price
        - If cumulative_volume >= size_ahead, order is eligible for fill
        - Apply fill_probability check

        A trade can advance queue for BOTH BUY and SELL orders:
        - A trade at 0.55 advances buy orders with price >= 0.55
        - A trade at 0.55 also advances sell orders with price <= 0.55

        Args:
            trade: Trade event from market tape

        Returns:
            List of order_ids that should now be filled
        """
        if not self._queue_state.entries:
            return []

        trade_price = Decimal(str(trade.price))
        trade_size = Decimal(str(trade.size))

        orders_to_fill = []

        # Check all queued orders for this asset
        for order_id, entry in list(self._queue_state.entries.items()):
            if entry.asset_id != trade.asset_id:
                continue

            # Check if trade price matches order's price level
            price_matches = False

            if entry.side == OrderSide.BUY:
                # Buy orders advance when trade occurs at their price or better
                if trade_price <= entry.price:
                    price_matches = True
            else:  # SELL
                # Sell orders advance when trade occurs at their price or better
                if trade_price >= entry.price:
                    price_matches = True

            if not price_matches:
                continue

            # Advance queue position
            entry.cumulative_volume_at_price += trade_size

            # Check if queue position reached
            if entry.cumulative_volume_at_price >= entry.size_ahead:
                # Apply fill probability (deterministic when 1.0, random otherwise)
                if self._fill_probability >= 1.0 or random.random() < self._fill_probability:
                    orders_to_fill.append(order_id)
                    logger.info(
                        "queue_position_reached",
                        order_id=order_id,
                        price=str(entry.price),
                        cumulative_volume=str(entry.cumulative_volume_at_price),
                        size_ahead=str(entry.size_ahead),
                    )

        return orders_to_fill

    def get_queue_position(self, order_id: str) -> Optional[QueueEntry]:
        """
        Get current queue state for an order.

        Args:
            order_id: ID of order to query

        Returns:
            QueueEntry if order is in queue, None otherwise
        """
        return self._queue_state.entries.get(order_id)

    def get_all_entries(self) -> dict[str, QueueEntry]:
        """
        Get all current queue entries.

        Returns:
            Dictionary of order_id -> QueueEntry
        """
        return self._queue_state.entries.copy()
