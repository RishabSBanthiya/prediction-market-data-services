"""
Execution engine for prediction market backtesting.

Simulates order matching against real historical L2 orderbook data.
Walks actual orderbook levels for exact slippage calculation.
"""

from decimal import Decimal
from typing import Optional
from uuid import uuid4
import structlog

from ..core.interfaces import IExecutionEngine
from ..models.order import Order, OrderSide, OrderType, OrderStatus, TimeInForce, Fill, FillReason, OrderRejectionReason
from ..models.config import FeeSchedule
from ..models.portfolio import Portfolio
from ..models.market_pair import MarketPairRegistry
from .queue_simulator import QueueSimulator
from models.orderbook import OrderbookSnapshot
from models.trade import Trade


logger = structlog.get_logger(__name__)


class ExecutionEngine(IExecutionEngine):
    """
    Core execution engine that matches orders against real orderbook data.

    Responsibilities:
    - Order submission and validation
    - Market order execution by walking orderbook levels
    - Limit order queue management and matching
    - Fill generation with exact pricing
    - Portfolio state updates via fills
    """

    def __init__(
        self,
        portfolio: Portfolio,
        fee_schedule: FeeSchedule,
        market_pairs: Optional[MarketPairRegistry] = None,
        logger_override=None,
        fill_probability: float = 1.0,
        order_max_age_ms: Optional[int] = None,
        verbose: bool = False,
    ):
        """
        Initialize execution engine.

        Args:
            portfolio: Portfolio to update on fills
            fee_schedule: Fee calculator for fills
            market_pairs: Optional registry for yes/no pair logic
            logger_override: Optional logger instance
            fill_probability: Probability for queue fills (default 1.0 = deterministic)
            order_max_age_ms: Maximum order age in milliseconds before auto-cancel (None = no timeout)
            verbose: Enable verbose INFO-level logging for all operations. When False,
                    routine operations log at DEBUG level for better performance.
        """
        self._portfolio = portfolio
        self._fee_schedule = fee_schedule
        self._market_pairs = market_pairs
        self._logger = logger_override or logger
        self._order_max_age_ms = order_max_age_ms
        self._verbose = verbose

        # Order management
        self._orders: dict[str, Order] = {}
        self._order_counter = 0

        # Secondary index for fast pending order lookup by asset
        self._pending_by_asset: dict[str, set[str]] = {}

        # Market state
        self._current_orderbooks: dict[str, OrderbookSnapshot] = {}
        self._current_timestamp = 0

        # Queue position simulator for limit orders
        self._queue_simulator = QueueSimulator(fill_probability=fill_probability)

        # Size constraints
        self._min_order_size = Decimal("0.1")
        self._max_order_size = Decimal("100000")

        self._logger.info("execution_engine_initialized", verbose=verbose)

    def submit_order(self, order: Order) -> str:
        """
        Submit an order for execution.

        Validates order, assigns ID, executes immediately if market/IOC/FOK,
        otherwise queues as pending limit order.

        Args:
            order: Order to submit (order_id will be assigned)

        Returns:
            str: Assigned order_id

        Raises:
            ValueError: If order validation fails
        """
        # Validate order size
        if order.quantity < self._min_order_size:
            self._logger.warning(
                "order_rejected_size_too_small",
                quantity=str(order.quantity),
                min_size=str(self._min_order_size),
            )
            order.status = OrderStatus.REJECTED
            order.rejection_reason = OrderRejectionReason.INVALID_SIZE
            order.order_id = self._generate_order_id()
            self._orders[order.order_id] = order
            return order.order_id

        if order.quantity > self._max_order_size:
            self._logger.warning(
                "order_rejected_size_too_large",
                quantity=str(order.quantity),
                max_size=str(self._max_order_size),
            )
            order.status = OrderStatus.REJECTED
            order.rejection_reason = OrderRejectionReason.INVALID_SIZE
            order.order_id = self._generate_order_id()
            self._orders[order.order_id] = order
            return order.order_id

        # Validate price bounds for prediction markets
        if order.price is not None:
            if not (Decimal("0") <= order.price <= Decimal("1")):
                self._logger.warning(
                    "order_rejected_invalid_price",
                    price=str(order.price),
                )
                order.status = OrderStatus.REJECTED
                order.rejection_reason = OrderRejectionReason.INVALID_PRICE
                order.order_id = self._generate_order_id()
                self._orders[order.order_id] = order
                return order.order_id

        # Validate buying power for buys
        if order.side == OrderSide.BUY:
            max_cost = order.quantity * (order.price or Decimal("1"))  # Worst case for market
            if self._portfolio.buying_power < max_cost:
                self._logger.warning(
                    "order_rejected_insufficient_funds",
                    required=str(max_cost),
                    available=str(self._portfolio.cash),
                )
                order.status = OrderStatus.REJECTED
                order.rejection_reason = OrderRejectionReason.INSUFFICIENT_FUNDS
                order.order_id = self._generate_order_id()
                self._orders[order.order_id] = order
                return order.order_id

        # Validate position exists for sells (or can be converted via market pairs)
        if order.side == OrderSide.SELL:
            pos = self._portfolio.get_position(order.asset_id)
            position_qty = pos.quantity if pos else Decimal("0")
            if position_qty < order.quantity:
                # Try complement conversion if market pairs available
                if self._market_pairs:
                    pair = self._market_pairs.get_pair_for_token(order.asset_id)
                    if pair:
                        complement_token = pair.get_complement_token(order.asset_id)
                        if complement_token and complement_token != order.asset_id:
                            # SELL Yes → BUY No conversion (Polymarket two-token pairs)
                            complement_price = pair.get_complement_price(order.price or Decimal("0.5"))
                            self._logger.info(
                                "converting_sell_to_complement_buy",
                                original_asset=order.asset_id,
                                complement_asset=complement_token,
                                complement_price=str(complement_price),
                            )
                            # Convert to buy order on complement
                            order.side = OrderSide.BUY
                            order.asset_id = complement_token
                            order.price = complement_price
                        elif complement_token == order.asset_id:
                            # Self-pair (Kalshi single-ticker): the orderbook
                            # natively supports both sides. Allow the sell to
                            # proceed without position — it will match against
                            # the ask side of the book.
                            self._logger.info(
                                "allowing_native_sell_on_single_ticker",
                                asset=order.asset_id,
                            )
                        else:
                            self._reject_order_insufficient_position(order, position_qty)
                            return order.order_id
                    else:
                        self._reject_order_insufficient_position(order, position_qty)
                        return order.order_id
                else:
                    self._reject_order_insufficient_position(order, position_qty)
                    return order.order_id

        # Assign order ID
        order.order_id = self._generate_order_id()
        order.submitted_at = self._current_timestamp
        order.status = OrderStatus.PENDING

        # Store order
        self._orders[order.order_id] = order

        # Execute immediately if market order or IOC/FOK with market type
        if order.order_type == OrderType.MARKET:
            snapshot = self._current_orderbooks.get(order.asset_id)
            if snapshot:
                fills = self._execute_market_order(order, snapshot)
                self._logger.info(
                    "market_order_executed",
                    order_id=order.order_id,
                    fills=len(fills),
                    status=order.status.value,
                )
            else:
                self._logger.warning(
                    "no_orderbook_available",
                    order_id=order.order_id,
                    asset_id=order.asset_id,
                )
                order.status = OrderStatus.REJECTED
                order.rejection_reason = OrderRejectionReason.NO_LIQUIDITY
        elif order.order_type == OrderType.LIMIT:
            # Check if limit order is immediately marketable
            snapshot = self._current_orderbooks.get(order.asset_id)
            if snapshot:
                is_marketable = self._is_limit_order_marketable(order, snapshot)
                if is_marketable:
                    # Handle IOC and FOK for limit orders
                    if order.time_in_force == TimeInForce.FOK:
                        # Check if fully fillable before executing
                        can_fill = self._can_fully_fill_limit_order(order, snapshot)
                        if not can_fill:
                            order.status = OrderStatus.REJECTED
                            order.rejection_reason = OrderRejectionReason.FOK_NOT_FILLABLE
                            self._logger.info(
                                "fok_limit_order_rejected",
                                order_id=order.order_id,
                                quantity=str(order.quantity),
                                price=str(order.price),
                            )
                        else:
                            # Execute immediately
                            fills = self._execute_limit_order(order, snapshot)
                            self._logger.info(
                                "fok_limit_order_executed",
                                order_id=order.order_id,
                                fills=len(fills),
                            )
                    elif order.time_in_force == TimeInForce.IOC:
                        # Execute immediately and cancel remainder
                        fills = self._execute_limit_order(order, snapshot)
                        # Cancel any unfilled quantity
                        if order.status == OrderStatus.PARTIAL:
                            order.status = OrderStatus.CANCELLED
                            self._logger.info(
                                "ioc_limit_order_partial_cancelled",
                                order_id=order.order_id,
                                filled=str(order.filled_quantity),
                                cancelled=str(order.remaining_quantity),
                            )
                        else:
                            self._logger.info(
                                "ioc_limit_order_executed",
                                order_id=order.order_id,
                                fills=len(fills),
                            )
                    else:
                        # GTC - Execute immediately
                        fills = self._execute_limit_order(order, snapshot)
                        self._logger.info(
                            "limit_order_immediately_marketable",
                            order_id=order.order_id,
                            fills=len(fills),
                        )
                else:
                    # Not immediately marketable
                    if order.time_in_force == TimeInForce.IOC:
                        # IOC with no immediate fill - cancel
                        order.status = OrderStatus.CANCELLED
                        order.rejection_reason = OrderRejectionReason.NO_LIQUIDITY
                        self._logger.info(
                            "ioc_limit_order_not_marketable",
                            order_id=order.order_id,
                        )
                    elif order.time_in_force == TimeInForce.FOK:
                        # FOK with no immediate fill - reject
                        order.status = OrderStatus.REJECTED
                        order.rejection_reason = OrderRejectionReason.FOK_NOT_FILLABLE
                        self._logger.info(
                            "fok_limit_order_not_marketable",
                            order_id=order.order_id,
                        )
                    else:
                        # GTC - Add to queue for tracking
                        self._queue_simulator.add_order(order, snapshot)
                        # Add to pending index
                        if snapshot.asset_id not in self._pending_by_asset:
                            self._pending_by_asset[snapshot.asset_id] = set()
                        self._pending_by_asset[snapshot.asset_id].add(order.order_id)
            else:
                self._logger.warning(
                    "no_orderbook_available_for_limit",
                    order_id=order.order_id,
                    asset_id=order.asset_id,
                )
                order.rejection_reason = OrderRejectionReason.NO_LIQUIDITY

        if self._verbose:
            self._logger.info(
                "order_submitted",
                order_id=order.order_id,
                asset_id=order.asset_id,
                side=order.side.value,
                type=order.order_type.value,
                price=str(order.price) if order.price else None,
                quantity=str(order.quantity),
            )
        else:
            self._logger.debug(
                "order_submitted",
                order_id=order.order_id,
                asset_id=order.asset_id,
                side=order.side.value,
                type=order.order_type.value,
                price=str(order.price) if order.price else None,
                quantity=str(order.quantity),
            )

        return order.order_id

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel a pending order.

        Args:
            order_id: ID of order to cancel

        Returns:
            bool: True if cancelled, False if not found or already terminal
        """
        order = self._orders.get(order_id)
        if not order:
            return False

        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
            return False

        order.status = OrderStatus.CANCELLED

        # Remove from queue simulator if it was tracking this order
        self._queue_simulator.remove_order(order_id)

        # Remove from pending index
        if order.asset_id in self._pending_by_asset:
            self._pending_by_asset[order.asset_id].discard(order_id)

        self._logger.info("order_cancelled", order_id=order_id)
        return True

    def get_open_orders(self, asset_id: Optional[str] = None) -> list[Order]:
        """
        Get all open (pending/partial) orders.

        Args:
            asset_id: Optional filter by asset

        Returns:
            List of open orders
        """
        orders = [
            o for o in self._orders.values()
            if o.status in [OrderStatus.PENDING, OrderStatus.PARTIAL]
        ]

        if asset_id:
            orders = [o for o in orders if o.asset_id == asset_id]

        return orders

    def get_order_status(self, order_id: str) -> OrderStatus:
        """
        Get status of an order.

        Args:
            order_id: ID of order

        Returns:
            OrderStatus or REJECTED if not found
        """
        order = self._orders.get(order_id)
        return order.status if order else OrderStatus.REJECTED

    def process_orderbook_update(self, snapshot: OrderbookSnapshot) -> list[Fill]:
        """
        Process orderbook update and match pending limit orders.

        Updates current orderbook state, then checks all pending limit orders
        for this asset to see if they're now marketable. Also expires old orders
        if order_max_age_ms is set.

        Args:
            snapshot: New orderbook snapshot

        Returns:
            List of fills generated from limit order matching
        """
        self._current_orderbooks[snapshot.asset_id] = snapshot
        self._current_timestamp = snapshot.timestamp

        fills = []

        # Check for expired orders if timeout is enabled
        if self._order_max_age_ms is not None:
            self._expire_old_orders()

        # Check all pending limit orders for this asset (using index for O(1) lookup)
        pending_order_ids = self._pending_by_asset.get(snapshot.asset_id, set())
        pending_orders = [
            self._orders[oid] for oid in pending_order_ids
            if oid in self._orders
            and self._orders[oid].status in [OrderStatus.PENDING, OrderStatus.PARTIAL]
            and self._orders[oid].order_type == OrderType.LIMIT
        ]

        for order in pending_orders:
            # Check if order is now marketable
            is_marketable = self._is_limit_order_marketable(order, snapshot)

            if is_marketable:
                # Remove from queue if it was tracked
                self._queue_simulator.remove_order(order.order_id)

                new_fills = self._execute_limit_order(order, snapshot)
                fills.extend(new_fills)

        if fills:
            if self._verbose:
                self._logger.info(
                    "orderbook_update_matched_orders",
                    asset_id=snapshot.asset_id,
                    fills=len(fills),
                )
            else:
                self._logger.debug(
                    "orderbook_update_matched_orders",
                    asset_id=snapshot.asset_id,
                    fills=len(fills),
                )

        return fills

    def process_trade(self, trade: Trade) -> list[Fill]:
        """
        Process trade event and advance queue positions.

        Tracks trades for queue position awareness. Orders in queue
        advance their position as volume trades through at their price.
        When enough volume has traded, orders become eligible for fill.

        Args:
            trade: Trade event

        Returns:
            List of fills generated from queue advancement
        """
        self._current_timestamp = trade.timestamp

        # Get orders that should fill based on queue advancement
        order_ids_to_fill = self._queue_simulator.process_trade(trade)

        fills = []
        for order_id in order_ids_to_fill:
            order = self._orders.get(order_id)
            if not order:
                self._logger.warning(
                    "queue_fill_order_not_found",
                    order_id=order_id,
                )
                continue

            # Remove from queue
            self._queue_simulator.remove_order(order_id)

            # Capture remaining qty before _create_fill updates filled_quantity
            fill_qty = order.remaining_quantity

            # Create fill at the order's limit price
            fill = self._create_fill(
                order=order,
                quantity=fill_qty,
                price=order.price,
                reason=FillReason.QUEUE_REACHED,
                is_maker=True,  # Queue fills are maker liquidity
            )
            fills.append(fill)

            if self._verbose:
                self._logger.info(
                    "queue_order_filled",
                    order_id=order_id,
                    price=str(order.price),
                    quantity=str(fill_qty),
                )
            else:
                self._logger.debug(
                    "queue_order_filled",
                    order_id=order_id,
                    price=str(order.price),
                    quantity=str(fill_qty),
                )

        return fills

    def _execute_market_order(self, order: Order, snapshot: OrderbookSnapshot) -> list[Fill]:
        """
        Execute market order by walking orderbook levels.

        Args:
            order: Market order to execute
            snapshot: Current orderbook state

        Returns:
            List of fills (typically 1 aggregated fill)
        """
        levels = snapshot.asks if order.side == OrderSide.BUY else snapshot.bids
        if not levels:
            self._logger.warning(
                "no_liquidity_available",
                order_id=order.order_id,
                side=order.side.value,
            )
            order.status = OrderStatus.REJECTED
            order.rejection_reason = OrderRejectionReason.NO_LIQUIDITY
            return []

        # Walk levels and calculate fill
        remaining_qty = order.remaining_quantity
        total_cost = Decimal("0")
        total_qty_filled = Decimal("0")

        for level in levels:
            level_price = Decimal(str(level.price))
            level_size = Decimal(str(level.size))

            qty_from_level = min(remaining_qty, level_size)
            total_qty_filled += qty_from_level
            total_cost += qty_from_level * level_price
            remaining_qty -= qty_from_level

            if remaining_qty <= 0:
                break

        # Check if we filled enough
        if total_qty_filled == 0:
            order.status = OrderStatus.REJECTED
            order.rejection_reason = OrderRejectionReason.NO_LIQUIDITY
            return []

        # Handle time-in-force
        if order.time_in_force == TimeInForce.FOK and total_qty_filled < order.quantity:
            # Fill-or-Kill: reject if not fully filled
            order.status = OrderStatus.REJECTED
            order.rejection_reason = OrderRejectionReason.FOK_NOT_FILLABLE
            self._logger.info(
                "fok_order_rejected",
                order_id=order.order_id,
                requested=str(order.quantity),
                available=str(total_qty_filled),
            )
            return []

        # Calculate volume-weighted average price
        avg_price = total_cost / total_qty_filled if total_qty_filled > 0 else Decimal("0")

        # Create fill
        fill = self._create_fill(
            order=order,
            quantity=total_qty_filled,
            price=avg_price,
            reason=FillReason.IMMEDIATE,
            is_maker=False,  # Market orders are always taker
        )

        return [fill]

    def _execute_limit_order(self, order: Order, snapshot: OrderbookSnapshot) -> list[Fill]:
        """
        Execute limit order that became marketable.

        Similar to market order but respects limit price.

        Args:
            order: Limit order to execute
            snapshot: Current orderbook state

        Returns:
            List of fills
        """
        levels = snapshot.asks if order.side == OrderSide.BUY else snapshot.bids
        if not levels:
            return []

        remaining_qty = order.remaining_quantity
        total_cost = Decimal("0")
        total_qty_filled = Decimal("0")

        for level in levels:
            level_price = Decimal(str(level.price))
            level_size = Decimal(str(level.size))

            # Respect limit price
            if order.side == OrderSide.BUY:
                if level_price > order.price:
                    break  # No more favorable levels
            else:  # SELL
                if level_price < order.price:
                    break

            qty_from_level = min(remaining_qty, level_size)
            total_qty_filled += qty_from_level
            total_cost += qty_from_level * level_price
            remaining_qty -= qty_from_level

            if remaining_qty <= 0:
                break

        if total_qty_filled == 0:
            return []

        avg_price = total_cost / total_qty_filled

        fill = self._create_fill(
            order=order,
            quantity=total_qty_filled,
            price=avg_price,
            reason=FillReason.QUEUE_REACHED,
            is_maker=True,  # Limit orders that rest are makers
        )

        return [fill]

    def _create_fill(
        self,
        order: Order,
        quantity: Decimal,
        price: Decimal,
        reason: FillReason,
        is_maker: bool,
    ) -> Fill:
        """
        Create fill, update order state, and apply to portfolio.

        Args:
            order: Order being filled
            quantity: Fill quantity
            price: Fill price
            reason: Why this fill occurred
            is_maker: Whether this is maker liquidity

        Returns:
            Fill object
        """
        # Calculate fees
        fees = self._fee_schedule.calculate_fee(
            quantity=quantity,
            price=price,
            is_maker=is_maker,
        )

        # Create fill
        fill = Fill(
            fill_id=str(uuid4()),
            order_id=order.order_id,
            asset_id=order.asset_id,
            side=order.side,
            price=price,
            quantity=quantity,
            fees=fees,
            timestamp_ms=self._current_timestamp,
            is_maker=is_maker,
            fill_reason=reason,
        )

        # Update order state
        order.filled_quantity += quantity

        # Recalculate average fill price
        if order.avg_fill_price is None:
            order.avg_fill_price = price
        else:
            # Weighted average
            prev_value = order.avg_fill_price * (order.filled_quantity - quantity)
            new_value = price * quantity
            order.avg_fill_price = (prev_value + new_value) / order.filled_quantity

        # Update order status
        if order.is_fully_filled:
            order.status = OrderStatus.FILLED
            # Remove from pending index when fully filled
            if order.asset_id in self._pending_by_asset:
                self._pending_by_asset[order.asset_id].discard(order.order_id)
        elif order.filled_quantity > 0:
            order.status = OrderStatus.PARTIAL
            # Check for dust orders - auto-cancel if remaining is too small
            if order.remaining_quantity < self._min_order_size:
                order.status = OrderStatus.CANCELLED
                # Remove from queue simulator if tracked
                self._queue_simulator.remove_order(order.order_id)
                # Remove from pending index
                if order.asset_id in self._pending_by_asset:
                    self._pending_by_asset[order.asset_id].discard(order.order_id)
                self._logger.info(
                    "dust_order_cancelled",
                    order_id=order.order_id,
                    remaining_quantity=str(order.remaining_quantity),
                    min_size=str(self._min_order_size),
                )

        # Apply to portfolio
        self._portfolio.apply_fill(fill)

        if self._verbose:
            self._logger.info(
                "fill_created",
                fill_id=fill.fill_id,
                order_id=order.order_id,
                asset_id=order.asset_id,
                side=order.side.value,
                price=str(price),
                quantity=str(quantity),
                fees=str(fees),
                is_maker=is_maker,
                reason=reason.value,
            )
        else:
            self._logger.debug(
                "fill_created",
                fill_id=fill.fill_id,
                order_id=order.order_id,
                asset_id=order.asset_id,
                side=order.side.value,
                price=str(price),
                quantity=str(quantity),
                fees=str(fees),
                is_maker=is_maker,
                reason=reason.value,
            )

        return fill

    def _generate_order_id(self) -> str:
        """Generate unique order ID."""
        self._order_counter += 1
        return f"order_{self._order_counter}"

    def _reject_order_insufficient_position(self, order: Order, current_position: Decimal):
        """Reject order due to insufficient position."""
        self._logger.warning(
            "order_rejected_insufficient_position",
            asset_id=order.asset_id,
            required=str(order.quantity),
            available=str(current_position),
        )
        order.status = OrderStatus.REJECTED
        order.rejection_reason = OrderRejectionReason.INSUFFICIENT_POSITION
        order.order_id = self._generate_order_id()
        self._orders[order.order_id] = order

    def _is_limit_order_marketable(self, order: Order, snapshot: OrderbookSnapshot) -> bool:
        """
        Check if a limit order would be immediately marketable.

        A limit order is marketable if it crosses the spread:
        - Buy limit: price >= best ask
        - Sell limit: price <= best bid

        Args:
            order: Limit order to check
            snapshot: Current orderbook snapshot

        Returns:
            True if order is marketable, False otherwise
        """
        if order.side == OrderSide.BUY:
            # Buy limit is marketable if price >= best ask
            if snapshot.best_ask and order.price >= Decimal(str(snapshot.best_ask)):
                return True
        else:  # SELL
            # Sell limit is marketable if price <= best bid
            if snapshot.best_bid and order.price <= Decimal(str(snapshot.best_bid)):
                return True

        return False

    def _can_fully_fill_limit_order(self, order: Order, snapshot: OrderbookSnapshot) -> bool:
        """
        Check if a limit order can be fully filled with available liquidity.

        Used for FOK (Fill-or-Kill) validation.

        Args:
            order: Limit order to check
            snapshot: Current orderbook snapshot

        Returns:
            True if order can be fully filled, False otherwise
        """
        levels = snapshot.asks if order.side == OrderSide.BUY else snapshot.bids
        if not levels:
            return False

        available_qty = Decimal("0")

        for level in levels:
            level_price = Decimal(str(level.price))
            level_size = Decimal(str(level.size))

            # Respect limit price
            if order.side == OrderSide.BUY:
                if level_price > order.price:
                    break  # No more favorable levels
            else:  # SELL
                if level_price < order.price:
                    break

            available_qty += level_size

            if available_qty >= order.remaining_quantity:
                return True

        return False

    def _expire_old_orders(self):
        """Cancel orders that have exceeded the maximum age."""
        if self._order_max_age_ms is None:
            return

        expired_orders = [
            o for o in self._orders.values()
            if o.status in [OrderStatus.PENDING, OrderStatus.PARTIAL]
            and o.submitted_at is not None
            and (self._current_timestamp - o.submitted_at) > self._order_max_age_ms
        ]

        for order in expired_orders:
            order.status = OrderStatus.CANCELLED
            order.rejection_reason = OrderRejectionReason.ORDER_EXPIRED

            # Remove from queue simulator if tracked
            self._queue_simulator.remove_order(order.order_id)

            # Remove from pending index
            if order.asset_id in self._pending_by_asset:
                self._pending_by_asset[order.asset_id].discard(order.order_id)

            self._logger.info(
                "order_expired",
                order_id=order.order_id,
                age_ms=self._current_timestamp - order.submitted_at,
                max_age_ms=self._order_max_age_ms,
            )
