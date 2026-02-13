"""
Simple Market Maker strategy for prediction market backtesting.

This strategy places limit orders on both sides of the orderbook at a
configurable spread around the mid price. It adjusts its quoting based
on current inventory to manage risk.

This is a TEACHING EXAMPLE -- it prioritizes clarity and readability over
sophistication. A production market maker would use more advanced inventory
management, dynamic spread adjustment, and risk controls.

Usage:
    from backtest.strategies.examples.market_maker import SimpleMarketMaker

    strategy = SimpleMarketMaker(
        spread_bps=300,       # 3 cents on a $1 market
        order_size=Decimal("10"),
        max_position=Decimal("100"),
    )
"""

from decimal import Decimal
from typing import Optional
import logging

from models.orderbook import OrderbookSnapshot
from models.trade import Trade

from ...core.strategy import Strategy, BacktestContext
from ...models.order import Order, OrderSide, OrderType, Fill


logger = logging.getLogger(__name__)

# Prediction market price bounds: contracts trade between $0.01 and $0.99
MIN_PRICE = Decimal("0.01")
MAX_PRICE = Decimal("0.99")

# Basis point divisor: 1 bps = 0.0001
BPS_DIVISOR = Decimal("10000")


class SimpleMarketMaker(Strategy):
    """
    Simple market making strategy for prediction markets.

    Places limit orders on both sides of the orderbook at a fixed spread
    around the mid price. Manages inventory to stay within position limits.

    The strategy works in three steps on each orderbook update:
    1. Determine if existing quotes are stale (mid moved beyond threshold)
    2. Cancel stale orders
    3. Place new bid and ask orders with inventory-adjusted spreads

    Parameters:
        spread_bps: Spread in basis points (e.g., 300 = 3 cents on a $1 market).
                    This is the TOTAL spread; each side gets half.
        order_size: Number of contracts per order.
        max_position: Maximum position size (long or short) per asset.
                      The strategy will not place orders that would exceed this.
        target_assets: Optional list of asset IDs to trade. If None, the
                       strategy will quote on all assets it receives data for.
        requote_threshold_bps: If the mid price moves more than this many basis
                               points from our last quoted mid, cancel and requote.
                               Default is 50 bps (0.5 cents).
    """

    def __init__(
        self,
        spread_bps: int = 300,
        order_size: Decimal = Decimal("10"),
        max_position: Decimal = Decimal("100"),
        target_assets: Optional[list[str]] = None,
        requote_threshold_bps: int = 50,
    ):
        super().__init__(name="SimpleMarketMaker")

        # ---- Configuration ----
        self.spread_bps = spread_bps
        self.order_size = order_size
        self.max_position = max_position
        self.target_assets = set(target_assets) if target_assets else None
        self.requote_threshold_bps = requote_threshold_bps

        # ---- Per-asset tracking state ----
        # Maps asset_id -> order_id for our current bid order
        self._bid_order_ids: dict[str, str] = {}
        # Maps asset_id -> order_id for our current ask order
        self._ask_order_ids: dict[str, str] = {}
        # Maps asset_id -> last mid price we quoted around (as Decimal)
        self._last_quoted_mid: dict[str, Decimal] = {}

    # ================================================================
    # Lifecycle hooks
    # ================================================================

    def on_start(self, context: BacktestContext) -> None:
        """Called once before the backtest begins. Log our configuration."""
        logger.info(
            "SimpleMarketMaker starting | spread_bps=%d order_size=%s "
            "max_position=%s requote_threshold_bps=%d target_assets=%s",
            self.spread_bps,
            self.order_size,
            self.max_position,
            self.requote_threshold_bps,
            self.target_assets or "ALL",
        )

    def on_end(self, context: BacktestContext) -> None:
        """Called once after the backtest completes. Log summary stats."""
        logger.info(
            "SimpleMarketMaker finished | assets_quoted=%d",
            len(self._last_quoted_mid),
        )

    # ================================================================
    # Core strategy logic: on_orderbook
    # ================================================================

    def on_orderbook(
        self,
        snapshot: OrderbookSnapshot,
        is_forward_filled: bool,
    ) -> None:
        """
        React to an orderbook update by quoting on both sides.

        This is the main entry point called by the backtest engine on every
        orderbook snapshot. The logic flow:

        1. Skip forward-filled snapshots (no real price change)
        2. Skip assets not in our target list
        3. Compute mid price; skip if unavailable
        4. Check if we need to requote (price moved beyond threshold)
        5. Cancel stale orders if requoting
        6. Calculate inventory-adjusted bid/ask prices
        7. Clamp prices to prediction market bounds [0.01, 0.99]
        8. Submit new orders respecting position limits
        """
        # Step 1: Skip forward-filled snapshots.
        # Forward-filled snapshots are synthetic copies emitted at regular
        # intervals when the orderbook hasn't changed. There's no new
        # information, so requoting would waste cycles and generate noise.
        if is_forward_filled:
            return

        asset_id = snapshot.asset_id

        # Step 2: Skip assets we don't want to trade.
        # If target_assets is configured, only trade those specific assets.
        if self.target_assets is not None and asset_id not in self.target_assets:
            return

        # Step 3: Calculate mid price from the snapshot.
        # The mid price is the average of best bid and best ask. If either
        # side of the book is empty, we can't determine a fair price.
        mid_price_float = snapshot.mid_price
        if mid_price_float is None:
            return

        # Convert to Decimal for precise arithmetic.
        # Using str() intermediate to avoid float precision issues.
        mid_price = Decimal(str(mid_price_float))

        # Step 4: Check if existing quotes need updating.
        # We only requote if the mid has moved significantly (beyond our
        # threshold). This avoids excessive order churn on small moves.
        if not self._should_requote(asset_id, mid_price):
            return

        # Step 5: Cancel stale orders before placing new ones.
        # We cancel both sides so our quotes are always fresh and symmetric.
        self._cancel_existing_orders(asset_id)

        # Step 6: Calculate inventory-adjusted bid and ask prices.
        bid_price, ask_price = self._calculate_quote_prices(asset_id, mid_price)

        # Step 7: Clamp prices to valid prediction market range [0.01, 0.99].
        bid_price = max(MIN_PRICE, min(MAX_PRICE, bid_price))
        ask_price = max(MIN_PRICE, min(MAX_PRICE, ask_price))

        # Sanity check: bid must be below ask to avoid self-crossing.
        if bid_price >= ask_price:
            logger.debug(
                "Skipping quotes for %s: bid=%s >= ask=%s (spread too tight after clamping)",
                asset_id,
                bid_price,
                ask_price,
            )
            return

        # Step 8: Submit orders respecting position limits.
        self._submit_quotes(asset_id, bid_price, ask_price)

        # Record the mid we quoted around for the threshold check next time.
        self._last_quoted_mid[asset_id] = mid_price

    # ================================================================
    # Fill and trade callbacks
    # ================================================================

    def on_fill(self, fill: Fill) -> None:
        """
        Called when one of our orders is filled (partially or fully).

        We log the fill for debugging and clear our tracked order ID if
        the order was fully filled. The portfolio is already updated by
        the engine before this callback fires.
        """
        logger.info(
            "Fill: %s %s %s @ %s (qty=%s, fees=%s, maker=%s)",
            fill.side.value.upper(),
            fill.asset_id,
            fill.fill_reason.value,
            fill.price,
            fill.quantity,
            fill.fees,
            fill.is_maker,
        )

        # Clean up tracked order IDs for filled orders.
        # If the order is fully filled, we need to remove it from our tracking
        # so the next orderbook update will place a fresh quote.
        if fill.side == OrderSide.BUY:
            if fill.asset_id in self._bid_order_ids:
                # Check if this order is still open; if not, clear the tracking.
                self._maybe_clear_filled_order(
                    fill.asset_id, fill.order_id, self._bid_order_ids
                )
        elif fill.side == OrderSide.SELL:
            if fill.asset_id in self._ask_order_ids:
                self._maybe_clear_filled_order(
                    fill.asset_id, fill.order_id, self._ask_order_ids
                )

    def on_trade(self, trade: Trade) -> None:
        """
        Called when a trade occurs on the market tape.

        For this simple strategy, we just log trades for observability.
        A more sophisticated strategy might use trade flow to adjust
        spreads or detect informed order flow.
        """
        logger.debug(
            "Trade: %s %s @ %.4f (size=%.2f)",
            trade.asset_id,
            trade.side,
            trade.price,
            trade.size,
        )

    # ================================================================
    # Internal helpers
    # ================================================================

    def _should_requote(self, asset_id: str, current_mid: Decimal) -> bool:
        """
        Determine if we need to update our quotes for this asset.

        Returns True if:
        - We have no existing quotes for this asset (first time quoting)
        - The mid price has moved beyond the requote threshold since our
          last quote

        This prevents excessive order churn on small price moves while
        ensuring we stay competitive when the market moves.
        """
        # Always quote if we haven't quoted this asset before.
        if asset_id not in self._last_quoted_mid:
            return True

        # Also requote if we no longer have live orders on either side
        # (e.g., they were filled).
        has_bid = asset_id in self._bid_order_ids
        has_ask = asset_id in self._ask_order_ids
        if not has_bid or not has_ask:
            return True

        # Calculate how far the mid has moved in basis points.
        last_mid = self._last_quoted_mid[asset_id]
        if last_mid == Decimal("0"):
            return True

        move_bps = abs(current_mid - last_mid) / last_mid * BPS_DIVISOR

        return move_bps >= self.requote_threshold_bps

    def _cancel_existing_orders(self, asset_id: str) -> None:
        """
        Cancel any outstanding bid and ask orders for the given asset.

        We cancel before requoting to ensure clean state. If cancellation
        fails (e.g., order already filled), we just clear the tracking.
        """
        if asset_id in self._bid_order_ids:
            order_id = self._bid_order_ids.pop(asset_id)
            self.cancel_order(order_id)

        if asset_id in self._ask_order_ids:
            order_id = self._ask_order_ids.pop(asset_id)
            self.cancel_order(order_id)

    def _calculate_quote_prices(
        self, asset_id: str, mid_price: Decimal
    ) -> tuple[Decimal, Decimal]:
        """
        Calculate bid and ask prices with inventory-based spread adjustment.

        The base spread (spread_bps) is split evenly between bid and ask.
        We then skew the spread based on our current inventory:

        - If we are LONG, we widen the bid side (less aggressive buying)
          to discourage accumulating more inventory.
        - If we are SHORT, we widen the ask side (less aggressive selling)
          to discourage going further short.

        The skew is proportional to the fraction of max_position used.
        At max_position, the skew adds 50% to that side's half-spread.

        Example with spread_bps=300 (1.5% half-spread) and 50% of max_position:
          - Base half-spread: 150 bps on each side
          - Long skew: bid gets +75 bps, ask stays at 150 bps
          - Result: bid 225 bps below mid, ask 150 bps above mid

        Returns:
            Tuple of (bid_price, ask_price) as Decimals
        """
        # Base half-spread in decimal form.
        # E.g., 300 bps -> half_spread = 150 bps = 0.0150
        half_spread = Decimal(str(self.spread_bps)) / (Decimal("2") * BPS_DIVISOR)

        # Determine current inventory for this asset.
        # If the portfolio hasn't been injected yet (shouldn't happen, but
        # handle gracefully), assume flat position.
        current_position = Decimal("0")
        try:
            position = self.portfolio.get_position(asset_id)
            if position is not None:
                current_position = position.quantity
        except RuntimeError:
            # Portfolio not injected yet -- defensive guard.
            pass

        # Calculate inventory skew as a fraction of max_position.
        # inventory_fraction ranges from -1.0 (max short) to +1.0 (max long).
        if self.max_position > Decimal("0"):
            inventory_fraction = current_position / self.max_position
        else:
            inventory_fraction = Decimal("0")

        # Clamp to [-1, 1] to handle edge cases.
        inventory_fraction = max(Decimal("-1"), min(Decimal("1"), inventory_fraction))

        # Apply asymmetric skew.
        # Positive inventory_fraction (long) -> widen bid, tighten ask.
        # Negative inventory_fraction (short) -> tighten bid, widen ask.
        # The skew factor maxes out at 50% of the half-spread.
        skew_factor = Decimal("0.5")
        bid_half_spread = half_spread * (Decimal("1") + skew_factor * inventory_fraction)
        ask_half_spread = half_spread * (Decimal("1") - skew_factor * inventory_fraction)

        # Ensure spreads don't go negative (defensive).
        bid_half_spread = max(Decimal("0"), bid_half_spread)
        ask_half_spread = max(Decimal("0"), ask_half_spread)

        # Calculate final prices.
        bid_price = mid_price * (Decimal("1") - bid_half_spread)
        ask_price = mid_price * (Decimal("1") + ask_half_spread)

        # Round to 2 decimal places (cents) for clean order prices.
        bid_price = bid_price.quantize(Decimal("0.01"))
        ask_price = ask_price.quantize(Decimal("0.01"))

        return bid_price, ask_price

    def _submit_quotes(
        self, asset_id: str, bid_price: Decimal, ask_price: Decimal
    ) -> None:
        """
        Submit bid and ask limit orders, respecting position limits.

        Position limit checks:
        - Don't submit a BUY if we're already at +max_position (fully long).
        - Don't submit a SELL if we're already at -max_position (fully short).

        In prediction markets, selling typically means selling contracts you
        already hold. A negative position implies short exposure, which may
        or may not be supported depending on the platform.
        """
        current_position = Decimal("0")
        try:
            position = self.portfolio.get_position(asset_id)
            if position is not None:
                current_position = position.quantity
        except RuntimeError:
            pass

        # Submit BID (buy order) if not at max long position.
        if current_position < self.max_position:
            # Ensure we don't exceed max_position with this order.
            # Reduce order size if needed so position stays within limits.
            room_to_buy = self.max_position - current_position
            buy_size = min(self.order_size, room_to_buy)

            if buy_size > Decimal("0"):
                buy_order = Order(
                    asset_id=asset_id,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    price=bid_price,
                    quantity=buy_size,
                )
                order_id = self.submit_order(buy_order)
                self._bid_order_ids[asset_id] = order_id

                logger.debug(
                    "Placed BID: %s %s @ %s (qty=%s)",
                    asset_id,
                    order_id,
                    bid_price,
                    buy_size,
                )

        # Submit ASK (sell order) if not at max short position.
        if current_position > -self.max_position:
            # Ensure we don't exceed max short position with this order.
            room_to_sell = self.max_position + current_position
            sell_size = min(self.order_size, room_to_sell)

            if sell_size > Decimal("0"):
                sell_order = Order(
                    asset_id=asset_id,
                    side=OrderSide.SELL,
                    order_type=OrderType.LIMIT,
                    price=ask_price,
                    quantity=sell_size,
                )
                order_id = self.submit_order(sell_order)
                self._ask_order_ids[asset_id] = order_id

                logger.debug(
                    "Placed ASK: %s %s @ %s (qty=%s)",
                    asset_id,
                    order_id,
                    ask_price,
                    sell_size,
                )

    def _maybe_clear_filled_order(
        self,
        asset_id: str,
        fill_order_id: str,
        order_id_map: dict[str, str],
    ) -> None:
        """
        Clear a tracked order ID if the filled order matches and is no longer open.

        After a fill, we check if the order is still open (partially filled).
        If it's fully filled or no longer in our open orders, we remove it
        from tracking so a new quote will be placed on the next update.
        """
        tracked_order_id = order_id_map.get(asset_id)
        if tracked_order_id != fill_order_id:
            return

        # Check if the order is still open (might be partially filled).
        try:
            open_orders = self.get_open_orders(asset_id)
            still_open = any(o.order_id == fill_order_id for o in open_orders)
            if not still_open:
                order_id_map.pop(asset_id, None)
        except RuntimeError:
            # Execution engine not available -- clear defensively.
            order_id_map.pop(asset_id, None)
