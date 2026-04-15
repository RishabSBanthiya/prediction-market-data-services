"""
Orderbook signal-driven strategy for prediction market backtesting.

Uses the OrderbookSignalAnalyzer to compute microprice, book imbalance,
and liquidity node signals, then takes directional positions when signals
align with sufficient confidence.

Trading logic:
- BUY when: imbalance is bullish + microprice diverges above mid +
  price is heatseeking toward bid-side liquidity nodes
- SELL when: imbalance is bearish + microprice diverges below mid +
  price is heatseeking toward ask-side liquidity nodes
- All signals must exceed configurable thresholds and pass the
  confidence gate before generating orders

Usage:
    from backtest.strategies.examples.signal_strategy import OrderbookSignalStrategy

    strategy = OrderbookSignalStrategy(
        imbalance_threshold=0.15,
        microprice_div_threshold=0.003,
        min_confidence=0.4,
        order_size=Decimal("25"),
        max_position=Decimal("200"),
    )
"""

from decimal import Decimal
from typing import Optional
import logging

from models.orderbook import OrderbookSnapshot
from models.trade import Trade

from ...core.strategy import Strategy, BacktestContext
from ...models.order import Order, OrderSide, OrderType, Fill
from .orderbook_signals import OrderbookSignalAnalyzer, SignalSnapshot


logger = logging.getLogger(__name__)

MIN_PRICE = Decimal("0.01")
MAX_PRICE = Decimal("0.99")


class OrderbookSignalStrategy(Strategy):
    """
    Directional strategy driven by orderbook microstructure signals.

    Enters positions when microprice divergence, book imbalance, and
    liquidity node direction all agree, gated by signal confidence.
    Exits when signals reverse or flatten.

    Parameters:
        imbalance_threshold: How far from 0.5 imbalance must be to signal.
            E.g., 0.15 means buy when imbalance > 0.65, sell when < 0.35.
        microprice_div_threshold: Minimum |microprice - mid| to signal.
            In price units (e.g., 0.003 = 0.3 cents).
        min_confidence: Minimum signal_confidence to allow trades (0-1).
        order_size: Number of contracts per order.
        max_position: Maximum position size per asset (long or short).
        rolling_window: Which rolling window (seconds) to use for
            smoothed signal evaluation. Must be in the analyzer's
            rolling_windows list.
        edge_offset_bps: Basis points of edge to demand beyond the signal.
            Orders are placed at microprice +/- this offset instead of
            at the raw microprice, to capture spread.
        exit_imbalance_band: Imbalance must return within this band
            around 0.5 to trigger position exit (e.g., 0.05 means
            exit when imbalance is between 0.45 and 0.55).
        n_levels: Number of orderbook levels for signal computation.
        node_threshold_multiplier: Multiplier over median size for node detection.
        min_total_depth: Minimum total book depth to compute signals.
        target_assets: Optional list of asset IDs to trade.
    """

    def __init__(
        self,
        imbalance_threshold: float = 0.15,
        microprice_div_threshold: float = 0.003,
        min_confidence: float = 0.4,
        order_size: Decimal = Decimal("25"),
        max_position: Decimal = Decimal("200"),
        rolling_window: int = 5,
        edge_offset_bps: int = 50,
        exit_imbalance_band: float = 0.05,
        n_levels: int = 3,
        node_threshold_multiplier: float = 2.0,
        min_total_depth: float = 100.0,
        target_assets: Optional[list[str]] = None,
    ):
        super().__init__(name="OrderbookSignalStrategy")

        # Signal thresholds
        self.imbalance_threshold = imbalance_threshold
        self.microprice_div_threshold = microprice_div_threshold
        self.min_confidence = min_confidence
        self.exit_imbalance_band = exit_imbalance_band

        # Order parameters
        self.order_size = order_size
        self.max_position = max_position
        self.edge_offset_bps = edge_offset_bps

        # Signal analyzer
        self.rolling_window = rolling_window
        rolling_windows = [5, 30, 300]
        if rolling_window not in rolling_windows:
            raise ValueError(
                f"rolling_window={rolling_window} not in {rolling_windows}"
            )
        self.analyzer = OrderbookSignalAnalyzer(
            n_levels=n_levels,
            node_threshold_multiplier=node_threshold_multiplier,
            min_total_depth=min_total_depth,
            rolling_windows=rolling_windows,
        )

        self.target_assets = set(target_assets) if target_assets else None

        # Per-asset state
        self._active_order_ids: dict[str, str] = {}  # asset_id -> order_id
        self._last_signal: dict[str, SignalSnapshot] = {}
        self._trade_count = 0

    # ================================================================
    # Lifecycle
    # ================================================================

    def on_start(self, context: BacktestContext) -> None:
        logger.info(
            "OrderbookSignalStrategy starting | imbalance_thresh=%.2f "
            "microprice_div_thresh=%.4f min_confidence=%.2f "
            "order_size=%s max_position=%s edge_offset_bps=%d",
            self.imbalance_threshold,
            self.microprice_div_threshold,
            self.min_confidence,
            self.order_size,
            self.max_position,
            self.edge_offset_bps,
        )

    def on_end(self, context: BacktestContext) -> None:
        logger.info(
            "OrderbookSignalStrategy finished | trades=%d assets_seen=%d",
            self._trade_count,
            len(self._last_signal),
        )

    # ================================================================
    # Core logic
    # ================================================================

    def on_orderbook(
        self,
        snapshot: OrderbookSnapshot,
        is_forward_filled: bool,
    ) -> None:
        """
        Process orderbook update through signal analyzer and act on signals.

        Flow:
        1. Feed snapshot to analyzer -> get signal
        2. Check if we have a position to potentially exit
        3. Check for entry signals if no/small position
        """
        # Skip forward-filled snapshots — no new information
        if is_forward_filled:
            return

        asset_id = snapshot.asset_id

        if self.target_assets is not None and asset_id not in self.target_assets:
            return

        # Compute signals
        signal = self.analyzer.update(snapshot)
        if signal is None:
            return

        self._last_signal[asset_id] = signal

        current_position = self._get_position_qty(asset_id)
        if current_position is None:
            return

        if current_position != Decimal("0"):
            self._check_exit(asset_id, signal, current_position)
            return

        self._check_entry(asset_id, signal, current_position)

    def on_fill(self, fill: Fill) -> None:
        self._trade_count += 1
        logger.info(
            "Signal fill: %s %s @ %s (qty=%s, reason=%s)",
            fill.side.value.upper(),
            fill.asset_id,
            fill.price,
            fill.quantity,
            fill.fill_reason.value,
        )
        # Clear tracked order if filled
        if fill.asset_id in self._active_order_ids:
            self._maybe_clear_order(fill.asset_id, fill.order_id)

    def on_trade(self, trade: Trade) -> None:
        pass

    # ================================================================
    # Entry logic
    # ================================================================

    def _check_entry(self, asset_id: str, signal: SignalSnapshot, current_position: Decimal) -> None:
        """
        Evaluate entry signals. All three must agree:
        1. Book imbalance exceeds threshold in one direction
        2. Microprice divergence confirms the direction
        3. Nearest liquidity node is in the same direction (heatseeking)

        Plus: confidence must exceed minimum.
        """
        if signal.signal_confidence < self.min_confidence:
            return

        # Already have a pending order for this asset
        if asset_id in self._active_order_ids:
            return

        # Use rolling values for smoother signals when available
        imbalance = signal.rolling_imbalance.get(
            self.rolling_window, signal.book_imbalance
        )
        microprice_div = signal.rolling_microprice_vs_mid.get(
            self.rolling_window, signal.microprice_vs_mid
        )

        # Check BUY signal: bullish imbalance + positive microprice div + bid node
        bullish_imbalance = imbalance > (0.5 + self.imbalance_threshold)
        bullish_microprice = microprice_div > self.microprice_div_threshold
        bullish_nodes = signal.nearest_node_direction == "bid"

        if bullish_imbalance and bullish_microprice and bullish_nodes:
            self._submit_entry(asset_id, signal, OrderSide.BUY, current_position)
            return

        # Check SELL signal: bearish imbalance + negative microprice div + ask node
        bearish_imbalance = imbalance < (0.5 - self.imbalance_threshold)
        bearish_microprice = microprice_div < -self.microprice_div_threshold
        bearish_nodes = signal.nearest_node_direction == "ask"

        if bearish_imbalance and bearish_microprice and bearish_nodes:
            self._submit_entry(asset_id, signal, OrderSide.SELL, current_position)
            return

    def _submit_entry(
        self,
        asset_id: str,
        signal: SignalSnapshot,
        side: OrderSide,
        current_position: Decimal,
    ) -> None:
        """Submit a limit order at microprice with edge offset."""
        microprice = Decimal(str(round(signal.microprice, 4)))
        offset = microprice * Decimal(str(self.edge_offset_bps)) / Decimal("10000")

        if side == OrderSide.BUY:
            price = (microprice - offset).quantize(Decimal("0.01"))
        else:
            price = (microprice + offset).quantize(Decimal("0.01"))

        price = self._clamp_price(price)

        if side == OrderSide.BUY:
            room = self.max_position - current_position
        else:
            room = self.max_position + current_position

        qty = min(self.order_size, room)
        if qty <= Decimal("0"):
            return

        order = Order(
            asset_id=asset_id,
            side=side,
            order_type=OrderType.LIMIT,
            price=price,
            quantity=qty,
        )
        order_id = self.submit_order(order)
        self._active_order_ids[asset_id] = order_id

        logger.info(
            "Entry %s %s @ %s (qty=%s, imbalance=%.3f, microprice_div=%.5f, "
            "confidence=%.2f, node_dir=%s)",
            side.value.upper(),
            asset_id,
            price,
            qty,
            signal.book_imbalance,
            signal.microprice_vs_mid,
            signal.signal_confidence,
            signal.nearest_node_direction,
        )

    # ================================================================
    # Exit logic
    # ================================================================

    def _check_exit(
        self,
        asset_id: str,
        signal: SignalSnapshot,
        current_position: Decimal,
    ) -> None:
        """
        Exit when signals flatten or reverse.

        Exit conditions:
        - Imbalance returns to neutral band (within exit_imbalance_band of 0.5)
        - Signals flip to the opposite direction
        """
        imbalance = signal.rolling_imbalance.get(
            self.rolling_window, signal.book_imbalance
        )

        is_long = current_position > Decimal("0")
        is_short = current_position < Decimal("0")

        # Cancel any pending entry orders first
        if asset_id in self._active_order_ids:
            self.cancel_order(self._active_order_ids.pop(asset_id))

        # Exit long if imbalance goes neutral or bearish
        should_exit = False
        if is_long:
            neutral = abs(imbalance - 0.5) < self.exit_imbalance_band
            bearish_flip = imbalance < (0.5 - self.imbalance_threshold)
            should_exit = neutral or bearish_flip

        # Exit short if imbalance goes neutral or bullish
        if is_short:
            neutral = abs(imbalance - 0.5) < self.exit_imbalance_band
            bullish_flip = imbalance > (0.5 + self.imbalance_threshold)
            should_exit = neutral or bullish_flip

        if not should_exit:
            return

        # Submit exit order at microprice (market-taking to get out)
        exit_side = OrderSide.SELL if is_long else OrderSide.BUY
        exit_qty = abs(current_position)
        microprice = Decimal(str(round(signal.microprice, 4)))

        if exit_side == OrderSide.SELL:
            # Sell slightly below microprice to ensure fill
            price = (microprice - Decimal("0.01")).quantize(Decimal("0.01"))
        else:
            # Buy slightly above microprice to ensure fill
            price = (microprice + Decimal("0.01")).quantize(Decimal("0.01"))

        price = self._clamp_price(price)

        order = Order(
            asset_id=asset_id,
            side=exit_side,
            order_type=OrderType.LIMIT,
            price=price,
            quantity=exit_qty,
        )
        order_id = self.submit_order(order)
        self._active_order_ids[asset_id] = order_id

        logger.info(
            "Exit %s %s @ %s (qty=%s, imbalance=%.3f, position=%s)",
            exit_side.value.upper(),
            asset_id,
            price,
            exit_qty,
            imbalance,
            current_position,
        )

    # ================================================================
    # Helpers
    # ================================================================

    def _get_position_qty(self, asset_id: str) -> Optional[Decimal]:
        """Returns position quantity, or None if portfolio is unavailable."""
        try:
            position = self.portfolio.get_position(asset_id)
            return position.quantity if position is not None else Decimal("0")
        except RuntimeError:
            return None

    @staticmethod
    def _clamp_price(price: Decimal) -> Decimal:
        return max(MIN_PRICE, min(MAX_PRICE, price))

    def _maybe_clear_order(self, asset_id: str, fill_order_id: str) -> None:
        """Clear tracked order if it matches the filled order."""
        tracked = self._active_order_ids.get(asset_id)
        if tracked != fill_order_id:
            return
        try:
            open_orders = self.get_open_orders(asset_id)
            still_open = any(o.order_id == fill_order_id for o in open_orders)
            if not still_open:
                self._active_order_ids.pop(asset_id, None)
        except RuntimeError:
            self._active_order_ids.pop(asset_id, None)
