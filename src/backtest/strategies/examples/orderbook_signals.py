"""
Orderbook signal analysis module for prediction markets.

Extracts price signals from orderbook depth data:
- Microprice (volume-weighted mid)
- Book imbalance (bid/ask depth ratio)
- Liquidity node detection (heatseeking)
- Signal quality / confidence scoring

This is a standalone module that receives orderbook updates and emits
signal snapshots. Can be consumed by any strategy.

Usage:
    from backtest.strategies.examples.orderbook_signals import (
        OrderbookSignalAnalyzer, SignalSnapshot
    )

    analyzer = OrderbookSignalAnalyzer(n_levels=3)
    signal = analyzer.update(snapshot)
    if signal and signal.signal_confidence > 0.5:
        print(f"Microprice: {signal.microprice}")
        print(f"Imbalance: {signal.book_imbalance}")
"""

import bisect
from collections import deque
from dataclasses import dataclass, field
from typing import Literal, Optional

from models.orderbook import OrderbookSnapshot


@dataclass
class LiquidityNode:
    """A price level with significantly higher resting size than surrounding levels."""
    price: float
    size: float


@dataclass
class SignalSnapshot:
    """
    Output of the signal analyzer for a single orderbook update.

    Attributes:
        timestamp_ms: Timestamp of the orderbook snapshot
        asset_id: Asset identifier
        microprice: Volume-weighted mid price across top N levels
        naive_mid: Simple (best_bid + best_ask) / 2
        microprice_vs_mid: Signed divergence (microprice - naive_mid).
            Positive = microprice above mid (bullish lean).
        book_imbalance: Ratio of bid depth to total depth across top N levels.
            Ranges 0 (all asks) to 1 (all bids). >0.5 = bullish.
        bid_nodes: Top liquidity clusters on the bid side
        ask_nodes: Top liquidity clusters on the ask side
        nearest_node_direction: Which side's node the microprice leans toward
        signal_confidence: 0-1 score based on depth, spread, and book quality
        rolling_imbalance: Windowed average imbalance {window_seconds: value}
        rolling_microprice_vs_mid: Windowed average divergence {window_seconds: value}
    """
    timestamp_ms: int
    asset_id: str
    microprice: float
    naive_mid: float
    microprice_vs_mid: float
    book_imbalance: float
    bid_nodes: list[LiquidityNode]
    ask_nodes: list[LiquidityNode]
    nearest_node_direction: Literal["bid", "ask"]
    signal_confidence: float
    rolling_imbalance: dict[int, float] = field(default_factory=dict)
    rolling_microprice_vs_mid: dict[int, float] = field(default_factory=dict)


@dataclass
class _TimestampedValue:
    """Internal: a value with a timestamp for rolling window calculations."""
    timestamp_ms: int
    value: float


class OrderbookSignalAnalyzer:
    """
    Computes orderbook-derived signals from prediction market snapshots.

    Parameters:
        n_levels: Number of top levels for microprice/imbalance (default: 3)
        node_threshold_multiplier: How much larger than median a level must be
            to qualify as a liquidity node (default: 2.0)
        max_nodes: Maximum number of nodes to report per side (default: 3)
        min_total_depth: Minimum combined book depth to emit signals (default: 100)
        rolling_windows: Window durations in seconds for rolling metrics
            (default: [5, 30, 300])
        persistence_min_seconds: Minimum time an order level must be present
            to count toward signals (default: 5). Applied to node detection.
    """

    def __init__(
        self,
        n_levels: int = 3,
        node_threshold_multiplier: float = 2.0,
        max_nodes: int = 3,
        min_total_depth: float = 100.0,
        rolling_windows: Optional[list[int]] = None,
        persistence_min_seconds: float = 5.0,
    ):
        self.n_levels = n_levels
        self.node_threshold_multiplier = node_threshold_multiplier
        self.max_nodes = max_nodes
        self.min_total_depth = min_total_depth
        self.rolling_windows = rolling_windows or [5, 30, 300]
        self.persistence_min_seconds = persistence_min_seconds

        # Rolling history per asset: {asset_id: deque of _TimestampedValue}
        self._imbalance_history: dict[str, deque[_TimestampedValue]] = {}
        self._microprice_div_history: dict[str, deque[_TimestampedValue]] = {}

        # Level persistence tracking per asset:
        # {asset_id: {price_str: first_seen_timestamp_ms}}
        self._level_first_seen: dict[str, dict[str, int]] = {}

        # Max history to keep (based on largest rolling window + buffer)
        max_window = max(self.rolling_windows) if self.rolling_windows else 300
        self._max_history_ms = (max_window + 60) * 1000

    def update(self, snapshot: OrderbookSnapshot) -> Optional[SignalSnapshot]:
        """
        Process an orderbook snapshot and return computed signals.

        Returns None if the book doesn't meet minimum depth requirements
        or if both sides are empty.

        Args:
            snapshot: Orderbook snapshot with bids and asks

        Returns:
            SignalSnapshot with all computed signals, or None
        """
        if not snapshot.bids or not snapshot.asks:
            return None

        asset_id = snapshot.asset_id
        timestamp_ms = snapshot.timestamp

        # Parse levels to (price, size) tuples, sorted appropriately
        bids = self._parse_levels(snapshot.bids, descending=True)
        asks = self._parse_levels(snapshot.asks, descending=False)

        if not bids or not asks:
            return None

        # Check minimum depth
        total_bid_depth = sum(size for _, size in bids)
        total_ask_depth = sum(size for _, size in asks)
        total_depth = total_bid_depth + total_ask_depth

        if total_depth < self.min_total_depth:
            return None

        self._update_persistence(asset_id, timestamp_ms, bids, asks)

        microprice = self._compute_microprice(bids, asks)
        naive_mid = (bids[0][0] + asks[0][0]) / 2.0
        microprice_vs_mid = microprice - naive_mid

        top_bids = bids[:self.n_levels]
        top_asks = asks[:self.n_levels]
        top_bid_depth = sum(size for _, size in top_bids)
        top_ask_depth = sum(size for _, size in top_asks)
        book_imbalance = top_bid_depth / (top_bid_depth + top_ask_depth)

        bid_nodes = self._detect_nodes(
            bids, asset_id, timestamp_ms, side="bid"
        )
        ask_nodes = self._detect_nodes(
            asks, asset_id, timestamp_ms, side="ask"
        )

        nearest_node_direction = self._nearest_node_direction(
            microprice, bid_nodes, ask_nodes
        )

        spread = asks[0][0] - bids[0][0]
        signal_confidence = self._compute_confidence(
            total_depth, spread, naive_mid, len(bids), len(asks)
        )

        self._append_history(
            self._imbalance_history, asset_id, timestamp_ms, book_imbalance
        )
        self._append_history(
            self._microprice_div_history, asset_id, timestamp_ms, microprice_vs_mid
        )

        rolling_imbalance = self._compute_rolling(
            self._imbalance_history, asset_id, timestamp_ms
        )
        rolling_microprice_vs_mid = self._compute_rolling(
            self._microprice_div_history, asset_id, timestamp_ms
        )

        return SignalSnapshot(
            timestamp_ms=timestamp_ms,
            asset_id=asset_id,
            microprice=microprice,
            naive_mid=naive_mid,
            microprice_vs_mid=microprice_vs_mid,
            book_imbalance=book_imbalance,
            bid_nodes=bid_nodes,
            ask_nodes=ask_nodes,
            nearest_node_direction=nearest_node_direction,
            signal_confidence=signal_confidence,
            rolling_imbalance=rolling_imbalance,
            rolling_microprice_vs_mid=rolling_microprice_vs_mid,
        )

    # ================================================================
    # Signal computation internals
    # ================================================================

    @staticmethod
    def _parse_levels(
        levels: list, descending: bool
    ) -> list[tuple[float, float]]:
        """Parse OrderLevel objects to (price, size) tuples, sorted."""
        parsed = []
        for level in levels:
            try:
                price = float(level.price)
                size = float(level.size)
                if size > 0:
                    parsed.append((price, size))
            except (ValueError, TypeError):
                continue
        parsed.sort(key=lambda x: x[0], reverse=descending)
        return parsed

    def _compute_microprice(
        self,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
    ) -> float:
        """
        Multi-level volume-weighted microprice.

        microprice = sum(bid_price_i * ask_size_i + ask_price_i * bid_size_i)
                     / sum(bid_size_i + ask_size_i)
        across top N levels.
        """
        n = min(self.n_levels, len(bids), len(asks))
        numerator = 0.0
        denominator = 0.0
        for i in range(n):
            bid_price, bid_size = bids[i]
            ask_price, ask_size = asks[i]
            numerator += bid_price * ask_size + ask_price * bid_size
            denominator += bid_size + ask_size
        if denominator == 0:
            return (bids[0][0] + asks[0][0]) / 2.0
        return numerator / denominator

    def _detect_nodes(
        self,
        levels: list[tuple[float, float]],
        asset_id: str,
        timestamp_ms: int,
        side: str,
    ) -> list[LiquidityNode]:
        """
        Find price levels with significantly more size than surrounding levels.

        A node is a level with size > node_threshold_multiplier * median_size.
        Filtered by persistence if tracking is available.
        """
        if len(levels) < 3:
            return []

        sizes = [size for _, size in levels]
        sorted_sizes = sorted(sizes)
        median_size = sorted_sizes[len(sorted_sizes) // 2]
        threshold = median_size * self.node_threshold_multiplier

        if threshold <= 0:
            return []

        persistence_ms = self.persistence_min_seconds * 1000
        level_ages = self._level_first_seen.get(asset_id, {})

        nodes = []
        for price, size in levels:
            if size <= threshold:
                continue

            # Check persistence: only include levels that have been
            # present for at least persistence_min_seconds
            price_key = f"{side}:{price:.6f}"
            first_seen = level_ages.get(price_key)
            if first_seen is not None:
                age_ms = timestamp_ms - first_seen
                if age_ms < persistence_ms:
                    continue

            nodes.append(LiquidityNode(price=price, size=size))

        # Sort by size descending and return top max_nodes
        nodes.sort(key=lambda n: n.size, reverse=True)
        return nodes[:self.max_nodes]

    def _nearest_node_direction(
        self,
        microprice: float,
        bid_nodes: list[LiquidityNode],
        ask_nodes: list[LiquidityNode],
    ) -> Literal["bid", "ask"]:
        """
        Determine which side's nearest node the microprice leans toward.

        Compares distance from microprice to the nearest bid node vs
        the nearest ask node. Closer node = the direction price is
        likely to heatseek toward.
        """
        nearest_bid_dist = float("inf")
        nearest_ask_dist = float("inf")

        if bid_nodes:
            nearest_bid_dist = min(
                abs(microprice - node.price) for node in bid_nodes
            )
        if ask_nodes:
            nearest_ask_dist = min(
                abs(microprice - node.price) for node in ask_nodes
            )

        if nearest_bid_dist <= nearest_ask_dist:
            return "bid"
        return "ask"

    def _compute_confidence(
        self,
        total_depth: float,
        spread: float,
        mid_price: float,
        n_bid_levels: int,
        n_ask_levels: int,
    ) -> float:
        """
        Compute signal confidence score from 0 to 1.

        Factors:
        - Depth score: total depth relative to min_total_depth
        - Spread score: tighter spread = higher confidence
        - Level count score: more levels = more informative book
        """
        # Depth score: ramps from 0 at min_total_depth to 1 at 5x min
        if self.min_total_depth > 0:
            depth_ratio = total_depth / self.min_total_depth
            depth_score = min(1.0, (depth_ratio - 1.0) / 4.0) if depth_ratio >= 1 else 0.0
        else:
            depth_score = 1.0 if total_depth > 0 else 0.0

        # Spread score: spread as fraction of mid, lower is better
        # A 1-cent spread on a 50-cent market = 2% = decent
        # A 5-cent spread on a 50-cent market = 10% = poor
        if mid_price > 0:
            spread_pct = spread / mid_price
            # Map: 0% spread -> 1.0, 10%+ spread -> 0.0
            spread_score = max(0.0, 1.0 - spread_pct * 10.0)
        else:
            spread_score = 0.0

        # Level count score: at least 3 levels on each side is good
        min_levels = min(n_bid_levels, n_ask_levels)
        level_score = min(1.0, min_levels / 3.0)

        # Weighted combination
        confidence = (
            0.4 * depth_score
            + 0.4 * spread_score
            + 0.2 * level_score
        )
        return max(0.0, min(1.0, confidence))

    # ================================================================
    # Rolling window management
    # ================================================================

    def _append_history(
        self,
        history: dict[str, deque[_TimestampedValue]],
        asset_id: str,
        timestamp_ms: int,
        value: float,
    ) -> None:
        """Append a timestamped value and prune old entries."""
        if asset_id not in history:
            history[asset_id] = deque()
        dq = history[asset_id]
        dq.append(_TimestampedValue(timestamp_ms=timestamp_ms, value=value))

        # Prune entries older than max window
        cutoff = timestamp_ms - self._max_history_ms
        while dq and dq[0].timestamp_ms < cutoff:
            dq.popleft()

    def _compute_rolling(
        self,
        history: dict[str, deque[_TimestampedValue]],
        asset_id: str,
        timestamp_ms: int,
    ) -> dict[int, float]:
        """Compute rolling averages for each configured window using prefix sums."""
        result: dict[int, float] = {}
        dq = history.get(asset_id)
        if not dq:
            return {w: 0.0 for w in self.rolling_windows}

        # Build prefix sums once, then use bisect for each window
        timestamps = [tv.timestamp_ms for tv in dq]
        prefix = [0.0] * (len(timestamps) + 1)
        for i, tv in enumerate(dq):
            prefix[i + 1] = prefix[i] + tv.value

        total = len(timestamps)
        for window_sec in self.rolling_windows:
            cutoff = timestamp_ms - window_sec * 1000
            lo = bisect.bisect_left(timestamps, cutoff)
            count = total - lo
            if count > 0:
                result[window_sec] = (prefix[total] - prefix[lo]) / count
            else:
                result[window_sec] = 0.0
        return result

    # ================================================================
    # Level persistence tracking
    # ================================================================

    def _update_persistence(
        self,
        asset_id: str,
        timestamp_ms: int,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
    ) -> None:
        """
        Track when each price level was first seen for persistence filtering.

        Levels that disappear from the book are removed from tracking.
        """
        if asset_id not in self._level_first_seen:
            self._level_first_seen[asset_id] = {}
        ages = self._level_first_seen[asset_id]

        # Build current level keys
        current_keys = set()
        for price, _ in bids:
            key = f"bid:{price:.6f}"
            current_keys.add(key)
            if key not in ages:
                ages[key] = timestamp_ms

        for price, _ in asks:
            key = f"ask:{price:.6f}"
            current_keys.add(key)
            if key not in ages:
                ages[key] = timestamp_ms

        for k in ages.keys() - current_keys:
            del ages[k]
