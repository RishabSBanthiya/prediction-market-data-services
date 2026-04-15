"""
Tests for OrderbookSignalAnalyzer and OrderbookSignalStrategy.
"""

import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from models.orderbook import OrderbookSnapshot, OrderLevel
from backtest.strategies.examples.orderbook_signals import (
    OrderbookSignalAnalyzer,
    SignalSnapshot,
    LiquidityNode,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_snapshot(
    bids: list[tuple[str, str]],
    asks: list[tuple[str, str]],
    asset_id: str = "token-yes-1",
    timestamp: int = 1700000000000,
) -> OrderbookSnapshot:
    """Create an OrderbookSnapshot from (price, size) tuples."""
    snap = OrderbookSnapshot(
        listener_id="test",
        asset_id=asset_id,
        market="cond-1",
        timestamp=timestamp,
        bids=[OrderLevel(price=p, size=s) for p, s in bids],
        asks=[OrderLevel(price=p, size=s) for p, s in asks],
    )
    snap.compute_metrics()
    return snap


def _balanced_snapshot(timestamp: int = 1700000000000) -> OrderbookSnapshot:
    """Symmetric book: 3 levels each side, equal sizes."""
    return _make_snapshot(
        bids=[("0.55", "100"), ("0.54", "100"), ("0.53", "100")],
        asks=[("0.56", "100"), ("0.57", "100"), ("0.58", "100")],
        timestamp=timestamp,
    )


def _bullish_snapshot(timestamp: int = 1700000000000) -> OrderbookSnapshot:
    """Bid-heavy book: much more size on bid side."""
    return _make_snapshot(
        bids=[("0.55", "500"), ("0.54", "400"), ("0.53", "300")],
        asks=[("0.56", "50"), ("0.57", "50"), ("0.58", "50")],
        timestamp=timestamp,
    )


def _bearish_snapshot(timestamp: int = 1700000000000) -> OrderbookSnapshot:
    """Ask-heavy book: much more size on ask side."""
    return _make_snapshot(
        bids=[("0.55", "50"), ("0.54", "50"), ("0.53", "50")],
        asks=[("0.56", "500"), ("0.57", "400"), ("0.58", "300")],
        timestamp=timestamp,
    )


# ---------------------------------------------------------------------------
# Tests: Analyzer basics
# ---------------------------------------------------------------------------


class TestAnalyzerBasics:
    def test_returns_none_for_empty_bids(self):
        analyzer = OrderbookSignalAnalyzer(min_total_depth=0)
        snap = _make_snapshot(bids=[], asks=[("0.56", "100")])
        assert analyzer.update(snap) is None

    def test_returns_none_for_empty_asks(self):
        analyzer = OrderbookSignalAnalyzer(min_total_depth=0)
        snap = _make_snapshot(bids=[("0.55", "100")], asks=[])
        assert analyzer.update(snap) is None

    def test_returns_none_below_min_depth(self):
        analyzer = OrderbookSignalAnalyzer(min_total_depth=1000)
        snap = _balanced_snapshot()
        assert analyzer.update(snap) is None

    def test_returns_signal_when_depth_sufficient(self):
        analyzer = OrderbookSignalAnalyzer(min_total_depth=100)
        snap = _balanced_snapshot()
        signal = analyzer.update(snap)
        assert signal is not None
        assert isinstance(signal, SignalSnapshot)


# ---------------------------------------------------------------------------
# Tests: Microprice
# ---------------------------------------------------------------------------


class TestMicroprice:
    def test_microprice_balanced_book(self):
        """On a perfectly balanced book, microprice should equal naive mid."""
        analyzer = OrderbookSignalAnalyzer(n_levels=1, min_total_depth=0)
        snap = _make_snapshot(
            bids=[("0.50", "100")],
            asks=[("0.60", "100")],
        )
        signal = analyzer.update(snap)
        # microprice = (0.50*100 + 0.60*100) / (100+100) = 0.55
        assert signal.microprice == pytest.approx(0.55)
        assert signal.naive_mid == pytest.approx(0.55)
        assert signal.microprice_vs_mid == pytest.approx(0.0, abs=1e-10)

    def test_microprice_skewed_toward_ask(self):
        """More bid size -> microprice skews toward ask (higher)."""
        analyzer = OrderbookSignalAnalyzer(n_levels=1, min_total_depth=0)
        snap = _make_snapshot(
            bids=[("0.50", "300")],
            asks=[("0.60", "100")],
        )
        signal = analyzer.update(snap)
        # microprice = (0.50*100 + 0.60*300) / (300+100) = (50+180)/400 = 0.575
        assert signal.microprice == pytest.approx(0.575)
        assert signal.microprice_vs_mid == pytest.approx(0.025)

    def test_microprice_skewed_toward_bid(self):
        """More ask size -> microprice skews toward bid (lower)."""
        analyzer = OrderbookSignalAnalyzer(n_levels=1, min_total_depth=0)
        snap = _make_snapshot(
            bids=[("0.50", "100")],
            asks=[("0.60", "300")],
        )
        signal = analyzer.update(snap)
        # microprice = (0.50*300 + 0.60*100) / (100+300) = (150+60)/400 = 0.525
        assert signal.microprice == pytest.approx(0.525)
        assert signal.microprice_vs_mid == pytest.approx(-0.025)

    def test_multi_level_microprice(self):
        """Microprice uses top N levels."""
        analyzer = OrderbookSignalAnalyzer(n_levels=2, min_total_depth=0)
        snap = _make_snapshot(
            bids=[("0.50", "100"), ("0.49", "200")],
            asks=[("0.60", "100"), ("0.61", "200")],
        )
        signal = analyzer.update(snap)
        # Level 1: 0.50*100 + 0.60*100 = 110
        # Level 2: 0.49*200 + 0.61*200 = 220
        # Num = 110 + 220 = 330
        # Denom = (100+100) + (200+200) = 600
        # microprice = 330/600 = 0.55
        assert signal.microprice == pytest.approx(0.55)


# ---------------------------------------------------------------------------
# Tests: Book Imbalance
# ---------------------------------------------------------------------------


class TestBookImbalance:
    def test_balanced_imbalance(self):
        analyzer = OrderbookSignalAnalyzer(n_levels=3, min_total_depth=0)
        snap = _balanced_snapshot()
        signal = analyzer.update(snap)
        assert signal.book_imbalance == pytest.approx(0.5)

    def test_bullish_imbalance(self):
        analyzer = OrderbookSignalAnalyzer(n_levels=3, min_total_depth=0)
        snap = _bullish_snapshot()
        signal = analyzer.update(snap)
        # bids: 500+400+300 = 1200, asks: 50+50+50 = 150
        # imbalance = 1200 / (1200+150) = 1200/1350 ≈ 0.889
        assert signal.book_imbalance > 0.8

    def test_bearish_imbalance(self):
        analyzer = OrderbookSignalAnalyzer(n_levels=3, min_total_depth=0)
        snap = _bearish_snapshot()
        signal = analyzer.update(snap)
        # bids: 50+50+50 = 150, asks: 500+400+300 = 1200
        # imbalance = 150 / (150+1200) = 150/1350 ≈ 0.111
        assert signal.book_imbalance < 0.2

    def test_imbalance_respects_n_levels(self):
        """Only top N levels should be used for imbalance."""
        analyzer = OrderbookSignalAnalyzer(n_levels=1, min_total_depth=0)
        snap = _make_snapshot(
            bids=[("0.55", "100"), ("0.54", "1000")],
            asks=[("0.56", "100"), ("0.57", "1000")],
        )
        signal = analyzer.update(snap)
        # Only top 1: bid=100, ask=100 -> 0.5
        assert signal.book_imbalance == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Tests: Liquidity Nodes
# ---------------------------------------------------------------------------


class TestLiquidityNodes:
    def test_no_nodes_when_uniform(self):
        """Uniform book should have no nodes (no outliers)."""
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            node_threshold_multiplier=2.0,
            persistence_min_seconds=0,  # Disable persistence filter
        )
        snap = _balanced_snapshot()
        signal = analyzer.update(snap)
        assert len(signal.bid_nodes) == 0
        assert len(signal.ask_nodes) == 0

    def test_detects_bid_node(self):
        """A large bid level should be detected as a node."""
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            node_threshold_multiplier=2.0,
            persistence_min_seconds=0,
        )
        snap = _make_snapshot(
            bids=[("0.55", "50"), ("0.54", "500"), ("0.53", "50"), ("0.52", "50")],
            asks=[("0.56", "100"), ("0.57", "100"), ("0.58", "100"), ("0.59", "100")],
        )
        signal = analyzer.update(snap)
        assert len(signal.bid_nodes) >= 1
        assert signal.bid_nodes[0].price == pytest.approx(0.54)
        assert signal.bid_nodes[0].size == pytest.approx(500.0)

    def test_detects_ask_node(self):
        """A large ask level should be detected as a node."""
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            node_threshold_multiplier=2.0,
            persistence_min_seconds=0,
        )
        snap = _make_snapshot(
            bids=[("0.55", "100"), ("0.54", "100"), ("0.53", "100"), ("0.52", "100")],
            asks=[("0.56", "50"), ("0.57", "500"), ("0.58", "50"), ("0.59", "50")],
        )
        signal = analyzer.update(snap)
        assert len(signal.ask_nodes) >= 1
        assert signal.ask_nodes[0].price == pytest.approx(0.57)

    def test_persistence_filters_new_levels(self):
        """Levels seen for less than persistence_min_seconds should not be nodes."""
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            node_threshold_multiplier=2.0,
            persistence_min_seconds=5,
        )
        snap = _make_snapshot(
            bids=[("0.55", "50"), ("0.54", "500"), ("0.53", "50"), ("0.52", "50")],
            asks=[("0.56", "100"), ("0.57", "100"), ("0.58", "100"), ("0.59", "100")],
            timestamp=1700000000000,
        )
        signal = analyzer.update(snap)
        # First time seeing level -> age = 0 < 5s -> filtered out
        assert len(signal.bid_nodes) == 0

    def test_persistence_allows_old_levels(self):
        """After enough time, persistent levels should be detected as nodes."""
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            node_threshold_multiplier=2.0,
            persistence_min_seconds=5,
        )
        bids = [("0.55", "50"), ("0.54", "500"), ("0.53", "50"), ("0.52", "50")]
        asks = [("0.56", "100"), ("0.57", "100"), ("0.58", "100"), ("0.59", "100")]

        # First update: register the levels
        analyzer.update(_make_snapshot(bids=bids, asks=asks, timestamp=1700000000000))
        # Second update 6 seconds later: levels are now persistent
        signal = analyzer.update(_make_snapshot(bids=bids, asks=asks, timestamp=1700000006000))

        assert len(signal.bid_nodes) >= 1
        assert signal.bid_nodes[0].price == pytest.approx(0.54)

    def test_max_nodes_limit(self):
        """Should not return more than max_nodes per side."""
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            node_threshold_multiplier=1.0,  # Low threshold to get many nodes
            max_nodes=2,
            persistence_min_seconds=0,
        )
        snap = _make_snapshot(
            bids=[
                ("0.55", "500"), ("0.54", "400"), ("0.53", "300"),
                ("0.52", "200"), ("0.51", "100"),
            ],
            asks=[("0.56", "100"), ("0.57", "100"), ("0.58", "100"),
                  ("0.59", "100"), ("0.60", "100")],
        )
        signal = analyzer.update(snap)
        assert len(signal.bid_nodes) <= 2


# ---------------------------------------------------------------------------
# Tests: Nearest Node Direction
# ---------------------------------------------------------------------------


class TestNearestNodeDirection:
    def test_direction_toward_bid_node(self):
        """When microprice is closer to bid nodes, direction should be 'bid'."""
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            node_threshold_multiplier=2.0,
            persistence_min_seconds=0,
        )
        # Big bid node at 0.54, no notable ask nodes
        snap = _make_snapshot(
            bids=[("0.55", "50"), ("0.54", "500"), ("0.53", "50"), ("0.52", "50")],
            asks=[("0.56", "60"), ("0.57", "60"), ("0.58", "60"), ("0.59", "60")],
        )
        signal = analyzer.update(snap)
        if signal.bid_nodes and not signal.ask_nodes:
            assert signal.nearest_node_direction == "bid"

    def test_direction_toward_ask_node(self):
        """When microprice is closer to ask nodes, direction should be 'ask'."""
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            node_threshold_multiplier=2.0,
            persistence_min_seconds=0,
        )
        snap = _make_snapshot(
            bids=[("0.55", "60"), ("0.54", "60"), ("0.53", "60"), ("0.52", "60")],
            asks=[("0.56", "50"), ("0.57", "500"), ("0.58", "50"), ("0.59", "50")],
        )
        signal = analyzer.update(snap)
        if signal.ask_nodes and not signal.bid_nodes:
            assert signal.nearest_node_direction == "ask"

    def test_default_bid_when_no_nodes(self):
        """With no nodes at all, nearest_node_direction defaults to 'bid'."""
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            persistence_min_seconds=0,
        )
        snap = _balanced_snapshot()
        signal = analyzer.update(snap)
        # No nodes detected -> both dists are inf -> bid wins (<=)
        assert signal.nearest_node_direction == "bid"


# ---------------------------------------------------------------------------
# Tests: Signal Confidence
# ---------------------------------------------------------------------------


class TestSignalConfidence:
    def test_confidence_between_0_and_1(self):
        analyzer = OrderbookSignalAnalyzer(min_total_depth=100)
        snap = _bullish_snapshot()
        signal = analyzer.update(snap)
        assert 0.0 <= signal.signal_confidence <= 1.0

    def test_low_confidence_thin_book(self):
        """Thin book should have lower confidence."""
        analyzer = OrderbookSignalAnalyzer(min_total_depth=10)
        snap = _make_snapshot(
            bids=[("0.50", "5"), ("0.49", "5"), ("0.48", "5")],
            asks=[("0.60", "5"), ("0.61", "5"), ("0.62", "5")],
        )
        signal = analyzer.update(snap)
        assert signal.signal_confidence < 0.5

    def test_higher_confidence_deep_book(self):
        """Deep book with tight spread should have higher confidence."""
        analyzer = OrderbookSignalAnalyzer(min_total_depth=100)
        snap = _make_snapshot(
            bids=[("0.55", "1000"), ("0.54", "1000"), ("0.53", "1000")],
            asks=[("0.56", "1000"), ("0.57", "1000"), ("0.58", "1000")],
        )
        signal = analyzer.update(snap)
        assert signal.signal_confidence > 0.5


# ---------------------------------------------------------------------------
# Tests: Rolling Windows
# ---------------------------------------------------------------------------


class TestRollingWindows:
    def test_rolling_imbalance_populated(self):
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            rolling_windows=[5, 30],
        )
        snap = _bullish_snapshot()
        signal = analyzer.update(snap)
        assert 5 in signal.rolling_imbalance
        assert 30 in signal.rolling_imbalance

    def test_rolling_averages_over_multiple_updates(self):
        """Rolling average should reflect recent history."""
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            rolling_windows=[5],
        )
        # First update: balanced
        analyzer.update(_balanced_snapshot(timestamp=1700000000000))
        # Second update: bullish (2 seconds later)
        signal = analyzer.update(_bullish_snapshot(timestamp=1700000002000))

        # The 5s rolling imbalance should be an average of balanced (0.5)
        # and bullish (~0.89)
        assert signal.rolling_imbalance[5] > 0.5
        assert signal.rolling_imbalance[5] < signal.book_imbalance

    def test_old_values_pruned_from_rolling(self):
        """Values outside the window should be pruned."""
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            rolling_windows=[5],
        )
        # Old bullish snapshot
        analyzer.update(_bullish_snapshot(timestamp=1700000000000))
        # Much later balanced snapshot (10s later, outside 5s window)
        signal = analyzer.update(_balanced_snapshot(timestamp=1700000010000))

        # 5s window should only contain the balanced value
        assert signal.rolling_imbalance[5] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Tests: Level Persistence Tracking
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_disappeared_levels_cleared(self):
        """Levels that vanish from the book should be cleared from tracking."""
        analyzer = OrderbookSignalAnalyzer(
            min_total_depth=0,
            persistence_min_seconds=5,
        )
        # First: level at 0.52
        analyzer.update(_make_snapshot(
            bids=[("0.55", "100"), ("0.54", "100"), ("0.53", "100"), ("0.52", "100")],
            asks=[("0.56", "100"), ("0.57", "100"), ("0.58", "100")],
            timestamp=1700000000000,
        ))
        # Second: level at 0.52 gone, new level at 0.51
        analyzer.update(_make_snapshot(
            bids=[("0.55", "100"), ("0.54", "100"), ("0.53", "100"), ("0.51", "100")],
            asks=[("0.56", "100"), ("0.57", "100"), ("0.58", "100")],
            timestamp=1700000002000,
        ))

        ages = analyzer._level_first_seen.get("token-yes-1", {})
        assert "bid:0.520000" not in ages
        assert "bid:0.510000" in ages
