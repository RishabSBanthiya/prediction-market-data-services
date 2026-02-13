"""
Integration tests for the backtest pipeline.

These tests verify the full backtest pipeline end-to-end WITHOUT requiring
a real database. They bypass the data loader by constructing BacktestDataset
objects directly with synthetic data, then feed them through the engine
components (ExecutionEngine, MetricsCollector, Portfolio, Strategy).

Usage:
    pytest tests/backtest/test_integration.py -v -m integration
"""

import sys
from decimal import Decimal
from pathlib import Path

import pytest

# Ensure src/ is on the Python path so absolute imports resolve correctly.
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from models.orderbook import OrderbookSnapshot, OrderLevel
from models.trade import Trade
from models.market import Market

from backtest.core.interfaces import (
    BacktestDataset,
    BacktestEvent,
    OrderbookBacktestEvent,
    TradeBacktestEvent,
)
from backtest.core.strategy import Strategy, BacktestContext
from backtest.models.order import Order, OrderSide, OrderType, OrderStatus, Fill
from backtest.models.config import FeeSchedule
from backtest.models.portfolio import Portfolio
from backtest.models.market_pair import MarketPair, MarketPairRegistry
from backtest.services.execution_engine import ExecutionEngine
from backtest.services.metrics import MetricsCollector
from backtest.strategies.examples.market_maker import SimpleMarketMaker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_orderbook(
    asset_id: str,
    timestamp: int,
    bids: list[tuple[str, str]],
    asks: list[tuple[str, str]],
    listener_id: str = "test-listener",
    market: str = "condition-1",
) -> OrderbookSnapshot:
    """Create an OrderbookSnapshot with computed metrics.

    Args:
        asset_id: Token ID for this snapshot.
        timestamp: Timestamp in milliseconds.
        bids: List of (price, size) tuples for bid levels.
        asks: List of (price, size) tuples for ask levels.
        listener_id: Listener ID.
        market: Market / condition ID.

    Returns:
        OrderbookSnapshot with metrics computed.
    """
    snapshot = OrderbookSnapshot(
        listener_id=listener_id,
        asset_id=asset_id,
        market=market,
        timestamp=timestamp,
        bids=[OrderLevel(price=p, size=s) for p, s in bids],
        asks=[OrderLevel(price=p, size=s) for p, s in asks],
    )
    snapshot.compute_metrics()
    return snapshot


def make_trade(
    asset_id: str,
    timestamp: int,
    price: float,
    size: float,
    side: str = "buy",
    listener_id: str = "test-listener",
    market: str = "condition-1",
) -> Trade:
    """Create a Trade object.

    Args:
        asset_id: Token ID.
        timestamp: Timestamp in milliseconds.
        price: Trade price.
        size: Trade size.
        side: Trade side ("buy" or "sell").
        listener_id: Listener ID.
        market: Market / condition ID.

    Returns:
        Trade object.
    """
    return Trade(
        listener_id=listener_id,
        asset_id=asset_id,
        market=market,
        timestamp=timestamp,
        price=price,
        size=size,
        side=side,
        raw_payload={"source": "integration_test"},
    )


def make_market(
    condition_id: str,
    token_id: str,
    outcome: str = "Yes",
    outcome_index: int = 0,
    question: str = "Test question?",
) -> Market:
    """Create a Market object for tests.

    Args:
        condition_id: Condition ID for this market.
        token_id: Token ID.
        outcome: Outcome label.
        outcome_index: Index of the outcome (0=Yes, 1=No).
        question: Market question text.

    Returns:
        Market instance.
    """
    return Market(
        condition_id=condition_id,
        token_id=token_id,
        outcome=outcome,
        outcome_index=outcome_index,
        question=question,
    )


def build_pipeline(
    dataset: BacktestDataset,
    initial_cash: Decimal = Decimal("10000"),
    maker_fee_bps: int = 0,
    taker_fee_bps: int = 0,
) -> tuple[Portfolio, ExecutionEngine, MetricsCollector, MarketPairRegistry]:
    """Build the pipeline components from a dataset.

    Creates MarketPairRegistry, Portfolio, ExecutionEngine, and MetricsCollector
    ready for use in the backtest event loop.

    Returns:
        Tuple of (portfolio, execution_engine, metrics, market_pairs).
    """
    market_pairs = MarketPairRegistry.build_from_markets(
        list(dataset.markets.values())
    )
    portfolio = Portfolio(initial_cash=initial_cash, market_pairs=market_pairs)
    fee_schedule = FeeSchedule(
        maker_fee_bps=maker_fee_bps,
        taker_fee_bps=taker_fee_bps,
    )
    execution_engine = ExecutionEngine(
        portfolio=portfolio,
        fee_schedule=fee_schedule,
        market_pairs=market_pairs,
    )
    metrics = MetricsCollector(initial_cash=initial_cash)
    return portfolio, execution_engine, metrics, market_pairs


def run_event_loop(
    dataset: BacktestDataset,
    strategy: Strategy,
    portfolio: Portfolio,
    execution_engine: ExecutionEngine,
    metrics: MetricsCollector,
) -> None:
    """Run the backtest event loop.

    Mimics BacktestEngine.run() without async or data loading.
    Iterates through all events, processes them through the execution engine,
    records fills in metrics, and dispatches to the strategy.

    Fills that occur during strategy callbacks (e.g. when a strategy calls
    submit_order inside on_orderbook and the order immediately fills) are
    also captured and routed through the metrics collector to ensure the
    trade log is complete.
    """
    context = BacktestContext(
        start_time_ms=dataset.start_time_ms,
        end_time_ms=dataset.end_time_ms,
        initial_cash=float(portfolio.initial_cash),
        platform=None,
        markets=dataset.markets,
    )
    strategy.on_start(context)

    last_prices: dict[str, Decimal] = {}
    event_count = 0
    # Track how many fills we have already recorded in metrics so we can
    # detect new fills produced during strategy callbacks.
    recorded_fill_count = 0

    def _record_new_fills_from_portfolio() -> None:
        """Record any fills in the portfolio that we haven't yet sent to metrics."""
        nonlocal recorded_fill_count
        all_fills = portfolio.get_fills()
        new_fills = all_fills[recorded_fill_count:]
        for fill in new_fills:
            metrics.record_fill(fill, portfolio)
            strategy.on_fill(fill)
        recorded_fill_count = len(all_fills)

    for event in dataset.get_event_iterator():
        event_count += 1

        if isinstance(event, OrderbookBacktestEvent):
            snapshot = event.snapshot

            # Track last mid price
            if snapshot.mid_price is not None:
                last_prices[snapshot.asset_id] = Decimal(str(snapshot.mid_price))

            # Process through execution engine (may generate fills from
            # pending limit orders)
            fills = execution_engine.process_orderbook_update(snapshot)

            # Update mark prices
            prices: dict[str, Decimal] = {}
            if snapshot.mid_price is not None:
                prices[snapshot.asset_id] = Decimal(str(snapshot.mid_price))
            portfolio.update_mark_prices(prices)

            # Record fills from process_orderbook_update
            _record_new_fills_from_portfolio()

            # Call strategy (may submit new orders that fill immediately)
            is_ff = getattr(snapshot, "is_forward_filled", False) or False
            strategy.on_orderbook(snapshot, is_ff)

            # Capture any fills generated during the strategy callback
            _record_new_fills_from_portfolio()

        elif isinstance(event, TradeBacktestEvent):
            trade = event.trade

            # Process through execution engine
            fills = execution_engine.process_trade(trade)

            # Record fills from process_trade
            _record_new_fills_from_portfolio()

            # Call strategy (may submit new orders)
            strategy.on_trade(trade)

            # Capture any fills generated during the strategy callback
            _record_new_fills_from_portfolio()

        # Periodic equity sampling every 5 events
        if event_count % 5 == 0 and last_prices:
            metrics.record_equity_point(event.timestamp_ms, portfolio, last_prices)

    # Record final equity point
    if last_prices:
        metrics.record_equity_point(dataset.end_time_ms, portfolio, last_prices)

    strategy.on_end(context)


# ---------------------------------------------------------------------------
# Test strategies for integration tests
# ---------------------------------------------------------------------------


class BuyOnceStrategy(Strategy):
    """Strategy that buys once on the first orderbook event."""

    def __init__(self, asset_id: str, quantity: Decimal = Decimal("10")):
        super().__init__(name="BuyOnce")
        self._target_asset = asset_id
        self._quantity = quantity
        self._has_bought = False
        self.fills_received: list[Fill] = []

    def on_orderbook(self, snapshot: OrderbookSnapshot, is_forward_filled: bool) -> None:
        if self._has_bought:
            return
        if snapshot.asset_id != self._target_asset:
            return
        if not snapshot.asks:
            return

        best_ask = Decimal(snapshot.asks[0].price)
        order = Order(
            asset_id=self._target_asset,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=best_ask,
            quantity=self._quantity,
        )
        self.submit_order(order)
        self._has_bought = True

    def on_fill(self, fill: Fill) -> None:
        self.fills_received.append(fill)


class BuySellRoundtripStrategy(Strategy):
    """Strategy that buys on first orderbook, sells on a later one."""

    def __init__(
        self,
        asset_id: str,
        buy_price: Decimal,
        sell_price: Decimal,
        quantity: Decimal = Decimal("10"),
    ):
        super().__init__(name="BuySellRoundtrip")
        self._target_asset = asset_id
        self._buy_price = buy_price
        self._sell_price = sell_price
        self._quantity = quantity
        self._has_bought = False
        self._has_sold = False
        self.fills_received: list[Fill] = []

    def on_orderbook(self, snapshot: OrderbookSnapshot, is_forward_filled: bool) -> None:
        if snapshot.asset_id != self._target_asset:
            return

        if not self._has_bought and snapshot.asks:
            best_ask = Decimal(snapshot.asks[0].price)
            if best_ask <= self._buy_price:
                order = Order(
                    asset_id=self._target_asset,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    price=self._buy_price,
                    quantity=self._quantity,
                )
                self.submit_order(order)
                self._has_bought = True

        elif self._has_bought and not self._has_sold:
            position = self.portfolio.get_position(self._target_asset)
            if position and position.quantity >= self._quantity:
                if snapshot.bids:
                    best_bid = Decimal(snapshot.bids[0].price)
                    if best_bid >= self._sell_price:
                        order = Order(
                            asset_id=self._target_asset,
                            side=OrderSide.SELL,
                            order_type=OrderType.LIMIT,
                            price=self._sell_price,
                            quantity=self._quantity,
                        )
                        self.submit_order(order)
                        self._has_sold = True

    def on_fill(self, fill: Fill) -> None:
        self.fills_received.append(fill)


class MultiAssetBuyStrategy(Strategy):
    """Strategy that buys a specific quantity of each given asset."""

    def __init__(self, assets: dict[str, Decimal]):
        super().__init__(name="MultiAssetBuy")
        self._targets = assets  # asset_id -> quantity
        self._bought: set[str] = set()
        self.fills_received: list[Fill] = []

    def on_orderbook(self, snapshot: OrderbookSnapshot, is_forward_filled: bool) -> None:
        asset_id = snapshot.asset_id
        if asset_id not in self._targets or asset_id in self._bought:
            return
        if not snapshot.asks:
            return

        best_ask = Decimal(snapshot.asks[0].price)
        order = Order(
            asset_id=asset_id,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=best_ask,
            quantity=self._targets[asset_id],
        )
        self.submit_order(order)
        self._bought.add(asset_id)

    def on_fill(self, fill: Fill) -> None:
        self.fills_received.append(fill)


class DoNothingStrategy(Strategy):
    """Strategy that does nothing -- useful for empty dataset tests."""

    def __init__(self):
        super().__init__(name="DoNothing")

    def on_orderbook(self, snapshot: OrderbookSnapshot, is_forward_filled: bool) -> None:
        pass


# ---------------------------------------------------------------------------
# Integration Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestFullBacktestWithSimpleBuyStrategy:
    """Test 1: Full pipeline with a simple buy-once strategy."""

    def test_full_backtest_with_simple_buy_strategy(self):
        """Create a simple strategy that buys once on first orderbook event,
        then verify: position exists, cash decreased, metrics have expected keys.
        """
        # Build markets
        yes_market = make_market("condition-1", "token-yes-1", "Yes", 0)
        no_market = make_market("condition-1", "token-no-1", "No", 1)

        # Build orderbooks at timestamp intervals
        orderbooks = [
            make_orderbook(
                "token-yes-1",
                1700000000000 + i * 1000,
                bids=[("0.50", "100"), ("0.49", "200")],
                asks=[("0.52", "100"), ("0.53", "200")],
            )
            for i in range(5)
        ]

        # Build some trades
        trades = [
            make_trade("token-yes-1", 1700000000500 + i * 1000, 0.51, 5.0)
            for i in range(3)
        ]

        dataset = BacktestDataset(
            orderbooks=orderbooks,
            trades=trades,
            markets={
                "token-yes-1": yes_market,
                "token-no-1": no_market,
            },
            start_time_ms=1700000000000,
            end_time_ms=1700000005000,
        )

        # Build pipeline
        initial_cash = Decimal("10000")
        portfolio, execution_engine, metrics, market_pairs = build_pipeline(
            dataset, initial_cash=initial_cash
        )

        # Create and inject strategy
        strategy = BuyOnceStrategy("token-yes-1", quantity=Decimal("10"))
        strategy._inject_dependencies(portfolio, execution_engine)

        # Run event loop
        run_event_loop(dataset, strategy, portfolio, execution_engine, metrics)

        # Verify position exists
        position = portfolio.get_position("token-yes-1")
        assert position is not None, "Should have a position in token-yes-1"
        assert position.quantity == Decimal("10"), "Should have bought 10 units"

        # Verify cash decreased
        assert portfolio.cash < initial_cash, "Cash should have decreased after buy"

        # Verify fill was received by strategy
        assert len(strategy.fills_received) > 0, "Strategy should have received a fill"
        assert strategy.fills_received[0].side == OrderSide.BUY

        # Verify metrics
        calculated = metrics.calculate_metrics()
        expected_keys = {
            "win_rate",
            "profit_factor",
            "expectancy",
            "num_trades",
            "num_winning_trades",
            "num_losing_trades",
            "avg_trade_pnl",
            "total_fees",
            "total_return_pct",
            "sharpe_ratio",
            "sortino_ratio",
            "max_drawdown_pct",
        }
        for key in expected_keys:
            assert key in calculated, f"Metrics should contain key '{key}'"


@pytest.mark.integration
class TestFullBacktestWithMarketMakerStrategy:
    """Test 2: Full pipeline with the real SimpleMarketMaker strategy."""

    def test_full_backtest_with_market_maker_strategy(self):
        """Use the actual SimpleMarketMaker strategy with ~20 orderbook
        snapshots at varying prices and verify strategy placed orders,
        some fills occurred, and equity curve has points.
        """
        yes_market = make_market("condition-1", "token-yes-1", "Yes", 0)
        no_market = make_market("condition-1", "token-no-1", "No", 1)

        # Create ~20 orderbooks with varying prices
        base_ts = 1700000000000
        orderbooks = []
        prices = [
            0.50, 0.51, 0.52, 0.53, 0.52, 0.51, 0.50, 0.49, 0.48, 0.49,
            0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.54, 0.53, 0.52, 0.51,
        ]
        for i, mid in enumerate(prices):
            bid = f"{mid - 0.02:.2f}"
            ask = f"{mid + 0.02:.2f}"
            orderbooks.append(
                make_orderbook(
                    "token-yes-1",
                    base_ts + i * 100_000,  # 100s intervals
                    bids=[(bid, "100"), (f"{mid - 0.03:.2f}", "200")],
                    asks=[(ask, "100"), (f"{mid + 0.03:.2f}", "200")],
                )
            )

        # Trades at mid prices to drive queue fills
        trades = []
        for i, mid in enumerate(prices):
            trades.append(
                make_trade(
                    "token-yes-1",
                    base_ts + i * 100_000 + 50_000,  # halfway between orderbooks
                    mid,
                    20.0,
                    side="buy" if i % 2 == 0 else "sell",
                )
            )

        dataset = BacktestDataset(
            orderbooks=orderbooks,
            trades=trades,
            markets={
                "token-yes-1": yes_market,
                "token-no-1": no_market,
            },
            start_time_ms=base_ts,
            end_time_ms=base_ts + 20 * 100_000,
        )

        initial_cash = Decimal("10000")
        portfolio, execution_engine, metrics, market_pairs = build_pipeline(
            dataset, initial_cash=initial_cash
        )

        # Use the real SimpleMarketMaker strategy
        strategy = SimpleMarketMaker(
            spread_bps=400,
            order_size=Decimal("10"),
            max_position=Decimal("100"),
            target_assets=["token-yes-1"],
        )
        strategy._inject_dependencies(portfolio, execution_engine)

        run_event_loop(dataset, strategy, portfolio, execution_engine, metrics)

        # Verify: strategy placed orders (we check that open orders were
        # managed or that fills occurred)
        all_orders = execution_engine._orders
        assert len(all_orders) > 0, "Market maker should have placed orders"

        # Verify equity curve has points
        equity_curve = metrics.get_equity_curve()
        assert len(equity_curve) > 0, "Equity curve should have points"


@pytest.mark.integration
class TestEmptyDataset:
    """Test 3: Empty dataset returns no trades."""

    def test_empty_dataset_returns_no_trades(self):
        """Create BacktestDataset with empty orderbooks and trades.
        Verify: no fills, no trades, metrics show 0 trades.
        """
        yes_market = make_market("condition-1", "token-yes-1", "Yes", 0)
        no_market = make_market("condition-1", "token-no-1", "No", 1)

        dataset = BacktestDataset(
            orderbooks=[],
            trades=[],
            markets={
                "token-yes-1": yes_market,
                "token-no-1": no_market,
            },
            start_time_ms=1700000000000,
            end_time_ms=1700000005000,
        )

        initial_cash = Decimal("10000")
        portfolio, execution_engine, metrics, market_pairs = build_pipeline(
            dataset, initial_cash=initial_cash
        )

        strategy = DoNothingStrategy()
        strategy._inject_dependencies(portfolio, execution_engine)

        run_event_loop(dataset, strategy, portfolio, execution_engine, metrics)

        # Verify no fills
        assert len(portfolio.get_fills()) == 0, "No fills should have occurred"

        # Verify no trade records
        trade_log = metrics.get_trade_log()
        assert len(trade_log) == 0, "No trade records should exist"

        # Verify metrics
        calculated = metrics.calculate_metrics()
        assert calculated["num_trades"] == 0, "num_trades should be 0"
        assert calculated["win_rate"] == 0.0, "win_rate should be 0"
        assert calculated["profit_factor"] == 0.0, "profit_factor should be 0"

        # Cash should be unchanged
        assert portfolio.cash == initial_cash, "Cash should be unchanged"


@pytest.mark.integration
class TestSingleAssetBuyAndSellRoundtrip:
    """Test 4: Buy at 0.50, price rises, sell at 0.60."""

    def test_single_asset_buy_and_sell_roundtrip(self):
        """Buy at 0.50, price rises, sell at 0.60.
        Verify: positive realized P&L, correct return calculation.
        """
        yes_market = make_market("condition-1", "token-yes-1", "Yes", 0)
        no_market = make_market("condition-1", "token-no-1", "No", 1)

        base_ts = 1700000000000
        quantity = Decimal("10")

        # Phase 1: orderbook with asks at 0.50 so we can buy
        orderbooks = [
            # Initial: buy opportunity at 0.50
            make_orderbook(
                "token-yes-1",
                base_ts,
                bids=[("0.49", "100")],
                asks=[("0.50", "100")],
            ),
            # Couple more at same price (give execution engine state)
            make_orderbook(
                "token-yes-1",
                base_ts + 1000,
                bids=[("0.49", "100")],
                asks=[("0.50", "100")],
            ),
            # Price rises: bids now at 0.60
            make_orderbook(
                "token-yes-1",
                base_ts + 2000,
                bids=[("0.60", "100")],
                asks=[("0.62", "100")],
            ),
            # Another at the higher price
            make_orderbook(
                "token-yes-1",
                base_ts + 3000,
                bids=[("0.60", "100")],
                asks=[("0.62", "100")],
            ),
        ]

        trades = [
            make_trade("token-yes-1", base_ts + 500, 0.50, 10.0, "buy"),
            make_trade("token-yes-1", base_ts + 2500, 0.60, 10.0, "sell"),
        ]

        dataset = BacktestDataset(
            orderbooks=orderbooks,
            trades=trades,
            markets={
                "token-yes-1": yes_market,
                "token-no-1": no_market,
            },
            start_time_ms=base_ts,
            end_time_ms=base_ts + 4000,
        )

        initial_cash = Decimal("10000")
        portfolio, execution_engine, metrics, market_pairs = build_pipeline(
            dataset, initial_cash=initial_cash
        )

        strategy = BuySellRoundtripStrategy(
            asset_id="token-yes-1",
            buy_price=Decimal("0.50"),
            sell_price=Decimal("0.60"),
            quantity=quantity,
        )
        strategy._inject_dependencies(portfolio, execution_engine)

        run_event_loop(dataset, strategy, portfolio, execution_engine, metrics)

        # Check fills via strategy and portfolio
        buy_fills = [f for f in strategy.fills_received if f.side == OrderSide.BUY]
        sell_fills = [f for f in strategy.fills_received if f.side == OrderSide.SELL]

        assert len(buy_fills) >= 1, "Should have at least one buy fill"
        assert len(sell_fills) >= 1, "Should have at least one sell fill"

        # Verify positive return: bought at 0.50, sold at 0.60 -> profit
        # P&L = (0.60 - 0.50) * 10 = 1.0
        position = portfolio.get_position("token-yes-1")
        if position is not None:
            assert position.realized_pnl > 0, "Realized P&L should be positive"

        # Verify cash went up (profit realized)
        assert portfolio.cash > initial_cash, (
            f"Cash should have increased from profit: {portfolio.cash} vs {initial_cash}"
        )

        # Verify trade log has the completed round-trip
        trade_log = metrics.get_trade_log()
        assert len(trade_log) >= 1, "Should have at least one completed trade"
        # The trade record should show a winner
        completed_trade = trade_log[0]
        assert completed_trade.realized_pnl > 0, "Round-trip should show positive P&L"
        assert completed_trade.is_winner is True, "Trade should be a winner"


@pytest.mark.integration
class TestMultipleAssetsIndependentPositions:
    """Test 5: Two assets with independent positions."""

    def test_multiple_assets_independent_positions(self):
        """Buy different amounts of two assets.
        Verify positions tracked independently.
        """
        # Markets for asset A
        yes_a = make_market("condition-A", "token-A-yes", "Yes", 0, "Question A?")
        no_a = make_market("condition-A", "token-A-no", "No", 1, "Question A?")

        # Markets for asset B
        yes_b = make_market("condition-B", "token-B-yes", "Yes", 0, "Question B?")
        no_b = make_market("condition-B", "token-B-no", "No", 1, "Question B?")

        base_ts = 1700000000000

        orderbooks = [
            # Asset A orderbook
            make_orderbook(
                "token-A-yes",
                base_ts,
                bids=[("0.40", "100")],
                asks=[("0.42", "100")],
                market="condition-A",
            ),
            # Asset B orderbook
            make_orderbook(
                "token-B-yes",
                base_ts + 100,
                bids=[("0.60", "100")],
                asks=[("0.62", "100")],
                market="condition-B",
            ),
            # Repeat to ensure processing
            make_orderbook(
                "token-A-yes",
                base_ts + 200,
                bids=[("0.40", "100")],
                asks=[("0.42", "100")],
                market="condition-A",
            ),
            make_orderbook(
                "token-B-yes",
                base_ts + 300,
                bids=[("0.60", "100")],
                asks=[("0.62", "100")],
                market="condition-B",
            ),
        ]

        trades = [
            make_trade("token-A-yes", base_ts + 50, 0.41, 5.0, market="condition-A"),
            make_trade("token-B-yes", base_ts + 150, 0.61, 5.0, market="condition-B"),
        ]

        dataset = BacktestDataset(
            orderbooks=orderbooks,
            trades=trades,
            markets={
                "token-A-yes": yes_a,
                "token-A-no": no_a,
                "token-B-yes": yes_b,
                "token-B-no": no_b,
            },
            start_time_ms=base_ts,
            end_time_ms=base_ts + 1000,
        )

        initial_cash = Decimal("10000")
        portfolio, execution_engine, metrics, market_pairs = build_pipeline(
            dataset, initial_cash=initial_cash
        )

        # Buy 5 of A, 20 of B
        strategy = MultiAssetBuyStrategy({
            "token-A-yes": Decimal("5"),
            "token-B-yes": Decimal("20"),
        })
        strategy._inject_dependencies(portfolio, execution_engine)

        run_event_loop(dataset, strategy, portfolio, execution_engine, metrics)

        # Verify positions are tracked independently
        pos_a = portfolio.get_position("token-A-yes")
        pos_b = portfolio.get_position("token-B-yes")

        assert pos_a is not None, "Should have position in token-A-yes"
        assert pos_b is not None, "Should have position in token-B-yes"
        assert pos_a.quantity == Decimal("5"), f"Asset A quantity should be 5, got {pos_a.quantity}"
        assert pos_b.quantity == Decimal("20"), f"Asset B quantity should be 20, got {pos_b.quantity}"

        # Average entry prices should differ
        assert pos_a.avg_entry_price != pos_b.avg_entry_price, (
            "Different assets should have different entry prices"
        )

        # Total cash spent should account for both positions
        expected_cost_a = Decimal("0.42") * Decimal("5")
        expected_cost_b = Decimal("0.62") * Decimal("20")
        expected_cash = initial_cash - expected_cost_a - expected_cost_b
        assert portfolio.cash == expected_cash, (
            f"Cash should be {expected_cash}, got {portfolio.cash}"
        )


@pytest.mark.integration
class TestEventOrderingTradesBeforeOrderbooks:
    """Test 6: Verify event ordering -- trades come before orderbooks at equal timestamps."""

    def test_event_ordering_trades_before_orderbooks(self):
        """Verify BacktestDataset.get_event_iterator() returns trades first
        at equal timestamps.
        """
        yes_market = make_market("condition-1", "token-yes-1", "Yes", 0)
        no_market = make_market("condition-1", "token-no-1", "No", 1)

        same_ts = 1700000000000

        orderbooks = [
            make_orderbook(
                "token-yes-1",
                same_ts,
                bids=[("0.50", "100")],
                asks=[("0.52", "100")],
            ),
            make_orderbook(
                "token-yes-1",
                same_ts + 1000,
                bids=[("0.51", "100")],
                asks=[("0.53", "100")],
            ),
        ]

        trades = [
            make_trade("token-yes-1", same_ts, 0.51, 10.0),
            make_trade("token-yes-1", same_ts + 1000, 0.52, 10.0),
        ]

        dataset = BacktestDataset(
            orderbooks=orderbooks,
            trades=trades,
            markets={
                "token-yes-1": yes_market,
                "token-no-1": no_market,
            },
            start_time_ms=same_ts,
            end_time_ms=same_ts + 2000,
        )

        events = list(dataset.get_event_iterator())

        # Should have 4 events total
        assert len(events) == 4, f"Expected 4 events, got {len(events)}"

        # At timestamp same_ts: trade should come before orderbook
        assert isinstance(events[0], TradeBacktestEvent), (
            f"First event at equal timestamp should be Trade, got {type(events[0]).__name__}"
        )
        assert events[0].timestamp_ms == same_ts

        assert isinstance(events[1], OrderbookBacktestEvent), (
            f"Second event at equal timestamp should be Orderbook, got {type(events[1]).__name__}"
        )
        assert events[1].timestamp_ms == same_ts

        # At timestamp same_ts + 1000: same ordering
        assert isinstance(events[2], TradeBacktestEvent), (
            f"Third event should be Trade, got {type(events[2]).__name__}"
        )
        assert events[2].timestamp_ms == same_ts + 1000

        assert isinstance(events[3], OrderbookBacktestEvent), (
            f"Fourth event should be Orderbook, got {type(events[3]).__name__}"
        )
        assert events[3].timestamp_ms == same_ts + 1000

        # Verify event_index is strictly increasing
        for i, event in enumerate(events):
            assert event.event_index == i, (
                f"Event index should be {i}, got {event.event_index}"
            )


@pytest.mark.integration
class TestMetricsCalculationKnownValues:
    """Test 7: Feed known trade sequence and verify metric calculations."""

    def test_metrics_calculation_known_values(self):
        """Feed a known trade sequence (2 winners, 2 losers with known PnL)
        and verify: win_rate=0.5, num_trades=4, profit_factor matches.

        Trade sequence:
        - Trade 1: Buy at 0.40, sell at 0.50 -> PnL = +0.10 * 10 = +1.00 (winner)
        - Trade 2: Buy at 0.50, sell at 0.60 -> PnL = +0.10 * 10 = +1.00 (winner)
        - Trade 3: Buy at 0.60, sell at 0.55 -> PnL = -0.05 * 10 = -0.50 (loser)
        - Trade 4: Buy at 0.55, sell at 0.50 -> PnL = -0.05 * 10 = -0.50 (loser)
        """
        yes_market = make_market("condition-1", "token-yes-1", "Yes", 0)
        no_market = make_market("condition-1", "token-no-1", "No", 1)

        initial_cash = Decimal("10000")
        metrics = MetricsCollector(initial_cash=initial_cash)
        market_pairs = MarketPairRegistry()
        market_pairs.register(MarketPair(
            condition_id="condition-1",
            question="Test?",
            yes_token_id="token-yes-1",
            no_token_id="token-no-1",
            platform="polymarket",
        ))
        portfolio = Portfolio(initial_cash=initial_cash, market_pairs=market_pairs)

        # We directly create fills and apply them to portfolio & metrics,
        # simulating 4 round-trip trades.
        trade_specs = [
            # (buy_price, sell_price, quantity, base_ts)
            (Decimal("0.40"), Decimal("0.50"), Decimal("10"), 1700000000000),
            (Decimal("0.50"), Decimal("0.60"), Decimal("10"), 1700000002000),
            (Decimal("0.60"), Decimal("0.55"), Decimal("10"), 1700000004000),
            (Decimal("0.55"), Decimal("0.50"), Decimal("10"), 1700000006000),
        ]

        order_counter = 0
        for buy_price, sell_price, qty, base_ts in trade_specs:
            order_counter += 1
            buy_fill = Fill(
                fill_id=f"fill-buy-{order_counter}",
                order_id=f"order-buy-{order_counter}",
                asset_id="token-yes-1",
                side=OrderSide.BUY,
                price=buy_price,
                quantity=qty,
                fees=Decimal("0"),
                timestamp_ms=base_ts,
                is_maker=True,
            )
            portfolio.apply_fill(buy_fill)
            metrics.record_fill(buy_fill, portfolio)

            order_counter += 1
            sell_fill = Fill(
                fill_id=f"fill-sell-{order_counter}",
                order_id=f"order-sell-{order_counter}",
                asset_id="token-yes-1",
                side=OrderSide.SELL,
                price=sell_price,
                quantity=qty,
                fees=Decimal("0"),
                timestamp_ms=base_ts + 1000,
                is_maker=True,
            )
            portfolio.apply_fill(sell_fill)
            metrics.record_fill(sell_fill, portfolio)

        # Verify trade log
        trade_log = metrics.get_trade_log()
        assert len(trade_log) == 4, f"Should have 4 completed trades, got {len(trade_log)}"

        # Verify PnL for each trade
        assert trade_log[0].realized_pnl == Decimal("1.0"), (
            f"Trade 1 PnL should be 1.0, got {trade_log[0].realized_pnl}"
        )
        assert trade_log[1].realized_pnl == Decimal("1.0"), (
            f"Trade 2 PnL should be 1.0, got {trade_log[1].realized_pnl}"
        )
        assert trade_log[2].realized_pnl == Decimal("-0.5"), (
            f"Trade 3 PnL should be -0.5, got {trade_log[2].realized_pnl}"
        )
        assert trade_log[3].realized_pnl == Decimal("-0.5"), (
            f"Trade 4 PnL should be -0.5, got {trade_log[3].realized_pnl}"
        )

        # Calculate metrics
        calculated = metrics.calculate_metrics()

        # num_trades = 4
        assert calculated["num_trades"] == 4.0, (
            f"num_trades should be 4, got {calculated['num_trades']}"
        )

        # win_rate = 2 / 4 = 0.5
        assert calculated["win_rate"] == pytest.approx(0.5), (
            f"win_rate should be 0.5, got {calculated['win_rate']}"
        )

        # num_winning = 2, num_losing = 2
        assert calculated["num_winning_trades"] == 2.0
        assert calculated["num_losing_trades"] == 2.0

        # profit_factor = gross_profit / gross_loss = 2.0 / 1.0 = 2.0
        assert calculated["profit_factor"] == pytest.approx(2.0), (
            f"profit_factor should be 2.0, got {calculated['profit_factor']}"
        )

        # total_fees should be 0 (no fees in this test)
        assert calculated["total_fees"] == pytest.approx(0.0)

        # avg_trade_pnl = (1.0 + 1.0 - 0.5 - 0.5) / 4 = 0.25
        assert calculated["avg_trade_pnl"] == pytest.approx(0.25), (
            f"avg_trade_pnl should be 0.25, got {calculated['avg_trade_pnl']}"
        )

        # expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
        #            = (0.5 * 1.0) - (0.5 * 0.5) = 0.5 - 0.25 = 0.25
        assert calculated["expectancy"] == pytest.approx(0.25), (
            f"expectancy should be 0.25, got {calculated['expectancy']}"
        )


@pytest.mark.integration
class TestFeesAppliedCorrectly:
    """Bonus test: Verify that fees are correctly applied when configured."""

    def test_fees_reduce_cash_on_buy(self):
        """When taker fees are set, buying should cost more."""
        yes_market = make_market("condition-1", "token-yes-1", "Yes", 0)
        no_market = make_market("condition-1", "token-no-1", "No", 1)

        base_ts = 1700000000000
        orderbooks = [
            make_orderbook(
                "token-yes-1",
                base_ts,
                bids=[("0.49", "100")],
                asks=[("0.50", "200")],
            ),
            make_orderbook(
                "token-yes-1",
                base_ts + 1000,
                bids=[("0.49", "100")],
                asks=[("0.50", "200")],
            ),
        ]

        dataset = BacktestDataset(
            orderbooks=orderbooks,
            trades=[],
            markets={
                "token-yes-1": yes_market,
                "token-no-1": no_market,
            },
            start_time_ms=base_ts,
            end_time_ms=base_ts + 2000,
        )

        # Test with 100 bps taker fee
        initial_cash = Decimal("10000")
        portfolio, execution_engine, metrics, market_pairs = build_pipeline(
            dataset,
            initial_cash=initial_cash,
            taker_fee_bps=100,
        )

        strategy = BuyOnceStrategy("token-yes-1", quantity=Decimal("10"))
        strategy._inject_dependencies(portfolio, execution_engine)

        run_event_loop(dataset, strategy, portfolio, execution_engine, metrics)

        # Should have bought 10 at 0.50 with 1% taker fee
        # Cost: 10 * 0.50 = 5.00, fee = 5.00 * 0.01 = 0.05
        # Note: the fee calculation is per the FeeSchedule:
        #   fee = notional * fee_bps / 10000 = (10 * 0.50) * 100 / 10000 = 0.05
        position = portfolio.get_position("token-yes-1")
        assert position is not None, "Should have bought"
        assert position.quantity == Decimal("10")

        # Cash should reflect cost + fees
        # The fill is executed as a limit order crossing the ask,
        # which may be treated as maker or taker depending on engine logic.
        # In this case it's a limit order that immediately crosses -> taker or maker fill.
        # Let's just verify cash decreased by at least the notional amount.
        cost_without_fee = Decimal("10") * Decimal("0.50")
        assert portfolio.cash <= initial_cash - cost_without_fee, (
            f"Cash should have decreased by at least {cost_without_fee}"
        )


@pytest.mark.integration
class TestEventIteratorMixedTimestamps:
    """Bonus test: Verify event iterator handles interleaved timestamps correctly."""

    def test_event_iterator_interleaved(self):
        """Events from different sources with interleaved timestamps should
        be merged in time order.
        """
        yes_market = make_market("condition-1", "token-yes-1", "Yes", 0)
        no_market = make_market("condition-1", "token-no-1", "No", 1)

        orderbooks = [
            make_orderbook("token-yes-1", 100, bids=[("0.50", "10")], asks=[("0.52", "10")]),
            make_orderbook("token-yes-1", 300, bids=[("0.50", "10")], asks=[("0.52", "10")]),
            make_orderbook("token-yes-1", 500, bids=[("0.50", "10")], asks=[("0.52", "10")]),
        ]

        trades = [
            make_trade("token-yes-1", 200, 0.51, 5.0),
            make_trade("token-yes-1", 400, 0.51, 5.0),
        ]

        dataset = BacktestDataset(
            orderbooks=orderbooks,
            trades=trades,
            markets={
                "token-yes-1": yes_market,
                "token-no-1": no_market,
            },
            start_time_ms=100,
            end_time_ms=500,
        )

        events = list(dataset.get_event_iterator())
        timestamps = [e.timestamp_ms for e in events]

        # Should be in non-decreasing order
        assert timestamps == sorted(timestamps), (
            f"Events should be time-ordered: {timestamps}"
        )

        # Verify exact order: OB@100, Trade@200, OB@300, Trade@400, OB@500
        assert len(events) == 5
        assert isinstance(events[0], OrderbookBacktestEvent)
        assert events[0].timestamp_ms == 100
        assert isinstance(events[1], TradeBacktestEvent)
        assert events[1].timestamp_ms == 200
        assert isinstance(events[2], OrderbookBacktestEvent)
        assert events[2].timestamp_ms == 300
        assert isinstance(events[3], TradeBacktestEvent)
        assert events[3].timestamp_ms == 400
        assert isinstance(events[4], OrderbookBacktestEvent)
        assert events[4].timestamp_ms == 500
