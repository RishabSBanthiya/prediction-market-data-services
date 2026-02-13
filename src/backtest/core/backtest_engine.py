"""
BacktestEngine: main orchestrator for prediction market backtesting.

Loads historical data, creates execution infrastructure, runs the event loop
dispatching to strategy callbacks, and returns a BacktestResult.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

import structlog

from models.orderbook import OrderbookSnapshot
from models.trade import Trade

from ..models.config import BacktestConfig, FeeSchedule, BacktestResult
from ..models.portfolio import Portfolio
from ..models.market_pair import MarketPairRegistry
from .interfaces import (
    BacktestDataset,
    OrderbookBacktestEvent,
    TradeBacktestEvent,
)
from .strategy import Strategy, BacktestContext
from ..services.data_loader import PostgresDataLoader
from ..services.execution_engine import ExecutionEngine
from ..services.metrics import MetricsCollector


logger = structlog.get_logger(__name__)


class BacktestEngine:
    """
    Main backtest orchestrator.

    Coordinates data loading, execution, strategy callbacks, metrics collection,
    and result generation for a single backtest run.

    Usage:
        engine = BacktestEngine(config)
        result = await engine.run(strategy)
    """

    def __init__(
        self,
        config: BacktestConfig,
        show_progress: bool = True,
        equity_sample_interval: int = 5000,
    ):
        """
        Initialize with backtest configuration.

        Args:
            config: Backtest configuration specifying time range, assets,
                    fees, initial capital, and database connection.
            show_progress: Show progress bar during backtest (requires tqdm).
            equity_sample_interval: Sample equity every N events. Higher values
                    improve performance at the cost of equity curve granularity.
        """
        self._config = config
        self._show_progress = show_progress
        self._equity_sample_interval = equity_sample_interval
        self._logger = logger.bind(
            platform=config.platform,
            start_time_ms=config.start_time_ms,
            end_time_ms=config.end_time_ms,
        )

    async def run(self, strategy: Strategy) -> BacktestResult:
        """
        Execute a full backtest.

        Steps:
        1. Initialize components (portfolio, execution engine, metrics, data loader)
        2. Load data from database
        3. Build MarketPairRegistry from loaded markets
        4. Inject dependencies into strategy
        5. Call strategy.on_start()
        6. Event loop: iterate through time-ordered events
           a. For OrderbookBacktestEvent:
              - Update execution engine (process_orderbook_update) -> fills
              - Update portfolio mark prices from snapshot
              - Record fills in metrics
              - Call strategy.on_fill() for each fill
              - Call strategy.on_orderbook()
           b. For TradeBacktestEvent:
              - Update execution engine (process_trade) -> fills
              - Record fills in metrics
              - Call strategy.on_fill() for each fill
              - Call strategy.on_trade()
           c. Periodically record equity point in metrics
        7. Call strategy.on_end()
        8. Calculate final metrics
        9. Build and return BacktestResult

        Args:
            strategy: Strategy instance to run against historical data.

        Returns:
            BacktestResult containing performance metrics, equity curve, etc.
        """
        self._logger.info(
            "backtest_starting",
            strategy=strategy.name,
            initial_cash=self._config.initial_cash,
        )

        # ------------------------------------------------------------------
        # 1. Load data
        # ------------------------------------------------------------------
        data_loader = PostgresDataLoader()
        try:
            dataset = await data_loader.load(self._config)
        finally:
            await data_loader.close()

        total_events = len(dataset.orderbooks) + len(dataset.trades)
        self._logger.info(
            "data_loaded",
            orderbooks=len(dataset.orderbooks),
            trades=len(dataset.trades),
            markets=len(dataset.markets),
            total_events=total_events,
        )

        # ------------------------------------------------------------------
        # 2. Build market pairs
        # ------------------------------------------------------------------
        market_pairs = MarketPairRegistry.build_from_markets(
            list(dataset.markets.values())
        )

        # ------------------------------------------------------------------
        # 3. Create portfolio
        # ------------------------------------------------------------------
        initial_cash_decimal = Decimal(str(self._config.initial_cash))
        portfolio = Portfolio(
            initial_cash=initial_cash_decimal,
            market_pairs=market_pairs,
        )

        # ------------------------------------------------------------------
        # 4. Create fee schedule from config
        # ------------------------------------------------------------------
        fee_schedule = FeeSchedule(
            maker_fee_bps=self._config.maker_fee_bps,
            taker_fee_bps=self._config.taker_fee_bps,
        )

        # ------------------------------------------------------------------
        # 5. Create execution engine
        # ------------------------------------------------------------------
        execution_engine = ExecutionEngine(
            portfolio=portfolio,
            fee_schedule=fee_schedule,
            market_pairs=market_pairs,
        )

        # ------------------------------------------------------------------
        # 6. Create metrics collector
        # ------------------------------------------------------------------
        metrics = MetricsCollector(initial_cash=initial_cash_decimal)

        # ------------------------------------------------------------------
        # 7. Inject dependencies into strategy
        # ------------------------------------------------------------------
        strategy._inject_dependencies(portfolio, execution_engine)

        # ------------------------------------------------------------------
        # 8. Build context
        # ------------------------------------------------------------------
        context = BacktestContext(
            start_time_ms=dataset.start_time_ms,
            end_time_ms=dataset.end_time_ms,
            initial_cash=float(self._config.initial_cash),
            platform=self._config.platform,
            markets=dataset.markets,
        )

        # ------------------------------------------------------------------
        # 9. Call strategy.on_start()
        # ------------------------------------------------------------------
        try:
            strategy.on_start(context)
        except Exception as e:
            self._logger.error("strategy_on_start_error", error=str(e))

        # ------------------------------------------------------------------
        # 10. Event loop
        # ------------------------------------------------------------------
        event_count = 0
        log_interval = 10_000
        last_prices: dict[str, Decimal] = {}

        # Set up progress bar (optional dependency)
        try:
            from tqdm import tqdm
            has_tqdm = True
        except ImportError:
            has_tqdm = False

        iterator = dataset.get_event_iterator()
        if self._show_progress and has_tqdm:
            iterator = tqdm(
                iterator,
                total=total_events,
                desc="Backtesting",
                unit="events",
            )

        for event in iterator:
            event_count += 1

            if event_count % log_interval == 0:
                self._logger.info(
                    "backtest_progress", events_processed=event_count
                )

            if isinstance(event, OrderbookBacktestEvent):
                # Track latest mid prices for equity sampling
                snapshot = event.snapshot
                if snapshot.mid_price is not None:
                    last_prices[snapshot.asset_id] = Decimal(str(snapshot.mid_price))

                self._process_orderbook_event(
                    event, execution_engine, portfolio, metrics, strategy
                )

            elif isinstance(event, TradeBacktestEvent):
                self._process_trade_event(
                    event, execution_engine, portfolio, metrics, strategy
                )

            # Periodic equity sampling (independent of fills)
            if event_count % self._equity_sample_interval == 0 and last_prices:
                metrics.record_equity_point(
                    event.timestamp_ms, portfolio, last_prices
                )

        # Record final equity point
        if last_prices:
            final_ts = dataset.end_time_ms
            metrics.record_equity_point(final_ts, portfolio, last_prices)

        self._logger.info(
            "event_loop_complete", total_events=event_count
        )

        # ------------------------------------------------------------------
        # 11. Call strategy.on_end()
        # ------------------------------------------------------------------
        try:
            strategy.on_end(context)
        except Exception as e:
            self._logger.error("strategy_on_end_error", error=str(e))

        # ------------------------------------------------------------------
        # 12. Calculate metrics and build result
        # ------------------------------------------------------------------
        result = self._build_result(
            strategy=strategy,
            dataset=dataset,
            portfolio=portfolio,
            metrics=metrics,
        )

        self._logger.info(
            "backtest_complete",
            strategy=strategy.name,
            final_equity=float(portfolio.total_value),
            total_return=result.total_return,
            num_trades=result.num_trades,
            events_processed=event_count,
        )

        return result

    # ------------------------------------------------------------------
    # Event processing helpers
    # ------------------------------------------------------------------

    def _process_orderbook_event(
        self,
        event: OrderbookBacktestEvent,
        execution_engine: ExecutionEngine,
        portfolio: Portfolio,
        metrics: MetricsCollector,
        strategy: Strategy,
    ) -> None:
        """
        Process a single orderbook event through the execution pipeline.

        Order of operations prevents lookahead bias:
        1. Feed to execution engine (may generate fills)
        2. Update portfolio mark prices
        3. Record and notify fills
        4. Call strategy.on_orderbook()
        """
        snapshot = event.snapshot

        # 1. Process through execution engine
        fills = execution_engine.process_orderbook_update(snapshot)

        # 2. Update mark prices for portfolio
        prices: dict[str, Decimal] = {}
        if snapshot.mid_price is not None:
            prices[snapshot.asset_id] = Decimal(str(snapshot.mid_price))
        portfolio.update_mark_prices(prices)

        # 3. Record fills and notify strategy
        for fill in fills:
            metrics.record_fill(fill, portfolio)
            try:
                strategy.on_fill(fill)
            except Exception as e:
                self._logger.error(
                    "strategy_on_fill_error",
                    error=str(e),
                    fill_id=fill.fill_id,
                )

        # 4. Call strategy with orderbook data
        is_ff = getattr(snapshot, "is_forward_filled", False) or False
        try:
            strategy.on_orderbook(snapshot, is_ff)
        except Exception as e:
            self._logger.error(
                "strategy_on_orderbook_error",
                error=str(e),
                asset_id=snapshot.asset_id,
                timestamp=snapshot.timestamp,
            )

    def _process_trade_event(
        self,
        event: TradeBacktestEvent,
        execution_engine: ExecutionEngine,
        portfolio: Portfolio,
        metrics: MetricsCollector,
        strategy: Strategy,
    ) -> None:
        """
        Process a single trade event through the execution pipeline.

        Order of operations prevents lookahead bias:
        1. Feed to execution engine (may generate fills via queue advancement)
        2. Record and notify fills
        3. Call strategy.on_trade()
        """
        trade = event.trade

        # 1. Process through execution engine
        fills = execution_engine.process_trade(trade)

        # 2. Record fills and notify strategy
        for fill in fills:
            metrics.record_fill(fill, portfolio)
            try:
                strategy.on_fill(fill)
            except Exception as e:
                self._logger.error(
                    "strategy_on_fill_error",
                    error=str(e),
                    fill_id=fill.fill_id,
                )

        # 3. Call strategy with trade data
        try:
            strategy.on_trade(trade)
        except Exception as e:
            self._logger.error(
                "strategy_on_trade_error",
                error=str(e),
                asset_id=trade.asset_id,
                timestamp=trade.timestamp,
            )

    # ------------------------------------------------------------------
    # Result construction
    # ------------------------------------------------------------------

    def _build_result(
        self,
        strategy: Strategy,
        dataset: BacktestDataset,
        portfolio: Portfolio,
        metrics: MetricsCollector,
    ) -> BacktestResult:
        """
        Build BacktestResult from collected metrics and portfolio state.

        Calculates final metrics, extracts equity/drawdown curves, and
        assembles the result object.

        Args:
            strategy: The strategy that was run.
            dataset: The loaded dataset (for time range).
            portfolio: Final portfolio state.
            metrics: Collected metrics throughout the backtest.

        Returns:
            BacktestResult with all performance data.
        """
        calculated = metrics.calculate_metrics()
        equity_curve = metrics.get_equity_curve()
        trade_log = metrics.get_trade_log()

        final_equity = float(portfolio.total_value)
        initial_cash = float(self._config.initial_cash)

        # Total return as a decimal fraction (e.g. 0.15 for 15%)
        total_return = (
            (final_equity - initial_cash) / initial_cash
            if initial_cash > 0
            else 0.0
        )

        # Build equity curve as list of (timestamp_ms, equity_float) tuples
        equity_curve_tuples: list[tuple[int, float]] = [
            (ep.timestamp_ms, float(ep.equity)) for ep in equity_curve
        ]

        # Build drawdown curve from equity curve
        drawdown_curve_tuples: list[tuple[int, float]] = (
            self._compute_drawdown_curve(equity_curve_tuples)
        )

        # Extract max drawdown as a fraction (e.g. -0.05 for 5% drawdown)
        max_drawdown_pct = calculated.get("max_drawdown_pct", 0.0)
        max_drawdown = max_drawdown_pct / 100.0  # convert pct to fraction

        # Trade statistics
        num_trades = int(calculated.get("num_trades", 0))
        num_winning = int(calculated.get("num_winning_trades", 0))
        num_losing = int(calculated.get("num_losing_trades", 0))

        # Win/loss averages
        winning_trades = [t for t in trade_log if t.realized_pnl > 0]
        losing_trades = [t for t in trade_log if t.realized_pnl <= 0]

        avg_win = (
            float(sum(t.realized_pnl for t in winning_trades) / len(winning_trades))
            if winning_trades
            else 0.0
        )
        avg_loss = (
            float(
                abs(sum(t.realized_pnl for t in losing_trades))
                / len(losing_trades)
            )
            if losing_trades
            else 0.0
        )

        result = BacktestResult(
            config=self._config,
            strategy_name=strategy.name,
            total_return=total_return,
            sharpe_ratio=calculated.get("sharpe_ratio"),
            sortino_ratio=calculated.get("sortino_ratio"),
            max_drawdown=max_drawdown,
            win_rate=calculated.get("win_rate", 0.0),
            profit_factor=calculated.get("profit_factor"),
            num_trades=num_trades,
            num_winning_trades=num_winning,
            num_losing_trades=num_losing,
            avg_win=avg_win,
            avg_loss=avg_loss,
            total_fees_paid=calculated.get("total_fees", 0.0),
            equity_curve=equity_curve_tuples,
            drawdown_curve=drawdown_curve_tuples,
            final_equity=final_equity,
        )

        return result

    @staticmethod
    def _compute_drawdown_curve(
        equity_curve: list[tuple[int, float]],
    ) -> list[tuple[int, float]]:
        """
        Compute drawdown curve from an equity curve.

        At each point, drawdown is defined as (equity - running_max) / running_max,
        expressed as a negative fraction (e.g. -0.05 for a 5% drawdown from peak).

        Args:
            equity_curve: List of (timestamp_ms, equity) tuples.

        Returns:
            List of (timestamp_ms, drawdown_fraction) tuples.
        """
        if not equity_curve:
            return []

        drawdown_curve: list[tuple[int, float]] = []
        running_max = equity_curve[0][1]

        for ts, equity in equity_curve:
            if equity > running_max:
                running_max = equity
            dd = (equity - running_max) / running_max if running_max > 0 else 0.0
            drawdown_curve.append((ts, dd))

        return drawdown_curve
