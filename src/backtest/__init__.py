"""
Backtesting framework for prediction market trading strategies.

Provides a complete pipeline for simulating trading strategies against
historical orderbook and trade data from Polymarket and Kalshi.

Quick start:
    from backtest import BacktestEngine, BacktestConfig, Strategy

    config = BacktestConfig(
        postgres_dsn="postgresql://...",
        listener_id="my-listener",
        start_time_ms=...,
        end_time_ms=...,
    )
    engine = BacktestEngine(config)
    result = await engine.run(my_strategy)
"""

# Lightweight imports (no external dependencies beyond pydantic/structlog)
from .core.strategy import Strategy, BacktestContext
from .models.config import BacktestConfig, FeeSchedule, BacktestResult
from .models.order import Order, Fill, OrderSide, OrderType, OrderStatus, TimeInForce, FillReason, OrderRejectionReason
from .models.portfolio import Portfolio, PortfolioView
from .models.position import Position, MarketPosition
from .models.market_pair import MarketPair, MarketPairRegistry


def __getattr__(name: str):
    """Lazy import for heavy dependencies (asyncpg, numpy, matplotlib)."""
    if name == "BacktestEngine":
        from .core.backtest_engine import BacktestEngine
        return BacktestEngine
    if name == "PostgresDataLoader":
        from .services.data_loader import PostgresDataLoader
        return PostgresDataLoader
    if name == "ExecutionEngine":
        from .services.execution_engine import ExecutionEngine
        return ExecutionEngine
    if name == "MetricsCollector":
        from .services.metrics import MetricsCollector
        return MetricsCollector
    if name == "ReportGenerator":
        from .services.report import ReportGenerator
        return ReportGenerator
    if name == "SimpleMarketMaker":
        from .strategies.examples.market_maker import SimpleMarketMaker
        return SimpleMarketMaker
    raise AttributeError(f"module 'backtest' has no attribute {name!r}")


__all__ = [
    # Engine (lazy)
    "BacktestEngine",
    # Strategy
    "Strategy",
    "BacktestContext",
    # Config & Results
    "BacktestConfig",
    "FeeSchedule",
    "BacktestResult",
    # Orders & Fills
    "Order",
    "Fill",
    "OrderSide",
    "OrderType",
    "OrderStatus",
    "TimeInForce",
    "FillReason",
    "OrderRejectionReason",
    # Portfolio
    "Portfolio",
    "PortfolioView",
    # Positions
    "Position",
    "MarketPosition",
    # Market Pairs
    "MarketPair",
    "MarketPairRegistry",
]

__version__ = "0.1.0"
