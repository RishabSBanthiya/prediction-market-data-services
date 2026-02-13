from .interfaces import (
    BacktestEvent,
    OrderbookBacktestEvent,
    TradeBacktestEvent,
    BacktestDataset,
    IDataLoader,
    IExecutionEngine,
)
from .strategy import Strategy, BacktestContext


def __getattr__(name: str):
    """Lazy import for BacktestEngine (depends on services with heavy deps)."""
    if name == "BacktestEngine":
        from .backtest_engine import BacktestEngine
        return BacktestEngine
    raise AttributeError(f"module 'backtest.core' has no attribute {name!r}")


__all__ = [
    "BacktestEngine",
    "BacktestEvent",
    "OrderbookBacktestEvent",
    "TradeBacktestEvent",
    "BacktestDataset",
    "IDataLoader",
    "IExecutionEngine",
    "Strategy",
    "BacktestContext",
]
