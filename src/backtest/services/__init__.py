def __getattr__(name: str):
    """Lazy imports for services (depend on asyncpg, numpy, matplotlib)."""
    if name == "PostgresDataLoader":
        from .data_loader import PostgresDataLoader
        return PostgresDataLoader
    if name == "ExecutionEngine":
        from .execution_engine import ExecutionEngine
        return ExecutionEngine
    if name == "MetricsCollector":
        from .metrics import MetricsCollector
        return MetricsCollector
    if name in ("TradeRecord", "EquityPoint"):
        from . import metrics
        return getattr(metrics, name)
    if name == "QueueSimulator":
        from .queue_simulator import QueueSimulator
        return QueueSimulator
    if name == "ReportGenerator":
        from .report import ReportGenerator
        return ReportGenerator
    raise AttributeError(f"module 'backtest.services' has no attribute {name!r}")


__all__ = [
    "PostgresDataLoader",
    "ExecutionEngine",
    "MetricsCollector",
    "TradeRecord",
    "EquityPoint",
    "QueueSimulator",
    "ReportGenerator",
]
