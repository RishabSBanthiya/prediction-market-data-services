#!/usr/bin/env python3
"""
Run a backtest with the OrderbookSignalStrategy against real data.

Uses the OrderbookSignalAnalyzer to compute microprice, book imbalance,
and liquidity node signals, then trades directionally when signals align.

Usage:
    python scripts/examples/run_signal_backtest.py
    python scripts/examples/run_signal_backtest.py --listener-id <UUID>
    python scripts/examples/run_signal_backtest.py --asset-ids TOKEN1 TOKEN2
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from backtest.core.backtest_engine import BacktestEngine
from backtest.models.config import BacktestConfig, BacktestResult
from backtest.strategies.examples.signal_strategy import OrderbookSignalStrategy


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a backtest with the OrderbookSignalStrategy.",
    )

    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--platform", type=str, default=None, choices=["polymarket", "kalshi"])
    parser.add_argument("--asset-ids", type=str, nargs="+", default=None)
    parser.add_argument("--listener-id", type=str, default=None)
    parser.add_argument("--initial-cash", type=float, default=10_000.0)
    parser.add_argument("--maker-fee-bps", type=int, default=0)
    parser.add_argument("--taker-fee-bps", type=int, default=100)

    # Signal strategy parameters
    parser.add_argument("--imbalance-threshold", type=float, default=0.15)
    parser.add_argument("--microprice-div-threshold", type=float, default=0.003)
    parser.add_argument("--min-confidence", type=float, default=0.3)
    parser.add_argument("--order-size", type=float, default=25.0)
    parser.add_argument("--max-position", type=float, default=200.0)
    parser.add_argument("--edge-offset-bps", type=int, default=50)
    parser.add_argument("--n-levels", type=int, default=3)

    # Output
    parser.add_argument("--output-dir", type=str, default="./backtest_results")
    parser.add_argument("--no-plots", action="store_true")
    parser.add_argument("--no-csv", action="store_true")

    parser.add_argument("--postgres-dsn", type=str, default=None)
    parser.add_argument("--no-forward-fills", action="store_true")

    return parser.parse_args()


def build_config(args: argparse.Namespace) -> BacktestConfig:
    postgres_dsn = args.postgres_dsn or os.environ.get("DATABASE_URL", "")
    if not postgres_dsn:
        # Fall back to local defaults
        postgres_dsn = "postgresql://polymarket:polymarket@localhost:5432/polymarket"

    now = datetime.now(timezone.utc)

    if args.start_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start = now - timedelta(days=60)

    if args.end_date:
        end = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end = now

    start_time_ms = int(start.timestamp() * 1000)
    end_time_ms = int(end.timestamp() * 1000)

    return BacktestConfig(
        postgres_dsn=postgres_dsn,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        initial_cash=args.initial_cash,
        platform=args.platform,
        asset_ids=args.asset_ids,
        listener_id=args.listener_id,
        include_forward_filled=not args.no_forward_fills,
        maker_fee_bps=args.maker_fee_bps,
        taker_fee_bps=args.taker_fee_bps,
    )


def print_header(config: BacktestConfig, strategy: OrderbookSignalStrategy) -> None:
    start_dt = datetime.fromtimestamp(config.start_time_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(config.end_time_ms / 1000, tz=timezone.utc)

    print()
    print("=" * 62)
    print("  Orderbook Signal Strategy Backtest")
    print("=" * 62)
    print()
    print(f"  Platform:           {config.platform or 'all'}")
    print(f"  Period:             {start_dt:%Y-%m-%d %H:%M} -> {end_dt:%Y-%m-%d %H:%M} UTC")
    print(f"  Initial Cash:       ${config.initial_cash:,.2f}")
    print(f"  Forward-Filled:     {config.include_forward_filled}")
    print(f"  Maker Fee:          {config.maker_fee_bps} bps")
    print(f"  Taker Fee:          {config.taker_fee_bps} bps")
    if config.asset_ids:
        ids_display = ", ".join(a[:20] + "..." if len(a) > 20 else a for a in config.asset_ids[:5])
        if len(config.asset_ids) > 5:
            ids_display += f" (+{len(config.asset_ids) - 5} more)"
        print(f"  Asset IDs:          {ids_display}")
    if config.listener_id:
        print(f"  Listener ID:        {config.listener_id}")
    print()
    print(f"  Strategy:           {strategy.name}")
    print(f"    imbalance_thresh: {strategy.imbalance_threshold}")
    print(f"    microprice_div:   {strategy.microprice_div_threshold}")
    print(f"    min_confidence:   {strategy.min_confidence}")
    print(f"    order_size:       {strategy.order_size}")
    print(f"    max_position:     {strategy.max_position}")
    print(f"    edge_offset_bps:  {strategy.edge_offset_bps}")
    print(f"    n_levels:         {strategy.analyzer.n_levels}")
    print()


def save_reports(result: BacktestResult, output_dir: Path, *, skip_plots: bool, skip_csv: bool) -> None:
    import csv

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_name = result.strategy_name.replace(" ", "_").lower()

    if not skip_plots and result.equity_curve:
        equity_plot_path = str(output_dir / f"{safe_name}_equity_{timestamp_tag}.png")
        try:
            result.plot_equity(equity_plot_path)
            print(f"  Equity plot saved:    {equity_plot_path}")
        except Exception as exc:
            print(f"  Warning: Could not generate equity plot: {exc}")

    if not skip_csv and result.equity_curve:
        equity_csv_path = str(output_dir / f"{safe_name}_equity_{timestamp_tag}.csv")
        with open(equity_csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp_ms", "equity"])
            for ts_ms, equity in result.equity_curve:
                writer.writerow([ts_ms, f"{equity:.4f}"])
        print(f"  Equity CSV saved:     {equity_csv_path}")


async def main() -> None:
    args = parse_args()
    config = build_config(args)

    strategy = OrderbookSignalStrategy(
        imbalance_threshold=args.imbalance_threshold,
        microprice_div_threshold=args.microprice_div_threshold,
        min_confidence=args.min_confidence,
        order_size=Decimal(str(args.order_size)),
        max_position=Decimal(str(args.max_position)),
        edge_offset_bps=args.edge_offset_bps,
        n_levels=args.n_levels,
    )

    print_header(config, strategy)

    print("Loading historical data and running backtest...")
    print()

    engine = BacktestEngine(config)

    try:
        result: BacktestResult = await engine.run(strategy)
    except ValueError as exc:
        print(f"Configuration error: {exc}")
        sys.exit(1)
    except ConnectionError as exc:
        print(f"Database connection error: {exc}")
        sys.exit(1)
    except Exception as exc:
        import traceback
        print(f"Backtest failed: {exc}")
        traceback.print_exc()
        sys.exit(1)

    # Print results
    print(result.summary())
    print()

    # Save reports
    output_dir = Path(args.output_dir)
    print(f"Saving reports to {output_dir.resolve()}/")
    save_reports(result, output_dir, skip_plots=args.no_plots, skip_csv=args.no_csv)
    print()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
