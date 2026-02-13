#!/usr/bin/env python3
"""
Example: Run a backtest with the SimpleMarketMaker strategy.

Loads historical orderbook and trade data from PostgreSQL, runs the
SimpleMarketMaker strategy through the BacktestEngine, prints a
performance summary, and optionally saves equity/drawdown plots and
a CSV trade log to an output directory.

Usage:
    python scripts/examples/run_backtest.py --asset-ids TOKEN_ABC TOKEN_XYZ
    python scripts/examples/run_backtest.py --platform kalshi --initial-cash 5000 --asset-ids TICKER_1
    python scripts/examples/run_backtest.py --start-date 2026-02-01 --end-date 2026-02-07 --asset-ids TOKEN_1
    python scripts/examples/run_backtest.py --listener-id 123e4567-e89b-12d3-a456-426614174000
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup: allow running as a standalone script from the repo root.
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from backtest.core.backtest_engine import BacktestEngine  # noqa: E402
from backtest.models.config import BacktestConfig, BacktestResult  # noqa: E402
from backtest.strategies.examples.market_maker import SimpleMarketMaker  # noqa: E402
from backtest.services.report import ReportGenerator  # noqa: E402


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a prediction market backtest with the SimpleMarketMaker strategy.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python scripts/examples/run_backtest.py --asset-ids TOKEN_ABC\n"
            "  python scripts/examples/run_backtest.py --platform kalshi --initial-cash 5000 --asset-ids T1\n"
            "  python scripts/examples/run_backtest.py --start-date 2026-02-01 --end-date 2026-02-07 --asset-ids T1\n"
        ),
    )

    # --- Time range ---
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format. Defaults to 7 days ago.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format. Defaults to now.",
    )

    # --- Scope ---
    parser.add_argument(
        "--platform",
        type=str,
        default="polymarket",
        choices=["polymarket", "kalshi"],
        help="Platform to backtest on (default: polymarket).",
    )
    parser.add_argument(
        "--asset-ids",
        type=str,
        nargs="+",
        default=None,
        help="Specific asset IDs (token IDs or tickers) to trade. Required unless --listener-id is provided.",
    )
    parser.add_argument(
        "--listener-id",
        type=str,
        default=None,
        help="Listener UUID whose discovered assets define the backtest scope. Alternative to --asset-ids.",
    )

    # --- Capital and fees ---
    parser.add_argument(
        "--initial-cash",
        type=float,
        default=10_000.0,
        help="Initial cash balance in dollars (default: 10000).",
    )
    parser.add_argument(
        "--maker-fee-bps",
        type=int,
        default=0,
        help="Maker fee in basis points (default: 0).",
    )
    parser.add_argument(
        "--taker-fee-bps",
        type=int,
        default=0,
        help="Taker fee in basis points (default: 0).",
    )

    # --- Strategy parameters ---
    parser.add_argument(
        "--spread-bps",
        type=int,
        default=300,
        help="Market maker spread in basis points (default: 300 = 3 cents).",
    )
    parser.add_argument(
        "--order-size",
        type=float,
        default=10.0,
        help="Order size (contracts) per side (default: 10).",
    )
    parser.add_argument(
        "--max-position",
        type=float,
        default=100.0,
        help="Maximum position per asset (default: 100).",
    )
    parser.add_argument(
        "--requote-threshold-bps",
        type=int,
        default=50,
        help="Re-quote when mid moves by this many bps (default: 50).",
    )

    # --- Output ---
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./backtest_results",
        help="Directory to save reports, plots, and CSV exports (default: ./backtest_results).",
    )
    parser.add_argument(
        "--no-plots",
        action="store_true",
        help="Skip generating equity and drawdown plots.",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Skip exporting trade log as CSV.",
    )

    # --- Data options ---
    parser.add_argument(
        "--postgres-dsn",
        type=str,
        default=None,
        help="PostgreSQL connection string. Falls back to DATABASE_URL env var.",
    )
    parser.add_argument(
        "--no-forward-fills",
        action="store_true",
        help="Exclude forward-filled snapshots from the backtest.",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Config construction
# ---------------------------------------------------------------------------

def build_config(args: argparse.Namespace) -> BacktestConfig:
    """Translate CLI arguments into a BacktestConfig."""

    # -- Database connection ------------------------------------------------
    postgres_dsn = args.postgres_dsn or os.environ.get("DATABASE_URL", "")
    if not postgres_dsn:
        print(
            "Error: No PostgreSQL DSN provided.\n"
            "Set the DATABASE_URL environment variable or pass --postgres-dsn."
        )
        sys.exit(1)

    # -- Time range ---------------------------------------------------------
    now = datetime.now(timezone.utc)

    if args.start_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    else:
        start = now - timedelta(days=7)

    if args.end_date:
        end = datetime.strptime(args.end_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    else:
        end = now

    start_time_ms = int(start.timestamp() * 1000)
    end_time_ms = int(end.timestamp() * 1000)

    # -- Scope validation ---------------------------------------------------
    if args.asset_ids is None and args.listener_id is None:
        print(
            "Error: You must specify either --asset-ids or --listener-id "
            "to define which markets the backtest covers."
        )
        sys.exit(1)

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


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def print_header(config: BacktestConfig, args: argparse.Namespace) -> None:
    """Print a formatted header summarizing the backtest parameters."""
    start_dt = datetime.fromtimestamp(config.start_time_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(config.end_time_ms / 1000, tz=timezone.utc)

    print()
    print("=" * 62)
    print("  Prediction Market Backtester")
    print("=" * 62)
    print()
    print(f"  Platform:         {config.platform or 'all'}")
    print(
        f"  Period:           {start_dt.strftime('%Y-%m-%d %H:%M')} -> "
        f"{end_dt.strftime('%Y-%m-%d %H:%M')} UTC"
    )
    print(f"  Initial Cash:     ${config.initial_cash:,.2f}")
    print(f"  Forward-Filled:   {config.include_forward_filled}")
    print(f"  Maker Fee:        {config.maker_fee_bps} bps")
    print(f"  Taker Fee:        {config.taker_fee_bps} bps")
    if config.asset_ids:
        ids_display = ", ".join(config.asset_ids[:5])
        if len(config.asset_ids) > 5:
            ids_display += f" ... (+{len(config.asset_ids) - 5} more)"
        print(f"  Asset IDs:        {ids_display}")
    if config.listener_id:
        print(f"  Listener ID:      {config.listener_id}")
    print()
    print(f"  Strategy:         SimpleMarketMaker")
    print(f"    spread:         {args.spread_bps} bps")
    print(f"    order_size:     {args.order_size}")
    print(f"    max_position:   {args.max_position}")
    print(f"    requote_thresh: {args.requote_threshold_bps} bps")
    print()


def print_result_summary(result: BacktestResult) -> None:
    """Print the built-in summary from BacktestResult."""
    print(result.summary())
    print()


# ---------------------------------------------------------------------------
# Report generation (plots and CSV)
# ---------------------------------------------------------------------------

def save_reports(
    result: BacktestResult,
    output_dir: Path,
    *,
    skip_plots: bool = False,
    skip_csv: bool = False,
) -> None:
    """
    Save equity plot, drawdown plot, and trade CSV using ReportGenerator.

    The ReportGenerator methods expect EquityPoint and TradeRecord objects.
    BacktestResult stores equity_curve as list[tuple[int, float]] and does
    not expose the raw trade log. We use BacktestResult.plot_equity() for
    plots (which is self-contained) and fall back to ReportGenerator for
    any format it supports natively.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_name = result.strategy_name.replace(" ", "_").lower()

    # -- Equity and drawdown plots via BacktestResult.plot_equity() ---------
    if not skip_plots:
        if result.equity_curve:
            equity_plot_path = str(
                output_dir / f"{safe_name}_equity_{timestamp_tag}.png"
            )
            try:
                result.plot_equity(equity_plot_path)
                print(f"  Equity plot saved:    {equity_plot_path}")
            except Exception as exc:
                print(f"  Warning: Could not generate equity plot: {exc}")
        else:
            print("  Skipping equity plot (no equity curve data).")

    # -- Trade CSV via ReportGenerator (needs TradeRecord objects) ----------
    # BacktestResult does not expose the raw trade log, so we cannot use
    # ReportGenerator.export_trades_csv directly. Instead, we write a
    # lightweight CSV from the equity curve for post-analysis.
    if not skip_csv and result.equity_curve:
        equity_csv_path = str(
            output_dir / f"{safe_name}_equity_curve_{timestamp_tag}.csv"
        )
        try:
            _export_equity_curve_csv(result.equity_curve, equity_csv_path)
            print(f"  Equity CSV saved:     {equity_csv_path}")
        except Exception as exc:
            print(f"  Warning: Could not export equity CSV: {exc}")

    if not skip_csv and result.drawdown_curve:
        dd_csv_path = str(
            output_dir / f"{safe_name}_drawdown_curve_{timestamp_tag}.csv"
        )
        try:
            _export_drawdown_csv(result.drawdown_curve, dd_csv_path)
            print(f"  Drawdown CSV saved:   {dd_csv_path}")
        except Exception as exc:
            print(f"  Warning: Could not export drawdown CSV: {exc}")


def _export_equity_curve_csv(
    equity_curve: list[tuple[int, float]], filepath: str
) -> None:
    """Write the equity curve as a simple two-column CSV."""
    import csv

    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "equity"])
        for ts_ms, equity in equity_curve:
            writer.writerow([ts_ms, f"{equity:.4f}"])


def _export_drawdown_csv(
    drawdown_curve: list[tuple[int, float]], filepath: str
) -> None:
    """Write the drawdown curve as a simple two-column CSV."""
    import csv

    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "drawdown_fraction"])
        for ts_ms, dd in drawdown_curve:
            writer.writerow([ts_ms, f"{dd:.6f}"])


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    args = parse_args()

    # Build configuration from CLI arguments
    config = build_config(args)

    # Print a readable summary of what we are about to do
    print_header(config, args)

    # Create the strategy
    strategy = SimpleMarketMaker(
        spread_bps=args.spread_bps,
        order_size=Decimal(str(args.order_size)),
        max_position=Decimal(str(args.max_position)),
        target_assets=args.asset_ids,
        requote_threshold_bps=args.requote_threshold_bps,
    )

    # Run the backtest
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
        print("Check your --postgres-dsn or DATABASE_URL environment variable.")
        sys.exit(1)
    except Exception as exc:
        print(f"Backtest failed: {exc}")
        sys.exit(1)

    # Print the summary
    print_result_summary(result)

    # Save reports and plots
    output_dir = Path(args.output_dir)

    print(f"Saving reports to {output_dir.resolve()}/")
    save_reports(
        result,
        output_dir,
        skip_plots=args.no_plots,
        skip_csv=args.no_csv,
    )
    print()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
