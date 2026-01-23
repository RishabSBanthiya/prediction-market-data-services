#!/usr/bin/env python3
"""
Polymarket vs Kalshi 15-Minute Market Comparison

Compares BTC 15-minute prediction markets across platforms with normalized pricing.

Key Difference:
- Polymarket: 2 tokens per event (Up + Down), each with separate token_id
- Kalshi: 1 ticker per event, YES/NO are orderbook sides (outcome = None)

Normalization:
- Polymarket UP ↔ Kalshi mid_price (YES probability)
- Polymarket DOWN ↔ 1 - mid_price (implied NO probability)

Usage:
    python scripts/compare_polymarket_kalshi.py
"""
import os
import re
import sys
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()


def get_supabase_client() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env")
        sys.exit(1)
    return create_client(url, key)


# =============================================================================
# TIME PARSING
# =============================================================================

def parse_polymarket_time(question: str) -> dict | None:
    """Parse time from 'Bitcoin Up or Down - January 22, 7:15PM-7:30PM ET'"""
    pattern = r'(\w+)\s+(\d+),?\s+(\d+):(\d+)(AM|PM)-(\d+):(\d+)(AM|PM)'
    match = re.search(pattern, question, re.IGNORECASE)
    if not match:
        return None

    month_str, day, start_h, start_m, start_ampm, _, _, _ = match.groups()

    months = {'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
              'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12}
    month = months.get(month_str.lower(), 1)

    start_hour = int(start_h)
    if start_ampm.upper() == 'PM' and start_hour != 12:
        start_hour += 12
    elif start_ampm.upper() == 'AM' and start_hour == 12:
        start_hour = 0

    return {'month': month, 'day': int(day), 'hour': start_hour, 'minute': int(start_m)}


def parse_kalshi_ticker(ticker: str) -> dict | None:
    """
    Parse time from Kalshi ticker format: KXBTC15M-{YY}{MON}{DD}{HHMM}-{MM}

    IMPORTANT: The time in the ticker is the END time of the 15-minute window!
    Example: KXBTC15M-26JAN222030-30 = window ENDS at 20:30, so START is 20:15

    We convert to START time for matching with Polymarket which uses start times.
    """
    pattern = r'KXBTC15M-(\d{2})([A-Z]{3})(\d{2})(\d{4})-'
    match = re.search(pattern, ticker, re.IGNORECASE)
    if not match:
        return None

    _year, month_str, day, time_str = match.groups()
    end_hour = int(time_str[:2])
    end_minute = int(time_str[2:])

    # Convert END time to START time (subtract 15 minutes)
    total_minutes = end_hour * 60 + end_minute - 15
    if total_minutes < 0:
        total_minutes += 24 * 60  # Handle day wrap
    start_hour = total_minutes // 60
    start_minute = total_minutes % 60

    months = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
              'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
    month = months.get(month_str.upper(), 1)

    return {'month': month, 'day': int(day), 'hour': start_hour, 'minute': start_minute}


def times_match(pm_time: dict, kalshi_time: dict, tolerance_minutes: int = 15) -> bool:
    """Check if two parsed times match within tolerance (default 15 min for exact window match)."""
    if pm_time is None or kalshi_time is None:
        return False

    if pm_time['month'] != kalshi_time['month'] or pm_time['day'] != kalshi_time['day']:
        return False

    pm_mins = pm_time['hour'] * 60 + pm_time['minute']
    kalshi_mins = kalshi_time['hour'] * 60 + kalshi_time['minute']

    return abs(pm_mins - kalshi_mins) <= tolerance_minutes


def time_diff_minutes(pm_time: dict, kalshi_time: dict) -> int:
    """Calculate time difference in minutes."""
    if pm_time is None or kalshi_time is None:
        return 9999
    pm_mins = pm_time['hour'] * 60 + pm_time['minute']
    kalshi_mins = kalshi_time['hour'] * 60 + kalshi_time['minute']
    return abs(pm_mins - kalshi_mins)


def get_time_window_ms(parsed_time: dict, buffer_minutes: int = 3, window_minutes: int = 15) -> tuple[int, int]:
    """
    Convert parsed time to start/end timestamps in milliseconds.

    Args:
        parsed_time: dict with month, day, hour, minute
        buffer_minutes: minutes to add before/after window (default 3)
        window_minutes: length of market window (default 15)

    Returns:
        (start_ms, end_ms) tuple
    """
    from datetime import datetime, timedelta, timezone

    # Use current year, assume ET (UTC-5)
    year = datetime.now().year
    # Create datetime in ET, then convert to UTC
    et_offset = timedelta(hours=-5)
    dt = datetime(
        year=year,
        month=parsed_time['month'],
        day=parsed_time['day'],
        hour=parsed_time['hour'],
        minute=parsed_time['minute'],
        tzinfo=timezone(et_offset)
    )

    # Convert to UTC
    dt_utc = dt.astimezone(timezone.utc)

    # Calculate window with buffer
    start_dt = dt_utc - timedelta(minutes=buffer_minutes)
    end_dt = dt_utc + timedelta(minutes=window_minutes + buffer_minutes)

    # Convert to milliseconds
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    return start_ms, end_ms


# =============================================================================
# DATA FETCHING
# =============================================================================

def fetch_markets(supabase: Client, platform: str, limit: int = 500) -> pd.DataFrame:
    response = supabase.table("markets").select("*").eq("platform", platform).limit(limit).execute()
    return pd.DataFrame(response.data)


def fetch_15m_markets(supabase: Client, platform: str) -> pd.DataFrame:
    df = fetch_markets(supabase, platform)
    if df.empty:
        return df
    if platform == 'polymarket':
        mask = df['question'].str.contains('Up or Down', case=False, na=False)
    else:
        mask = df['token_id'].str.contains('KXBTC15M|KXETH15M', case=False, na=False)
    return df[mask] if mask.any() else df


def fetch_snapshots(supabase: Client, token_id: str, platform: str = None, limit: int = 50000,
                    start_ms: int = None, end_ms: int = None) -> pd.DataFrame:
    if not token_id:
        return pd.DataFrame()
    query = supabase.table("orderbook_snapshots").select("*").eq("asset_id", token_id).order("timestamp").limit(limit)
    if platform:
        query = query.eq("platform", platform)
    if start_ms is not None:
        query = query.gte("timestamp", start_ms)
    if end_ms is not None:
        query = query.lte("timestamp", end_ms)
    df = pd.DataFrame(query.execute().data)
    if not df.empty:
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.sort_values('timestamp')
    return df


# =============================================================================
# MARKET MATCHING
# =============================================================================

def find_matching_markets(pm_markets: pd.DataFrame, kalshi_markets: pd.DataFrame) -> list[dict]:
    """Find markets that match by time window, sorted by closest match first."""
    pm_parsed = []
    if not pm_markets.empty and 'question' in pm_markets.columns:
        for q in pm_markets['question'].unique():
            parsed = parse_polymarket_time(q)
            if parsed:
                pm_parsed.append({'question': q, 'parsed': parsed})

    kalshi_parsed = []
    if not kalshi_markets.empty and 'token_id' in kalshi_markets.columns:
        for t in kalshi_markets['token_id'].unique():
            parsed = parse_kalshi_ticker(t)
            if parsed:
                q = kalshi_markets[kalshi_markets['token_id'] == t].iloc[0].get('question', '')
                kalshi_parsed.append({'ticker': t, 'question': q, 'parsed': parsed})

    matched = []
    for pm in pm_parsed:
        for k in kalshi_parsed:
            if times_match(pm['parsed'], k['parsed']):
                diff = time_diff_minutes(pm['parsed'], k['parsed'])
                matched.append({
                    'polymarket_question': pm['question'],
                    'kalshi_ticker': k['ticker'],
                    'kalshi_question': k['question'],
                    'pm_time': pm['parsed'],
                    'kalshi_time': k['parsed'],
                    'time_diff': diff
                })

    # Sort by time difference (exact matches first)
    matched.sort(key=lambda x: x['time_diff'])
    return matched


def get_polymarket_tokens(markets: pd.DataFrame, question: str) -> tuple[str, str]:
    """Get Up and Down token IDs for a Polymarket question."""
    up_token, down_token = None, None
    selected = markets[markets['question'] == question]
    for _, row in selected.iterrows():
        outcome = str(row.get('outcome', '')).lower()
        if 'up' in outcome:
            up_token = row['token_id']
        elif 'down' in outcome:
            down_token = row['token_id']
    return up_token, down_token


# =============================================================================
# ANALYSIS
# =============================================================================

def align_series(df1: pd.DataFrame, df2: pd.DataFrame, col: str = 'mid_price', freq: str = '1s'):
    """Align two time series by resampling."""
    if df1.empty or df2.empty or col not in df1.columns or col not in df2.columns:
        return pd.Series(dtype=float), pd.Series(dtype=float)

    s1 = df1.set_index('datetime')[col].astype(float).resample(freq).last().dropna()
    s2 = df2.set_index('datetime')[col].astype(float).resample(freq).last().dropna()

    if s1.empty or s2.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float)

    common_start = max(s1.index.min(), s2.index.min())
    common_end = min(s1.index.max(), s2.index.max())
    common_idx = s1[common_start:common_end].index.intersection(s2[common_start:common_end].index)

    return s1[common_idx], s2[common_idx]


def compute_correlation(s1: pd.Series, s2: pd.Series) -> dict:
    """Compute correlation statistics."""
    if len(s1) < 3:
        return {'correlation': None, 'p_value': None, 'points': len(s1)}

    corr, p_val = stats.pearsonr(s1, s2)
    diff = s1 - s2

    return {
        'correlation': corr,
        'p_value': p_val,
        'points': len(s1),
        'diff_mean': diff.mean(),
        'diff_std': diff.std(),
        'diff_min': diff.min(),
        'diff_max': diff.max()
    }


# =============================================================================
# LIQUIDITY HELPERS
# =============================================================================

def compute_depth(df: pd.DataFrame) -> pd.DataFrame:
    """Compute total bid/ask depth from orderbook snapshots."""
    if df.empty or 'bids' not in df.columns or 'asks' not in df.columns:
        return df

    def sum_depth(levels):
        if not levels or not isinstance(levels, list):
            return 0.0
        return sum(float(level.get('size', 0)) for level in levels if isinstance(level, dict))

    df = df.copy()
    df['bid_depth'] = df['bids'].apply(sum_depth)
    df['ask_depth'] = df['asks'].apply(sum_depth)
    df['total_depth'] = df['bid_depth'] + df['ask_depth']

    # Ensure spread is numeric
    if 'spread' in df.columns:
        df['spread'] = pd.to_numeric(df['spread'], errors='coerce')

    return df


# =============================================================================
# PLOTTING
# =============================================================================

def plot_combined_prices(pm_up: pd.DataFrame, pm_down: pd.DataFrame, kalshi: pd.DataFrame, title: str):
    """Plot all 4 price series with liquidity metrics."""
    # Compute depth for all dataframes
    pm_up = compute_depth(pm_up) if not pm_up.empty else pm_up
    pm_down = compute_depth(pm_down) if not pm_down.empty else pm_down
    kalshi = compute_depth(kalshi) if not kalshi.empty else kalshi

    # Create subplots: prices on top, spread in middle, depth on bottom
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.5, 0.25, 0.25],
        subplot_titles=('Price (Probability)', 'Spread', 'Total Depth')
    )

    # Row 1: Prices
    if not pm_up.empty:
        fig.add_trace(go.Scatter(x=pm_up['datetime'], y=pm_up['mid_price'],
                                  name='PM UP', line=dict(color='#2563eb', width=2)), row=1, col=1)
    if not pm_down.empty:
        fig.add_trace(go.Scatter(x=pm_down['datetime'], y=pm_down['mid_price'],
                                  name='PM DOWN', line=dict(color='#60a5fa', width=2, dash='dash')), row=1, col=1)
    if not kalshi.empty:
        fig.add_trace(go.Scatter(x=kalshi['datetime'], y=kalshi['mid_price'],
                                  name='Kalshi YES', line=dict(color='#ea580c', width=2)), row=1, col=1)
        fig.add_trace(go.Scatter(x=kalshi['datetime'], y=kalshi['down_price'],
                                  name='Kalshi NO', line=dict(color='#fdba74', width=2, dash='dash')), row=1, col=1)

    # Row 2: Spread
    if not pm_up.empty and 'spread' in pm_up.columns:
        fig.add_trace(go.Scatter(x=pm_up['datetime'], y=pm_up['spread'],
                                  name='PM UP Spread', line=dict(color='#2563eb', width=1),
                                  showlegend=False), row=2, col=1)
    if not pm_down.empty and 'spread' in pm_down.columns:
        fig.add_trace(go.Scatter(x=pm_down['datetime'], y=pm_down['spread'],
                                  name='PM DOWN Spread', line=dict(color='#60a5fa', width=1, dash='dash'),
                                  showlegend=False), row=2, col=1)
    if not kalshi.empty and 'spread' in kalshi.columns:
        fig.add_trace(go.Scatter(x=kalshi['datetime'], y=kalshi['spread'],
                                  name='Kalshi Spread', line=dict(color='#ea580c', width=1),
                                  showlegend=False), row=2, col=1)

    # Row 3: Total Depth
    if not pm_up.empty and 'total_depth' in pm_up.columns:
        fig.add_trace(go.Scatter(x=pm_up['datetime'], y=pm_up['total_depth'],
                                  name='PM UP Depth', line=dict(color='#2563eb', width=1),
                                  showlegend=False), row=3, col=1)
    if not pm_down.empty and 'total_depth' in pm_down.columns:
        fig.add_trace(go.Scatter(x=pm_down['datetime'], y=pm_down['total_depth'],
                                  name='PM DOWN Depth', line=dict(color='#60a5fa', width=1, dash='dash'),
                                  showlegend=False), row=3, col=1)
    if not kalshi.empty and 'total_depth' in kalshi.columns:
        fig.add_trace(go.Scatter(x=kalshi['datetime'], y=kalshi['total_depth'],
                                  name='Kalshi Depth', line=dict(color='#ea580c', width=1),
                                  showlegend=False), row=3, col=1)

    fig.update_layout(
        title=title,
        height=700,
        hovermode='x unified',
        template='plotly_white',
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
    )
    fig.update_yaxes(title_text="Probability", range=[0, 1], row=1, col=1)
    fig.update_yaxes(title_text="Spread", row=2, col=1)
    fig.update_yaxes(title_text="Depth", row=3, col=1)
    fig.update_xaxes(title_text="Time", row=3, col=1)
    fig.show()


def plot_split_comparison(pm_up: pd.DataFrame, pm_down: pd.DataFrame, kalshi: pd.DataFrame):
    """Plot UP vs UP and DOWN vs DOWN with liquidity comparison."""
    # Compute depth
    pm_up = compute_depth(pm_up) if not pm_up.empty else pm_up
    pm_down = compute_depth(pm_down) if not pm_down.empty else pm_down
    kalshi = compute_depth(kalshi) if not kalshi.empty else kalshi

    fig = make_subplots(
        rows=2, cols=2,
        shared_xaxes=True,
        vertical_spacing=0.12,
        horizontal_spacing=0.08,
        subplot_titles=('UP: Price Comparison', 'UP: Liquidity (Depth)',
                        'DOWN: Price Comparison', 'DOWN: Liquidity (Depth)')
    )

    # Row 1: UP comparison
    if not pm_up.empty:
        fig.add_trace(go.Scatter(x=pm_up['datetime'], y=pm_up['mid_price'], name='PM Up',
                                  line=dict(color='blue')), row=1, col=1)
        if 'total_depth' in pm_up.columns:
            fig.add_trace(go.Scatter(x=pm_up['datetime'], y=pm_up['total_depth'], name='PM Up Depth',
                                      line=dict(color='blue'), showlegend=False), row=1, col=2)
    if not kalshi.empty:
        fig.add_trace(go.Scatter(x=kalshi['datetime'], y=kalshi['mid_price'], name='Kalshi YES',
                                  line=dict(color='orange')), row=1, col=1)
        if 'total_depth' in kalshi.columns:
            fig.add_trace(go.Scatter(x=kalshi['datetime'], y=kalshi['total_depth'], name='Kalshi Depth',
                                      line=dict(color='orange'), showlegend=False), row=1, col=2)

    # Row 2: DOWN comparison
    if not pm_down.empty:
        fig.add_trace(go.Scatter(x=pm_down['datetime'], y=pm_down['mid_price'], name='PM Down',
                                  line=dict(color='blue', dash='dash')), row=2, col=1)
        if 'total_depth' in pm_down.columns:
            fig.add_trace(go.Scatter(x=pm_down['datetime'], y=pm_down['total_depth'], name='PM Down Depth',
                                      line=dict(color='blue', dash='dash'), showlegend=False), row=2, col=2)
    if not kalshi.empty:
        fig.add_trace(go.Scatter(x=kalshi['datetime'], y=kalshi['down_price'], name='Kalshi NO',
                                  line=dict(color='orange', dash='dash')), row=2, col=1)
        if 'total_depth' in kalshi.columns:
            fig.add_trace(go.Scatter(x=kalshi['datetime'], y=kalshi['total_depth'], name='Kalshi Depth',
                                      line=dict(color='orange', dash='dash'), showlegend=False), row=2, col=2)

    fig.update_layout(title="Price & Liquidity Comparison", height=700, hovermode='x unified', template='plotly_white')
    fig.update_yaxes(title_text="Probability", row=1, col=1)
    fig.update_yaxes(title_text="Depth", row=1, col=2)
    fig.update_yaxes(title_text="Probability", row=2, col=1)
    fig.update_yaxes(title_text="Depth", row=2, col=2)
    fig.show()


def plot_scatter_correlation(pm_up_aligned, kalshi_up_aligned, pm_down_aligned, kalshi_down_aligned):
    """Plot scatter plots with regression lines."""
    fig = make_subplots(rows=1, cols=2, subplot_titles=('UP: PM vs Kalshi', 'DOWN: PM vs Kalshi'))

    if len(pm_up_aligned) > 0:
        fig.add_trace(go.Scatter(x=pm_up_aligned, y=kalshi_up_aligned, mode='markers',
                                  marker=dict(size=4, opacity=0.5, color='blue'), name='Up'), row=1, col=1)
        if len(pm_up_aligned) > 2:
            slope, intercept, r, _, _ = stats.linregress(pm_up_aligned, kalshi_up_aligned)
            x_line = np.array([pm_up_aligned.min(), pm_up_aligned.max()])
            fig.add_trace(go.Scatter(x=x_line, y=slope*x_line+intercept, mode='lines',
                                      name=f'R²={r**2:.3f}', line=dict(color='red', dash='dash')), row=1, col=1)

    if len(pm_down_aligned) > 0:
        fig.add_trace(go.Scatter(x=pm_down_aligned, y=kalshi_down_aligned, mode='markers',
                                  marker=dict(size=4, opacity=0.5, color='orange'), name='Down'), row=1, col=2)
        if len(pm_down_aligned) > 2:
            slope, intercept, r, _, _ = stats.linregress(pm_down_aligned, kalshi_down_aligned)
            x_line = np.array([pm_down_aligned.min(), pm_down_aligned.max()])
            fig.add_trace(go.Scatter(x=x_line, y=slope*x_line+intercept, mode='lines',
                                      name=f'R²={r**2:.3f}', line=dict(color='red', dash='dash')), row=1, col=2)

    # 45-degree reference lines
    for col in [1, 2]:
        fig.add_trace(go.Scatter(x=[0,1], y=[0,1], mode='lines',
                                  line=dict(color='gray', dash='dot'), showlegend=False), row=1, col=col)

    fig.update_layout(title="Price Correlation Scatter", height=450, template='plotly_white')
    fig.update_xaxes(title_text="Polymarket", row=1, col=1)
    fig.update_xaxes(title_text="Polymarket", row=1, col=2)
    fig.update_yaxes(title_text="Kalshi", row=1, col=1)
    fig.update_yaxes(title_text="Kalshi", row=1, col=2)
    fig.show()


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("="*70)
    print("POLYMARKET vs KALSHI 15-MINUTE MARKET COMPARISON")
    print("="*70)

    # Connect
    supabase = get_supabase_client()
    print("\n✓ Connected to Supabase")

    # Fetch markets
    print("\n--- Discovering Markets ---")
    pm_markets = fetch_15m_markets(supabase, 'polymarket')
    kalshi_markets = fetch_15m_markets(supabase, 'kalshi')
    print(f"Polymarket 15m markets: {len(pm_markets)}")
    print(f"Kalshi 15m markets: {len(kalshi_markets)}")

    if pm_markets.empty or kalshi_markets.empty:
        print("\nERROR: No markets found. Make sure data is being collected.")
        return

    # Find matching pairs
    print("\n--- Finding Time-Matched Pairs ---")
    matched = find_matching_markets(pm_markets, kalshi_markets)
    print(f"Found {len(matched)} matching pairs")

    if not matched:
        print("\nNo matching markets found. Showing available markets:")
        print("\nPolymarket questions:")
        for q in pm_markets['question'].unique()[:5]:
            t = parse_polymarket_time(q)
            if t:
                print(f"  {t['month']}/{t['day']} {t['hour']:02d}:{t['minute']:02d} - {q[:50]}...")
        print("\nKalshi tickers:")
        for ticker in kalshi_markets['token_id'].unique()[:5]:
            t = parse_kalshi_ticker(ticker)
            if t:
                print(f"  {t['month']}/{t['day']} {t['hour']:02d}:{t['minute']:02d} - {ticker}")
        return

    # Enrich matches with token availability
    for m in matched:
        up, down = get_polymarket_tokens(pm_markets, m['polymarket_question'])
        m['has_up'] = up is not None
        m['has_down'] = down is not None

    # Show matches (prioritize exact matches)
    for i, m in enumerate(matched[:10]):
        pm_t = m['pm_time']
        diff = m.get('time_diff', 0)
        match_quality = "EXACT" if diff == 0 else f"+/-{diff}min"
        tokens = []
        if m['has_up']:
            tokens.append("Up")
        if m['has_down']:
            tokens.append("Down")
        token_str = f"[{'+'.join(tokens)}]" if tokens else "[None]"
        print(f"\n{i+1}. [{match_quality}] {token_str} {pm_t['month']}/{pm_t['day']} {pm_t['hour']:02d}:{pm_t['minute']:02d}")
        print(f"   PM: {m['polymarket_question'][:55]}...")
        print(f"   Kalshi: {m['kalshi_ticker']}")

    # Find one Up-token market and one Down-token market (different time windows)
    up_match = None
    down_match = None
    for m in matched:
        if m['has_up'] and m.get('time_diff', 0) == 0 and not up_match:
            up_match = m
        if m['has_down'] and m.get('time_diff', 0) == 0 and not down_match:
            down_match = m
        if up_match and down_match:
            break

    if not up_match and not down_match:
        print("\nNo markets with tokens found.")
        return

    print("\n" + "="*70)
    print("SELECTED MARKETS (Note: Different time windows due to data availability)")
    print("="*70)

    # Load UP data
    pm_up = pd.DataFrame()
    kalshi_up = pd.DataFrame()
    if up_match:
        up_question = up_match['polymarket_question']
        up_kalshi_ticker = up_match['kalshi_ticker']
        up_token, _ = get_polymarket_tokens(pm_markets, up_question)
        print(f"\nUP COMPARISON:")
        print(f"  PM: {up_question[:55]}...")
        print(f"  Kalshi: {up_kalshi_ticker}")
        print(f"  PM Up Token: {up_token[:30] if up_token else None}...")

        # Get time window with ±3 minute buffer
        start_ms, end_ms = get_time_window_ms(up_match['pm_time'], buffer_minutes=3)
        print(f"  Time window: {pd.to_datetime(start_ms, unit='ms')} to {pd.to_datetime(end_ms, unit='ms')}")

        pm_up = fetch_snapshots(supabase, up_token, 'polymarket', start_ms=start_ms, end_ms=end_ms)
        kalshi_up = fetch_snapshots(supabase, up_kalshi_ticker, 'kalshi', start_ms=start_ms, end_ms=end_ms)
        print(f"  PM Up snapshots: {len(pm_up)}, Kalshi snapshots: {len(kalshi_up)}")

    # Load DOWN data
    pm_down = pd.DataFrame()
    kalshi_down = pd.DataFrame()
    if down_match:
        down_question = down_match['polymarket_question']
        down_kalshi_ticker = down_match['kalshi_ticker']
        _, down_token = get_polymarket_tokens(pm_markets, down_question)
        print(f"\nDOWN COMPARISON:")
        print(f"  PM: {down_question[:55]}...")
        print(f"  Kalshi: {down_kalshi_ticker}")
        print(f"  PM Down Token: {down_token[:30] if down_token else None}...")

        # Get time window with ±3 minute buffer
        start_ms, end_ms = get_time_window_ms(down_match['pm_time'], buffer_minutes=3)
        print(f"  Time window: {pd.to_datetime(start_ms, unit='ms')} to {pd.to_datetime(end_ms, unit='ms')}")

        pm_down = fetch_snapshots(supabase, down_token, 'polymarket', start_ms=start_ms, end_ms=end_ms)
        kalshi_down = fetch_snapshots(supabase, down_kalshi_ticker, 'kalshi', start_ms=start_ms, end_ms=end_ms)
        # Compute Kalshi implied NO price
        if not kalshi_down.empty and 'mid_price' in kalshi_down.columns:
            kalshi_down['down_price'] = 1.0 - kalshi_down['mid_price'].astype(float)
        print(f"  PM Down snapshots: {len(pm_down)}, Kalshi snapshots: {len(kalshi_down)}")

    if pm_up.empty and pm_down.empty:
        print("\nNo orderbook data found for selected markets.")
        return

    # For combined charts, use kalshi_up as primary kalshi data
    kalshi = kalshi_up if not kalshi_up.empty else kalshi_down
    if not kalshi.empty and 'mid_price' in kalshi.columns:
        kalshi['down_price'] = 1.0 - kalshi['mid_price'].astype(float)

    # Show time ranges
    print("\n--- Time Ranges ---")
    if not pm_up.empty:
        print(f"  PM Up:      {pm_up['datetime'].min()} to {pm_up['datetime'].max()}")
    if not kalshi_up.empty:
        print(f"  Kalshi Up:  {kalshi_up['datetime'].min()} to {kalshi_up['datetime'].max()}")
    if not pm_down.empty:
        print(f"  PM Down:    {pm_down['datetime'].min()} to {pm_down['datetime'].max()}")
    if not kalshi_down.empty:
        print(f"  Kalshi Down:{kalshi_down['datetime'].min()} to {kalshi_down['datetime'].max()}")

    # Correlation analysis
    print("\n" + "="*70)
    print("CORRELATION ANALYSIS")
    print("="*70)

    # UP comparison: PM Up vs Kalshi YES (using kalshi_up data)
    pm_up_aligned, kalshi_up_aligned_series = align_series(pm_up, kalshi_up)

    if len(pm_up_aligned) > 2:
        up_stats = compute_correlation(pm_up_aligned, kalshi_up_aligned_series)
        print(f"\nUP (PM Up vs Kalshi YES) - Time window: {up_match['polymarket_question'].split(' - ')[1][:30] if up_match else 'N/A'}")
        print(f"  Aligned points: {up_stats['points']}")
        print(f"  Correlation: {up_stats['correlation']:.4f} (p={up_stats['p_value']:.2e})")
        print(f"  Price diff: mean={up_stats['diff_mean']:.4f}, std={up_stats['diff_std']:.4f}")
    else:
        print(f"\nUP: Not enough aligned data ({len(pm_up_aligned)} points)")

    # DOWN comparison: PM Down vs Kalshi NO (using kalshi_down data)
    kalshi_down_for_align = kalshi_down.copy()
    if not kalshi_down_for_align.empty and 'down_price' in kalshi_down_for_align.columns:
        kalshi_down_for_align['mid_price'] = kalshi_down_for_align['down_price']
    pm_down_aligned, kalshi_down_aligned_series = align_series(pm_down, kalshi_down_for_align)

    if len(pm_down_aligned) > 2:
        down_stats = compute_correlation(pm_down_aligned, kalshi_down_aligned_series)
        print(f"\nDOWN (PM Down vs Kalshi NO) - Time window: {down_match['polymarket_question'].split(' - ')[1][:30] if down_match else 'N/A'}")
        print(f"  Aligned points: {down_stats['points']}")
        print(f"  Correlation: {down_stats['correlation']:.4f} (p={down_stats['p_value']:.2e})")
        print(f"  Price diff: mean={down_stats['diff_mean']:.4f}, std={down_stats['diff_std']:.4f}")
    else:
        print(f"\nDOWN: Not enough aligned data ({len(pm_down_aligned)} points)")

    # Compute depth for summary stats
    pm_up = compute_depth(pm_up) if not pm_up.empty else pm_up
    pm_down = compute_depth(pm_down) if not pm_down.empty else pm_down
    kalshi = compute_depth(kalshi) if not kalshi.empty else kalshi

    # Summary stats
    print("\n" + "="*70)
    print("SUMMARY STATISTICS")
    print("="*70)

    for name, df in [("PM Up", pm_up), ("PM Down", pm_down), ("Kalshi", kalshi)]:
        if not df.empty and 'mid_price' in df.columns:
            print(f"\n{name}:")
            print(f"  Snapshots: {len(df)}")
            print(f"  Price: min={df['mid_price'].min():.4f}, max={df['mid_price'].max():.4f}, avg={df['mid_price'].mean():.4f}")
            if 'spread' in df.columns:
                spread = pd.to_numeric(df['spread'], errors='coerce')
                print(f"  Spread: min={spread.min():.4f}, max={spread.max():.4f}, avg={spread.mean():.4f}")
            if 'total_depth' in df.columns:
                print(f"  Depth:  min={df['total_depth'].min():.2f}, max={df['total_depth'].max():.2f}, avg={df['total_depth'].mean():.2f}")
            if 'bid_depth' in df.columns and 'ask_depth' in df.columns:
                print(f"  Bid depth avg: {df['bid_depth'].mean():.2f}, Ask depth avg: {df['ask_depth'].mean():.2f}")

    # Liquidity comparison
    print("\n" + "="*70)
    print("LIQUIDITY COMPARISON")
    print("="*70)

    pm_data = pm_up if not pm_up.empty else pm_down
    if not pm_data.empty and not kalshi.empty:
        if 'total_depth' in pm_data.columns and 'total_depth' in kalshi.columns:
            pm_avg_depth = pm_data['total_depth'].mean()
            kalshi_avg_depth = kalshi['total_depth'].mean()
            print(f"\nAverage Total Depth:")
            print(f"  Polymarket: {pm_avg_depth:.2f}")
            print(f"  Kalshi:     {kalshi_avg_depth:.2f}")
            if kalshi_avg_depth > 0:
                print(f"  Ratio (PM/Kalshi): {pm_avg_depth/kalshi_avg_depth:.2f}x")

        if 'spread' in pm_data.columns and 'spread' in kalshi.columns:
            pm_spread = pd.to_numeric(pm_data['spread'], errors='coerce')
            kalshi_spread = pd.to_numeric(kalshi['spread'], errors='coerce')
            print(f"\nAverage Spread:")
            print(f"  Polymarket: {pm_spread.mean():.4f}")
            print(f"  Kalshi:     {kalshi_spread.mean():.4f}")
            print(f"  (Lower spread = tighter market = better liquidity)")

    # Sanity check
    print("\n" + "="*70)
    print("SANITY CHECK")
    print("="*70)

    # Note: Up and Down are from different time windows, so can't directly sum
    print("Note: PM Up and Down are from different time windows (data collection issue)")
    print("      Each should ideally be close to 1 - (other outcome price)")

    if not pm_up.empty:
        avg_up = pm_up['mid_price'].astype(float).mean()
        print(f"  PM Up avg: {avg_up:.4f} -> implied Down: {1-avg_up:.4f}")
    if not pm_down.empty:
        avg_down = pm_down['mid_price'].astype(float).mean()
        print(f"  PM Down avg: {avg_down:.4f} -> implied Up: {1-avg_down:.4f}")

    print(f"\nKalshi YES + NO: 1.0000 (by construction - single orderbook)")

    # Plots
    print("\n" + "="*70)
    print("GENERATING CHARTS...")
    print("="*70)

    title = "Polymarket vs Kalshi 15-Minute BTC Markets"

    # For combined chart, use both kalshi datasets
    # Create a unified kalshi df for plotting (use kalshi_up for UP comparison visualization)
    plot_combined_prices(pm_up, pm_down, kalshi_up if not kalshi_up.empty else kalshi_down, title)
    plot_split_comparison(pm_up, pm_down, kalshi)

    if len(pm_up_aligned) > 0 or len(pm_down_aligned) > 0:
        plot_scatter_correlation(pm_up_aligned, kalshi_up_aligned_series, pm_down_aligned, kalshi_down_aligned_series)

    print("\n✓ Done!")


if __name__ == "__main__":
    main()
