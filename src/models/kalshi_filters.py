"""Kalshi-specific filter models for market discovery."""
from typing import Optional
from pydantic import BaseModel, Field


class KalshiListenerFilters(BaseModel):
    """Filters for Kalshi market discovery.

    Kalshi markets are organized as: Series -> Events -> Markets

    Example series tickers:
    - KXELECTION: Elections
    - KXECON: Economics
    - KXFINANCE: Finance
    - KXWEATHER: Weather
    """

    # Filter by series ticker (e.g., ["KXELECTION"])
    series_tickers: list[str] = Field(default_factory=list)

    # Filter by event ticker (e.g., ["KXPRESWIN"])
    event_tickers: list[str] = Field(default_factory=list)

    # Filter by specific market tickers
    market_tickers: list[str] = Field(default_factory=list)

    # Status filter: "open", "closed", "settled", "unopened"
    status: Optional[str] = "open"

    # Minimum volume threshold (in contracts)
    min_volume: Optional[float] = None

    # Minimum open interest threshold
    min_open_interest: Optional[float] = None

    # Text search in market title
    title_contains: Optional[str] = None
