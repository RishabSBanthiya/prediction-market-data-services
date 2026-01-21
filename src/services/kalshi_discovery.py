"""
Kalshi Market Discovery Service using REST API.

Kalshi markets are organized as:
- Series: Groups of related events (e.g., KXELECTION for elections)
- Events: Specific questions or topics
- Markets: Individual tradeable contracts within an event

Unlike Polymarket where each outcome has a separate token_id,
Kalshi uses a single ticker per market with yes/no sides.
"""
import ssl
import certifi
from typing import Optional

import aiohttp

from core.interfaces import IMarketDiscovery
from models import Market
from models.kalshi_filters import KalshiListenerFilters


class KalshiDiscoveryService(IMarketDiscovery):
    """
    Discovers Kalshi markets via REST API.

    Key differences from Polymarket:
    - Single ticker per market (yes/no are sides, not separate tokens)
    - Uses cursor-based pagination
    - Prices in cents (0-100)
    - Public market data requires no authentication
    """

    BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

    def __init__(self, logger, authenticator=None):
        """
        Initialize the discovery service.

        Args:
            logger: Logger instance
            authenticator: Optional KalshiAuthenticator (not needed for public endpoints)
        """
        self._logger = logger
        self._authenticator = authenticator
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def discover_markets(self, filters: dict) -> list[Market]:
        """
        Discover markets matching filters.

        Returns one Market per ticker (not per outcome like Polymarket).
        """
        markets: list[Market] = []
        parsed = KalshiListenerFilters(**filters)

        # Fetch by series tickers
        for series_ticker in parsed.series_tickers:
            found = await self._fetch_by_series(series_ticker, parsed.status)
            markets.extend(found)

        # Fetch by event tickers
        for event_ticker in parsed.event_tickers:
            found = await self._fetch_by_event(event_ticker, parsed.status)
            markets.extend(found)

        # Fetch specific market tickers
        for market_ticker in parsed.market_tickers:
            market = await self.get_market_details(market_ticker)
            markets.extend(market)

        # If no filters specified, fetch all open markets
        if not parsed.series_tickers and not parsed.event_tickers and not parsed.market_tickers:
            found = await self._fetch_markets(status=parsed.status)
            markets.extend(found)

        # Apply additional filters
        markets = self._apply_filters(markets, parsed)

        # Deduplicate by ticker (token_id)
        seen = set()
        unique = []
        for m in markets:
            if m.token_id not in seen:
                seen.add(m.token_id)
                unique.append(m)

        self._logger.info("kalshi_discovery_complete", count=len(unique))
        return unique

    async def get_market_details(self, condition_id: str) -> list[Market]:
        """Get single market by ticker."""
        session = await self._get_session()
        url = f"{self.BASE_URL}/markets/{condition_id}"

        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    self._logger.warning(
                        "kalshi_get_market_failed",
                        ticker=condition_id,
                        status=resp.status,
                    )
                    return []
                data = await resp.json()
                if data.get("market"):
                    return [self._parse_market(data["market"])]
        except Exception as e:
            self._logger.error("kalshi_get_market_error", ticker=condition_id, error=str(e))
        return []

    async def _fetch_by_series(self, series_ticker: str, status: Optional[str]) -> list[Market]:
        """Fetch all markets in a series."""
        return await self._fetch_markets(series_ticker=series_ticker, status=status)

    async def _fetch_by_event(self, event_ticker: str, status: Optional[str]) -> list[Market]:
        """Fetch all markets in an event."""
        return await self._fetch_markets(event_ticker=event_ticker, status=status)

    async def _fetch_markets(
        self,
        series_ticker: Optional[str] = None,
        event_ticker: Optional[str] = None,
        status: Optional[str] = "open",
        limit: int = 200,
    ) -> list[Market]:
        """Generic paginated market fetch."""
        session = await self._get_session()
        url = f"{self.BASE_URL}/markets"
        markets = []
        cursor = None

        while True:
            params = {"limit": limit}
            if status:
                params["status"] = status
            if series_ticker:
                params["series_ticker"] = series_ticker
            if event_ticker:
                params["event_ticker"] = event_ticker
            if cursor:
                params["cursor"] = cursor

            try:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        self._logger.error(
                            "kalshi_api_error",
                            status=resp.status,
                            series=series_ticker,
                            event=event_ticker,
                        )
                        break
                    data = await resp.json()

                    for market_data in data.get("markets", []):
                        markets.append(self._parse_market(market_data))

                    cursor = data.get("cursor")
                    if not cursor:
                        break

            except Exception as e:
                self._logger.error("kalshi_fetch_failed", error=str(e))
                break

        return markets

    def _parse_market(self, data: dict) -> Market:
        """
        Parse Kalshi market data into Market model.

        Note: Kalshi uses a single ticker for the market.
        The token_id field stores the ticker (unique market ID).
        """
        ticker = data.get("ticker", "")

        return Market(
            # Use ticker as both condition_id and token_id
            # (Kalshi has single ID per market vs Polymarket's separate outcome tokens)
            condition_id=ticker,
            token_id=ticker,
            market_slug=ticker,
            event_slug=data.get("event_ticker"),
            question=data.get("title"),
            outcome=None,  # Kalshi has yes/no sides, not separate outcomes
            outcome_index=None,
            event_id=data.get("event_ticker"),
            event_title=data.get("subtitle"),
            category=data.get("category"),
            subcategory=data.get("sub_title"),
            series_id=data.get("series_ticker"),
            tags=None,
            description=data.get("rules_primary"),
            # Volume (Kalshi provides in contracts)
            volume=float(data.get("volume", 0) or 0),
            # Use open_interest as liquidity proxy
            liquidity=float(data.get("open_interest", 0) or 0),
            is_active=data.get("status") == "open",
            is_closed=data.get("status") in ("closed", "settled"),
        )

    def _apply_filters(
        self,
        markets: list[Market],
        filters: KalshiListenerFilters,
    ) -> list[Market]:
        """Apply threshold and text filters."""
        result = markets

        if filters.min_volume is not None:
            result = [m for m in result if (m.volume or 0) >= filters.min_volume]

        if filters.min_open_interest is not None:
            result = [m for m in result if (m.liquidity or 0) >= filters.min_open_interest]

        if filters.title_contains:
            pattern = filters.title_contains.lower()
            result = [m for m in result if pattern in (m.question or "").lower()]

        return result
