import fnmatch
import json
import ssl
import certifi
from typing import Optional

import aiohttp

from core.interfaces import IMarketDiscovery
from models import Market, ListenerFilters


class PolymarketDiscoveryService(IMarketDiscovery):
    GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

    def __init__(self, logger):
        self._logger = logger
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
        markets: list[Market] = []
        parsed = ListenerFilters(**filters)

        for series_id in parsed.series_ids:
            found = await self._fetch_by_series(series_id)
            markets.extend(found)

        for tag_id in parsed.tag_ids:
            found = await self._fetch_by_tag(tag_id)
            markets.extend(found)

        for condition_id in parsed.condition_ids:
            found = await self.get_market_details(condition_id)
            markets.extend(found)

        if parsed.slug_patterns:
            markets = self._filter_by_slug(markets, parsed.slug_patterns)

        markets = self._apply_thresholds(markets, parsed)

        seen = set()
        unique = []
        for m in markets:
            if m.token_id not in seen:
                seen.add(m.token_id)
                unique.append(m)
        return unique

    async def get_market_details(self, condition_id: str) -> list[Market]:
        """Get market details, returning a Market for each token ID."""
        session = await self._get_session()
        url = f"{self.GAMMA_BASE_URL}/markets"
        params = {"condition_id": condition_id}
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                if data:
                    return self._parse_market(data[0])
        except Exception as e:
            self._logger.error("get_market_details_failed", error=str(e))
        return []

    async def _fetch_by_series(self, series_id: str) -> list[Market]:
        session = await self._get_session()
        url = f"{self.GAMMA_BASE_URL}/events"
        params = {"series_id": series_id, "active": "true", "closed": "false"}
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    self._logger.error("gamma_api_error", status=resp.status)
                    return []
                events = await resp.json()
                return self._parse_events(events)
        except Exception as e:
            self._logger.error("fetch_by_series_failed", error=str(e))
            return []

    async def _fetch_by_tag(self, tag_id: int) -> list[Market]:
        session = await self._get_session()
        url = f"{self.GAMMA_BASE_URL}/events"
        params = {"tag_id": tag_id, "active": "true", "closed": "false"}
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return []
                events = await resp.json()
                return self._parse_events(events)
        except Exception as e:
            self._logger.error("fetch_by_tag_failed", error=str(e))
            return []

    def _parse_events(self, events: list[dict]) -> list[Market]:
        markets = []
        for event in events:
            for market_data in event.get("markets", []):
                markets.extend(self._parse_market(market_data, event))
        return markets

    def _parse_market(self, data: dict, event: dict = None) -> list[Market]:
        """Parse market data and return a Market for each token ID (outcome)."""
        clob_ids_raw = data.get("clobTokenIds") or []
        if isinstance(clob_ids_raw, str):
            clob_ids = json.loads(clob_ids_raw) if clob_ids_raw else []
        else:
            clob_ids = clob_ids_raw

        # Parse outcomes to associate with tokens
        outcomes_raw = data.get("outcomes") or "[]"
        if isinstance(outcomes_raw, str):
            outcomes = json.loads(outcomes_raw) if outcomes_raw else []
        else:
            outcomes = outcomes_raw

        # Extract tags from event level (array of {id, label, slug} objects)
        tags = None
        if event and event.get("tags"):
            tags = event.get("tags")

        # Extract series_id from event level (event.series[0].id)
        series_id = None
        if event and event.get("series"):
            series_list = event.get("series")
            if series_list and len(series_list) > 0:
                series_id = str(series_list[0].get("id"))

        # Derive category/subcategory from tags if available
        category = None
        subcategory = None
        if tags and len(tags) > 0:
            category = tags[0].get("label")  # First tag as category (e.g., "Sports")
            if len(tags) > 1:
                subcategory = tags[1].get("label")  # Second tag as subcategory (e.g., "NBA")

        markets = []
        for i, token_id in enumerate(clob_ids):
            outcome = outcomes[i] if i < len(outcomes) else None
            markets.append(Market(
                condition_id=data.get("conditionId", ""),
                token_id=token_id,
                market_slug=data.get("slug"),
                event_slug=event.get("slug") if event else None,
                question=data.get("question"),
                outcome=outcome,
                outcome_index=i,
                event_id=str(event.get("id")) if event else None,
                event_title=event.get("title") if event else None,
                category=category,
                subcategory=subcategory,
                series_id=series_id,
                tags=tags,
                description=data.get("description"),
                volume=float(data.get("volume") or 0),
                liquidity=float(data.get("liquidity") or 0),
                is_active=data.get("active", True),
                is_closed=data.get("closed", False),
            ))

        # Return at least one market even if no token IDs
        if not markets:
            markets.append(Market(
                condition_id=data.get("conditionId", ""),
                token_id="",
                market_slug=data.get("slug"),
                event_slug=event.get("slug") if event else None,
                question=data.get("question"),
                outcome=data.get("outcome"),
                outcome_index=data.get("outcomeIndex"),
                event_id=str(event.get("id")) if event else None,
                event_title=event.get("title") if event else None,
                category=category,
                subcategory=subcategory,
                series_id=series_id,
                tags=tags,
                description=data.get("description"),
                volume=float(data.get("volume") or 0),
                liquidity=float(data.get("liquidity") or 0),
                is_active=data.get("active", True),
                is_closed=data.get("closed", False),
            ))

        return markets

    def _filter_by_slug(self, markets: list[Market], patterns: list[str]) -> list[Market]:
        filtered = []
        for m in markets:
            slug = m.market_slug or m.event_slug or ""
            for pattern in patterns:
                fn_pattern = pattern.replace("%", "*").replace("_", "?")
                if fnmatch.fnmatch(slug.lower(), fn_pattern.lower()):
                    filtered.append(m)
                    break
        return filtered

    def _apply_thresholds(self, markets: list[Market], filters: ListenerFilters) -> list[Market]:
        result = markets
        if filters.min_liquidity is not None:
            result = [m for m in result if (m.liquidity or 0) >= filters.min_liquidity]
        if filters.min_volume is not None:
            result = [m for m in result if (m.volume or 0) >= filters.min_volume]
        return result
