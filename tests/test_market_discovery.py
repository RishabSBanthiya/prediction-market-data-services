import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pytest

from services.market_discovery import PolymarketDiscoveryService
from utils.logger import LoggerFactory


@pytest.fixture
def logger():
    factory = LoggerFactory("INFO")
    return factory.create("test")


@pytest.fixture
def discovery_service(logger):
    return PolymarketDiscoveryService(logger)


@pytest.mark.asyncio
async def test_fetch_sports_markets_by_tag(discovery_service):
    # Tag 100639 is for sports/game bets
    markets = await discovery_service.discover_markets({"tag_ids": [100639]})
    await discovery_service.close()

    assert len(markets) > 0
    for market in markets[:5]:
        assert market.token_id != ""
        assert market.condition_id != ""


@pytest.mark.asyncio
async def test_fetch_with_slug_pattern(discovery_service):
    markets = await discovery_service.discover_markets({
        "tag_ids": [100639],
        "slug_patterns": ["%nba%"],
    })
    await discovery_service.close()

    # May or may not have NBA markets depending on season
    for market in markets:
        slug = (market.market_slug or market.event_slug or "").lower()
        assert "nba" in slug


@pytest.mark.asyncio
async def test_empty_filters_returns_empty(discovery_service):
    markets = await discovery_service.discover_markets({})
    await discovery_service.close()
    assert markets == []
