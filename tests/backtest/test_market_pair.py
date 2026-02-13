"""
Tests for MarketPair and MarketPairRegistry.

Covers complement lookups, price complement, token identification,
registry registration, and build_from_markets().
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from backtest.models.market_pair import MarketPair, MarketPairRegistry


# ======================================================================
# MarketPair
# ======================================================================


class TestMarketPair:

    @pytest.fixture
    def pair(self) -> MarketPair:
        return MarketPair(
            condition_id="cond-1",
            question="Will team X win?",
            yes_token_id="token-yes",
            no_token_id="token-no",
            platform="polymarket",
        )

    def test_get_complement_token_from_yes(self, pair):
        assert pair.get_complement_token("token-yes") == "token-no"

    def test_get_complement_token_from_no(self, pair):
        assert pair.get_complement_token("token-no") == "token-yes"

    def test_get_complement_token_unknown_returns_none(self, pair):
        assert pair.get_complement_token("unknown") is None

    def test_get_complement_price(self, pair):
        assert pair.get_complement_price(Decimal("0.60")) == Decimal("0.40")

    def test_get_complement_price_boundary_zero(self, pair):
        assert pair.get_complement_price(Decimal("0")) == Decimal("1")

    def test_get_complement_price_boundary_one(self, pair):
        assert pair.get_complement_price(Decimal("1")) == Decimal("0")

    def test_is_yes_token_true(self, pair):
        assert pair.is_yes_token("token-yes") is True

    def test_is_yes_token_false(self, pair):
        assert pair.is_yes_token("token-no") is False

    def test_is_no_token_true(self, pair):
        assert pair.is_no_token("token-no") is True

    def test_is_no_token_false(self, pair):
        assert pair.is_no_token("token-yes") is False

    def test_contains_token_yes(self, pair):
        assert pair.contains_token("token-yes") is True

    def test_contains_token_no(self, pair):
        assert pair.contains_token("token-no") is True

    def test_contains_token_unknown(self, pair):
        assert pair.contains_token("other") is False


# ======================================================================
# MarketPairRegistry
# ======================================================================


class TestMarketPairRegistry:

    def test_register_and_get_pair_by_condition(self, market_pair_registry):
        pair = market_pair_registry.get_pair_by_condition("condition-1")
        assert pair is not None
        assert pair.yes_token_id == "token-yes-1"
        assert pair.no_token_id == "token-no-1"

    def test_get_pair_for_yes_token(self, market_pair_registry):
        pair = market_pair_registry.get_pair_for_token("token-yes-1")
        assert pair is not None
        assert pair.condition_id == "condition-1"

    def test_get_pair_for_no_token(self, market_pair_registry):
        pair = market_pair_registry.get_pair_for_token("token-no-1")
        assert pair is not None
        assert pair.condition_id == "condition-1"

    def test_get_pair_for_unknown_token_returns_none(self, market_pair_registry):
        assert market_pair_registry.get_pair_for_token("unknown") is None

    def test_get_pair_by_unknown_condition_returns_none(self, market_pair_registry):
        assert market_pair_registry.get_pair_by_condition("unknown") is None

    def test_get_all_pairs(self, market_pair_registry):
        pairs = market_pair_registry.get_all_pairs()
        assert len(pairs) == 1
        assert pairs[0].condition_id == "condition-1"

    def test_register_multiple_pairs(self):
        registry = MarketPairRegistry()
        pair1 = MarketPair(
            condition_id="cond-1", question="Q1",
            yes_token_id="yes-1", no_token_id="no-1", platform="polymarket",
        )
        pair2 = MarketPair(
            condition_id="cond-2", question="Q2",
            yes_token_id="yes-2", no_token_id="no-2", platform="polymarket",
        )
        registry.register(pair1)
        registry.register(pair2)

        assert len(registry.get_all_pairs()) == 2
        assert registry.get_pair_for_token("yes-1").condition_id == "cond-1"
        assert registry.get_pair_for_token("yes-2").condition_id == "cond-2"


# ======================================================================
# MarketPairRegistry.build_from_markets()
# ======================================================================


class TestMarketPairRegistryBuildFromMarkets:

    def _make_mock_market(
        self, condition_id, token_id, outcome, outcome_index, question="Test?"
    ):
        m = MagicMock()
        m.condition_id = condition_id
        m.token_id = token_id
        m.outcome = outcome
        m.outcome_index = outcome_index
        m.question = question
        m.platform = "polymarket"
        return m

    def test_build_from_two_markets_yes_no(self):
        yes_market = self._make_mock_market("cond-1", "tok-yes", "Yes", 0)
        no_market = self._make_mock_market("cond-1", "tok-no", "No", 1)

        registry = MarketPairRegistry.build_from_markets([yes_market, no_market])
        pairs = registry.get_all_pairs()
        assert len(pairs) == 1
        assert pairs[0].yes_token_id == "tok-yes"
        assert pairs[0].no_token_id == "tok-no"

    def test_build_from_markets_uses_outcome_index_fallback(self):
        # outcome field is empty string, so fall back to outcome_index
        m0 = self._make_mock_market("cond-1", "tok-0", "", 0)
        m1 = self._make_mock_market("cond-1", "tok-1", "", 1)

        registry = MarketPairRegistry.build_from_markets([m0, m1])
        pairs = registry.get_all_pairs()
        assert len(pairs) == 1
        assert pairs[0].yes_token_id == "tok-0"
        assert pairs[0].no_token_id == "tok-1"

    def test_build_skips_non_binary_markets(self):
        m1 = self._make_mock_market("cond-1", "tok-1", "Yes", 0)
        m2 = self._make_mock_market("cond-1", "tok-2", "No", 1)
        m3 = self._make_mock_market("cond-1", "tok-3", "Maybe", 2)

        registry = MarketPairRegistry.build_from_markets([m1, m2, m3])
        assert len(registry.get_all_pairs()) == 0

    def test_build_handles_multiple_conditions(self):
        yes1 = self._make_mock_market("cond-1", "yes-1", "Yes", 0)
        no1 = self._make_mock_market("cond-1", "no-1", "No", 1)
        yes2 = self._make_mock_market("cond-2", "yes-2", "Yes", 0)
        no2 = self._make_mock_market("cond-2", "no-2", "No", 1)

        registry = MarketPairRegistry.build_from_markets([yes1, no1, yes2, no2])
        assert len(registry.get_all_pairs()) == 2

    def test_build_empty_list_returns_empty_registry(self):
        registry = MarketPairRegistry.build_from_markets([])
        assert len(registry.get_all_pairs()) == 0

    def test_build_skips_unpaireable_markets(self):
        # Both have outcome "Yes" -- cannot determine no_market
        m1 = self._make_mock_market("cond-1", "tok-1", "Yes", 0)
        m2 = self._make_mock_market("cond-1", "tok-2", "Yes", 0)

        registry = MarketPairRegistry.build_from_markets([m1, m2])
        # The pairing logic tries outcome first, both get assigned as yes_market,
        # no_market stays None, so this pair is skipped.
        assert len(registry.get_all_pairs()) == 0
