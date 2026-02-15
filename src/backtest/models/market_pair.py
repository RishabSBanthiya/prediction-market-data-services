from __future__ import annotations

import structlog
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

logger = structlog.get_logger(__name__)


@dataclass
class MarketPair:
    """Represents a Yes/No token pair for a binary prediction market.

    In prediction markets, Yes and No tokens are complementary - selling Yes
    is equivalent to buying No at (1 - price). This class links paired tokens
    and provides conversion utilities.
    """

    condition_id: str
    question: str
    yes_token_id: str
    no_token_id: str
    platform: str

    def get_complement_token(self, token_id: str) -> Optional[str]:
        """Returns the paired token for a given token ID."""
        if token_id == self.yes_token_id:
            return self.no_token_id
        elif token_id == self.no_token_id:
            return self.yes_token_id
        return None

    def get_complement_price(self, price: Decimal) -> Decimal:
        """Converts a token price to its complement: 1 - price."""
        return Decimal("1") - price

    def is_yes_token(self, token_id: str) -> bool:
        return token_id == self.yes_token_id

    def is_no_token(self, token_id: str) -> bool:
        return token_id == self.no_token_id

    def contains_token(self, token_id: str) -> bool:
        return token_id in (self.yes_token_id, self.no_token_id)


class MarketPairRegistry:
    """Registry for managing market pairs and providing efficient lookups.

    Maintains bidirectional mappings between condition IDs, token IDs, and
    market pairs for fast lookup during backtesting operations.
    """

    def __init__(self):
        self._pairs: dict[str, MarketPair] = {}
        self._token_to_condition: dict[str, str] = {}

    def register(self, pair: MarketPair) -> None:
        """Register a market pair and build reverse lookup indexes."""
        self._pairs[pair.condition_id] = pair
        self._token_to_condition[pair.yes_token_id] = pair.condition_id
        self._token_to_condition[pair.no_token_id] = pair.condition_id

        logger.debug(
            "registered_market_pair",
            condition_id=pair.condition_id,
            yes_token=pair.yes_token_id,
            no_token=pair.no_token_id,
            platform=pair.platform,
        )

    def get_pair_for_token(self, token_id: str) -> Optional[MarketPair]:
        """Find the market pair containing the given token."""
        condition_id = self._token_to_condition.get(token_id)
        if condition_id is None:
            return None
        return self._pairs.get(condition_id)

    def get_pair_by_condition(self, condition_id: str) -> Optional[MarketPair]:
        """Retrieve a market pair by its condition ID."""
        return self._pairs.get(condition_id)

    def get_all_pairs(self) -> list[MarketPair]:
        """Get all registered market pairs."""
        return list(self._pairs.values())

    @classmethod
    def build_from_markets(cls, markets: list) -> MarketPairRegistry:
        """Build a registry from a list of Market objects.

        Groups markets by condition_id and identifies Yes/No pairs based on
        the outcome field or outcome_index. Markets with more than two
        outcomes or ambiguous pairing are skipped with warnings.
        """
        registry = cls()

        condition_groups: dict[str, list] = {}
        for market in markets:
            condition_id = market.condition_id
            if condition_id not in condition_groups:
                condition_groups[condition_id] = []
            condition_groups[condition_id].append(market)

        for condition_id, group_markets in condition_groups.items():
            if len(group_markets) == 1:
                # Single-ticker market (e.g., Kalshi): the orderbook already
                # has yes (bids) and no (asks) sides. Create a self-pair so
                # the execution engine allows selling without a position
                # (selling yes = buying no, which is native to the orderbook).
                market = group_markets[0]
                platform = getattr(market, "platform", "kalshi")
                pair = MarketPair(
                    condition_id=condition_id,
                    question=market.question or "",
                    yes_token_id=market.token_id,
                    no_token_id=market.token_id,
                    platform=platform,
                )
                registry.register(pair)
                continue

            if len(group_markets) != 2:
                logger.warning(
                    "skipping_non_binary_market",
                    condition_id=condition_id,
                    token_count=len(group_markets),
                )
                continue

            yes_market = None
            no_market = None

            for market in group_markets:
                if market.outcome and market.outcome.strip():
                    outcome_lower = market.outcome.lower().strip()
                    if outcome_lower == "yes":
                        yes_market = market
                    elif outcome_lower == "no":
                        no_market = market

                if yes_market is None and market.outcome_index == 0:
                    yes_market = market
                elif no_market is None and market.outcome_index == 1:
                    no_market = market

            if yes_market is None or no_market is None:
                logger.warning(
                    "failed_to_pair_tokens",
                    condition_id=condition_id,
                    markets=[
                        {
                            "token_id": m.token_id,
                            "outcome": m.outcome,
                            "outcome_index": m.outcome_index,
                        }
                        for m in group_markets
                    ],
                )
                continue

            platform = getattr(yes_market, "platform", "polymarket")

            pair = MarketPair(
                condition_id=condition_id,
                question=yes_market.question or "",
                yes_token_id=yes_market.token_id,
                no_token_id=no_market.token_id,
                platform=platform,
            )
            registry.register(pair)

        logger.info(
            "market_pair_registry_built",
            total_markets=len(markets),
            total_pairs=len(registry.get_all_pairs()),
            total_conditions=len(condition_groups),
        )

        return registry
