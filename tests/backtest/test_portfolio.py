"""
Tests for Portfolio and PortfolioView.

Covers apply_fill, get_position, total_value with mark prices,
buying_power, and verifying that PortfolioView cannot be instantiated.
"""

from decimal import Decimal

import pytest

from backtest.models.order import Fill, OrderSide, FillReason
from backtest.models.portfolio import Portfolio, PortfolioView
from backtest.models.market_pair import MarketPair, MarketPairRegistry


# ======================================================================
# PortfolioView cannot be instantiated
# ======================================================================


class TestPortfolioViewAbstract:
    """Verify PortfolioView is abstract."""

    def test_cannot_instantiate_portfolio_view(self):
        with pytest.raises(TypeError):
            PortfolioView()


# ======================================================================
# Portfolio.apply_fill()
# ======================================================================


class TestPortfolioApplyFill:
    """Test Portfolio.apply_fill() for buy and sell fills."""

    def _make_fill(self, side: OrderSide, price: str, qty: str, fees: str = "0") -> Fill:
        return Fill(
            order_id="order-1",
            asset_id="token-yes-1",
            side=side,
            price=Decimal(price),
            quantity=Decimal(qty),
            fees=Decimal(fees),
            timestamp_ms=1700000000000,
            is_maker=True,
            fill_reason=FillReason.IMMEDIATE,
        )

    def test_buy_fill_decreases_cash(self, portfolio):
        fill = self._make_fill(OrderSide.BUY, "0.55", "10", "0")
        portfolio.apply_fill(fill)
        # Cash = 10000 - (0.55 * 10 + 0) = 10000 - 5.50 = 9994.50
        assert portfolio.cash == Decimal("9994.50")

    def test_buy_fill_with_fees_decreases_cash_further(self, portfolio):
        fill = self._make_fill(OrderSide.BUY, "0.55", "10", "0.10")
        portfolio.apply_fill(fill)
        # Cash = 10000 - (5.50 + 0.10) = 9994.40
        assert portfolio.cash == Decimal("9994.40")

    def test_sell_fill_increases_cash(self, portfolio):
        # First buy to have a position
        buy_fill = self._make_fill(OrderSide.BUY, "0.50", "10", "0")
        portfolio.apply_fill(buy_fill)

        sell_fill = Fill(
            order_id="order-2",
            asset_id="token-yes-1",
            side=OrderSide.SELL,
            price=Decimal("0.60"),
            quantity=Decimal("10"),
            fees=Decimal("0"),
            timestamp_ms=1700000001000,
            is_maker=True,
            fill_reason=FillReason.IMMEDIATE,
        )
        portfolio.apply_fill(sell_fill)
        # Cash = 10000 - 5.00 + 6.00 = 10001.00
        assert portfolio.cash == Decimal("10001.00")

    def test_sell_fill_with_fees_reduces_proceeds(self, portfolio):
        buy_fill = self._make_fill(OrderSide.BUY, "0.50", "10", "0")
        portfolio.apply_fill(buy_fill)

        sell_fill = Fill(
            order_id="order-2",
            asset_id="token-yes-1",
            side=OrderSide.SELL,
            price=Decimal("0.60"),
            quantity=Decimal("10"),
            fees=Decimal("0.10"),
            timestamp_ms=1700000001000,
            is_maker=False,
            fill_reason=FillReason.IMMEDIATE,
        )
        portfolio.apply_fill(sell_fill)
        # Cash = 10000 - 5.00 + (6.00 - 0.10) = 10000.90
        assert portfolio.cash == Decimal("10000.90")


# ======================================================================
# Portfolio.get_position()
# ======================================================================


class TestPortfolioGetPosition:

    def test_get_position_returns_none_for_unknown(self, portfolio):
        assert portfolio.get_position("unknown-token") is None

    def test_get_position_returns_position_after_fill(self, portfolio):
        fill = Fill(
            order_id="order-1",
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            price=Decimal("0.55"),
            quantity=Decimal("10"),
            fees=Decimal("0"),
            timestamp_ms=1700000000000,
            is_maker=True,
            fill_reason=FillReason.IMMEDIATE,
        )
        portfolio.apply_fill(fill)
        pos = portfolio.get_position("token-yes-1")
        assert pos is not None
        assert pos.quantity == Decimal("10")
        assert pos.avg_entry_price == Decimal("0.55")


# ======================================================================
# Portfolio.total_value with mark prices
# ======================================================================


class TestPortfolioTotalValue:

    def test_total_value_cash_only(self, portfolio):
        assert portfolio.total_value == Decimal("10000")

    def test_total_value_with_position_and_mark_prices(self, portfolio):
        fill = Fill(
            order_id="order-1",
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            price=Decimal("0.50"),
            quantity=Decimal("100"),
            fees=Decimal("0"),
            timestamp_ms=1700000000000,
            is_maker=True,
            fill_reason=FillReason.IMMEDIATE,
        )
        portfolio.apply_fill(fill)
        # Cash = 10000 - 50.00 = 9950.00
        assert portfolio.cash == Decimal("9950.00")

        # Update mark prices to 0.60
        portfolio.update_mark_prices({"token-yes-1": Decimal("0.60")})
        # total_value = 9950 + 100 * 0.60 = 9950 + 60 = 10010
        assert portfolio.total_value == Decimal("10010.00")

    def test_total_value_without_mark_price_ignores_position(self, portfolio):
        fill = Fill(
            order_id="order-1",
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            price=Decimal("0.50"),
            quantity=Decimal("100"),
            fees=Decimal("0"),
            timestamp_ms=1700000000000,
            is_maker=True,
            fill_reason=FillReason.IMMEDIATE,
        )
        portfolio.apply_fill(fill)
        # No mark prices set, so position value is 0 in total_value
        assert portfolio.total_value == Decimal("9950.00")


# ======================================================================
# Portfolio.buying_power
# ======================================================================


class TestPortfolioBuyingPower:

    def test_buying_power_equals_cash(self, portfolio):
        assert portfolio.buying_power == portfolio.cash
        assert portfolio.buying_power == Decimal("10000")

    def test_buying_power_decreases_after_buy(self, portfolio):
        fill = Fill(
            order_id="order-1",
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            price=Decimal("0.50"),
            quantity=Decimal("100"),
            fees=Decimal("0"),
            timestamp_ms=1700000000000,
            is_maker=True,
            fill_reason=FillReason.IMMEDIATE,
        )
        portfolio.apply_fill(fill)
        assert portfolio.buying_power == Decimal("9950.00")


# ======================================================================
# Portfolio auxiliary methods
# ======================================================================


class TestPortfolioAuxiliary:

    def test_initial_cash(self, portfolio):
        assert portfolio.initial_cash == Decimal("10000")

    def test_get_all_positions_empty(self, portfolio):
        assert portfolio.get_all_positions() == {}

    def test_get_all_positions_returns_copy(self, portfolio):
        fill = Fill(
            order_id="order-1",
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            price=Decimal("0.50"),
            quantity=Decimal("10"),
            fees=Decimal("0"),
            timestamp_ms=1700000000000,
            is_maker=True,
            fill_reason=FillReason.IMMEDIATE,
        )
        portfolio.apply_fill(fill)
        positions = portfolio.get_all_positions()
        assert "token-yes-1" in positions
        # Modifying the copy should not affect internal state
        positions.pop("token-yes-1")
        assert portfolio.get_position("token-yes-1") is not None

    def test_get_fills(self, portfolio):
        fill = Fill(
            order_id="order-1",
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            price=Decimal("0.50"),
            quantity=Decimal("10"),
            fees=Decimal("0"),
            timestamp_ms=1700000000000,
            is_maker=True,
            fill_reason=FillReason.IMMEDIATE,
        )
        portfolio.apply_fill(fill)
        fills = portfolio.get_fills()
        assert len(fills) == 1
        assert fills[0].order_id == "order-1"

    def test_get_total_fees_paid(self, portfolio):
        fill1 = Fill(
            order_id="order-1",
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            price=Decimal("0.50"),
            quantity=Decimal("10"),
            fees=Decimal("0.05"),
            timestamp_ms=1700000000000,
            is_maker=True,
            fill_reason=FillReason.IMMEDIATE,
        )
        fill2 = Fill(
            order_id="order-2",
            asset_id="token-yes-1",
            side=OrderSide.SELL,
            price=Decimal("0.55"),
            quantity=Decimal("10"),
            fees=Decimal("0.055"),
            timestamp_ms=1700000001000,
            is_maker=False,
            fill_reason=FillReason.IMMEDIATE,
        )
        portfolio.apply_fill(fill1)
        portfolio.apply_fill(fill2)
        assert portfolio.get_total_fees_paid() == Decimal("0.105")

    def test_get_return_no_trades(self, portfolio):
        assert portfolio.get_return() == 0.0

    def test_get_return_positive(self, portfolio):
        # Buy 100 at 0.50, mark at 0.60 => position value=60, cash=9950
        fill = Fill(
            order_id="order-1",
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            price=Decimal("0.50"),
            quantity=Decimal("100"),
            fees=Decimal("0"),
            timestamp_ms=1700000000000,
            is_maker=True,
            fill_reason=FillReason.IMMEDIATE,
        )
        portfolio.apply_fill(fill)
        portfolio.update_mark_prices({"token-yes-1": Decimal("0.60")})
        # total_value = 9950 + 60 = 10010
        # return = (10010 - 10000) / 10000 = 0.001
        assert abs(portfolio.get_return() - 0.001) < 1e-9

    def test_market_position_updated_with_registry(self):
        registry = MarketPairRegistry()
        pair = MarketPair(
            condition_id="cond-1",
            question="Will X win?",
            yes_token_id="token-yes-1",
            no_token_id="token-no-1",
            platform="polymarket",
        )
        registry.register(pair)
        p = Portfolio(initial_cash=Decimal("10000"), market_pairs=registry)

        fill = Fill(
            order_id="order-1",
            asset_id="token-yes-1",
            side=OrderSide.BUY,
            price=Decimal("0.50"),
            quantity=Decimal("10"),
            fees=Decimal("0"),
            timestamp_ms=1700000000000,
            is_maker=True,
            fill_reason=FillReason.IMMEDIATE,
        )
        p.apply_fill(fill)

        mp = p.get_market_position("cond-1")
        assert mp is not None
        assert "token-yes-1" in mp.positions
