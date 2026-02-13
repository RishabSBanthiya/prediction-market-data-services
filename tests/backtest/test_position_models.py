"""
Tests for Position and MarketPosition models.

Covers apply_fill for long/short positions, unrealized P&L,
market_value, is_flat, and MarketPosition aggregation.
"""

from decimal import Decimal

import pytest

from backtest.models.position import Position, MarketPosition, PositionSide


# ======================================================================
# Position.apply_fill() — long positions
# ======================================================================


class TestPositionApplyFillLong:
    """Test apply_fill() for building and reducing long positions."""

    def test_buy_opens_long_position(self):
        pos = Position(asset_id="token-1")
        realized = pos.apply_fill(
            side=PositionSide.BUY,
            price=Decimal("0.50"),
            quantity=Decimal("10"),
            fees=Decimal("0"),
        )
        assert pos.quantity == Decimal("10")
        assert pos.avg_entry_price == Decimal("0.50")
        assert realized == Decimal("0")

    def test_buy_increases_long_position_updates_avg_price(self):
        pos = Position(asset_id="token-1")
        pos.apply_fill(
            side=PositionSide.BUY, price=Decimal("0.50"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        pos.apply_fill(
            side=PositionSide.BUY, price=Decimal("0.60"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        assert pos.quantity == Decimal("20")
        # avg = (0.50*10 + 0.60*10) / 20 = 11/20 = 0.55
        assert pos.avg_entry_price == Decimal("0.55")

    def test_sell_reduces_long_with_positive_realized_pnl(self):
        pos = Position(asset_id="token-1")
        pos.apply_fill(
            side=PositionSide.BUY, price=Decimal("0.50"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        realized = pos.apply_fill(
            side=PositionSide.SELL, price=Decimal("0.60"),
            quantity=Decimal("5"), fees=Decimal("0"),
        )
        # realized = (0.60 - 0.50) * 5 = 0.50
        assert realized == Decimal("0.50")
        assert pos.realized_pnl == Decimal("0.50")
        assert pos.quantity == Decimal("5")

    def test_sell_reduces_long_with_negative_realized_pnl(self):
        pos = Position(asset_id="token-1")
        pos.apply_fill(
            side=PositionSide.BUY, price=Decimal("0.60"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        realized = pos.apply_fill(
            side=PositionSide.SELL, price=Decimal("0.50"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        # realized = (0.50 - 0.60) * 10 = -1.00
        assert realized == Decimal("-1.00")
        assert pos.quantity == Decimal("0")
        assert pos.avg_entry_price == Decimal("0")

    def test_sell_closes_long_completely_resets_avg_price(self):
        pos = Position(asset_id="token-1")
        pos.apply_fill(
            side=PositionSide.BUY, price=Decimal("0.50"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        pos.apply_fill(
            side=PositionSide.SELL, price=Decimal("0.55"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        assert pos.quantity == Decimal("0")
        assert pos.avg_entry_price == Decimal("0")
        assert pos.is_flat is True

    def test_fees_are_accumulated(self):
        pos = Position(asset_id="token-1")
        pos.apply_fill(
            side=PositionSide.BUY, price=Decimal("0.50"),
            quantity=Decimal("10"), fees=Decimal("0.05"),
        )
        pos.apply_fill(
            side=PositionSide.SELL, price=Decimal("0.55"),
            quantity=Decimal("10"), fees=Decimal("0.055"),
        )
        assert pos.total_fees_paid == Decimal("0.105")


# ======================================================================
# Position.apply_fill() — short positions
# ======================================================================


class TestPositionApplyFillShort:
    """Test apply_fill() for building and reducing short positions."""

    def test_sell_opens_short_position(self):
        pos = Position(asset_id="token-1")
        realized = pos.apply_fill(
            side=PositionSide.SELL, price=Decimal("0.60"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        assert pos.quantity == Decimal("-10")
        assert pos.avg_entry_price == Decimal("0.60")
        assert realized == Decimal("0")

    def test_sell_increases_short_position(self):
        pos = Position(asset_id="token-1")
        pos.apply_fill(
            side=PositionSide.SELL, price=Decimal("0.60"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        pos.apply_fill(
            side=PositionSide.SELL, price=Decimal("0.70"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        assert pos.quantity == Decimal("-20")
        # avg = (0.60*10 + 0.70*10) / 20 = 0.65
        assert pos.avg_entry_price == Decimal("0.65")

    def test_buy_reduces_short_with_positive_realized_pnl(self):
        pos = Position(asset_id="token-1")
        pos.apply_fill(
            side=PositionSide.SELL, price=Decimal("0.60"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        realized = pos.apply_fill(
            side=PositionSide.BUY, price=Decimal("0.50"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        # Short PnL = (entry - exit) * qty = (0.60 - 0.50) * 10 = 1.00
        assert realized == Decimal("1.00")
        assert pos.quantity == Decimal("0")

    def test_buy_reduces_short_with_negative_realized_pnl(self):
        pos = Position(asset_id="token-1")
        pos.apply_fill(
            side=PositionSide.SELL, price=Decimal("0.50"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        realized = pos.apply_fill(
            side=PositionSide.BUY, price=Decimal("0.60"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        # Short PnL = (0.50 - 0.60) * 10 = -1.00
        assert realized == Decimal("-1.00")


# ======================================================================
# Position.update_unrealized_pnl()
# ======================================================================


class TestPositionUnrealizedPnl:
    """Test update_unrealized_pnl() for long, short, and flat positions."""

    def test_unrealized_pnl_long_position(self):
        pos = Position(asset_id="token-1")
        pos.apply_fill(
            side=PositionSide.BUY, price=Decimal("0.50"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        pos.update_unrealized_pnl(Decimal("0.60"))
        # unrealized = (0.60 - 0.50) * 10 = 1.00
        assert pos.unrealized_pnl == Decimal("1.00")

    def test_unrealized_pnl_short_position(self):
        pos = Position(asset_id="token-1")
        pos.apply_fill(
            side=PositionSide.SELL, price=Decimal("0.60"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        pos.update_unrealized_pnl(Decimal("0.50"))
        # unrealized = (0.60 - 0.50) * 10 = 1.00
        assert pos.unrealized_pnl == Decimal("1.00")

    def test_unrealized_pnl_flat_position_is_zero(self):
        pos = Position(asset_id="token-1")
        pos.update_unrealized_pnl(Decimal("0.50"))
        assert pos.unrealized_pnl == Decimal("0")


# ======================================================================
# Position.market_value() and is_flat
# ======================================================================


class TestPositionMarketValueAndIsFlat:

    def test_market_value_long(self):
        pos = Position(asset_id="token-1", quantity=Decimal("10"))
        assert pos.market_value(Decimal("0.55")) == Decimal("5.50")

    def test_market_value_short(self):
        pos = Position(asset_id="token-1", quantity=Decimal("-10"))
        # market_value = quantity * price = -10 * 0.55 = -5.50
        assert pos.market_value(Decimal("0.55")) == Decimal("-5.50")

    def test_market_value_flat(self):
        pos = Position(asset_id="token-1", quantity=Decimal("0"))
        assert pos.market_value(Decimal("0.55")) == Decimal("0")

    def test_is_flat_true_when_zero(self):
        pos = Position(asset_id="token-1", quantity=Decimal("0"))
        assert pos.is_flat is True

    def test_is_flat_false_when_long(self):
        pos = Position(asset_id="token-1", quantity=Decimal("5"))
        assert pos.is_flat is False

    def test_is_flat_false_when_short(self):
        pos = Position(asset_id="token-1", quantity=Decimal("-5"))
        assert pos.is_flat is False


# ======================================================================
# Position.total_pnl
# ======================================================================


class TestPositionTotalPnl:

    def test_total_pnl_combines_realized_and_unrealized(self):
        pos = Position(
            asset_id="token-1",
            realized_pnl=Decimal("2.00"),
            unrealized_pnl=Decimal("1.50"),
        )
        assert pos.total_pnl == Decimal("3.50")


# ======================================================================
# MarketPosition
# ======================================================================


class TestMarketPosition:
    """Test MarketPosition aggregation."""

    def test_get_position_returns_none_for_missing(self):
        mp = MarketPosition(market_id="cond-1")
        assert mp.get_position("unknown") is None

    def test_get_or_create_position_creates_new(self):
        mp = MarketPosition(market_id="cond-1")
        pos = mp.get_or_create_position("token-1")
        assert pos.asset_id == "token-1"
        assert pos.quantity == Decimal("0")

    def test_get_or_create_position_returns_existing(self):
        mp = MarketPosition(market_id="cond-1")
        pos1 = mp.get_or_create_position("token-1")
        pos1.quantity = Decimal("10")
        pos2 = mp.get_or_create_position("token-1")
        assert pos2.quantity == Decimal("10")
        assert pos1 is pos2

    def test_update_unrealized_pnl_updates_all(self):
        mp = MarketPosition(market_id="cond-1")
        pos_yes = mp.get_or_create_position("token-yes")
        pos_yes.apply_fill(
            side=PositionSide.BUY, price=Decimal("0.50"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )
        pos_no = mp.get_or_create_position("token-no")
        pos_no.apply_fill(
            side=PositionSide.BUY, price=Decimal("0.40"),
            quantity=Decimal("10"), fees=Decimal("0"),
        )

        mp.update_unrealized_pnl({
            "token-yes": Decimal("0.60"),
            "token-no": Decimal("0.50"),
        })
        assert pos_yes.unrealized_pnl == Decimal("1.00")
        assert pos_no.unrealized_pnl == Decimal("1.00")

    def test_total_pnl_across_positions(self):
        mp = MarketPosition(market_id="cond-1")
        pos_yes = mp.get_or_create_position("token-yes")
        pos_yes.realized_pnl = Decimal("2.00")
        pos_yes.unrealized_pnl = Decimal("1.00")

        pos_no = mp.get_or_create_position("token-no")
        pos_no.realized_pnl = Decimal("0.50")
        pos_no.unrealized_pnl = Decimal("-0.25")

        # total = (2 + 1) + (0.5 - 0.25) = 3.25
        assert mp.total_pnl == Decimal("3.25")

    def test_net_exposure(self):
        mp = MarketPosition(market_id="cond-1")
        pos_yes = mp.get_or_create_position("token-yes")
        pos_yes.quantity = Decimal("10")
        pos_no = mp.get_or_create_position("token-no")
        pos_no.quantity = Decimal("5")

        prices = {
            "token-yes": Decimal("0.60"),
            "token-no": Decimal("0.40"),
        }
        # 10 * 0.60 + 5 * 0.40 = 6.0 + 2.0 = 8.0
        assert mp.net_exposure(prices) == Decimal("8.0")
