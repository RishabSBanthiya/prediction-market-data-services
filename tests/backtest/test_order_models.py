"""
Tests for backtest order and fill models.

Covers Order creation/validation, Fill creation/validation,
enum values, and computed properties (remaining_quantity, is_fully_filled).
"""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from backtest.models.order import (
    Order,
    Fill,
    OrderSide,
    OrderType,
    OrderStatus,
    TimeInForce,
    FillReason,
    OrderRejectionReason,
)


# ======================================================================
# Order creation
# ======================================================================


class TestOrderCreation:
    """Test basic Order construction for different types."""

    def test_buy_limit_order_creation(self):
        order = Order(
            asset_id="token-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.55"),
            quantity=Decimal("10"),
        )
        assert order.side == OrderSide.BUY
        assert order.order_type == OrderType.LIMIT
        assert order.price == Decimal("0.55")
        assert order.quantity == Decimal("10")
        assert order.status == OrderStatus.PENDING
        assert order.filled_quantity == Decimal("0")
        assert order.avg_fill_price is None

    def test_sell_market_order_creation(self):
        order = Order(
            asset_id="token-1",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("5"),
        )
        assert order.side == OrderSide.SELL
        assert order.order_type == OrderType.MARKET
        assert order.price is None
        assert order.quantity == Decimal("5")

    def test_order_default_time_in_force_is_gtc(self):
        order = Order(
            asset_id="token-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0.50"),
            quantity=Decimal("1"),
        )
        assert order.time_in_force == TimeInForce.GTC

    def test_order_with_fok_time_in_force(self):
        order = Order(
            asset_id="token-1",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            time_in_force=TimeInForce.FOK,
        )
        assert order.time_in_force == TimeInForce.FOK


# ======================================================================
# Order validation
# ======================================================================


class TestOrderValidation:
    """Test Order field validators and model validators."""

    def test_quantity_must_be_positive(self):
        with pytest.raises(ValidationError, match="quantity must be greater than 0"):
            Order(
                asset_id="token-1",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("0.50"),
                quantity=Decimal("0"),
            )

    def test_negative_quantity_rejected(self):
        with pytest.raises(ValidationError, match="quantity must be greater than 0"):
            Order(
                asset_id="token-1",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("0.50"),
                quantity=Decimal("-1"),
            )

    def test_price_must_be_between_zero_and_one(self):
        with pytest.raises(ValidationError, match="price must be between 0 and 1"):
            Order(
                asset_id="token-1",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("1.5"),
                quantity=Decimal("10"),
            )

    def test_negative_price_rejected(self):
        with pytest.raises(ValidationError, match="price must be between 0 and 1"):
            Order(
                asset_id="token-1",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("-0.1"),
                quantity=Decimal("10"),
            )

    def test_market_order_cannot_have_price(self):
        with pytest.raises(ValidationError, match="market orders cannot have a price"):
            Order(
                asset_id="token-1",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                price=Decimal("0.50"),
                quantity=Decimal("10"),
            )

    def test_limit_order_must_have_price(self):
        with pytest.raises(ValidationError, match="limit orders must have a price"):
            Order(
                asset_id="token-1",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("10"),
            )

    def test_boundary_price_zero_is_valid(self):
        order = Order(
            asset_id="token-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("0"),
            quantity=Decimal("10"),
        )
        assert order.price == Decimal("0")

    def test_boundary_price_one_is_valid(self):
        order = Order(
            asset_id="token-1",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("1"),
            quantity=Decimal("10"),
        )
        assert order.price == Decimal("1")


# ======================================================================
# Order properties
# ======================================================================


class TestOrderProperties:
    """Test remaining_quantity and is_fully_filled."""

    def test_remaining_quantity_unfilled(self, sample_order_buy_limit):
        assert sample_order_buy_limit.remaining_quantity == Decimal("10")

    def test_remaining_quantity_partially_filled(self, sample_order_buy_limit):
        sample_order_buy_limit.filled_quantity = Decimal("3")
        assert sample_order_buy_limit.remaining_quantity == Decimal("7")

    def test_remaining_quantity_fully_filled(self, sample_order_buy_limit):
        sample_order_buy_limit.filled_quantity = Decimal("10")
        assert sample_order_buy_limit.remaining_quantity == Decimal("0")

    def test_is_fully_filled_false_when_unfilled(self, sample_order_buy_limit):
        assert sample_order_buy_limit.is_fully_filled is False

    def test_is_fully_filled_false_when_partially_filled(self, sample_order_buy_limit):
        sample_order_buy_limit.filled_quantity = Decimal("5")
        assert sample_order_buy_limit.is_fully_filled is False

    def test_is_fully_filled_true_when_fully_filled(self, sample_order_buy_limit):
        sample_order_buy_limit.filled_quantity = Decimal("10")
        assert sample_order_buy_limit.is_fully_filled is True

    def test_is_fully_filled_true_when_overfilled(self, sample_order_buy_limit):
        sample_order_buy_limit.filled_quantity = Decimal("15")
        assert sample_order_buy_limit.is_fully_filled is True


# ======================================================================
# Fill creation and validation
# ======================================================================


class TestFillCreation:
    """Test Fill creation and validation."""

    def test_fill_creation_with_explicit_id(self):
        fill = Fill(
            fill_id="fill-1",
            order_id="order-1",
            asset_id="token-1",
            side=OrderSide.BUY,
            price=Decimal("0.55"),
            quantity=Decimal("10"),
            fees=Decimal("0.055"),
            timestamp_ms=1700000000000,
            is_maker=True,
        )
        assert fill.fill_id == "fill-1"
        assert fill.price == Decimal("0.55")
        assert fill.quantity == Decimal("10")
        assert fill.fees == Decimal("0.055")
        assert fill.is_maker is True

    def test_fill_auto_generates_id(self):
        fill = Fill(
            order_id="order-1",
            asset_id="token-1",
            side=OrderSide.BUY,
            price=Decimal("0.55"),
            quantity=Decimal("10"),
            timestamp_ms=1700000000000,
            is_maker=False,
        )
        assert fill.fill_id is not None
        assert len(fill.fill_id) > 0

    def test_fill_default_fill_reason(self):
        fill = Fill(
            order_id="order-1",
            asset_id="token-1",
            side=OrderSide.BUY,
            price=Decimal("0.55"),
            quantity=Decimal("10"),
            timestamp_ms=1700000000000,
            is_maker=False,
        )
        assert fill.fill_reason == FillReason.QUEUE_REACHED

    def test_fill_quantity_must_be_positive(self):
        with pytest.raises(ValidationError, match="quantity must be greater than 0"):
            Fill(
                order_id="order-1",
                asset_id="token-1",
                side=OrderSide.BUY,
                price=Decimal("0.55"),
                quantity=Decimal("0"),
                timestamp_ms=1700000000000,
                is_maker=False,
            )

    def test_fill_price_must_be_in_range(self):
        with pytest.raises(ValidationError, match="price must be between 0 and 1"):
            Fill(
                order_id="order-1",
                asset_id="token-1",
                side=OrderSide.BUY,
                price=Decimal("1.5"),
                quantity=Decimal("10"),
                timestamp_ms=1700000000000,
                is_maker=False,
            )

    def test_fill_fees_cannot_be_negative(self):
        with pytest.raises(ValidationError, match="fees cannot be negative"):
            Fill(
                order_id="order-1",
                asset_id="token-1",
                side=OrderSide.BUY,
                price=Decimal("0.55"),
                quantity=Decimal("10"),
                fees=Decimal("-1"),
                timestamp_ms=1700000000000,
                is_maker=False,
            )


# ======================================================================
# Enum values
# ======================================================================


class TestEnumValues:
    """Test enum members exist with expected values."""

    def test_order_side_values(self):
        assert OrderSide.BUY.value == "buy"
        assert OrderSide.SELL.value == "sell"

    def test_order_type_values(self):
        assert OrderType.LIMIT.value == "limit"
        assert OrderType.MARKET.value == "market"

    def test_order_status_values(self):
        assert OrderStatus.PENDING.value == "pending"
        assert OrderStatus.PARTIAL.value == "partial"
        assert OrderStatus.FILLED.value == "filled"
        assert OrderStatus.CANCELLED.value == "cancelled"
        assert OrderStatus.REJECTED.value == "rejected"

    def test_time_in_force_values(self):
        assert TimeInForce.GTC.value == "gtc"
        assert TimeInForce.IOC.value == "ioc"
        assert TimeInForce.FOK.value == "fok"

    def test_fill_reason_values(self):
        assert FillReason.IMMEDIATE.value == "immediate"
        assert FillReason.QUEUE_REACHED.value == "queue_reached"
        assert FillReason.SETTLEMENT.value == "settlement"

    def test_order_rejection_reason_values(self):
        assert OrderRejectionReason.INSUFFICIENT_FUNDS.value == "insufficient_funds"
        assert OrderRejectionReason.NO_LIQUIDITY.value == "no_liquidity"
        assert OrderRejectionReason.FOK_NOT_FILLABLE.value == "fok_not_fillable"
