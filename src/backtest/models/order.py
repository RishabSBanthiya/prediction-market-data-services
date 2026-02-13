from decimal import Decimal
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, field_validator, model_validator


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    LIMIT = "limit"
    MARKET = "market"


class OrderStatus(str, Enum):
    PENDING = "pending"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class TimeInForce(str, Enum):
    GTC = "gtc"  # Good 'til cancelled
    IOC = "ioc"  # Immediate or cancel
    FOK = "fok"  # Fill or kill


class FillReason(str, Enum):
    IMMEDIATE = "immediate"  # Matched immediately against existing liquidity
    QUEUE_REACHED = "queue_reached"  # Order was in queue and got matched
    SETTLEMENT = "settlement"  # Filled at market settlement


class OrderRejectionReason(str, Enum):
    INSUFFICIENT_FUNDS = "insufficient_funds"
    INSUFFICIENT_POSITION = "insufficient_position"
    NO_LIQUIDITY = "no_liquidity"
    INVALID_PRICE = "invalid_price"
    INVALID_SIZE = "invalid_size"
    FOK_NOT_FILLABLE = "fok_not_fillable"
    ORDER_EXPIRED = "order_expired"


class Order(BaseModel):
    order_id: Optional[str] = None
    asset_id: str
    side: OrderSide
    order_type: OrderType
    price: Optional[Decimal] = None
    quantity: Decimal
    time_in_force: TimeInForce = TimeInForce.GTC
    status: OrderStatus = OrderStatus.PENDING
    submitted_at: Optional[int] = None
    filled_quantity: Decimal = Decimal("0")
    avg_fill_price: Optional[Decimal] = None
    rejection_reason: Optional["OrderRejectionReason"] = None

    @field_validator("quantity")
    @classmethod
    def validate_quantity(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("quantity must be greater than 0")
        return v

    @field_validator("price")
    @classmethod
    def validate_price(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        if v is not None:
            if v < 0 or v > 1:
                raise ValueError("price must be between 0 and 1 for prediction markets")
        return v

    @model_validator(mode="after")
    def validate_order_type_price_consistency(self) -> "Order":
        if self.order_type == OrderType.MARKET and self.price is not None:
            raise ValueError("market orders cannot have a price")
        if self.order_type == OrderType.LIMIT and self.price is None:
            raise ValueError("limit orders must have a price")
        return self

    @property
    def remaining_quantity(self) -> Decimal:
        """Calculate the remaining unfilled quantity."""
        return self.quantity - self.filled_quantity

    @property
    def is_fully_filled(self) -> bool:
        """Check if the order has been completely filled."""
        return self.filled_quantity >= self.quantity


class Fill(BaseModel):
    fill_id: str
    order_id: str
    asset_id: str
    side: OrderSide
    price: Decimal
    quantity: Decimal
    fees: Decimal = Decimal("0")
    timestamp_ms: int
    is_maker: bool
    fill_reason: FillReason = FillReason.QUEUE_REACHED

    def __init__(self, **data):
        if "fill_id" not in data:
            data["fill_id"] = str(uuid4())
        super().__init__(**data)

    @field_validator("quantity")
    @classmethod
    def validate_quantity(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("quantity must be greater than 0")
        return v

    @field_validator("price")
    @classmethod
    def validate_price(cls, v: Decimal) -> Decimal:
        if v < 0 or v > 1:
            raise ValueError("price must be between 0 and 1 for prediction markets")
        return v

    @field_validator("fees")
    @classmethod
    def validate_fees(cls, v: Decimal) -> Decimal:
        if v < 0:
            raise ValueError("fees cannot be negative")
        return v
