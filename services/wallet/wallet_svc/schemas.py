from pydantic import BaseModel, Field


class DepositIn(BaseModel):
    amount_micro: int = Field(gt=0)
    idempotency_key: str = Field(min_length=1, max_length=120)


class HoldIn(BaseModel):
    hold_id: str = Field(min_length=1, max_length=64)
    user_id: str = Field(min_length=1, max_length=64)
    amount_micro: int = Field(gt=0)


class CaptureIn(BaseModel):
    amount_micro: int = Field(gt=0)
    idempotency_key: str = Field(min_length=1, max_length=120)


class IncreaseIn(BaseModel):
    additional_micro: int = Field(gt=0)
    idempotency_key: str = Field(min_length=1, max_length=120)


class ReleaseIn(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=120)


class WalletOut(BaseModel):
    user_id: str
    available_micro: int
    held_micro: int


class HoldOut(BaseModel):
    hold_id: str
    user_id: str
    amount_micro: int
    captured_micro: int
    released_micro: int
    remaining_micro: int
    status: str


class TxnOut(BaseModel):
    txn_id: int
    replayed: bool
    refunded_micro: int | None = None
