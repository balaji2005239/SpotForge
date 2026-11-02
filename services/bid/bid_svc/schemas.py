from pydantic import BaseModel, Field
from spotcommon.config import GPU_TYPES


class BidIn(BaseModel):
    user_id: str = Field(min_length=1, max_length=64)  # replaced by JWT subject once the gateway lands
    gpu_type: str
    qty: int = Field(ge=1, le=64)
    max_price_micro: int = Field(ge=0, description="micro-dollars per GPU-hour")
    duration_s: int = Field(ge=60, le=86_400)
    divisible: bool = False
    checkpointable: bool = False
    priority: int = Field(default=0, ge=0, le=9)
    client_request_id: str | None = Field(default=None, max_length=80)

    def validate_gpu(self) -> None:
        if self.gpu_type not in GPU_TYPES:
            raise ValueError(f"gpu_type must be one of {GPU_TYPES}")


class BidReprice(BaseModel):
    max_price_micro: int = Field(ge=0, description="new max, micro-dollars per GPU-hour; can only go up")


class BidOut(BaseModel):
    bid_id: str
    user_id: str
    gpu_type: str
    qty: int
    max_price_micro: int
    duration_s: int
    divisible: bool
    checkpointable: bool
    priority: int
    hold_amount_micro: int
    status: str
    reject_reason: str | None = None
    seq: int
