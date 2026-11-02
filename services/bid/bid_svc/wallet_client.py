from __future__ import annotations

import httpx


class InsufficientFunds(Exception):
    pass


class HoldClosed(Exception):
    """The escrow is no longer ACTIVE (the job already ended)."""


class WalletUnavailable(Exception):
    """Transport error / 5xx: outcome unknown, safe to retry (all wallet calls are idempotent)."""


class WalletClient:
    def __init__(self, client: httpx.Client):
        self._c = client

    @classmethod
    def from_url(cls, base_url: str, timeout: float = 2.0) -> WalletClient:
        return cls(httpx.Client(base_url=base_url, timeout=timeout))

    def hold(self, user_id: str, hold_id: str, amount_micro: int) -> None:
        try:
            r = self._c.post("/v1/holds", json={"hold_id": hold_id, "user_id": user_id,
                                                "amount_micro": amount_micro})
        except httpx.TransportError as e:
            raise WalletUnavailable(str(e)) from e
        if r.status_code == 402:
            raise InsufficientFunds(r.json().get("detail", "insufficient funds"))
        if r.status_code >= 500:
            raise WalletUnavailable(f"wallet returned {r.status_code}")
        r.raise_for_status()

    def increase_hold(self, hold_id: str, additional_micro: int, key: str) -> None:
        try:
            r = self._c.post(f"/v1/holds/{hold_id}/increase",
                             json={"additional_micro": additional_micro, "idempotency_key": key})
        except httpx.TransportError as e:
            raise WalletUnavailable(str(e)) from e
        if r.status_code == 402:
            raise InsufficientFunds(r.json().get("detail", "insufficient funds"))
        if r.status_code >= 500:
            raise WalletUnavailable(f"wallet returned {r.status_code}")
        if r.status_code == 409:
            raise HoldClosed(r.json().get("detail", "hold is closed"))
        r.raise_for_status()

    def release(self, hold_id: str) -> None:
        try:
            r = self._c.post(f"/v1/holds/{hold_id}/release", json={"idempotency_key": f"cancel:{hold_id}"})
        except httpx.TransportError as e:
            raise WalletUnavailable(str(e)) from e
        if r.status_code >= 500:
            raise WalletUnavailable(f"wallet returned {r.status_code}")
        if r.status_code not in (200, 409):  # 409 = already closed, treat as done
            r.raise_for_status()
