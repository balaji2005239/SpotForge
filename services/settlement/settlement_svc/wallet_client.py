from __future__ import annotations

import httpx


class Transient(Exception):
    """Outcome unknown or service unavailable: retry later (all wallet calls are idempotent)."""


class Permanent(Exception):
    """Wallet definitively refused (hold closed / unknown / insufficient): retrying cannot help."""


class SettlementWallet:
    def __init__(self, client: httpx.Client):
        self._c = client

    @classmethod
    def from_url(cls, base_url: str, timeout: float = 3.0) -> SettlementWallet:
        return cls(httpx.Client(base_url=base_url, timeout=timeout))

    def _post(self, path: str, body: dict) -> dict:
        try:
            r = self._c.post(path, json=body)
        except httpx.TransportError as e:
            raise Transient(str(e)) from e
        if r.status_code >= 500:
            raise Transient(f"wallet {r.status_code}")
        if r.status_code >= 400:
            raise Permanent(f"wallet {r.status_code}: {r.text[:120]}")
        return r.json()

    def capture(self, hold_id: str, amount_micro: int, key: str) -> None:
        self._post(f"/v1/holds/{hold_id}/capture", {"amount_micro": amount_micro, "idempotency_key": key})

    def release(self, hold_id: str, key: str) -> int:
        return int(self._post(f"/v1/holds/{hold_id}/release", {"idempotency_key": key}).get("refunded_micro") or 0)

    def get_hold(self, hold_id: str) -> dict | None:
        try:
            r = self._c.get(f"/v1/holds/{hold_id}")
        except httpx.TransportError as e:
            raise Transient(str(e)) from e
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            raise Transient(f"wallet {r.status_code}")
        return r.json()
