"""Who may call what. Default deny: a route that is not listed below is admin-only.

Services trust the network, not the caller, so this is the one place identity is enforced:
users see and move only their own money and jobs; market data is public; fleet and ops are admin."""
from __future__ import annotations

import re
from dataclasses import dataclass

from fastapi import HTTPException

from .security import Claims

PUBLIC = (
    ("market", "GET", re.compile(r"^/v1/markets/(latest|[A-Z0-9]+/(price|ticks|book))$")),
    ("bid", "GET", re.compile(r"^/v1/markets$")),
)
WALLET = re.compile(r"^/v1/wallets/([^/]+)(/holds|/deposit)?$")
BID_ONE = re.compile(r"^/v1/bids/([^/]+)$")


@dataclass(frozen=True)
class Rule:
    inject_user: bool = False       # force/validate user_id in the JSON body (bid submit)
    owner_bid_id: str | None = None  # caller must own this bid (look it up before forwarding)


def authorize(svc: str, method: str, path: str, claims: Claims | None, query: dict[str, str],
              allow_test_deposit: bool = True) -> Rule:
    if any(svc == s and method == m and rx.match(path) for s, m, rx in PUBLIC):
        return Rule()
    if claims is None:
        raise HTTPException(401, "Sign in first.")
    if claims.role == "admin":
        return Rule()

    me = claims.sub
    if svc == "wallet":
        m = WALLET.match(path)
        if m and m.group(1) == me:
            sub = m.group(2)
            if method == "GET" and sub in (None, "/holds"):
                return Rule()
            if method == "POST" and sub == "/deposit" and allow_test_deposit:
                return Rule()
    elif svc == "bid":
        if method == "POST" and path == "/v1/bids":
            return Rule(inject_user=True)
        if method == "GET" and path == "/v1/bids" and query.get("user_id") == me:
            return Rule()
        m = BID_ONE.match(path)
        if m and method in ("GET", "DELETE", "PATCH"):
            return Rule(owner_bid_id=m.group(1))
    elif svc == "market":
        if method == "GET" and path == "/v1/allocations" and query.get("user_id") == me:
            return Rule()
    raise HTTPException(403, "You can't do that with this account.")
