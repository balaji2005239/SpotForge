"""Double-entry ledger with escrow holds. Every mutation is idempotent on `idem_key`.

Money flow:
  deposit : EXTERNAL            -> user:available
  hold    : user:available      -> user:held        (escrow at bid time)
  capture : user:held           -> platform:revenue (per-tick charge)
  release : user:held           -> user:available   (refund of the unused remainder)
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import Account, Entry, Hold, Txn

EXTERNAL = "external:funding"
REVENUE = "platform:revenue"


class LedgerError(Exception):
    pass


class InsufficientFunds(LedgerError):
    pass


class NotFound(LedgerError):
    pass


class Conflict(LedgerError):
    """Same idempotency key reused with different parameters, or an invalid state transition."""


@dataclass(frozen=True)
class Result:
    txn_id: int
    replayed: bool


def avail(user: str) -> str:
    return f"user:{user}:available"


def held(user: str) -> str:
    return f"user:{user}:held"


def _kind_owner(acct_id: str) -> tuple[str, str | None]:
    if acct_id == EXTERNAL:
        return "EXTERNAL", None
    if acct_id == REVENUE:
        return "REVENUE", None
    _, owner, kind = acct_id.split(":")
    return kind.upper(), owner


def _ensure_accounts(db: Session, ids: list[str]) -> None:
    for aid in ids:
        if db.get(Account, aid) is None:
            kind, owner = _kind_owner(aid)
            try:
                with db.begin_nested():
                    db.add(Account(id=aid, owner_id=owner, kind=kind, balance=0))
                    db.flush()
            except IntegrityError:  # lost a creation race; the row exists now
                pass


def _lock_accounts(db: Session, ids: list[str]) -> dict[str, Account]:
    # Sorted order -> no deadlocks between concurrent postings.
    rows = db.scalars(
        select(Account).where(Account.id.in_(ids)).order_by(Account.id).with_for_update()
    ).all()
    return {a.id: a for a in rows}


def _fingerprint(kind: str, ref: str | None, legs: list[tuple[str, int]]) -> str:
    raw = f"{kind}|{ref}|" + ",".join(f"{a}:{n}" for a, n in sorted(legs))
    return hashlib.sha256(raw.encode()).hexdigest()


def _post(db: Session, idem_key: str, kind: str, ref: str | None,
          legs: list[tuple[str, int]]) -> Result | None:
    """Apply a balanced set of legs atomically. Returns Result(replayed=True) for a repeat key.
    Caller holds the session transaction and commits."""
    if sum(n for _, n in legs) != 0:
        raise LedgerError("unbalanced transaction")
    ids = sorted({a for a, _ in legs})
    _ensure_accounts(db, ids)
    accounts = _lock_accounts(db, ids)  # lock BEFORE the idempotency check -> no race window

    fp = _fingerprint(kind, ref, legs)
    existing = db.scalar(select(Txn).where(Txn.idem_key == idem_key))
    if existing:
        if existing.fingerprint != fp:
            raise Conflict(f"idempotency key {idem_key!r} reused with different parameters")
        return Result(existing.id, replayed=True)

    for acct_id, n in legs:
        acct = accounts[acct_id]
        if acct.kind != "EXTERNAL" and acct.balance + n < 0:
            raise InsufficientFunds(f"{acct_id} would go negative")
        acct.balance += n

    txn = Txn(idem_key=idem_key, fingerprint=fp, kind=kind, ref=ref)
    db.add(txn)
    db.flush()
    for acct_id, n in legs:
        db.add(Entry(txn_id=txn.id, account_id=acct_id, amount=n))
    db.flush()
    return Result(txn.id, replayed=False)


def deposit(db: Session, user: str, amount: int, idem_key: str) -> Result:
    if amount <= 0:
        raise LedgerError("amount must be > 0")
    return _post(db, idem_key, "DEPOSIT", user, [(EXTERNAL, -amount), (avail(user), amount)])


def place_hold(db: Session, user: str, hold_id: str, amount: int) -> Result:
    if amount <= 0:
        raise LedgerError("amount must be > 0")
    idem = f"hold:{hold_id}"
    res = _post(db, idem, "HOLD", hold_id, [(avail(user), -amount), (held(user), amount)])
    if not res.replayed:
        db.add(Hold(id=hold_id, user_id=user, amount=amount))
        db.flush()
    return res


def increase_hold(db: Session, hold_id: str, additional: int, idem_key: str) -> Result:
    """Escrow more on an ACTIVE hold (the bidder raised their max price): available -> held, hold.amount grows."""
    if additional <= 0:
        raise LedgerError("amount must be > 0")
    h = _lock_hold(db, hold_id)
    if db.scalar(select(Txn).where(Txn.idem_key == idem_key)) is None and h.status != "ACTIVE":
        raise Conflict(f"hold {hold_id} is {h.status}")
    res = _post(db, idem_key, "HOLD_INCREASE", hold_id,
                [(avail(h.user_id), -additional), (held(h.user_id), additional)])
    if not res.replayed:
        h.amount += additional
        db.flush()
    return res


def _lock_hold(db: Session, hold_id: str) -> Hold:
    h = db.scalar(select(Hold).where(Hold.id == hold_id).with_for_update())
    if h is None:
        raise NotFound(f"hold {hold_id} not found")
    return h


def capture(db: Session, hold_id: str, amount: int, idem_key: str) -> Result:
    """Charge `amount` against a hold (one tick of a running job)."""
    if amount <= 0:
        raise LedgerError("amount must be > 0")
    h = _lock_hold(db, hold_id)
    legs = [(held(h.user_id), -amount), (REVENUE, amount)]
    # Replays must succeed even after the hold closed, so check idempotency first.
    prior = db.scalar(select(Txn).where(Txn.idem_key == idem_key))
    if prior is None:
        if h.status != "ACTIVE":
            raise Conflict(f"hold {hold_id} is {h.status}")
        if amount > h.remaining:
            raise Conflict(f"capture {amount} exceeds remaining {h.remaining} on hold {hold_id}")
    res = _post(db, idem_key, "CAPTURE", hold_id, legs)
    if not res.replayed:
        h.captured += amount
        if h.remaining == 0:
            h.status = "CLOSED"
        db.flush()
    return res


def release(db: Session, hold_id: str, idem_key: str) -> tuple[Result, int]:
    """Refund whatever is left on the hold and close it. Returns (result, refunded_amount)."""
    h = _lock_hold(db, hold_id)
    prior = db.scalar(select(Txn).where(Txn.idem_key == idem_key))
    if prior is not None:
        refunded = db.scalar(
            select(Entry.amount).where(Entry.txn_id == prior.id, Entry.account_id == avail(h.user_id))
        ) or 0
        return Result(prior.id, replayed=True), refunded
    if h.status != "ACTIVE":
        raise Conflict(f"hold {hold_id} is already {h.status}")
    amount = h.remaining
    if amount == 0:
        h.status = "CLOSED"
        raise Conflict(f"hold {hold_id} has nothing left to release")
    res = _post(db, idem_key, "RELEASE", hold_id, [(held(h.user_id), -amount), (avail(h.user_id), amount)])
    h.released += amount
    h.status = "CLOSED"
    db.flush()
    return res, amount


def balances(db: Session, user: str) -> tuple[int, int]:
    a = db.get(Account, avail(user))
    hd = db.get(Account, held(user))
    return (a.balance if a else 0, hd.balance if hd else 0)


def verify_integrity(db: Session) -> list[str]:
    """Reconciliation: returns a list of violations (empty == ledger is sound)."""
    problems: list[str] = []
    for txn_id, total in db.execute(
        select(Entry.txn_id, func.sum(Entry.amount)).group_by(Entry.txn_id)
    ):
        if total != 0:
            problems.append(f"txn {txn_id} unbalanced by {total}")
    sums = dict(db.execute(select(Entry.account_id, func.sum(Entry.amount)).group_by(Entry.account_id)).all())
    for a in db.scalars(select(Account)):
        if a.balance != sums.get(a.id, 0):
            problems.append(f"account {a.id}: cached {a.balance} != entries {sums.get(a.id, 0)}")
    for h in db.scalars(select(Hold)):
        held_bal = h.remaining
        if held_bal < 0:
            problems.append(f"hold {h.id} over-spent")
    # Sum of per-user held accounts must equal sum of remaining on ACTIVE holds.
    total_held = db.scalar(select(func.coalesce(func.sum(Account.balance), 0)).where(Account.kind == "HELD")) or 0
    total_remaining = sum(h.remaining for h in db.scalars(select(Hold)))
    if total_held != total_remaining:
        problems.append(f"held accounts {total_held} != hold remainders {total_remaining}")
    return problems
