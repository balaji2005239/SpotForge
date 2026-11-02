"""Password hashing (scrypt, stdlib) and signed access tokens (HS256 JWT via PyJWT)."""
from __future__ import annotations

import hashlib
import hmac
import os
import time
from dataclasses import dataclass

import jwt

_N, _R, _P = 2**14, 8, 1


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    h = hashlib.scrypt(password.encode(), salt=salt, n=_N, r=_R, p=_P)
    return f"scrypt${salt.hex()}${h.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        _, salt, h = stored.split("$")
        got = hashlib.scrypt(password.encode(), salt=bytes.fromhex(salt), n=_N, r=_R, p=_P)
        return hmac.compare_digest(got, bytes.fromhex(h))
    except ValueError:
        return False


DUMMY_HASH = hash_password("not-a-real-password")  # burn the same time for unknown users (no username probing)


@dataclass(frozen=True)
class Claims:
    sub: str
    role: str


class TokenError(Exception):
    pass


def make_token(sub: str, role: str, secret: str, ttl_s: int, now: float | None = None) -> str:
    iat = int(now if now is not None else time.time())
    return jwt.encode({"sub": sub, "role": role, "iat": iat, "exp": iat + ttl_s}, secret, algorithm="HS256")


def read_token(token: str, secret: str) -> Claims:
    try:
        d = jwt.decode(token, secret, algorithms=["HS256"], options={"require": ["exp", "sub"]})  # never "none"
    except jwt.PyJWTError as e:
        raise TokenError(str(e)) from e
    return Claims(sub=d["sub"], role=d.get("role", "user"))
