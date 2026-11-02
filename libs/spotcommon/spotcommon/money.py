"""All money is integer micro-dollars (1 USD = 1_000_000). Never use floats for money."""

MICRO = 1_000_000
SECONDS_PER_HOUR = 3600


def _ceil_div(a: int, b: int) -> int:
    return -(-a // b)


def usd_to_micro(usd: str | int) -> int:
    """Parse '1.25' -> 1_250_000 exactly (no float)."""
    s = str(usd)
    whole, _, frac = s.partition(".")
    frac = (frac + "000000")[:6]
    return int(whole or "0") * MICRO + int(frac)


def micro_to_usd(micro: int) -> str:
    sign = "-" if micro < 0 else ""
    micro = abs(micro)
    return f"{sign}{micro // MICRO}.{micro % MICRO:06d}"


def tick_charge(price_per_gpu_hr: int, qty: int, tick_s: int) -> int:
    """Charge for one tick at the clearing price. Rounded up (platform-favourable, deterministic)."""
    return _ceil_div(price_per_gpu_hr * qty * tick_s, SECONDS_PER_HOUR)


def hold_amount(max_price_per_gpu_hr: int, qty: int, duration_s: int, tick_s: int = 5) -> int:
    """Worst-case cost of a bid, sized to match per-tick billing exactly.

    A job runs at most ceil(duration/tick) ticks and pays at most `max_price` each tick, and
    tick_charge is monotone in price, so the sum of captures can never exceed this hold. (Sizing the
    hold from the un-rounded total would let per-tick rounding exceed it by a few micro-dollars.)
    """
    ticks = _ceil_div(duration_s, tick_s)
    return ticks * tick_charge(max_price_per_gpu_hr, qty, tick_s)
