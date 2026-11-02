from hypothesis import given
from hypothesis import strategies as st
from spotcommon.money import hold_amount, micro_to_usd, tick_charge, usd_to_micro


def test_parse_and_format_roundtrip():
    assert usd_to_micro("1.25") == 1_250_000 and usd_to_micro("2") == 2_000_000
    assert micro_to_usd(1_250_000) == "1.250000" and micro_to_usd(-5) == "-0.000005"


@given(maxp=st.integers(0, 50_000_000), price_frac=st.floats(0, 1), qty=st.integers(1, 64),
       dur=st.integers(60, 86_400), tick=st.sampled_from([1, 5, 7, 10]))
def test_total_tick_charges_never_exceed_hold(maxp, price_frac, qty, dur, tick):
    """Core money invariant: a job that runs its whole duration at any price <= its bid cannot
    out-spend its escrow, no matter how per-tick rounding falls."""
    price = int(maxp * price_frac)
    ticks = -(-dur // tick)
    assert ticks * tick_charge(price, qty, tick) <= hold_amount(maxp, qty, dur, tick)
