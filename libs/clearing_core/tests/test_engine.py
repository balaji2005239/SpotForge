import random

import pytest
from clearing_core import BidIn, PricingRule, clear
from hypothesis import given, settings
from hypothesis import strategies as st


def B(i, qty, price, seq=None, divisible=False, running=False):
    return BidIn(f"b{i}", qty, price, seq if seq is not None else i, divisible, running)


def run(bids, capacity=10, reserve=100, rule=PricingRule.HIGHEST_REJECTED):
    return clear("snap-1", "A100", bids, capacity, reserve, rule)


def test_undersubscribed_clears_at_reserve():
    r = run([B(1, 3, 500), B(2, 2, 400)], capacity=10)
    assert r.clearing_price == 100
    assert {f.bid_id for f in r.fills} == {"b1", "b2"}
    assert r.filled_qty == 5 and r.rejected == ()


def test_oversubscribed_price_is_highest_rejected():
    # capacity 10: b1(6)+b2(4) fill; b3 (price 300) is rejected and sets the price
    r = run([B(1, 6, 900), B(2, 4, 500), B(3, 5, 300), B(4, 1, 200)])
    assert [f.bid_id for f in r.fills] == ["b1", "b2"]
    assert r.clearing_price == 300
    assert r.rejected == ("b3", "b4")


def test_lowest_accepted_rule():
    r = run([B(1, 6, 900), B(2, 4, 500), B(3, 5, 300)], rule=PricingRule.LOWEST_ACCEPTED)
    assert r.clearing_price == 500


def test_below_reserve_never_wins():
    r = run([B(1, 1, 99), B(2, 1, 100)])
    assert [f.bid_id for f in r.fills] == ["b2"]
    assert "b1" in r.rejected


def test_tie_broken_by_arrival_so_incumbent_keeps_gpus():
    inc = B(1, 10, 500, seq=1, running=True)
    new = B(2, 10, 500, seq=2)
    r = run([new, inc])
    assert [f.bid_id for f in r.fills] == ["b1"]
    assert r.clearing_price == 500  # the tie loser sets the price
    assert r.preempted == ()


def test_running_job_outbid_is_preempted():
    inc = B(1, 10, 300, seq=1, running=True)
    whale = B(2, 10, 800, seq=2)
    r = run([inc, whale])
    assert r.preempted == ("b1",)
    assert r.clearing_price == 300


def test_indivisible_skipped_but_smaller_lower_bid_still_fills():
    r = run([B(1, 8, 900), B(2, 5, 800), B(3, 2, 400)])
    assert [f.bid_id for f in r.fills] == ["b1", "b3"]
    assert r.rejected == ("b2",)
    # b2 (800) lost but is above the lowest winner (400): it must NOT set the price.
    assert r.clearing_price == 100
    assert all(r.clearing_price <= {"b1": 900, "b3": 400}[f.bid_id] for f in r.fills)


def test_divisible_partial_fill():
    r = run([B(1, 6, 900), B(2, 8, 500, divisible=True)])
    assert [(f.bid_id, f.qty) for f in r.fills] == [("b1", 6), ("b2", 4)]
    assert r.filled_qty == 10
    assert r.clearing_price == 500  # its unmet demand sets the price


def test_zero_capacity_and_empty_book():
    assert run([], capacity=10).clearing_price == 100
    r = run([B(1, 1, 999)], capacity=0)
    assert r.fills == () and r.rejected == ("b1",)


def test_validation():
    with pytest.raises(ValueError):
        run([B(1, 0, 500)])
    with pytest.raises(ValueError):
        run([B(1, 1, 500), B(1, 2, 400)])


def test_digest_stable_and_input_order_invariant():
    bids = [B(i, random.Random(i).randint(1, 5), 100 + i * 7, divisible=i % 3 == 0) for i in range(30)]
    a = run(bids, capacity=20)
    shuffled = bids[:]
    random.Random(7).shuffle(shuffled)
    b = run(shuffled, capacity=20)
    assert a == b and a.digest == b.digest


bid_strategy = st.lists(
    st.builds(
        lambda q, p, d, r: (q, p, d, r),
        st.integers(1, 8),
        st.integers(0, 2000),
        st.booleans(),
        st.booleans(),
    ),
    max_size=40,
)


@settings(max_examples=300, deadline=None)
@given(raw=bid_strategy, capacity=st.integers(0, 50), reserve=st.integers(0, 500),
       rule=st.sampled_from(list(PricingRule)))
def test_invariants(raw, capacity, reserve, rule):
    bids = [BidIn(f"b{i}", q, p, i, d, r) for i, (q, p, d, r) in enumerate(raw)]
    res = clear("s", "H100", bids, capacity, reserve, rule)
    by_id = {b.bid_id: b for b in bids}

    # 1. never over-allocate
    assert res.filled_qty == sum(f.qty for f in res.fills) <= capacity
    # 2. nobody pays more than they bid, nobody below reserve wins
    assert res.clearing_price >= reserve
    for f in res.fills:
        b = by_id[f.bid_id]
        assert b.max_price >= res.clearing_price
        assert 0 < f.qty <= b.qty
        assert f.qty == b.qty or b.divisible
    # 3. every bid is either filled or rejected, exactly once
    ids = [f.bid_id for f in res.fills] + list(res.rejected)
    assert sorted(ids) == sorted(by_id)
    # 4. replay determinism
    assert clear("s", "H100", list(reversed(bids)), capacity, reserve, rule).digest == res.digest
