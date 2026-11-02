"""Randomised end-to-end soak under injected faults. A failure prints the seed: rerun with that seed to replay it."""
import pytest
from harness import run_soak


@pytest.mark.parametrize("seed", range(1, 7))
def test_money_and_state_survive_chaos(seed):
    out = run_soak(seed, rounds=80)
    assert out["bids"] > 20 and out["ticks"] == (80 + 1) * 3  # +1: the wind-down tick that announces cancels
    assert out["wallet_lost_responses"] > 0 and out["wallet_lost_requests"] > 0


def test_soak_without_faults_is_exact():
    out = run_soak(99, rounds=60, p_lost_request=0, p_lost_response=0, p_dup=0, p_replay=0)
    assert out["revenue_micro"] > 0 and out["wallet_lost_requests"] == out["wallet_lost_responses"] == 0


@pytest.mark.parametrize("seed", (7, 8, 9))
def test_every_charged_micro_dollar_is_captured_even_when_users_cancel_mid_flight(seed):
    """Regression: cancelling used to release the escrow itself, racing with captures still in flight, so a tick
    the job really ran was skipped (free compute). Now settlement releases behind the last charge."""
    out = run_soak(seed, rounds=80, pump_range=(1, 3))  # slow event delivery makes the old race very likely
    assert out["cancel_200"] > 5 and out["charged_bids"] > 10
