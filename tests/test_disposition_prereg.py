"""Tests for analysis/prereg/disposition.py — the frozen disposition-score pre-registration.

Synthetic data only: these check the mechanics (intervals, states, eligibility, the
due-date gate), never a real outcome. The constants test is a tamper guard — changing a
frozen parameter must come with a dated amendment in analysis/prereg/PREREG.md.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "analysis" / "prereg"))
import disposition as d  # noqa: E402


def test_frozen_parameters_unchanged():
    assert (d.STEP_MIN, d.SPY_BAND, d.OPT_BAND, d.EARLY_MIN, d.T1_MIN_EACH, d.T2_MIN_EACH) == (5, 0.05, 0.02, 60, 3, 5)
    assert (d.PASS_RHO, d.PERMS, d.SEED, d.SPLIT, d.REG_END) == (-0.20, 10_000, 7, "2026-07-01", "2026-10-02")
    assert (d.OOS_START, d.OOS_STEP_MIN, d.OOS_MIN_DAYS, d.OOS_EARLIEST, d.OOS_LATEST, d.OOS_FLOOR, d.OOS_P) == (
        "2026-10-06", 1, 20, "2026-12-04", "2027-01-29", 15, 0.05)


def order(gid, en, exits, iso="2026-10-07", px=1.00, ck="2026-10-07|call|580"):
    """Rows for one order: one row per exit, as spy_trades.csv has them."""
    en = pd.Timestamp(f"{iso} {en}")
    return [dict(**{"Group ID": gid}, en=en, ex=pd.Timestamp(f"{iso} {x}"), iso=iso, px_in=px, ck=ck,
                 sign=1, pl=0.0, cost=100.0) for x in exits]


def test_intervals_5min_grid_and_sold_flag():
    df = pd.DataFrame(order("a", "10:00:00", ["10:07:00", "10:12:00"]))
    J = d.intervals(df, lambda g, t: "green", step=5)
    assert list(J.t.dt.strftime("%H:%M")) == ["10:05", "10:10"]   # first read 5 min after entry
    assert list(J.sold) == [True, True]                           # exits in (10:05,10:10] and (10:10,10:15]


def test_short_hold_is_invisible_at_5min_but_seen_at_1min():
    # The reason for the 2026-10-29 amendment: a 4-minute hold has no 5-minute interval.
    df = pd.DataFrame(order("a", "10:00:00", ["10:04:00"]))
    assert d.intervals(df, lambda g, t: "red", step=5).empty
    J1 = d.intervals(df, lambda g, t: "red", step=1)
    assert len(J1) == 3 and list(J1.sold) == [False, False, True]


def test_option_state_uses_completed_bars_and_2pct_band():
    idx = pd.date_range("2026-10-07 10:00", periods=4, freq="1min")
    P = {("2026-10-07", "2026-10-07|call|580"): (pd.Series([1.00, 1.01, 1.03, 0.97], index=idx), 1)}
    g = pd.DataFrame(order("a", "10:00:00", ["10:30:00"]))
    st = d.opt_state_fn(P)
    assert st(g, pd.Timestamp("2026-10-07 10:00:30")) is None             # no bar finished yet
    assert st(g, pd.Timestamp("2026-10-07 10:02")) == "flat"              # 10:01 bar: +1% inside band
    assert st(g, pd.Timestamp("2026-10-07 10:03")) == "green"             # 10:02 bar: +3%
    assert st(g, pd.Timestamp("2026-10-07 10:04")) == "red"               # 10:03 bar: −3%


def test_score_is_green_sell_rate_minus_red_sell_rate():
    x = pd.DataFrame(dict(state=["green"] * 4 + ["red"] * 4, sold=[1, 1, 0, 0, 1, 0, 0, 0]))
    assert d.score(x).D == pytest.approx(0.5 - 0.25)


def test_eligibility_never_needs_outcomes():
    # status() must not be able to peek: eligible_days works with P/L and cost columns removed.
    rows = order("a", "09:35:00", ["10:20:00"]) + order("b", "10:40:00", ["10:45:00"])
    df = pd.DataFrame(rows).drop(columns=["pl", "cost"])
    states = iter(["green", "red"] * 50)
    J = d.intervals(df, lambda g, t: next(states), step=1)
    assert d.eligible_days(df, J) == ["2026-10-07"]


@pytest.mark.parametrize("n,today,due", [
    (25, "2026-11-30", False),   # enough days, too early
    (19, "2026-12-10", False),   # date ok, too few days
    (20, "2026-12-04", True),
    (3, "2027-01-29", True),     # final date regardless (then reported inconclusive if < 15)
])
def test_due_rule(n, today, due):
    assert d.is_due(n, today) is due


def test_oos_refuses_before_due(monkeypatch):
    rows = order("a", "09:35:00", ["10:20:00"]) + order("b", "10:40:00", ["10:45:00"])
    df = pd.DataFrame(rows)
    J = d.intervals(df, lambda g, t: "green", step=1)
    monkeypatch.setattr(d, "oos_sample", lambda root=None: (df, dict(primary=J, opt5=J, spy5=J)))
    monkeypatch.setattr(d, "test1_table", lambda *a, **k: pytest.fail("computed outcomes before due"))
    assert d.oos(today="2026-11-01") is None


def test_perm_p_is_deterministic():
    a, b = np.arange(12), np.arange(12)[::-1]
    assert d.perm_p(a, b, n=500) == d.perm_p(a, b, n=500)
    rho, p = d.perm_p(a, b, n=500)
    assert rho == pytest.approx(-1.0) and p < 0.01
