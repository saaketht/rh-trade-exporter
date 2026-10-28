"""Tests for backfill_history.py — extending a CSV backwards from a raw rebuild."""

from datetime import datetime, timedelta, timezone

import pandas as pd

import backfill_history as bf
import hood


def paired(day, hour=14, strike=580.0, pl=100.0, symbol="SPY"):
    """A row as pair_into_trade_rows emits it (14:00 UTC = 10:00 ET in EDT)."""
    entry = datetime(2026, 3, day, hour, 0, tzinfo=timezone.utc)
    return {"entry_dt": entry, "exit_dt": entry + timedelta(minutes=5), "trade_date": entry.date(),
            "expiry_date": f"2026-03-{day:02d}", "option_type": "call", "quantity": 1,
            "entry_cost": 200.0, "exit_credit": 200.0 + pl, "pl": pl, "pl_pct": pl / 2,
            "fees": 0.08, "hold_min": 5, "strike_price": strike, "chain_symbol": symbol,
            "group_id": hood.make_group_id(entry, strike, "call"), "dte": 0, "account_number": "1"}


def csv_rows(rows, vwap="Above"):
    df = hood.build_trade_df(rows, {})
    df["VWAP"], df["Asset Open"] = vwap, 580.5
    return df


def test_adds_only_rows_before_target_and_carries_sticky():
    target = csv_rows([paired(20)])                       # target starts 3/20
    sticky = csv_rows([paired(10), paired(20)], vwap="Below")
    combined, rep = bf.backfill(target, [paired(10), paired(20)], sticky)
    assert rep["ok"] and rep["rows"] == 1 and rep["misses"] == 0
    assert list(pd.to_datetime(combined["Date"]).dt.day) == [10, 20]
    assert combined.iloc[0]["VWAP"] == "Below"            # carried from the sticky source
    assert combined.iloc[1]["VWAP"] == "Above"            # target row untouched
    assert combined.iloc[0]["Fees ($)"] == 0.08           # rebuilt rows bring fees
    assert list(combined["Trade #"]) == [1, 2]
    assert combined.iloc[-1]["Cumulative P/L ($)"] == 200


def test_position_only_in_sticky_source_blocks_write():
    target = csv_rows([paired(20)])
    sticky = csv_rows([paired(10), paired(11, strike=585.0)])
    _, rep = bf.backfill(target, [paired(10)], sticky)
    assert not rep["ok"] and len(rep["missing_from_rebuilt"]) == 1


def test_rebuilt_row_without_sticky_match_blocks_write():
    target = csv_rows([paired(20)])
    _, rep = bf.backfill(target, [paired(10), paired(11)], csv_rows([paired(10)]))
    assert not rep["ok"] and rep["misses"] == 1


def test_other_symbols_stay_out_of_spy_file():
    target = csv_rows([paired(20)])
    sticky = csv_rows([paired(10)])
    combined, rep = bf.backfill(target, [paired(10), paired(10, hour=15, symbol="QQQ")], sticky)
    assert rep["ok"] and set(combined["Symbol"]) == {"SPY"}
