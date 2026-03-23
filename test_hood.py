"""Tests for hood.py pure logic — no network calls."""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import hood

ET = ZoneInfo("America/New_York")


# ──────────────────────────────────────────────
# TOKEN VALIDATION
# ──────────────────────────────────────────────
class TestValidateToken:
    def test_bare_token(self):
        assert hood.validate_token("abc123") == "Bearer abc123"

    def test_bearer_prefix_preserved(self):
        assert hood.validate_token("Bearer abc123") == "Bearer abc123"

    def test_bearer_case_insensitive(self):
        assert hood.validate_token("bearer abc123") == "Bearer abc123"

    def test_whitespace_stripped(self):
        assert hood.validate_token("  Bearer abc123  ") == "Bearer abc123"

    def test_empty_token_exits(self):
        with pytest.raises(SystemExit):
            hood.validate_token("")

    def test_bearer_only_exits(self):
        with pytest.raises(SystemExit):
            hood.validate_token("Bearer ")


# ──────────────────────────────────────────────
# ORDER CLASSIFICATION
# ──────────────────────────────────────────────
class TestClassifyOrders:
    def test_basic_states(self):
        orders = [
            {"state": "filled"},
            {"state": "filled"},
            {"state": "cancelled"},
            {"state": "rejected"},
            {"state": "failed"},
        ]
        result = hood.classify_orders(orders)
        assert len(result["filled"]) == 2
        assert len(result["cancelled"]) == 1
        assert len(result["rejected"]) == 1
        assert len(result["failed"]) == 1

    def test_confirmed_treated_as_filled(self):
        result = hood.classify_orders([{"state": "confirmed"}])
        assert len(result["filled"]) == 1

    def test_unknown_goes_to_other(self):
        result = hood.classify_orders([{"state": "pending"}])
        assert len(result["other"]) == 1

    def test_empty_orders(self):
        assert hood.classify_orders([]) == {}


# ──────────────────────────────────────────────
# FIFO PAIRING
# ──────────────────────────────────────────────
def _make_exec(position_effect, side, qty, price, ts_offset_min=0,
               option_url="https://api.robinhood.com/options/instruments/AAA/",
               chain_symbol="SPY", option_type="call", strike=580.0, expiry="2026-03-20"):
    """Helper to build a fake execution dict."""
    base = datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc)
    return {
        "order_id": "order1",
        "dt": base + timedelta(minutes=ts_offset_min),
        "position_effect": position_effect,
        "side": side,
        "quantity": qty,
        "price_per_share": price,
        "option_type": option_type,
        "strike_price": strike,
        "expiration_date": expiry,
        "chain_symbol": chain_symbol,
        "option_url": option_url,
        "account_number": "12345",
    }


class TestPairIntoTradeRows:
    def test_simple_open_close(self):
        execs = [
            _make_exec("open", "buy", 1, 2.00, ts_offset_min=0),
            _make_exec("close", "sell", 1, 3.00, ts_offset_min=5),
        ]
        rows, unmatched = hood.pair_into_trade_rows(execs)
        assert len(rows) == 1
        assert len(unmatched) == 0
        r = rows[0]
        assert r["quantity"] == 1
        assert r["entry_cost"] == 200.0  # 2.00 * 1 * 100
        assert r["exit_credit"] == 300.0  # 3.00 * 1 * 100
        assert r["pl"] == 100.0  # buy side: exit - entry
        assert r["hold_min"] == 5

    def test_partial_close(self):
        """Open 3, close 2 then 1 → 2 rows."""
        execs = [
            _make_exec("open", "buy", 3, 1.50, ts_offset_min=0),
            _make_exec("close", "sell", 2, 2.00, ts_offset_min=10),
            _make_exec("close", "sell", 1, 2.50, ts_offset_min=20),
        ]
        rows, unmatched = hood.pair_into_trade_rows(execs)
        assert len(rows) == 2
        assert len(unmatched) == 0
        # Same group ID for both (same entry)
        assert rows[0]["group_id"] == rows[1]["group_id"]
        assert rows[0]["quantity"] == 2
        assert rows[1]["quantity"] == 1

    def test_unmatched_open(self):
        """Open with no close → unmatched."""
        execs = [
            _make_exec("open", "buy", 2, 1.00, ts_offset_min=0),
        ]
        rows, unmatched = hood.pair_into_trade_rows(execs)
        assert len(rows) == 0
        assert len(unmatched) == 1
        assert unmatched[0]["unmatched_qty"] == 2

    def test_sell_to_open(self):
        """Sell-to-open (credit spread entry): P/L is entry - exit."""
        execs = [
            _make_exec("open", "sell", 1, 3.00, ts_offset_min=0),
            _make_exec("close", "buy", 1, 1.00, ts_offset_min=5),
        ]
        rows, unmatched = hood.pair_into_trade_rows(execs)
        assert len(rows) == 1
        r = rows[0]
        assert r["pl"] == 200.0  # sell side: entry_cost(300) - exit_credit(100)

    def test_multiple_contracts(self):
        """Two different contracts paired independently."""
        execs = [
            _make_exec("open", "buy", 1, 1.00, ts_offset_min=0,
                        option_url="https://api.robinhood.com/options/instruments/AAA/"),
            _make_exec("open", "buy", 1, 2.00, ts_offset_min=1,
                        option_url="https://api.robinhood.com/options/instruments/BBB/"),
            _make_exec("close", "sell", 1, 1.50, ts_offset_min=5,
                        option_url="https://api.robinhood.com/options/instruments/AAA/"),
            _make_exec("close", "sell", 1, 2.50, ts_offset_min=6,
                        option_url="https://api.robinhood.com/options/instruments/BBB/"),
        ]
        rows, unmatched = hood.pair_into_trade_rows(execs)
        assert len(rows) == 2
        assert len(unmatched) == 0

    def test_dte_calculation(self):
        """DTE = expiry - trade_date."""
        execs = [
            _make_exec("open", "buy", 1, 1.00, ts_offset_min=0, expiry="2026-03-20"),
            _make_exec("close", "sell", 1, 1.50, ts_offset_min=5, expiry="2026-03-20"),
        ]
        rows, _ = hood.pair_into_trade_rows(execs)
        assert rows[0]["dte"] == 0  # same day

    def test_pl_percentage(self):
        execs = [
            _make_exec("open", "buy", 1, 2.00, ts_offset_min=0),
            _make_exec("close", "sell", 1, 3.00, ts_offset_min=5),
        ]
        rows, _ = hood.pair_into_trade_rows(execs)
        # P/L% = (100 / 200) * 100 = 50%
        assert rows[0]["pl_pct"] == 50.0

    def test_empty_input(self):
        rows, unmatched = hood.pair_into_trade_rows([])
        assert rows == []
        assert unmatched == []


# ──────────────────────────────────────────────
# VWAP / EMA
# ──────────────────────────────────────────────
def _make_bars(n=10, date="2026-03-20"):
    """Generate n 5-min bars starting at 9:30 ET (13:30 UTC)."""
    bars = []
    for i in range(n):
        ts = datetime(2026, 3, 20, 13, 30 + i * 5, tzinfo=timezone.utc)
        price = 580.0 + i * 0.5
        bars.append({
            "begins_at": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "close": price,
            "high": price + 0.2,
            "low": price - 0.2,
            "volume": 1000,
        })
    return bars


class TestComputeVwap:
    def test_basic_vwap(self):
        bars = _make_bars(3)
        # up_to after all 3 bars
        up_to = datetime(2026, 3, 20, 13, 50, tzinfo=timezone.utc)
        result = hood.compute_vwap(bars, up_to)
        assert result is not None
        assert isinstance(result, float)

    def test_vwap_partial_day(self):
        bars = _make_bars(5)
        # Only include first 2 bars
        up_to = datetime(2026, 3, 20, 13, 39, tzinfo=timezone.utc)
        result = hood.compute_vwap(bars, up_to)
        assert result is not None

    def test_vwap_wrong_date(self):
        bars = _make_bars(3)
        up_to = datetime(2026, 3, 21, 14, 0, tzinfo=timezone.utc)
        assert hood.compute_vwap(bars, up_to) is None

    def test_vwap_empty_bars(self):
        assert hood.compute_vwap([], datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc)) is None


class TestComputeEma:
    def test_basic_ema(self):
        bars = _make_bars(10)
        up_to = datetime(2026, 3, 20, 14, 30, tzinfo=timezone.utc)
        result = hood.compute_ema(bars, up_to, period=8)
        assert result is not None

    def test_insufficient_bars(self):
        bars = _make_bars(3)
        up_to = datetime(2026, 3, 20, 14, 30, tzinfo=timezone.utc)
        assert hood.compute_ema(bars, up_to, period=8) is None

    def test_ema_empty_bars(self):
        assert hood.compute_ema([], datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc)) is None


# ──────────────────────────────────────────────
# TIME / DATE FORMATTING
# ──────────────────────────────────────────────
class TestFormatting:
    def test_to_eastern(self):
        utc_dt = datetime(2026, 3, 20, 18, 30, tzinfo=timezone.utc)
        et = hood.to_eastern(utc_dt)
        assert et.tzinfo == ET
        assert et.hour == 14  # 18:30 UTC = 14:30 ET (EDT)

    def test_to_eastern_none(self):
        assert hood.to_eastern(None) is None

    def test_fmt_time_excel(self):
        dt = datetime(2026, 3, 20, 18, 30, 45, tzinfo=timezone.utc)
        result = hood.fmt_time(dt, "excel")
        assert result == "14:30:45"

    def test_fmt_time_ampm(self):
        dt = datetime(2026, 3, 20, 18, 30, 45, tzinfo=timezone.utc)
        result = hood.fmt_time(dt, "ampm")
        assert "2:30:45 PM" in result

    def test_fmt_time_none(self):
        assert hood.fmt_time(None) == ""

    def test_fmt_date_date_obj(self):
        from datetime import date
        assert hood.fmt_date(date(2026, 3, 5)) == "3/5/2026"

    def test_fmt_date_string(self):
        assert hood.fmt_date("2026-03-05") == "3/5/2026"

    def test_fmt_date_none(self):
        assert hood.fmt_date(None) == ""


# ──────────────────────────────────────────────
# BUILD TRADE DATAFRAME
# ──────────────────────────────────────────────
class TestBuildTradeDf:
    def _make_paired_rows(self):
        base = datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc)
        return [{
            "entry_dt": base,
            "exit_dt": base + timedelta(minutes=5),
            "trade_date": base.date(),
            "expiry_date": "2026-03-20",
            "option_type": "call",
            "quantity": 2,
            "entry_cost": 400.0,
            "exit_credit": 600.0,
            "pl": 200.0,
            "pl_pct": 50.0,
            "hold_min": 5,
            "strike_price": 580.0,
            "chain_symbol": "SPY",
            "group_id": "G1",
            "dte": 0,
            "account_number": "12345",
            "vwap": 581.50,
            "ema8": 581.20,
            "delta": 0.45,
        }]

    def test_basic_output_columns(self):
        rows = self._make_paired_rows()
        df = hood.build_trade_df(rows, {})
        expected_cols = [
            "Trade #", "Date", "Day", "Symbol", "Type", "Strike", "Qty",
            "Entry Time", "Exit Time", "Hold Time (min)", "Entry Cost",
            "Exit Credit", "P/L ($)", "Win/Loss", "Group ID", "DTE",
        ]
        for col in expected_cols:
            assert col in df.columns, f"Missing column: {col}"

    def test_trade_numbering(self):
        rows = self._make_paired_rows() * 3
        df = hood.build_trade_df(rows, {})
        assert list(df["Trade #"]) == [1, 2, 3]

    def test_entry_cost_negative(self):
        """Entry Cost should be negative (money out)."""
        rows = self._make_paired_rows()
        df = hood.build_trade_df(rows, {})
        assert df.iloc[0]["Entry Cost"] == -400

    def test_win_loss_label(self):
        rows = self._make_paired_rows()
        df = hood.build_trade_df(rows, {})
        assert df.iloc[0]["Win/Loss"] == "WIN"
        assert df.iloc[0]["Is Win"] == 1

    def test_cumulative_pl(self):
        rows = self._make_paired_rows() * 3
        df = hood.build_trade_df(rows, {})
        assert list(df["Cumulative P/L ($)"]) == [200, 400, 600]

    def test_date_filter_start(self):
        rows = self._make_paired_rows()
        df = hood.build_trade_df(rows, {}, start_date="2026-03-21")
        assert len(df) == 0

    def test_date_filter_end(self):
        rows = self._make_paired_rows()
        df = hood.build_trade_df(rows, {}, end_date="2026-03-19")
        assert len(df) == 0

    def test_market_data_enrichment(self):
        rows = self._make_paired_rows()
        market = {
            ("SPY", "2026-03-20"): {
                "Asset Open": 579.0,
                "Asset High": 583.0,
                "Asset Low": 578.0,
                "Asset Close": 582.0,
            },
            ("^VIX", "2026-03-20"): 18.5,
        }
        df = hood.build_trade_df(rows, market)
        assert df.iloc[0]["Asset Open"] == 579.0
        assert df.iloc[0]["VIX"] == 18.5

    def test_empty_rows(self):
        df = hood.build_trade_df([], {})
        assert len(df) == 0


# ──────────────────────────────────────────────
# MAKE HEADERS
# ──────────────────────────────────────────────
class TestMakeHeaders:
    def test_authorization_header(self):
        h = hood.make_headers("Bearer abc")
        assert h["Authorization"] == "Bearer abc"
        assert h["Accept"] == "application/json"
