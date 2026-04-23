"""Tests for cash_flow.py — transfer categorization, summary math, JSONL output."""

import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import cash_flow


# ──────────────────────────────────────────────
# FIXTURES
# ──────────────────────────────────────────────

@pytest.fixture
def tmp_env(tmp_path):
    """Patch SCRIPT_DIR and TOKEN_FILE to temp dir with a valid token."""
    token_file = tmp_path / ".rh_token"
    token_file.write_text("Bearer test-token")
    accts_file = tmp_path / ".rh_accounts.json"
    accts_file.write_text(json.dumps({"account_numbers": ["12345"]}))
    with patch.object(cash_flow, "SCRIPT_DIR", tmp_path), \
         patch.object(cash_flow, "TOKEN_FILE", token_file):
        yield tmp_path


def mock_response(status_code=200, json_data=None):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = json_data or {}
    return r


# ──────────────────────────────────────────────
# load_token
# ──────────────────────────────────────────────

class TestLoadToken:
    def test_loads_bearer_token(self, tmp_path):
        f = tmp_path / ".rh_token"
        f.write_text("Bearer abc123")
        with patch.object(cash_flow, "TOKEN_FILE", f):
            assert cash_flow.load_token() == "Bearer abc123"

    def test_adds_bearer_prefix(self, tmp_path):
        f = tmp_path / ".rh_token"
        f.write_text("abc123")
        with patch.object(cash_flow, "TOKEN_FILE", f):
            assert cash_flow.load_token() == "Bearer abc123"

    def test_strips_whitespace(self, tmp_path):
        f = tmp_path / ".rh_token"
        f.write_text("  Bearer abc123  \n")
        with patch.object(cash_flow, "TOKEN_FILE", f):
            assert cash_flow.load_token() == "Bearer abc123"

    def test_missing_file_exits(self, tmp_path):
        with patch.object(cash_flow, "TOKEN_FILE", tmp_path / "nope"):
            with pytest.raises(SystemExit):
                cash_flow.load_token()


# ──────────────────────────────────────────────
# headers
# ──────────────────────────────────────────────

class TestHeaders:
    def test_returns_auth_header(self):
        h = cash_flow.headers("Bearer xyz")
        assert h["Authorization"] == "Bearer xyz"
        assert "Accept" in h


# ──────────────────────────────────────────────
# paginate
# ──────────────────────────────────────────────

class TestPaginate:
    def test_single_page(self):
        with patch("cash_flow.requests.get") as mock_get:
            mock_get.return_value = mock_response(200, {"results": [{"a": 1}], "next": None})
            result = cash_flow.paginate("http://example.com", {})
            assert result == [{"a": 1}]

    def test_multiple_pages(self):
        page1 = mock_response(200, {"results": [{"a": 1}], "next": "http://example.com?p=2"})
        page2 = mock_response(200, {"results": [{"b": 2}], "next": None})
        with patch("cash_flow.requests.get", side_effect=[page1, page2]):
            result = cash_flow.paginate("http://example.com", {})
            assert result == [{"a": 1}, {"b": 2}]

    def test_stops_on_error(self):
        with patch("cash_flow.requests.get") as mock_get:
            mock_get.return_value = mock_response(401, {})
            result = cash_flow.paginate("http://example.com", {})
            assert result == []


# ──────────────────────────────────────────────
# main() — full integration with mocked HTTP
# ──────────────────────────────────────────────

def build_mock_get(transfers=None, fees=None, divs=None, refs=None,
                   equity=10000.0, cash=5000.0):
    """Build a side_effect function for requests.get that routes by URL."""
    transfers = transfers or []
    fees = fees or []
    divs = divs or []
    refs = refs or []

    def mock_get(url, headers=None):
        if "/user/" in url:
            return mock_response(200, {"username": "test"})
        if "unified_transfers" in url:
            return mock_response(200, {"results": transfers, "next": None})
        if "subscription_fees" in url:
            return mock_response(200, {"results": fees, "next": None})
        if "/dividends/" in url:
            return mock_response(200, {"results": divs, "next": None})
        if "/referral/" in url:
            return mock_response(200, {"results": refs, "next": None})
        if "/accounts/12345/" in url:
            return mock_response(200, {
                "type": "individual", "portfolio_cash": str(cash),
            })
        if "/portfolios/12345/" in url:
            return mock_response(200, {"equity": str(equity), "extended_hours_equity": str(equity)})
        # Fallback
        return mock_response(200, {"results": [], "next": None})

    return mock_get


class TestMainJsonMode:
    """Test main(as_json=True) which suppresses output and writes JSONL."""

    def test_basic_snapshot(self, tmp_env):
        transfers = [
            {"amount": "1000", "state": "completed", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-01"},
        ]
        with patch("cash_flow.requests.get", side_effect=build_mock_get(
            transfers=transfers, equity=1100.0
        )):
            cash_flow.main(as_json=True)

        jsonl = tmp_env / "outputs" / "cash_flow.jsonl"
        assert jsonl.exists()
        entry = json.loads(jsonl.read_text().strip())
        assert entry["deposits"] == 1000.0
        assert entry["withdrawals"] == 0.0
        assert entry["net_deposited"] == 1000.0
        assert entry["current_equity"] == 1100.0
        assert entry["all_time_pnl"] == 100.0

    def test_deposits_and_withdrawals(self, tmp_env):
        transfers = [
            {"amount": "5000", "state": "completed", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-01"},
            {"amount": "1000", "state": "completed", "direction": "push",
             "transfer_type": "ach", "created_at": "2026-02-01"},
        ]
        with patch("cash_flow.requests.get", side_effect=build_mock_get(
            transfers=transfers, equity=4500.0
        )):
            cash_flow.main(as_json=True)

        entry = json.loads((tmp_env / "outputs" / "cash_flow.jsonl").read_text().strip())
        assert entry["deposits"] == 5000.0
        assert entry["withdrawals"] == 1000.0
        assert entry["net_deposited"] == 4000.0
        # total_return = equity + withdrawals - deposits = 4500 + 1000 - 5000 = 500
        assert entry["total_return"] == 500.0

    def test_internal_transfers_excluded(self, tmp_env):
        transfers = [
            {"amount": "2000", "state": "completed", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-01"},
            {"amount": "500", "state": "completed", "direction": "pull",
             "transfer_type": "internal", "created_at": "2026-01-02"},
        ]
        with patch("cash_flow.requests.get", side_effect=build_mock_get(
            transfers=transfers, equity=2100.0
        )):
            cash_flow.main(as_json=True)

        entry = json.loads((tmp_env / "outputs" / "cash_flow.jsonl").read_text().strip())
        # Internal $500 should NOT count as a deposit
        assert entry["deposits"] == 2000.0
        assert entry["net_deposited"] == 2000.0

    def test_failed_transfers_excluded(self, tmp_env):
        transfers = [
            {"amount": "1000", "state": "completed", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-01"},
            {"amount": "9999", "state": "failed", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-02"},
        ]
        with patch("cash_flow.requests.get", side_effect=build_mock_get(
            transfers=transfers, equity=1000.0
        )):
            cash_flow.main(as_json=True)

        entry = json.loads((tmp_env / "outputs" / "cash_flow.jsonl").read_text().strip())
        assert entry["deposits"] == 1000.0

    def test_pending_transfers_tracked_separately(self, tmp_env):
        transfers = [
            {"amount": "1000", "state": "completed", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-01"},
            {"amount": "500", "state": "pending", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-05"},
        ]
        with patch("cash_flow.requests.get", side_effect=build_mock_get(
            transfers=transfers, equity=1000.0
        )):
            cash_flow.main(as_json=True)

        entry = json.loads((tmp_env / "outputs" / "cash_flow.jsonl").read_text().strip())
        # Only completed deposits counted
        assert entry["deposits"] == 1000.0

    def test_gold_fees_deducted(self, tmp_env):
        transfers = [
            {"amount": "1000", "state": "completed", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-01"},
        ]
        fees = [
            {"amount": "5.00", "date": "2026-01-15", "state": "charged"},
            {"amount": "5.00", "date": "2026-02-15", "state": "charged"},
        ]
        with patch("cash_flow.requests.get", side_effect=build_mock_get(
            transfers=transfers, fees=fees, equity=1000.0
        )):
            cash_flow.main(as_json=True)

        entry = json.loads((tmp_env / "outputs" / "cash_flow.jsonl").read_text().strip())
        assert entry["gold_fees"] == 10.0
        # cost_basis = net_deposited - gold + divs + referrals = 1000 - 10 + 0 + 0 = 990
        # pnl = 1000 - 990 = 10
        assert entry["net_cash_basis"] == 990.0
        assert entry["all_time_pnl"] == 10.0

    def test_dividends_counted(self, tmp_env):
        transfers = [
            {"amount": "1000", "state": "completed", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-01"},
        ]
        divs = [
            {"amount": "3.50", "payable_date": "2026-02-01", "state": "paid"},
            {"amount": "100.00", "payable_date": "2026-02-01", "state": "voided"},
        ]
        with patch("cash_flow.requests.get", side_effect=build_mock_get(
            transfers=transfers, divs=divs, equity=1000.0
        )):
            cash_flow.main(as_json=True)

        entry = json.loads((tmp_env / "outputs" / "cash_flow.jsonl").read_text().strip())
        # Voided dividend should NOT be counted
        assert entry["dividends"] == 3.5

    def test_referral_grants_counted(self, tmp_env):
        transfers = [
            {"amount": "1000", "state": "completed", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-01"},
        ]
        refs = [
            {"created_at": "2026-01-10", "direction": "to", "state": "received",
             "reward": {"stocks": [{"symbol": "AAPL", "cost_basis": "12.50", "state": "granted"}], "cash": None}},
            {"created_at": "2026-01-11", "direction": "to", "state": "received",
             "reward": {"stocks": [{"symbol": "BAD", "cost_basis": "99.00", "state": "voided"}], "cash": None}},
            {"created_at": "2026-01-12", "direction": "to", "state": "received",
             "reward": {"stocks": [], "cash": {"amount": "5.00", "state": "paid"}}},
        ]
        with patch("cash_flow.requests.get", side_effect=build_mock_get(
            transfers=transfers, refs=refs, equity=1000.0
        )):
            cash_flow.main(as_json=True)

        entry = json.loads((tmp_env / "outputs" / "cash_flow.jsonl").read_text().strip())
        # $12.50 stock + $5.00 cash, voided excluded
        assert entry["referral_grants"] == 17.5

    def test_jsonl_appends(self, tmp_env):
        """Running main twice should produce 2 lines."""
        transfers = [
            {"amount": "1000", "state": "completed", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-01"},
        ]
        mock = build_mock_get(transfers=transfers, equity=1000.0)
        with patch("cash_flow.requests.get", side_effect=mock):
            cash_flow.main(as_json=True)
        with patch("cash_flow.requests.get", side_effect=mock):
            cash_flow.main(as_json=True)

        lines = (tmp_env / "outputs" / "cash_flow.jsonl").read_text().strip().split("\n")
        assert len(lines) == 2
        # Both should be valid JSON
        for line in lines:
            json.loads(line)

    def test_expired_token_exits(self, tmp_env):
        with patch("cash_flow.requests.get", return_value=mock_response(401)):
            with pytest.raises(SystemExit):
                cash_flow.main(as_json=True)


# ──────────────────────────────────────────────
# Summary math verification
# ──────────────────────────────────────────────

class TestSummaryMath:
    """Verify the P/L and return calculations."""

    def test_pnl_formula(self, tmp_env):
        """pnl = equity - (net_deposited - gold + divs + referrals)"""
        transfers = [
            {"amount": "5000", "state": "completed", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-01"},
            {"amount": "500", "state": "completed", "direction": "push",
             "transfer_type": "ach", "created_at": "2026-03-01"},
        ]
        fees = [{"amount": "10.00", "date": "2026-02-01", "state": "charged"}]
        divs = [{"amount": "25.00", "payable_date": "2026-02-15", "state": "paid"}]
        refs = [{"created_at": "2026-01-10", "direction": "to", "state": "received",
                 "reward": {"stocks": [{"symbol": "X", "cost_basis": "15.00", "state": "granted"}], "cash": None}}]

        with patch("cash_flow.requests.get", side_effect=build_mock_get(
            transfers=transfers, fees=fees, divs=divs, refs=refs, equity=5000.0
        )):
            cash_flow.main(as_json=True)

        entry = json.loads((tmp_env / "outputs" / "cash_flow.jsonl").read_text().strip())
        # net_deposited = 5000 - 500 = 4500
        assert entry["net_deposited"] == 4500.0
        # cost_basis = 4500 - 10 + 25 + 15 = 4530
        assert entry["net_cash_basis"] == 4530.0
        # pnl = 5000 - 4530 = 470
        assert entry["all_time_pnl"] == 470.0
        # pnl_pct = 470 / 5000 * 100 = 9.4
        assert entry["all_time_pnl_pct"] == 9.4
        # total_return = 5000 + 500 - 5000 = 500
        assert entry["total_return"] == 500.0
        # total_return_pct = 500 / 5000 * 100 = 10.0
        assert entry["total_return_pct"] == 10.0

    def test_negative_pnl(self, tmp_env):
        transfers = [
            {"amount": "1000", "state": "completed", "direction": "pull",
             "transfer_type": "ach", "created_at": "2026-01-01"},
        ]
        with patch("cash_flow.requests.get", side_effect=build_mock_get(
            transfers=transfers, equity=800.0
        )):
            cash_flow.main(as_json=True)

        entry = json.loads((tmp_env / "outputs" / "cash_flow.jsonl").read_text().strip())
        assert entry["all_time_pnl"] == -200.0
        assert entry["all_time_pnl_pct"] == -20.0
