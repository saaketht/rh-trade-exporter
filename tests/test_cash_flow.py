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

    def test_non_originated_ach_credit_is_deposit(self, tmp_env):
        """IRS tax refund: external→rhs_account, direction=push, must count as deposit."""
        transfers = [
            {"amount": "1000", "state": "completed", "direction": "pull",
             "transfer_type": "originated_ach",
             "originating_account_type": "rhs_account",
             "receiving_account_type": "ach_relationship",
             "created_at": "2026-01-01"},
            {"amount": "431", "state": "completed", "direction": "push",
             "transfer_type": "non_originated_ach",
             "originating_account_type": "external",
             "receiving_account_type": "rhs_account",
             "details": {"originator_name": "IRS  Treas 310", "description": "TAX REF"},
             "created_at": "2026-05-01"},
        ]
        with patch("cash_flow.requests.get", side_effect=build_mock_get(
            transfers=transfers, equity=1431.0
        )):
            cash_flow.main(as_json=True)

        entry = json.loads((tmp_env / "outputs" / "cash_flow.jsonl").read_text().strip())
        assert entry["deposits"] == 1431.0
        assert entry["withdrawals"] == 0.0
        assert entry["net_deposited"] == 1431.0

    def test_account_type_pair_overrides_direction(self, tmp_env):
        """rhs→ach with direction=push is a withdrawal (the standard case)."""
        transfers = [
            {"amount": "200", "state": "completed", "direction": "push",
             "transfer_type": "originated_ach",
             "originating_account_type": "rhs_account",
             "receiving_account_type": "ach_relationship",
             "created_at": "2026-03-01"},
        ]
        with patch("cash_flow.requests.get", side_effect=build_mock_get(
            transfers=transfers, equity=0.0
        )):
            cash_flow.main(as_json=True)

        entry = json.loads((tmp_env / "outputs" / "cash_flow.jsonl").read_text().strip())
        assert entry["deposits"] == 0.0
        assert entry["withdrawals"] == 200.0

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


# ──────────────────────────────────────────────
# Backfill helpers
# ──────────────────────────────────────────────

def _perf_payload(points):
    """Build a /portfolio/performance/ response with given (label, dollar_amount) points."""
    return {
        "lines": [{
            "segments": [{
                "points": [
                    {
                        "x": i / max(len(points) - 1, 1),
                        "y": 0.0,
                        "cursor_data": {
                            "label": {"value": label},
                            "price_chart_data": {
                                "dollar_value": {"amount": str(amt)},
                            },
                        },
                    }
                    for i, (label, amt) in enumerate(points)
                ],
            }],
        }],
    }


class TestFetchPortfolioPerformance:
    def test_parses_label_and_dollar(self):
        payload = _perf_payload([("Jan 5, 2024", "100.50"), ("Jul 1, 2024", "200.75")])
        with patch("cash_flow.requests.get", return_value=mock_response(200, payload)):
            out = cash_flow.fetch_portfolio_performance("acct1", {})
        assert out == [("2024-01-05", 100.50), ("2024-07-01", 200.75)]

    def test_skips_malformed(self):
        payload = {
            "lines": [{
                "segments": [{
                    "points": [
                        {"cursor_data": {"label": {"value": "Bad date"},
                                         "price_chart_data": {"dollar_value": {"amount": "1.0"}}}},
                        {"cursor_data": {"label": None,
                                         "price_chart_data": {"dollar_value": {"amount": "1.0"}}}},
                        {"cursor_data": {"label": {"value": "Mar 1, 2024"},
                                         "price_chart_data": {"dollar_value": {"amount": "50.0"}}}},
                    ],
                }],
            }],
        }
        with patch("cash_flow.requests.get", return_value=mock_response(200, payload)):
            out = cash_flow.fetch_portfolio_performance("acct1", {})
        assert out == [("2024-03-01", 50.0)]

    def test_returns_empty_on_http_error(self):
        with patch("cash_flow.requests.get", return_value=mock_response(404, {})):
            assert cash_flow.fetch_portfolio_performance("acct1", {}) == []


class TestTotalEquityByDate:
    def test_carry_forward_across_accounts(self):
        per = {
            "A": [("2024-01-01", 100.0), ("2024-01-15", 150.0)],
            "B": [("2024-01-10", 50.0), ("2024-01-20", 70.0)],
        }
        out = cash_flow.total_equity_by_date(per)
        # Union of dates: 1, 10, 15, 20
        assert out["2024-01-01"] == 100.0           # A=100, B=0 (no data yet)
        assert out["2024-01-10"] == 150.0           # A=100, B=50
        assert out["2024-01-15"] == 200.0           # A=150, B=50
        assert out["2024-01-20"] == 220.0           # A=150, B=70

    def test_empty_input(self):
        assert cash_flow.total_equity_by_date({}) == {}


class TestCollectDatedCashflows:
    def test_classifies_irs_refund_as_deposit(self):
        transfers = [
            {"amount": "431", "state": "completed", "direction": "push",
             "transfer_type": "non_originated_ach",
             "originating_account_type": "external",
             "receiving_account_type": "rhs_account",
             "created_at": "2026-05-01T18:41:06-04:00"},
        ]
        d = cash_flow.collect_dated_cashflows(transfers, [], [], [])
        assert d["deposits"] == [("2026-05-01", 431.0)]
        assert d["withdrawals"] == []

    def test_excludes_internal_and_failed(self):
        transfers = [
            {"amount": "100", "state": "completed", "direction": "push",
             "transfer_type": "internal",
             "originating_account_type": "rhs_account",
             "receiving_account_type": "rhs_account",
             "created_at": "2026-01-01"},
            {"amount": "200", "state": "failed", "direction": "pull",
             "transfer_type": "originated_ach",
             "originating_account_type": "rhs_account",
             "receiving_account_type": "ach_relationship",
             "created_at": "2026-01-02"},
        ]
        d = cash_flow.collect_dated_cashflows(transfers, [], [], [])
        assert d["deposits"] == []
        assert d["withdrawals"] == []

    def test_voided_dividends_excluded(self):
        divs = [
            {"amount": "1.00", "payable_date": "2024-01-01", "state": "paid"},
            {"amount": "5.00", "payable_date": "2024-02-01", "state": "voided"},
        ]
        d = cash_flow.collect_dated_cashflows([], [], divs, [])
        assert d["dividends"] == [("2024-01-01", 1.0)]


class TestBuildHistoricalSnapshots:
    def test_running_totals_align_with_dates(self):
        equity_by_date = {
            "2024-01-01": 100.0,
            "2024-02-01": 150.0,
            "2024-03-01": 200.0,
        }
        dated = {
            "deposits":   [("2024-01-01", 50.0), ("2024-02-15", 100.0)],
            "withdrawals": [("2024-03-01", 25.0)],
            "gold":       [("2024-02-10", 5.0)],
            "dividends":  [],
            "referrals":  [],
        }
        snaps = cash_flow.build_historical_snapshots(equity_by_date, dated)
        assert len(snaps) == 3

        # 2024-01-01: only the $50 deposit counts
        assert snaps[0]["timestamp"].startswith("2024-01-01")
        assert snaps[0]["deposits"] == 50.0
        assert snaps[0]["gold_fees"] == 0.0
        assert snaps[0]["net_cash_basis"] == 50.0

        # 2024-02-01: still just the first deposit (Feb 15 deposit hasn't happened)
        assert snaps[1]["deposits"] == 50.0
        assert snaps[1]["gold_fees"] == 0.0

        # 2024-03-01: both deposits ($150), $25 withdrawal, $5 gold fee
        assert snaps[2]["deposits"] == 150.0
        assert snaps[2]["withdrawals"] == 25.0
        assert snaps[2]["gold_fees"] == 5.0
        assert snaps[2]["net_deposited"] == 125.0
        assert snaps[2]["net_cash_basis"] == 120.0  # 125 - 5 + 0 + 0
        assert snaps[2]["all_time_pnl"] == 80.0     # 200 - 120
        assert snaps[2]["synthetic"] is True

    def test_empty_equity_returns_empty(self):
        assert cash_flow.build_historical_snapshots({}, {"deposits": [], "withdrawals": [],
                                                          "gold": [], "dividends": [],
                                                          "referrals": []}) == []


class TestCmdBackfill:
    def test_writes_historical_jsonl(self, tmp_env):
        transfers = [
            {"amount": "1000", "state": "completed", "direction": "pull",
             "transfer_type": "originated_ach",
             "originating_account_type": "rhs_account",
             "receiving_account_type": "ach_relationship",
             "created_at": "2024-01-15"},
        ]
        perf = _perf_payload([
            ("Jan 1, 2024", "0"),
            ("Feb 1, 2024", "1100.00"),
        ])

        def mock_get(url, headers=None):
            if "/user/" in url:
                return mock_response(200, {"username": "test"})
            if "unified_transfers" in url:
                return mock_response(200, {"results": transfers, "next": None})
            if "subscription_fees" in url or "/dividends/" in url or "/referral/" in url:
                return mock_response(200, {"results": [], "next": None})
            if "/portfolio/performance/" in url:
                return mock_response(200, perf)
            return mock_response(200, {"results": [], "next": None})

        with patch("cash_flow.requests.get", side_effect=mock_get):
            cash_flow.cmd_backfill(as_json=True)

        out_file = tmp_env / "outputs" / "cash_flow_historical.jsonl"
        assert out_file.exists()
        lines = out_file.read_text().strip().split("\n")
        assert len(lines) == 2
        first, last = json.loads(lines[0]), json.loads(lines[1])
        assert first["timestamp"].startswith("2024-01-01")
        assert first["current_equity"] == 0.0
        assert first["deposits"] == 0.0
        assert first["synthetic"] is True
        assert last["timestamp"].startswith("2024-02-01")
        assert last["current_equity"] == 1100.0
        assert last["deposits"] == 1000.0
        assert last["all_time_pnl"] == 100.0  # 1100 - 1000

    def test_overwrites_existing_historical(self, tmp_env):
        # Pre-populate with stale data
        out_file = tmp_env / "outputs" / "cash_flow_historical.jsonl"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(json.dumps({"timestamp": "2099-01-01", "stale": True}) + "\n")

        perf = _perf_payload([("Mar 1, 2024", "500.00")])

        def mock_get(url, headers=None):
            if "/user/" in url:
                return mock_response(200, {})
            if "/portfolio/performance/" in url:
                return mock_response(200, perf)
            return mock_response(200, {"results": [], "next": None})

        with patch("cash_flow.requests.get", side_effect=mock_get):
            cash_flow.cmd_backfill(as_json=True)

        content = out_file.read_text()
        assert "stale" not in content
        assert "2024-03-01" in content
