"""Tests for server.py — API endpoints, auth, CSV parsing, notes."""

import csv
import json
import os
import pytest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import server
from server import app, _normalize_date, _convert, COLUMN_MAP


# ──────────────────────────────────────────────
# FIXTURES
# ──────────────────────────────────────────────

@pytest.fixture
def client():
    """TestClient with no auth (no .server_token)."""
    with patch.object(server, "TOKEN_FILE", Path("/nonexistent/.server_token")):
        yield TestClient(app)


@pytest.fixture
def authed_client(tmp_path):
    """TestClient with auth enabled via a temp .server_token."""
    token_file = tmp_path / ".server_token"
    token_file.write_text("test-secret-123")
    with patch.object(server, "TOKEN_FILE", token_file):
        yield TestClient(app)


@pytest.fixture
def tmp_outputs(tmp_path):
    """Patch OUTPUTS_DIR to a temp dir and return it."""
    with patch.object(server, "OUTPUTS_DIR", tmp_path):
        with patch.object(server, "NOTES_FILE", tmp_path / "journal_notes.json"):
            yield tmp_path


def write_csv(path, headers, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for row in rows:
            w.writerow(row)


# ──────────────────────────────────────────────
# UNIT: _normalize_date
# ──────────────────────────────────────────────

class TestNormalizeDate:
    def test_mdy_to_iso(self):
        assert _normalize_date("3/5/2026") == "2026-03-05"

    def test_single_digit_month_day(self):
        assert _normalize_date("1/2/2026") == "2026-01-02"

    def test_double_digit(self):
        assert _normalize_date("12/15/2025") == "2025-12-15"

    def test_already_iso(self):
        assert _normalize_date("2026-03-05") == "2026-03-05"

    def test_empty_string(self):
        assert _normalize_date("") == ""

    def test_none_passthrough(self):
        assert _normalize_date(None) is None

    def test_malformed_returns_as_is(self):
        assert _normalize_date("not-a-date") == "not-a-date"


# ──────────────────────────────────────────────
# UNIT: _convert
# ──────────────────────────────────────────────

class TestConvert:
    def test_empty_string_returns_none(self):
        assert _convert("pl", "") is None

    def test_int_field(self):
        assert _convert("trade_num", "5") == 5
        assert _convert("qty", "3") == 3
        assert _convert("is_win", "1") == 1

    def test_int_field_from_float_string(self):
        assert _convert("trade_num", "5.0") == 5

    def test_float_field(self):
        assert _convert("pl", "63.5") == 63.5
        assert _convert("strike", "659.0") == 659.0

    def test_float_rounds_to_2_decimals(self):
        assert _convert("pl_pct", "26.252525") == 26.25

    def test_string_field_passthrough(self):
        assert _convert("wl", "WIN") == "WIN"
        assert _convert("group_id", "G53") == "G53"

    def test_unparseable_int_returns_string(self):
        assert _convert("trade_num", "abc") == "abc"

    def test_unparseable_float_returns_string(self):
        assert _convert("pl", "N/A") == "N/A"


# ──────────────────────────────────────────────
# AUTH
# ──────────────────────────────────────────────

class TestAuth:
    def test_no_token_file_allows_access(self, client, tmp_outputs):
        r = client.get("/api/trades")
        assert r.status_code == 200

    def test_missing_token_returns_401(self, authed_client, tmp_outputs):
        r = authed_client.get("/api/trades")
        assert r.status_code == 401

    def test_query_param_auth(self, authed_client, tmp_outputs):
        r = authed_client.get("/api/trades?token=test-secret-123")
        assert r.status_code == 200

    def test_bearer_header_auth(self, authed_client, tmp_outputs):
        r = authed_client.get(
            "/api/trades",
            headers={"Authorization": "Bearer test-secret-123"},
        )
        assert r.status_code == 200

    def test_wrong_token_returns_401(self, authed_client, tmp_outputs):
        r = authed_client.get("/api/trades?token=wrong")
        assert r.status_code == 401

    def test_wrong_bearer_returns_401(self, authed_client, tmp_outputs):
        r = authed_client.get(
            "/api/trades",
            headers={"Authorization": "Bearer wrong"},
        )
        assert r.status_code == 401


# ──────────────────────────────────────────────
# API: /api/trades
# ──────────────────────────────────────────────

class TestGetTrades:
    def test_empty_when_no_csvs(self, client, tmp_outputs):
        r = client.get("/api/trades")
        assert r.status_code == 200
        assert r.json() == []

    def test_reads_spy_and_other(self, client, tmp_outputs):
        headers = ["Trade #", "Date", "Day", "Symbol", "P/L ($)", "Win/Loss", "Is Win", "Qty"]
        write_csv(tmp_outputs / "spy_trades.csv", headers, [
            {"Trade #": "1", "Date": "3/5/2026", "Day": "Thu", "Symbol": "SPY", "P/L ($)": "100", "Win/Loss": "WIN", "Is Win": "1", "Qty": "2"},
            {"Trade #": "2", "Date": "3/6/2026", "Day": "Fri", "Symbol": "SPY", "P/L ($)": "-50", "Win/Loss": "LOSS", "Is Win": "0", "Qty": "1"},
        ])
        write_csv(tmp_outputs / "other_trades.csv", headers, [
            {"Trade #": "3", "Date": "3/5/2026", "Day": "Thu", "Symbol": "HIMS", "P/L ($)": "200", "Win/Loss": "WIN", "Is Win": "1", "Qty": "5"},
        ])
        r = client.get("/api/trades")
        data = r.json()
        assert len(data) == 3
        assert data[0]["symbol"] == "SPY"
        assert data[2]["symbol"] == "HIMS"

    def test_symbol_filter(self, client, tmp_outputs):
        headers = ["Trade #", "Date", "Symbol", "P/L ($)"]
        write_csv(tmp_outputs / "spy_trades.csv", headers, [
            {"Trade #": "1", "Date": "3/5/2026", "Symbol": "SPY", "P/L ($)": "100"},
        ])
        write_csv(tmp_outputs / "other_trades.csv", headers, [
            {"Trade #": "2", "Date": "3/5/2026", "Symbol": "HIMS", "P/L ($)": "200"},
        ])
        r = client.get("/api/trades?symbol=SPY")
        assert len(r.json()) == 1
        assert r.json()[0]["symbol"] == "SPY"

    def test_symbol_filter_case_insensitive(self, client, tmp_outputs):
        headers = ["Trade #", "Date", "Symbol", "P/L ($)"]
        write_csv(tmp_outputs / "spy_trades.csv", headers, [
            {"Trade #": "1", "Date": "3/5/2026", "Symbol": "SPY", "P/L ($)": "100"},
        ])
        write_csv(tmp_outputs / "other_trades.csv", headers, [])
        r = client.get("/api/trades?symbol=spy")
        assert len(r.json()) == 1

    def test_date_normalized(self, client, tmp_outputs):
        headers = ["Trade #", "Date", "Expiry Date", "Symbol"]
        write_csv(tmp_outputs / "spy_trades.csv", headers, [
            {"Trade #": "1", "Date": "3/5/2026", "Expiry Date": "3/5/2026", "Symbol": "SPY"},
        ])
        write_csv(tmp_outputs / "other_trades.csv", headers, [])
        data = client.get("/api/trades").json()
        assert data[0]["date"] == "2026-03-05"
        assert data[0]["expiry_date"] == "2026-03-05"

    def test_numeric_conversion(self, client, tmp_outputs):
        headers = ["Trade #", "Date", "Symbol", "P/L ($)", "Strike", "Qty", "Is Win"]
        write_csv(tmp_outputs / "spy_trades.csv", headers, [
            {"Trade #": "1", "Date": "3/5/2026", "Symbol": "SPY", "P/L ($)": "63.5", "Strike": "659.0", "Qty": "2", "Is Win": "1"},
        ])
        write_csv(tmp_outputs / "other_trades.csv", headers, [])
        t = client.get("/api/trades").json()[0]
        assert t["pl"] == 63.5
        assert t["strike"] == 659.0
        assert t["qty"] == 2
        assert t["is_win"] == 1
        assert t["trade_num"] == 1

    def test_empty_fields_become_none(self, client, tmp_outputs):
        headers = ["Trade #", "Date", "Symbol", "VIX", "Delta"]
        write_csv(tmp_outputs / "spy_trades.csv", headers, [
            {"Trade #": "1", "Date": "3/5/2026", "Symbol": "SPY", "VIX": "", "Delta": ""},
        ])
        write_csv(tmp_outputs / "other_trades.csv", headers, [])
        t = client.get("/api/trades").json()[0]
        assert t["vix"] is None
        assert t["delta"] is None


# ──────────────────────────────────────────────
# API: /api/trades/daily
# ──────────────────────────────────────────────

class TestGetDaily:
    def test_empty(self, client, tmp_outputs):
        r = client.get("/api/trades/daily")
        assert r.json() == []

    def test_aggregates_by_date(self, client, tmp_outputs):
        headers = ["Trade #", "Date", "Symbol", "P/L ($)", "Is Win", "Cumulative P/L ($)", "VIX"]
        write_csv(tmp_outputs / "spy_trades.csv", headers, [
            {"Trade #": "1", "Date": "3/5/2026", "Symbol": "SPY", "P/L ($)": "100", "Is Win": "1", "Cumulative P/L ($)": "100", "VIX": "20.5"},
            {"Trade #": "2", "Date": "3/5/2026", "Symbol": "SPY", "P/L ($)": "-30", "Is Win": "0", "Cumulative P/L ($)": "70", "VIX": "20.5"},
            {"Trade #": "3", "Date": "3/6/2026", "Symbol": "SPY", "P/L ($)": "50", "Is Win": "1", "Cumulative P/L ($)": "120", "VIX": "19.0"},
        ])
        data = client.get("/api/trades/daily").json()
        assert len(data) == 2
        assert data[0]["date"] == "2026-03-05"
        assert data[0]["pl"] == 70
        assert data[0]["num_trades"] == 2
        assert data[0]["wins"] == 1
        assert data[1]["date"] == "2026-03-06"
        assert data[1]["pl"] == 50
        assert data[1]["num_trades"] == 1
        assert data[1]["wins"] == 1

    def test_sorted_by_date(self, client, tmp_outputs):
        headers = ["Trade #", "Date", "Symbol", "P/L ($)", "Is Win", "Cumulative P/L ($)", "VIX"]
        write_csv(tmp_outputs / "spy_trades.csv", headers, [
            {"Trade #": "1", "Date": "3/10/2026", "Symbol": "SPY", "P/L ($)": "10", "Is Win": "1", "Cumulative P/L ($)": "10", "VIX": "20"},
            {"Trade #": "2", "Date": "3/5/2026", "Symbol": "SPY", "P/L ($)": "20", "Is Win": "1", "Cumulative P/L ($)": "20", "VIX": "21"},
        ])
        data = client.get("/api/trades/daily").json()
        assert data[0]["date"] == "2026-03-05"
        assert data[1]["date"] == "2026-03-10"


# ──────────────────────────────────────────────
# API: /api/summary
# ──────────────────────────────────────────────

class TestGetSummary:
    def test_empty(self, client, tmp_outputs):
        r = client.get("/api/summary")
        assert r.json()["total_trades"] == 0

    def test_computes_stats(self, client, tmp_outputs):
        headers = ["Trade #", "Date", "Symbol", "P/L ($)", "Win/Loss", "Is Win"]
        write_csv(tmp_outputs / "spy_trades.csv", headers, [
            {"Trade #": "1", "Date": "3/5/2026", "Symbol": "SPY", "P/L ($)": "100", "Win/Loss": "WIN", "Is Win": "1"},
            {"Trade #": "2", "Date": "3/5/2026", "Symbol": "SPY", "P/L ($)": "-50", "Win/Loss": "LOSS", "Is Win": "0"},
            {"Trade #": "3", "Date": "3/6/2026", "Symbol": "SPY", "P/L ($)": "200", "Win/Loss": "WIN", "Is Win": "1"},
        ])
        write_csv(tmp_outputs / "other_trades.csv", headers, [])
        s = client.get("/api/summary").json()
        assert s["total_trades"] == 3
        assert s["spy_trades"] == 3
        assert s["total_pl"] == 250.0
        assert s["win_rate"] == 66.7
        assert s["avg_win"] == 150.0
        assert s["avg_loss"] == -50.0
        assert s["best_trade"] == 200
        assert s["worst_trade"] == -50
        assert s["last_updated"] == "2026-03-06"


# ──────────────────────────────────────────────
# API: /api/notes
# ──────────────────────────────────────────────

class TestNotes:
    def test_get_empty(self, client, tmp_outputs):
        r = client.get("/api/notes")
        assert r.json() == {}

    def test_save_and_get(self, client, tmp_outputs):
        r = client.post("/api/notes", json={"group_id": "G53", "note": "First trade"})
        assert r.json() == {"ok": True}
        r = client.get("/api/notes")
        assert r.json() == {"G53": "First trade"}

    def test_update_note(self, client, tmp_outputs):
        client.post("/api/notes", json={"group_id": "G53", "note": "v1"})
        client.post("/api/notes", json={"group_id": "G53", "note": "v2"})
        assert client.get("/api/notes").json()["G53"] == "v2"

    def test_delete_note_with_empty_string(self, client, tmp_outputs):
        client.post("/api/notes", json={"group_id": "G53", "note": "v1"})
        client.post("/api/notes", json={"group_id": "G53", "note": ""})
        assert "G53" not in client.get("/api/notes").json()

    def test_missing_group_id_returns_400(self, client, tmp_outputs):
        r = client.post("/api/notes", json={"note": "orphan"})
        assert r.status_code == 400

    def test_multiple_notes(self, client, tmp_outputs):
        client.post("/api/notes", json={"group_id": "G1", "note": "one"})
        client.post("/api/notes", json={"group_id": "G2", "note": "two"})
        notes = client.get("/api/notes").json()
        assert notes == {"G1": "one", "G2": "two"}


# ──────────────────────────────────────────────
# API: /api/trades/open, /api/cash-flow
# ──────────────────────────────────────────────

class TestOtherEndpoints:
    def test_open_empty(self, client, tmp_outputs):
        r = client.get("/api/trades/open")
        assert r.json() == []

    def test_cash_flow_empty(self, client, tmp_outputs):
        r = client.get("/api/cash-flow")
        assert r.json() == []

    def test_cash_flow_reads_jsonl(self, client, tmp_outputs):
        (tmp_outputs / "cash_flow.jsonl").write_text(
            '{"timestamp":"2026-03-05T16:00:00+00:00","equity":10000}\n'
            '{"timestamp":"2026-03-06T16:00:00+00:00","equity":10200}\n'
        )
        data = client.get("/api/cash-flow").json()
        assert len(data) == 2
        assert data[0]["equity"] == 10000

    def test_cash_flow_merges_historical_and_live(self, client, tmp_outputs):
        """Historical (synthetic) merges with live; live wins on date collision."""
        (tmp_outputs / "cash_flow_historical.jsonl").write_text(
            '{"timestamp":"2024-01-01T20:00:00+00:00","equity":500,"synthetic":true}\n'
            '{"timestamp":"2024-06-01T20:00:00+00:00","equity":700,"synthetic":true}\n'
            '{"timestamp":"2026-03-05T20:00:00+00:00","equity":900,"synthetic":true}\n'
        )
        (tmp_outputs / "cash_flow.jsonl").write_text(
            # Same date as last historical line — live should win
            '{"timestamp":"2026-03-05T22:00:00+00:00","equity":950,"synthetic":false}\n'
            '{"timestamp":"2026-03-06T22:00:00+00:00","equity":1000}\n'
        )
        data = client.get("/api/cash-flow").json()
        # 4 unique dates: 2024-01-01, 2024-06-01, 2026-03-05, 2026-03-06
        assert len(data) == 4
        # Sorted ascending
        assert data[0]["timestamp"].startswith("2024-01-01")
        assert data[-1]["timestamp"].startswith("2026-03-06")
        # On the 2026-03-05 collision, live (equity=950) wins
        on_clash = [d for d in data if d["timestamp"].startswith("2026-03-05")][0]
        assert on_clash["equity"] == 950
        assert on_clash.get("synthetic") is False

    def test_cash_flow_historical_only(self, client, tmp_outputs):
        """If only the historical file exists, it's still served."""
        (tmp_outputs / "cash_flow_historical.jsonl").write_text(
            '{"timestamp":"2024-01-01T20:00:00+00:00","equity":500,"synthetic":true}\n'
        )
        data = client.get("/api/cash-flow").json()
        assert len(data) == 1
        assert data[0]["equity"] == 500


# ──────────────────────────────────────────────
# ROUTES: / and /dashboard
# ──────────────────────────────────────────────

class TestRoutes:
    def test_root_redirects_to_dashboard(self, client):
        r = client.get("/", follow_redirects=False)
        assert r.status_code == 307
        assert r.headers["location"] == "/dashboard"

    def test_dashboard_serves_html(self, client):
        r = client.get("/dashboard")
        assert r.status_code == 200
        assert "Trade Dashboard" in r.text
        assert "<nav" in r.text

    def test_dashboard_requires_auth_when_enabled(self, authed_client):
        r = authed_client.get("/dashboard")
        assert r.status_code == 401

    def test_dashboard_with_token_query(self, authed_client):
        r = authed_client.get("/dashboard?token=test-secret-123")
        assert r.status_code == 200

    def test_static_files_served(self, client):
        r = client.get("/static/style.css")
        assert r.status_code == 200
        assert "top-nav" in r.text

    def test_static_view_served(self, client):
        r = client.get("/static/views/analysis.html")
        assert r.status_code == 200
        assert "Equity Curve" in r.text


# ──────────────────────────────────────────────
# COLUMN_MAP coverage
# ──────────────────────────────────────────────

class TestColumnMap:
    def test_all_csv_columns_mapped(self):
        """Ensure COLUMN_MAP covers the known CSV header."""
        csv_header = (
            "Trade #,Date,Day,Account,Symbol,Expiry Date,Type,Strike,Qty,"
            "Asset Open,Asset High,Asset Low,Asset Close,VWAP,8 EMA,"
            "Entry Time,Exit Time,Hold Time (min),Entry Hour,Entry Cost,"
            "Risk ($),Exit Credit,P/L ($),Cumulative P/L ($),P/L (%),"
            "Win/Loss,Is Win,VIX,Delta,Group ID,DTE"
        )
        for col in csv_header.split(","):
            assert col in COLUMN_MAP, f"CSV column '{col}' not in COLUMN_MAP"

    def test_no_duplicate_json_keys(self):
        values = list(COLUMN_MAP.values())
        assert len(values) == len(set(values)), "Duplicate JSON keys in COLUMN_MAP"


# ──────────────────────────────────────────────
# Admin: token status, token update, run jobs
# ──────────────────────────────────────────────

import base64 as _b64
import json as _json
import time as _time
from unittest.mock import MagicMock


def _make_jwt(exp_ts: int) -> str:
    """Make a fake JWT with the given exp claim. Signature isn't verified."""
    h = _b64.urlsafe_b64encode(_json.dumps({"alg": "ES256"}).encode()).rstrip(b"=").decode()
    p = _b64.urlsafe_b64encode(_json.dumps({"exp": int(exp_ts)}).encode()).rstrip(b"=").decode()
    s = "x" * 30  # not validated
    return f"{h}.{p}.{s}"


@pytest.fixture
def admin_env(tmp_path):
    """Patch server paths into a temp dir, reset job state."""
    rh_token_file = tmp_path / ".rh_token"
    jobs_dir = tmp_path / ".admin_jobs"
    with patch.object(server, "RH_TOKEN_FILE", rh_token_file), \
         patch.object(server, "JOBS_DIR", jobs_dir), \
         patch.object(server, "OUTPUTS_DIR", tmp_path):
        # Reset job table
        with server._jobs_lock:
            server._jobs.clear()
        yield {"rh_token_file": rh_token_file, "jobs_dir": jobs_dir, "tmp": tmp_path}


class TestTokenStatus:
    def test_no_token_returns_invalid(self, client, admin_env):
        r = client.get("/api/admin/token-status")
        assert r.status_code == 200
        d = r.json()
        assert d["valid"] is False
        assert d["masked"] is None
        assert d["exp"] is None

    def test_active_token_decodes_exp(self, client, admin_env):
        future = int(_time.time()) + 3600
        admin_env["rh_token_file"].write_text(f"Bearer {_make_jwt(future)}")
        d = client.get("/api/admin/token-status").json()
        assert d["valid"] is True
        assert d["expires_in_seconds"] is not None
        assert 3500 <= d["expires_in_seconds"] <= 3600
        assert d["masked"] is not None
        assert "…" in d["masked"]

    def test_expired_token_invalid(self, client, admin_env):
        past = int(_time.time()) - 60
        admin_env["rh_token_file"].write_text(f"Bearer {_make_jwt(past)}")
        d = client.get("/api/admin/token-status").json()
        assert d["valid"] is False
        assert d["expires_in_seconds"] < 0

    def test_token_without_bearer_prefix(self, client, admin_env):
        future = int(_time.time()) + 7200
        admin_env["rh_token_file"].write_text(_make_jwt(future))  # no 'Bearer'
        d = client.get("/api/admin/token-status").json()
        assert d["valid"] is True

    def test_probe_hits_rh(self, client, admin_env):
        future = int(_time.time()) + 3600
        admin_env["rh_token_file"].write_text(f"Bearer {_make_jwt(future)}")
        mock_resp = MagicMock(); mock_resp.status_code = 200
        with patch.object(server.requests, "get", return_value=mock_resp) as mg:
            d = client.get("/api/admin/token-status?probe=true").json()
        assert d["probed"] is True
        assert d["probe_ok"] is True
        assert d["probe_status"] == 200
        # Probe should have called RH
        assert any("api.robinhood.com/user" in str(c) for c in mg.call_args_list)

    def test_probe_401_invalidates(self, client, admin_env):
        future = int(_time.time()) + 3600
        admin_env["rh_token_file"].write_text(f"Bearer {_make_jwt(future)}")
        mock_resp = MagicMock(); mock_resp.status_code = 401
        with patch.object(server.requests, "get", return_value=mock_resp):
            d = client.get("/api/admin/token-status?probe=true").json()
        assert d["valid"] is False
        assert d["probe_ok"] is False

    def test_token_status_requires_auth(self, authed_client, admin_env):
        r = authed_client.get("/api/admin/token-status")
        assert r.status_code == 401


class TestExtractToken:
    def test_raw_jwt(self):
        jwt = _make_jwt(_time.time() + 3600)
        assert server._extract_token(jwt) == f"Bearer {jwt}"

    def test_bearer_prefix(self):
        jwt = _make_jwt(_time.time() + 3600)
        assert server._extract_token(f"Bearer {jwt}") == f"Bearer {jwt}"

    def test_authorization_header(self):
        jwt = _make_jwt(_time.time() + 3600)
        assert server._extract_token(f"Authorization: Bearer {jwt}") == f"Bearer {jwt}"

    def test_curl_paste(self):
        jwt = _make_jwt(_time.time() + 3600)
        curl = f"""curl 'https://x' -X 'GET' \\
            -H 'Authorization: Bearer {jwt}' \\
            -H 'Other: stuff'"""
        assert server._extract_token(curl) == f"Bearer {jwt}"

    def test_empty_returns_none(self):
        assert server._extract_token("") is None
        assert server._extract_token("   ") is None
        assert server._extract_token("not a token") is None


class TestSetToken:
    def test_rh_validates_and_saves(self, client, admin_env):
        jwt = _make_jwt(_time.time() + 3600)
        mock_resp = MagicMock(); mock_resp.status_code = 200
        with patch.object(server.requests, "get", return_value=mock_resp):
            r = client.post("/api/admin/token", json={"token": f"Bearer {jwt}"})
        assert r.status_code == 200
        assert admin_env["rh_token_file"].exists()
        saved = admin_env["rh_token_file"].read_text()
        assert jwt in saved
        assert saved.startswith("Bearer ")
        # 600 perms
        st = admin_env["rh_token_file"].stat()
        assert (st.st_mode & 0o777) == 0o600

    def test_rh_401_does_not_save(self, client, admin_env):
        jwt = _make_jwt(_time.time() + 3600)
        mock_resp = MagicMock(); mock_resp.status_code = 401
        with patch.object(server.requests, "get", return_value=mock_resp):
            r = client.post("/api/admin/token", json={"token": f"Bearer {jwt}"})
        assert r.status_code == 400
        assert not admin_env["rh_token_file"].exists()

    def test_garbage_input_400(self, client, admin_env):
        r = client.post("/api/admin/token", json={"token": "complete nonsense"})
        assert r.status_code == 400
        assert not admin_env["rh_token_file"].exists()

    def test_set_token_requires_auth(self, authed_client, admin_env):
        r = authed_client.post("/api/admin/token", json={"token": "x"})
        assert r.status_code == 401


class TestRunJobs:
    def test_unknown_script_400(self, client, admin_env):
        r = client.post("/api/admin/run", json={"script": "rm-rf"})
        assert r.status_code == 400
        assert "Allowed" in r.json()["detail"]

    def test_run_spawns_and_completes(self, client, admin_env):
        # Use a fake Popen that finishes immediately and writes to the log
        class FakeProc:
            returncode = 0
            def __init__(self, *a, **kw):
                # Write some output to the log file before "finishing"
                if "stdout" in kw and hasattr(kw["stdout"], "write"):
                    kw["stdout"].write("hello from job\n")
                    kw["stdout"].flush()
            def wait(self):
                return 0
        with patch.object(server.subprocess, "Popen", FakeProc):
            r = client.post("/api/admin/run", json={"script": "cash_flow"})
        assert r.status_code == 200
        d = r.json()
        assert d["script"] == "cash_flow"
        assert d["state"] in ("running", "done")  # watcher thread may or may not have flipped

        # Wait briefly for the watcher thread to flip state
        for _ in range(20):
            r2 = client.get(f"/api/admin/run/{d['id']}")
            if r2.json()["state"] == "done":
                break
            _time.sleep(0.05)
        final = client.get(f"/api/admin/run/{d['id']}").json()
        assert final["state"] == "done"
        assert final["exit_code"] == 0
        assert "hello from job" in final["log_tail"]

    def test_concurrent_run_409s(self, client, admin_env):
        # Stub a still-running job in the table
        with server._jobs_lock:
            server._jobs["existing"] = {
                "id": "existing", "script": "hood", "state": "running",
                "started_at": "2026-01-01T00:00:00+00:00",
                "ended_at": None, "exit_code": None,
                "log_path": str(admin_env["jobs_dir"] / "existing.log"),
            }
        r = client.post("/api/admin/run", json={"script": "cash_flow"})
        assert r.status_code == 409
        assert "existing" in r.json()["detail"]

    def test_status_404_for_unknown_job(self, client, admin_env):
        r = client.get("/api/admin/run/does-not-exist")
        assert r.status_code == 404

    def test_run_requires_auth(self, authed_client, admin_env):
        r = authed_client.post("/api/admin/run", json={"script": "cash_flow"})
        assert r.status_code == 401

    def test_recent_runs_listing(self, client, admin_env):
        with server._jobs_lock:
            server._jobs["a"] = {
                "id": "a", "script": "cash_flow", "state": "done",
                "started_at": "2026-01-01T00:00:00+00:00",
                "ended_at": "2026-01-01T00:00:30+00:00", "exit_code": 0,
                "log_path": "x",
            }
            server._jobs["b"] = {
                "id": "b", "script": "hood", "state": "failed",
                "started_at": "2026-01-02T00:00:00+00:00",
                "ended_at": "2026-01-02T00:00:05+00:00", "exit_code": 1,
                "log_path": "x",
            }
        d = client.get("/api/admin/runs").json()
        assert len(d) == 2
        # Newest first
        assert d[0]["id"] == "b"
        assert d[0]["state"] == "failed"
        # No log_path / log_tail in the listing
        assert "log_path" not in d[0]
