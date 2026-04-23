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
            '{"date":"2026-03-05","equity":10000}\n'
            '{"date":"2026-03-06","equity":10200}\n'
        )
        data = client.get("/api/cash-flow").json()
        assert len(data) == 2
        assert data[0]["equity"] == 10000


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
