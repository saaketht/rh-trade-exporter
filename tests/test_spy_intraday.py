"""Tests for spy_intraday.py — token loading, pagination, rate-limit pacing, cache I/O.

All network calls are mocked; tests run fully offline.
"""

import json
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

import spy_intraday


@pytest.fixture
def tmp_env(tmp_path, monkeypatch):
    """Patch SCRIPT_DIR / OUTPUTS_DIR / CACHE_DIR / ENV_FILE to a temp dir."""
    outputs = tmp_path / "outputs"
    cache = outputs / "spy_intraday"
    cache.mkdir(parents=True)
    env = tmp_path / ".env"
    env.write_text("MASSIVE_API_KEY=test-key-abcd\n")
    monkeypatch.setattr(spy_intraday, "SCRIPT_DIR", tmp_path)
    monkeypatch.setattr(spy_intraday, "OUTPUTS_DIR", outputs)
    monkeypatch.setattr(spy_intraday, "CACHE_DIR", cache)
    monkeypatch.setattr(spy_intraday, "ENV_FILE", env)
    # Patch out time.sleep so rate-limit pacing doesn't slow the test suite.
    monkeypatch.setattr(spy_intraday.time, "sleep", lambda *_: None)
    return tmp_path


def _mock_response(status=200, body=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = body or {}
    r.text = json.dumps(body or {})
    return r


# ──────────────────────────────────────────────
# load_polygon_key
# ──────────────────────────────────────────────

class TestLoadKey:
    def test_reads_from_env_file(self, tmp_env):
        assert spy_intraday.load_polygon_key() == "test-key-abcd"

    def test_env_var_overrides_file(self, tmp_env, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "from-env")
        assert spy_intraday.load_polygon_key() == "from-env"

    def test_missing_env_returns_none(self, tmp_env, monkeypatch):
        spy_intraday.ENV_FILE.unlink()
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        assert spy_intraday.load_polygon_key() is None

    def test_strips_quotes(self, tmp_env, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        spy_intraday.ENV_FILE.write_text('MASSIVE_API_KEY="quoted-value"\n')
        assert spy_intraday.load_polygon_key() == "quoted-value"


# ──────────────────────────────────────────────
# fetch_day
# ──────────────────────────────────────────────

class TestFetchDay:
    def _payload(self, n_results):
        return {"results": [{"t": 1715600400000 + i * 300000,
                             "o": 510 + i * 0.1, "h": 511, "l": 509, "c": 510.5, "v": 1000}
                            for i in range(n_results)],
                "next_url": None}

    def test_single_page_success(self, tmp_env):
        with patch("spy_intraday.requests.get",
                   return_value=_mock_response(200, self._payload(3))):
            payload = spy_intraday.fetch_day("2026-05-13", "k", log=lambda *_: None)
        assert "bars" in payload
        assert len(payload["bars"]) == 3
        assert payload["bars"][0]["t"] == 1715600400  # ms→s conversion
        assert payload["source"] == "polygon"
        assert payload["interval"] == "5m"

    def test_paginates_via_next_url(self, tmp_env):
        page1 = {"results": [{"t": 1715600400000, "o": 510, "h": 511, "l": 509, "c": 510.5}],
                 "next_url": "https://api.polygon.io/v2/aggs/next-page"}
        page2 = self._payload(2)
        with patch("spy_intraday.requests.get",
                   side_effect=[_mock_response(200, page1), _mock_response(200, page2)]):
            payload = spy_intraday.fetch_day("2026-05-13", "k", log=lambda *_: None)
        assert len(payload["bars"]) == 3

    def test_403_returns_out_of_plan(self, tmp_env):
        body = {"status": "NOT_AUTHORIZED", "message": "Your plan doesn't include this data timeframe."}
        with patch("spy_intraday.requests.get", return_value=_mock_response(403, body)):
            payload = spy_intraday.fetch_day("2020-01-01", "k", log=lambda *_: None)
        assert payload["available"] is False
        assert payload["reason"] == "out_of_plan"

    def test_empty_results_returns_no_data(self, tmp_env):
        with patch("spy_intraday.requests.get",
                   return_value=_mock_response(200, {"results": [], "next_url": None})):
            payload = spy_intraday.fetch_day("2026-05-09", "k", log=lambda *_: None)  # a Saturday
        assert payload["available"] is False
        assert payload["reason"] == "no_data"

    def test_429_retries_once(self, tmp_env):
        ok = self._payload(1)
        with patch("spy_intraday.requests.get",
                   side_effect=[_mock_response(429, {}), _mock_response(200, ok)]):
            payload = spy_intraday.fetch_day("2026-05-13", "k", log=lambda *_: None)
        assert len(payload["bars"]) == 1

    def test_timeout_retries_once_then_succeeds(self, tmp_env):
        import requests as real_requests
        ok = self._payload(2)
        with patch("spy_intraday.requests.get",
                   side_effect=[real_requests.ReadTimeout("read timed out"),
                                _mock_response(200, ok)]):
            payload = spy_intraday.fetch_day("2026-05-13", "k", log=lambda *_: None)
        assert "bars" in payload
        assert len(payload["bars"]) == 2

    def test_timeout_both_attempts_returns_error(self, tmp_env):
        import requests as real_requests
        with patch("spy_intraday.requests.get",
                   side_effect=[real_requests.ReadTimeout("read timed out"),
                                real_requests.ReadTimeout("read timed out again")]):
            payload = spy_intraday.fetch_day("2026-05-13", "k", log=lambda *_: None)
        assert payload["available"] is False
        assert payload["reason"] == "error"
        assert "read timed out" in payload["message"]


# ──────────────────────────────────────────────
# Date selection
# ──────────────────────────────────────────────

class TestDateSelection:
    def test_weekdays_between_skips_weekends(self):
        start = date(2026, 5, 11)  # Mon
        end   = date(2026, 5, 17)  # Sun
        days = list(spy_intraday.weekdays_between(start, end))
        # Mon, Tue, Wed, Thu, Fri = 5 weekdays
        assert len(days) == 5
        assert all(d.weekday() < 5 for d in days)

    def test_missing_dates_excludes_existing_cache(self, tmp_env):
        # Pre-cache 2026-05-12
        (spy_intraday.CACHE_DIR / "2026-05-12.json").write_text("{}")
        miss = spy_intraday.missing_dates(date(2026, 5, 11), date(2026, 5, 13))
        # Mon (11) and Wed (13) should be missing; Tue (12) is cached
        assert [d.isoformat() for d in miss] == ["2026-05-11", "2026-05-13"]

    def test_default_targets_monday_picks_friday(self, monkeypatch):
        # On a Monday, "previous trading day" should be Friday (skip weekend).
        class FakeDate(date):
            @classmethod
            def today(cls):
                return date(2026, 5, 18)  # Mon
        monkeypatch.setattr(spy_intraday, "date", FakeDate)
        targets = spy_intraday.default_targets()
        assert [d.isoformat() for d in targets] == ["2026-05-15", "2026-05-18"]

    def test_default_targets_weekday_picks_prev_day(self, monkeypatch):
        # Tue–Fri: previous trading day is just yesterday.
        class FakeDate(date):
            @classmethod
            def today(cls):
                return date(2026, 5, 20)  # Wed
        monkeypatch.setattr(spy_intraday, "date", FakeDate)
        targets = spy_intraday.default_targets()
        assert [d.isoformat() for d in targets] == ["2026-05-19", "2026-05-20"]

    def test_default_targets_saturday_only_friday(self, monkeypatch):
        # Weekend runs (e.g. a manual click on Saturday) should refresh just
        # Friday — today is a weekend, exclude it.
        class FakeDate(date):
            @classmethod
            def today(cls):
                return date(2026, 5, 23)  # Sat
        monkeypatch.setattr(spy_intraday, "date", FakeDate)
        targets = spy_intraday.default_targets()
        assert [d.isoformat() for d in targets] == ["2026-05-22"]


# ──────────────────────────────────────────────
# write_cache
# ──────────────────────────────────────────────

class TestWriteCache:
    def test_writes_json_at_expected_path(self, tmp_env):
        payload = {"date": "2026-05-13", "bars": [{"t": 1, "o": 1, "h": 1, "l": 1, "c": 1}]}
        path = spy_intraday.write_cache(payload)
        assert path == spy_intraday.CACHE_DIR / "2026-05-13.json"
        assert path.exists()
        assert json.loads(path.read_text())["date"] == "2026-05-13"


# ──────────────────────────────────────────────
# Orchestration
# ──────────────────────────────────────────────

class TestRun:
    def test_skips_already_cached_dates(self, tmp_env):
        (spy_intraday.CACHE_DIR / "2026-05-13.json").write_text("{}")
        with patch("spy_intraday.requests.get") as get_mock:
            summary = spy_intraday.run([date(2026, 5, 13)], "k", log=lambda *_: None)
        # No HTTP call should have happened
        assert get_mock.call_count == 0
        assert summary["skipped_cached"] == 1
        assert summary["fetched"] == 0

    def test_writes_each_target(self, tmp_env):
        payload = {"results": [{"t": 1715600400000, "o": 510, "h": 511, "l": 509, "c": 510.5}],
                   "next_url": None}
        with patch("spy_intraday.requests.get", return_value=_mock_response(200, payload)):
            summary = spy_intraday.run([date(2026, 5, 12), date(2026, 5, 13)], "k",
                                       log=lambda *_: None)
        assert summary["fetched"] == 2
        assert (spy_intraday.CACHE_DIR / "2026-05-12.json").exists()
        assert (spy_intraday.CACHE_DIR / "2026-05-13.json").exists()
