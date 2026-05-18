"""Tests for spy_daily.py — payload assembly + idempotent cache writes.

Mocks yfinance so tests run fully offline. We're not testing yfinance itself,
just our merging / pct-change / range / IO logic.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

import spy_daily


def _df(rows):
    """Build a yfinance-shaped DataFrame from a list of (date_iso, o, h, l, c)."""
    idx = pd.DatetimeIndex([r[0] for r in rows])
    return pd.DataFrame(
        {"Open": [r[1] for r in rows], "High": [r[2] for r in rows],
         "Low":  [r[3] for r in rows], "Close": [r[4] for r in rows]},
        index=idx,
    )


@pytest.fixture
def tmp_outputs(tmp_path, monkeypatch):
    """Patch SCRIPT_DIR / OUT_FILE / TRADES_CSV onto a temp dir."""
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    monkeypatch.setattr(spy_daily, "SCRIPT_DIR", tmp_path)
    monkeypatch.setattr(spy_daily, "OUTPUTS_DIR", outputs)
    monkeypatch.setattr(spy_daily, "OUT_FILE", outputs / "spy_daily.json")
    monkeypatch.setattr(spy_daily, "TRADES_CSV", outputs / "spy_trades.csv")
    return outputs


class TestBuildPayload:
    def test_combines_spy_and_vix_with_pct_change(self, tmp_outputs):
        """Each day gets SPY OHLC + VIX close + spy_pct vs prior close."""
        spy_df = _df([
            ("2026-05-12", 509.0, 511.0, 508.5, 510.00),
            ("2026-05-13", 510.5, 513.2, 510.0, 512.55),  # +0.50% vs 510.00
        ])
        vix_df = _df([
            ("2026-05-12", 17.0, 17.5, 16.8, 17.0),
            ("2026-05-13", 17.5, 17.9, 17.2, 17.5),
        ])
        with patch("spy_daily.yf.download", side_effect=[spy_df, vix_df]):
            payload = spy_daily.build_payload(log=lambda *a, **k: None)
        days = payload["days"]
        assert len(days) == 2
        assert days[0]["date"] == "2026-05-12"
        assert days[0]["spy_close"] == 510.0
        assert days[0]["vix_close"] == 17.0
        # First day has no prior close → no spy_pct
        assert "spy_pct" not in days[0]
        # Second day's spy_pct = (512.55 - 510.00) / 510.00 * 100 = 0.50
        assert days[1]["spy_pct"] == 0.50
        assert days[1]["vix_close"] == 17.5

    def test_handles_missing_vix_gracefully(self, tmp_outputs):
        spy_df = _df([("2026-05-12", 509.0, 511.0, 508.5, 510.00)])
        with patch("spy_daily.yf.download", side_effect=[spy_df, pd.DataFrame()]):
            payload = spy_daily.build_payload(log=lambda *a, **k: None)
        assert len(payload["days"]) == 1
        assert "vix_close" not in payload["days"][0]
        assert payload["days"][0]["spy_close"] == 510.0

    def test_range_inferred_from_trades_csv(self, tmp_outputs):
        """If spy_trades.csv exists, since defaults to earliest trade − 7d."""
        from datetime import date, timedelta
        # Use a recent trade so the inferred since is recent (avoids huge fetches in real use)
        recent = (date.today() - timedelta(days=10)).strftime("%-m/%-d/%Y")
        (tmp_outputs / "spy_trades.csv").write_text(f"Date\n{recent}\n")
        empty_df = pd.DataFrame()
        captured_starts = []
        def _capture(symbol, **kw):
            captured_starts.append(kw.get("start"))
            return empty_df
        with patch("spy_daily.yf.download", side_effect=_capture):
            spy_daily.build_payload(log=lambda *a, **k: None)
        # Both calls (SPY then VIX) should use the same since
        assert len(captured_starts) == 2
        assert captured_starts[0] == captured_starts[1]


class TestMain:
    def test_writes_jsonl_to_out_file(self, tmp_outputs):
        spy_df = _df([("2026-05-13", 510.0, 513.0, 509.5, 512.0)])
        vix_df = _df([("2026-05-13", 17.0, 17.5, 16.8, 17.2)])
        with patch("spy_daily.yf.download", side_effect=[spy_df, vix_df]):
            spy_daily.main(as_json=True, since="2026-05-12")
        out = json.loads((tmp_outputs / "spy_daily.json").read_text())
        assert out["range"]["start"] == "2026-05-13"
        assert out["range"]["end"] == "2026-05-13"
        assert len(out["days"]) == 1
        assert out["days"][0]["spy_close"] == 512.0
        assert out["days"][0]["vix_close"] == 17.2

    def test_overwrites_existing_cache(self, tmp_outputs):
        """A re-run produces a fresh file (idempotent / non-cumulative)."""
        (tmp_outputs / "spy_daily.json").write_text('{"stale": true}')
        spy_df = _df([("2026-05-13", 510.0, 513.0, 509.5, 512.0)])
        with patch("spy_daily.yf.download", side_effect=[spy_df, pd.DataFrame()]):
            spy_daily.main(as_json=True, since="2026-05-12")
        out = json.loads((tmp_outputs / "spy_daily.json").read_text())
        assert "stale" not in out
        assert "days" in out
