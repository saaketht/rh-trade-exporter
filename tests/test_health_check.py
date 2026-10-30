"""Tests for health_check.py — gap days, step failures, option-bar capture, posting rules."""
import json

import pytest

import health_check as hc


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    monkeypatch.setattr(hc, "fetch_executions", lambda since: pytest.fail("real RH call"))
    monkeypatch.setattr(hc.tw, "notify", lambda *a: pytest.fail("real Discord post"))


def write_trades(out, rows):
    out.mkdir(exist_ok=True)
    (out / "spy_trades.csv").write_text("Date,Entry Time,Hold Time (min)\n" + "".join(f"{r}\n" for r in rows))


def refresh_state(out, ended="2026-10-29T20:10:00+00:00", **steps):
    (out / ".last_refresh.json").write_text(json.dumps({"started_at": ended, "ended_at": ended,
                                                        "steps": {"hood": 0, **steps}}))


def test_execution_dates_are_eastern():
    orders = [{"legs": [{"executions": [{"timestamp": "2026-10-16T19:30:00Z"},      # 15:30 ET
                                        {"timestamp": "2026-10-17T03:00:00.123Z"}]}]}]  # 23:00 ET on 10/16
    assert hc.execution_dates(orders) == {"2026-10-16": 2}


def test_covered_dates_include_entries_exits_and_open_positions(tmp_path):
    write_trades(tmp_path, ["10/14/2026,15:50:00,1080"])            # held overnight → exits 10/15
    (tmp_path / "unmatched_opens.csv").write_text("Date,Expiry\n10/20/2026,10/24/2026\n")
    assert hc.covered_dates(tmp_path) == {"2026-10-14", "2026-10-15", "2026-10-20"}


def test_gap_days_skip_covered_and_self_healing_dates():
    execs = {"2026-10-15": 3, "2026-10-16": 13, "2026-10-28": 5}
    issues = hc.gap_issues(execs, covered={"2026-10-15"}, cursor="2026-10-27")
    assert len(issues) == 1 and "gap day 2026-10-16: 13 filled executions" in issues[0]
    assert "hood.py --after-date 2026-10-15" in issues[0]   # 10-28 is after the cursor: next run refetches it


def test_step_issues(tmp_path):
    assert "no refresh record" in hc.step_issues(tmp_path / ".last_refresh.json", "2026-10-29")[0]
    refresh_state(tmp_path, cash_flow=1)
    assert hc.step_issues(tmp_path / ".last_refresh.json", "2026-10-29") == ["step `cash_flow` exited 1"]
    assert "not today" in hc.step_issues(tmp_path / ".last_refresh.json", "2026-10-30")[0]


def test_option_issues(tmp_path):
    assert hc.option_issues(tmp_path, "2026-10-29", traded_today=False) == []
    assert hc.option_issues(tmp_path, "2026-10-29", traded_today=True) == ["no option bars saved for today"]
    (tmp_path / "option_intraday").mkdir()
    (tmp_path / "option_intraday" / "2026-10-29.json").write_text(json.dumps(
        {"missing": [{"symbol": "SPY", "strike": 771.0, "type": "call", "reason": "no_instrument"}]}))
    assert "SPY 771.0C (no_instrument)" in hc.option_issues(tmp_path, "2026-10-29", traded_today=True)[0]


def test_run_all_clear(tmp_path):
    write_trades(tmp_path, ["10/28/2026,10:00:00,5", "10/29/2026,10:00:00,5"])
    refresh_state(tmp_path)
    (tmp_path / "option_intraday").mkdir()
    (tmp_path / "option_intraday" / "2026-10-29.json").write_text('{"missing": []}')
    assert hc.run("2026-10-29", tmp_path, fetch=lambda since: {"2026-10-28": 2, "2026-10-29": 4}) == []


@pytest.mark.parametrize("err", [RuntimeError("HTTP 401"), SystemExit(1)])
def test_rh_failure_is_reported_not_raised(tmp_path, err):
    write_trades(tmp_path, ["10/29/2026,10:00:00,5"])
    refresh_state(tmp_path)

    def boom(since):
        raise err
    issues = hc.run("2026-10-29", tmp_path, fetch=boom)
    assert any(i.startswith("couldn't check gap days") for i in issues)


def test_main_posts_only_when_something_is_off(monkeypatch, capsys):
    sent = []
    monkeypatch.setattr(hc.tw, "notify", lambda s, b, w, e: sent.append(b) or ["discord"])
    monkeypatch.setattr(hc.tw, "env_value", lambda k: "https://hook" if k == "DISCORD_WEBHOOK_URL" else "")
    monkeypatch.setattr(hc, "run", lambda: [])
    hc.main([])
    assert sent == []                                   # all clear: quiet
    hc.main(["--always"])
    assert sent and sent[-1].startswith("✅")
    monkeypatch.setattr(hc, "run", lambda: ["gap day 2026-10-16: …"])
    hc.main(["--dry-run"])
    assert len(sent) == 1                               # dry run never posts
    hc.main([])
    assert sent[-1].startswith("⚠️") and "gap day 2026-10-16" in sent[-1]
