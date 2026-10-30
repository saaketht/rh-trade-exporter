"""Tests for vps/run.sh + daily_refresh.py — the real scripts against a stub interpreter.

The repo layout is copied into a temp dir with `.venv/bin/python` replaced by a
stub: it runs daily_refresh.py for real (so the shared chain is exercised) and
only records every other script call, so nothing touches Robinhood. Guards the
2026-10 regression where hood.py was called with a fixed `--after-date
<yesterday> --symbol SPY`, which bypassed the incremental cursor and silently lost
any day a failed run missed.
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")


def run_with_stub(tmp_path, fail_script=None, dotenv=None):
    """Run a copy of run.sh; returns (proc, calls) where calls are 'script args' lines."""
    (tmp_path / "vps").mkdir()
    if dotenv is not None:
        (tmp_path / ".env").write_text(dotenv)
    shutil.copy(REPO / "vps" / "run.sh", tmp_path / "vps" / "run.sh")
    shutil.copy(REPO / "daily_refresh.py", tmp_path / "daily_refresh.py")
    bindir = tmp_path / ".venv" / "bin"
    bindir.mkdir(parents=True)
    calls = tmp_path / "calls.log"
    stub = bindir / "python"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        'args=("$@"); [ "${args[0]}" = "-u" ] && args=("${args[@]:1}")\n'
        'script="$(basename "${args[0]}")"\n'
        f'[ "$script" = "daily_refresh.py" ] && exec "{sys.executable}" "$@"\n'
        f'echo "$script ${{args[*]:1}}" | sed "s/ *$//" >> "{calls}"\n'
        f'[ "$script" = "{fail_script or "-"}" ] && exit 1\n'
        "exit 0\n"
    )
    stub.chmod(0o755)
    env = {k: v for k, v in os.environ.items() if k not in ("DISCORD_WEBHOOK_URL", "ALERT_EMAIL")}
    proc = subprocess.run(["bash", str(tmp_path / "vps" / "run.sh")],
                          capture_output=True, text=True, env=env)
    lines = calls.read_text().splitlines() if calls.exists() else []
    return proc, lines


def test_hood_uses_incremental_cursor(tmp_path):
    proc, calls = run_with_stub(tmp_path)
    assert proc.returncode == 0, proc.stderr
    hood = [c for c in calls if c.startswith("hood.py")]
    assert hood == ["hood.py"], f"hood.py must run bare so its cursor applies, got {hood}"


@pytest.mark.parametrize("dotenv", [None, "MASSIVE_API_KEY=x\n"], ids=["no-dotenv", "dotenv-without-alert-keys"])
def test_missing_alert_config_does_not_abort(tmp_path, dotenv):
    """gener's shape: .env holds only MASSIVE_API_KEY and ALERT_EMAIL is unset.
    The .env fallback lookup used to fail under pipefail and exit before hood.py."""
    proc, calls = run_with_stub(tmp_path, dotenv=dotenv)
    assert proc.returncode == 0, proc.stderr
    assert calls and calls[0] == "hood.py"


def test_chain_order_then_health_check(tmp_path):
    _, calls = run_with_stub(tmp_path)
    assert calls == [
        "hood.py", "option_intraday.py --json", "cash_flow.py --json",
        "spy_daily.py --json", "spy_intraday.py --json", "health_check.py",
    ]
    state = json.loads((tmp_path / "outputs" / ".last_refresh.json").read_text())
    assert state["steps"] == dict.fromkeys(
        ["hood", "option_intraday", "cash_flow", "spy_daily", "spy_intraday"], 0)


def test_hood_failure_is_fatal(tmp_path):
    proc, calls = run_with_stub(tmp_path, fail_script="hood.py")
    assert proc.returncode == 1
    assert calls == ["hood.py"]          # nothing downstream runs on stale data, no health check
    assert "hood.py failed" not in proc.stdout  # alerts go to the log/webhook, not stdout
    log = (tmp_path / "cron.log").read_text()
    assert "❌ hood.py failed (exit 1)" in log and "[STEP] daily_refresh ABORTED at hood" in log


def test_later_failures_are_non_fatal(tmp_path):
    proc, calls = run_with_stub(tmp_path, fail_script="cash_flow.py")
    assert proc.returncode == 0
    assert calls[-2:] == ["spy_intraday.py --json", "health_check.py"]   # chain kept going
    log = (tmp_path / "cron.log").read_text()
    assert "[STEP] cash_flow END exit=1" in log
    assert "⚠️ refresh: hood ok, a later step failed" in log
    assert json.loads((tmp_path / "outputs" / ".last_refresh.json").read_text())["steps"]["cash_flow"] == 1
