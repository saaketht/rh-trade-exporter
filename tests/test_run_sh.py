"""Tests for vps/run.sh — runs the real script against a stub interpreter.

The repo layout is copied into a temp dir with `.venv/bin/python` replaced by a
stub that logs its argv, so we can assert exactly what cron would invoke without
touching Robinhood. Guards the 2026-10 regression where hood.py was called with a
fixed `--after-date <yesterday> --symbol SPY`, which bypassed the incremental
cursor and silently lost any day a failed run missed.
"""
import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
RUN_SH = REPO / "vps" / "run.sh"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")


def run_with_stub(tmp_path, fail_script=None, dotenv=None):
    """Run a copy of run.sh whose python stub records each call; returns (proc, calls)."""
    (tmp_path / "vps").mkdir()
    if dotenv is not None:
        (tmp_path / ".env").write_text(dotenv)
    shutil.copy(RUN_SH, tmp_path / "vps" / "run.sh")
    bindir = tmp_path / ".venv" / "bin"
    bindir.mkdir(parents=True)
    calls = tmp_path / "calls.log"
    stub = bindir / "python"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        f'echo "$*" >> "{calls}"\n'
        f'[ "$1" = "{fail_script or "-"}" ] && exit 3\n'
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


def test_pipeline_order(tmp_path):
    _, calls = run_with_stub(tmp_path)
    assert [c.split()[0] for c in calls] == [
        "hood.py", "option_intraday.py", "cash_flow.py", "spy_daily.py", "spy_intraday.py",
    ]


def test_hood_failure_is_fatal(tmp_path):
    proc, calls = run_with_stub(tmp_path, fail_script="hood.py")
    assert proc.returncode == 3
    assert calls == ["hood.py"]          # nothing downstream runs on stale data
    assert "hood.py failed" not in proc.stdout  # alerts go to the log/webhook, not stdout


def test_later_failures_are_non_fatal(tmp_path):
    proc, calls = run_with_stub(tmp_path, fail_script="cash_flow.py")
    assert proc.returncode == 0
    assert calls[-1].startswith("spy_intraday.py")
    assert "cash_flow.py failed (non-fatal)" in (tmp_path / "cron.log").read_text()
