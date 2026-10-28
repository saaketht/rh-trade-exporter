"""Tests for vps/pull_data.sh — runs the real script against a fake "gener".

An `rsync` shim on PATH rewrites `gener:<path>` to a local directory and then
runs the real rsync, so the test covers the actual flags (backups, excludes,
dry run) and the cash-flow merge without any network.
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
REAL_RSYNC = shutil.which("rsync")
pytestmark = pytest.mark.skipif(not REAL_RSYNC or not shutil.which("bash"), reason="needs bash + rsync")


def jsonl(path, rows):
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))


def read_jsonl(path):
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


@pytest.fixture
def env(tmp_path):
    local, remote = tmp_path / "local", tmp_path / "remote" / "rh-trade-exporter"
    (local / "vps").mkdir(parents=True)
    shutil.copy(REPO / "vps" / "pull_data.sh", local / "vps" / "pull_data.sh")
    shutil.copy(REPO / "cash_flow.py", local / "cash_flow.py")
    py = local / ".venv" / "bin" / "python"   # wrapper, not a symlink: keeps this venv's packages
    py.parent.mkdir(parents=True)
    py.write_text(f'#!/usr/bin/env bash\nexec "{sys.executable}" "$@"\n')
    py.chmod(0o755)
    (local / "outputs").mkdir()
    (remote / "outputs").mkdir(parents=True)

    shim = tmp_path / "bin" / "rsync"
    shim.parent.mkdir()
    shim.write_text("#!/usr/bin/env bash\nargs=()\nfor a in \"$@\"; do args+=(\"${a/#gener:/"
                    f"{tmp_path / 'remote'}/}}\"); done\nexec {REAL_RSYNC} \"${{args[@]}}\"\n")
    shim.chmod(0o755)

    lo, re = local / "outputs", remote / "outputs"
    (lo / "spy_trades.csv").write_text("local copy\n")
    (re / "spy_trades.csv").write_text("gener copy, newer\n")
    (lo / "journal_notes.json").write_text('{"x": "mine"}')
    (re / "journal_notes.json").write_text('{"x": "theirs"}')
    (re / "spy_trades.csv.prefragfix.bak").write_text("bak")
    jsonl(lo / "cash_flow.jsonl", [{"timestamp": "2026-10-01T03:00:00"}])
    jsonl(re / "cash_flow.jsonl", [{"timestamp": "2026-09-30T20:00:00"}, {"timestamp": "2026-10-01T03:00:00"}])
    jsonl(lo / "cash_flow_events.jsonl", [{"id": "a", "date": "2026-10-01", "kind": "deposit", "state": "pending"}])
    jsonl(re / "cash_flow_events.jsonl", [{"id": "a", "date": "2026-10-01", "kind": "deposit", "state": "completed"},
                                          {"id": "b", "date": "2026-10-02", "kind": "dividend"}])
    run_env = {**os.environ, "PATH": f"{shim.parent}:{os.environ['PATH']}"}
    return local, run_env


def run(local, run_env, *args):
    return subprocess.run(["bash", str(local / "vps" / "pull_data.sh"), *args],
                          capture_output=True, text=True, env=run_env)


def test_pull_mirrors_merges_and_backs_up(env):
    local, run_env = env
    p = run(local, run_env)
    assert p.returncode == 0, p.stderr
    out = local / "outputs"
    assert (out / "spy_trades.csv").read_text() == "gener copy, newer\n"
    assert json.loads((out / "journal_notes.json").read_text()) == {"x": "mine"}   # never clobbered
    assert not (out / "spy_trades.csv.prefragfix.bak").exists()                    # backups excluded
    assert [r["timestamp"] for r in read_jsonl(out / "cash_flow.jsonl")] == [
        "2026-09-30T20:00:00", "2026-10-01T03:00:00"]                             # union, sorted, no dup
    ev = {r["id"]: r for r in read_jsonl(out / "cash_flow_events.jsonl")}
    assert set(ev) == {"a", "b"} and ev["a"]["state"] == "completed"                # gener wins on conflict
    (bak,) = (out / ".prepull").iterdir()
    assert (bak / "spy_trades.csv").read_text() == "local copy\n"                       # replaced file kept
    assert len(read_jsonl(bak / "cash_flow.jsonl")) == 1


def test_dry_run_changes_nothing(env):
    local, run_env = env
    before = {p.name: p.read_text() for p in (local / "outputs").iterdir()}
    p = run(local, run_env, "--dry-run")
    assert p.returncode == 0, p.stderr
    assert {p.name: p.read_text() for p in (local / "outputs").iterdir()} == before
    assert "1 from gener" in p.stdout
