"""Daily refresh — the one definition of the nightly chain.

Used by both the VPS cron (vps/run.sh) and the admin panel ("Daily refresh"
button), so the two can't drift:

    hood → option_intraday → cash_flow → spy_daily → spy_intraday

hood is fatal: a stale RH token fails it first, and nothing downstream should
run on stale data. The rest are non-fatal: each failure is recorded and the
chain keeps going. option_intraday runs right after hood because today's
1-minute option bars are only fetchable the same day.

Exit codes: 0 all steps ok · 3 hood ok but a later step failed · otherwise
hood's own exit code (hood only uses 0/1).

Emits sentinel markers on stdout so the admin UI can mirror per-step status
into the individual script rows in real time:
    [STEP] <name> START
    [STEP] <name> END exit=<code>
    [STEP] daily_refresh ABORTED at <name>      (hood failed)
    [STEP] daily_refresh DONE WITH FAILURES: <names>
    [STEP] daily_refresh ALL DONE

and writes outputs/.last_refresh.json ({started_at, ended_at, steps: {name: rc}})
for health_check.py. Child scripts run with `python -u` so their progress
flushes into the shared log. --quiet passes --json to the steps that support
it (the cron log doesn't need their progress output).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent
# Prefer the repo's venv (what cron and the dashboard run under); fall back to
# whatever is running this file.
_VENV_PY = BASE / ".venv" / "bin" / "python"
PY = str(_VENV_PY) if _VENV_PY.exists() else sys.executable
STATE_FILE = BASE / "outputs" / ".last_refresh.json"
PARTIAL = 3

# (name, script, fatal, supports --json)
STEPS = [
    ("hood",            "hood.py",            True,  False),
    ("option_intraday", "option_intraday.py", False, True),
    ("cash_flow",       "cash_flow.py",       False, True),
    ("spy_daily",       "spy_daily.py",       False, True),
    ("spy_intraday",    "spy_intraday.py",    False, True),
]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--quiet", action="store_true", help="pass --json to steps that support it (cron)")
    a = ap.parse_args(argv)

    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    results: dict[str, int] = {}

    def save():
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps({"started_at": started,
                                          "ended_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                          "steps": results}, indent=2))

    for name, script, fatal, has_json in STEPS:
        cmd = [PY, "-u", str(BASE / script)] + (["--json"] if a.quiet and has_json else [])
        print(f"[STEP] {name} START", flush=True)
        rc = subprocess.run(cmd).returncode
        print(f"[STEP] {name} END exit={rc}", flush=True)
        results[name] = rc
        if rc != 0 and fatal:
            print(f"[STEP] daily_refresh ABORTED at {name}", flush=True)
            save()
            return rc

    save()
    failed = [n for n, rc in results.items() if rc != 0]
    if failed:
        print(f"[STEP] daily_refresh DONE WITH FAILURES: {', '.join(failed)}", flush=True)
        return PARTIAL
    print("[STEP] daily_refresh ALL DONE", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
