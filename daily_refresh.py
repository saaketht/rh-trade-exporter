#!/usr/bin/env python3
"""Daily refresh orchestrator: hood → cash_flow → spy_intraday → spy_daily.

Invoked from the admin panel ("Daily refresh" button). Runs each step
sequentially, aborting on the first non-zero exit so a stale RH token (which
fails hood.py first) doesn't waste the slow spy_* fetches.

Emits sentinel markers on stdout so the admin UI can mirror per-step status
into the individual script rows in real time:
    [STEP] <name> START
    [STEP] <name> END exit=<code>
    [STEP] daily_refresh ABORTED at <name>   (on failure)
    [STEP] daily_refresh ALL DONE            (on success)

Child scripts are launched with `python -u` so their own progress lines
flush into the shared log without buffering delays.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
PY = sys.executable

STEPS = [
    ("hood",         [PY, "-u", str(BASE / "hood.py")]),
    ("cash_flow",    [PY, "-u", str(BASE / "cash_flow.py")]),
    ("spy_intraday", [PY, "-u", str(BASE / "spy_intraday.py")]),
    ("spy_daily",    [PY, "-u", str(BASE / "spy_daily.py")]),
]


def main() -> int:
    for name, cmd in STEPS:
        print(f"[STEP] {name} START", flush=True)
        rc = subprocess.run(cmd).returncode
        print(f"[STEP] {name} END exit={rc}", flush=True)
        if rc != 0:
            print(f"[STEP] daily_refresh ABORTED at {name}", flush=True)
            return rc
    print("[STEP] daily_refresh ALL DONE", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
