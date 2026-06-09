"""Pytest shim so `pytest tests/` (CI + local) also runs the JS leg-render tests.

The actual assertions live in tests/test_legs.mjs (node), which extracts and
exercises the real buildLegs + event-render code from static/views/calendar.html.
This wrapper just shells out to node and surfaces its output. Skips if node is
unavailable so a Python-only environment doesn't hard-fail.
"""
import shutil
import subprocess
from pathlib import Path

import pytest

MJS = Path(__file__).parent / "test_legs.mjs"


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_leg_render_js():
    result = subprocess.run(
        ["node", str(MJS)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(f"node {MJS.name} failed:\n{result.stdout}\n{result.stderr}")
