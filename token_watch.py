#!/usr/bin/env python3
"""Warn before the Robinhood token dies, and alert when it already has.

Runs from cron on the VPS (hourly, daytime). Reads the JWT's `exp` claim for
the countdown, and also asks Robinhood directly via GET /user/ — the only way to
catch a session RH ended before `exp` (logout, security reset). Tokens seen so
far lived several days (the VPS token that broke the 2026-09-29 cron run had
expired Sep 28 5:30 PM ET).

Statuses (worst first):
  missing   no .rh_token file
  rejected  RH answered 401/403 → hood.py will fail
  expired   exp is in the past
  expiring  RH accepts it, but exp is within --warn-hours (default 24)
  ok        RH accepts it and exp is comfortably ahead
  unknown   couldn't reach RH and exp looks fine (network blip; no alert)

Alerts go to Discord (DISCORD_WEBHOOK_URL) and email via `mail` (ALERT_EMAIL),
read from the environment or .env. De-duplicated with a small state file:
  - a bad status alerts when it first appears, then every --repeat-hours
  - "expiring" alerts once per token
  - a fresh working token after a bad/expiring one sends one "recovered" note
  - a first-ever run that finds everything ok stays quiet

Also writes a one-line status to outputs/.token_status for the SSH login
banner (vps/login_banner.sh).
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

BASE = Path(__file__).resolve().parent
TOKEN_FILE = BASE / ".rh_token"
ENV_FILE = BASE / ".env"
STATE_FILE = BASE / "outputs" / ".token_watch.json"
STATUS_FILE = BASE / "outputs" / ".token_status"
USER_URL = "https://api.robinhood.com/user/"
ET = ZoneInfo("America/New_York")
BAD = {"missing", "rejected", "expired"}
ICON = {"ok": "🟢", "expiring": "🟡", "unknown": "⚪", "missing": "🔴", "rejected": "🔴", "expired": "🔴"}

FIX_CMD = "cd ~/rh-trade-exporter && read -rs T && printf '%s\\n' \"${T#Bearer }\" > .rh_token && chmod 600 .rh_token"
FIX = ("Fix: grab a fresh token from robinhood.com DevTools, then on gener run\n"
       f"  {FIX_CMD}\n"
       "(paste the token at the silent prompt, press Enter). Then: .venv/bin/python token_watch.py")


# ──────────────────────────────────────────────
# Token inspection
# ──────────────────────────────────────────────

def read_token(path: Path | None = None) -> str | None:
    path = path or TOKEN_FILE
    if not path.exists():
        return None
    raw = path.read_text().strip()
    if raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    return raw or None


def decode_exp(token: str) -> int | None:
    """JWT `exp` claim, unverified (we only want the timestamp)."""
    m = re.fullmatch(r"[A-Za-z0-9_-]+\.([A-Za-z0-9_-]+)\.[A-Za-z0-9_-]*", token or "")
    if not m:
        return None
    seg = m.group(1) + "=" * (-len(m.group(1)) % 4)
    try:
        exp = json.loads(base64.urlsafe_b64decode(seg)).get("exp")
        return int(exp) if exp is not None else None
    except (ValueError, json.JSONDecodeError):
        return None


def fingerprint(token: str | None) -> str | None:
    return hashlib.sha256(token.encode()).hexdigest()[:12] if token else None


def probe(token: str, session=requests, timeout: float = 15) -> tuple[str, int | None]:
    """('ok'|'rejected'|'error', http_status) from RH's /user/."""
    try:
        r = session.get(USER_URL, headers={"Authorization": f"Bearer {token}", "Accept": "application/json",
                                           "User-Agent": "Mozilla/5.0"}, timeout=timeout)
    except requests.RequestException:
        return "error", None
    if r.status_code == 200:
        return "ok", 200
    if r.status_code in (401, 403):
        return "rejected", r.status_code
    return "error", r.status_code


def evaluate(token: str | None, now: float, probe_result: tuple[str, int | None] | None,
             warn_hours: float = 24) -> dict:
    if not token:
        return {"status": "missing", "exp": None, "hours_left": None, "detail": "no .rh_token file"}
    exp = decode_exp(token)
    hours_left = round((exp - now) / 3600, 1) if exp else None
    res, code = probe_result or ("skipped", None)
    if res == "rejected":
        detail = f"Robinhood rejected it (HTTP {code})"
        if hours_left is not None and hours_left > 0:
            detail += f" even though exp is {hours_left:g}h away (revoked early)"
        return {"status": "rejected", "exp": exp, "hours_left": hours_left, "detail": detail}
    if hours_left is not None and hours_left <= 0:
        return {"status": "expired", "exp": exp, "hours_left": hours_left, "detail": "exp has passed"}
    if res == "error":
        return {"status": "unknown", "exp": exp, "hours_left": hours_left,
                "detail": f"couldn't reach Robinhood ({code or 'network error'})"}
    if hours_left is not None and hours_left <= warn_hours:
        return {"status": "expiring", "exp": exp, "hours_left": hours_left,
                "detail": f"expires in {hours_left:g}h"}
    return {"status": "ok", "exp": exp, "hours_left": hours_left,
            "detail": "accepted by Robinhood" if res == "ok" else "exp looks fine (not probed)"}


# ──────────────────────────────────────────────
# Alert policy
# ──────────────────────────────────────────────

def decide(prev: dict | None, cur: dict, fp: str | None, now: float, repeat_hours: float = 12) -> str | None:
    """Return 'alert', 'recovered', or None."""
    prev = prev or {}
    st, pst = cur["status"], prev.get("status")
    new_token = fp != prev.get("fingerprint")
    since = (now - prev["last_alert_at"]) / 3600 if prev.get("last_alert_at") else None
    if st in BAD:
        if st != pst or new_token or since is None or since >= repeat_hours:
            return "alert"
        return None
    if st == "expiring":
        return "alert" if (pst != "expiring" or new_token) else None
    if st == "ok" and pst in BAD | {"expiring"} and (new_token or pst in BAD):
        return "recovered"
    return None


def fmt_exp(exp: int | None) -> str:
    if not exp:
        return "unknown expiry"
    return datetime.fromtimestamp(exp, ET).strftime("%a %b %-d %-I:%M %p ET")


def status_line(cur: dict, now: float) -> str:
    checked = datetime.fromtimestamp(now, ET).strftime("%a %-I:%M %p")
    exp = f" · exp {fmt_exp(cur['exp'])}" if cur.get("exp") else ""
    return f"{ICON[cur['status']]} RH token {cur['status'].upper()}: {cur['detail']}{exp} · checked {checked}"


def message(kind: str, cur: dict, now: float) -> tuple[str, str]:
    if kind == "recovered":
        subj = "RH token OK again"
        body = f"✅ **Robinhood token is working again.** Valid until {fmt_exp(cur['exp'])}."
    elif cur["status"] == "expiring":
        subj = "RH token expiring soon"
        body = (f"🟡 **Robinhood token expires in {cur['hours_left']:g}h** ({fmt_exp(cur['exp'])}). "
                f"The 4:05 PM cron will fail after that.\n{FIX}")
    else:
        subj = f"RH token {cur['status']}"
        body = (f"🔴 **Robinhood token {cur['status']}** — {cur['detail']}. "
                f"hood.py and option_intraday.py will fail until it's replaced.\n{FIX}")
    return subj, body


# ──────────────────────────────────────────────
# Delivery
# ──────────────────────────────────────────────

def env_value(key: str, env_file: Path | None = None) -> str:
    env_file = env_file or ENV_FILE
    if os.environ.get(key):
        return os.environ[key].strip()
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def notify(subject: str, body: str, webhook: str, email: str, session=requests, run=subprocess.run) -> list[str]:
    sent = []
    if webhook:
        try:
            r = session.post(webhook, json={"content": body[:1900]}, timeout=15)
            if r.status_code < 300:
                sent.append("discord")
        except requests.RequestException:
            pass
    if email:
        try:
            run(["mail", "-s", subject, email], input=body, text=True, timeout=30, check=True)
            sent.append("email")
        except (OSError, subprocess.SubprocessError):
            pass
    return sent


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--no-probe", action="store_true", help="Skip the Robinhood /user/ check (exp claim only)")
    p.add_argument("--warn-hours", type=float, default=24)
    p.add_argument("--repeat-hours", type=float, default=12)
    p.add_argument("--dry-run", action="store_true", help="Evaluate and print, but don't alert or save state")
    p.add_argument("--banner", action="store_true",
                   help="SSH-login mode: live check, print the status (+ fix command if bad); "
                        "never alerts and never touches the alert state")
    p.add_argument("--test-alert", action="store_true",
                   help="Send a test message through the configured channels and report what was delivered")
    p.add_argument("--json", action="store_true")
    a = p.parse_args(argv)

    now = datetime.now(timezone.utc).timestamp()
    if a.test_alert:
        sent = notify("RH token monitor test", "🧪 Test alert from token_watch.py on "
                      f"{os.uname().nodename} — if you can read this, token alerts will reach you.",
                      env_value("DISCORD_WEBHOOK_URL"), env_value("ALERT_EMAIL"))
        print(f"Test alert delivered via: {', '.join(sent) or 'nothing (no DISCORD_WEBHOOK_URL / ALERT_EMAIL configured)'}")
        return 0 if sent else 1

    token = read_token()
    if a.banner:
        cur = evaluate(token, now, probe(token, timeout=4) if token else None, a.warn_hours)
        print(status_line(cur, now))
        if cur["status"] in BAD | {"expiring"}:
            print(f"  Fix: {FIX_CMD}")
        return 0

    cur = evaluate(token, now, None if (a.no_probe or not token) else probe(token), a.warn_hours)
    fp = fingerprint(token)
    try:
        prev = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else None
    except json.JSONDecodeError:
        prev = None
    kind = decide(prev, cur, fp, now, a.repeat_hours)
    line = status_line(cur, now)

    sent = []
    if kind and not a.dry_run:
        subj, body = message(kind, cur, now)
        sent = notify(subj, body, env_value("DISCORD_WEBHOOK_URL"), env_value("ALERT_EMAIL"))
    if not a.dry_run:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        # Only a delivered alert resets the repeat clock; an undelivered one retries next run.
        last = now if (kind and sent) else (prev or {}).get("last_alert_at")
        STATE_FILE.write_text(json.dumps({"status": cur["status"], "fingerprint": fp, "exp": cur["exp"],
                                          "checked_at": now, "last_alert_at": last}))
        STATUS_FILE.write_text(line + "\n")

    if a.json:
        print(json.dumps({**cur, "alert": kind, "sent": sent}))
    else:
        print(line)
        if kind:
            print(f"  → {kind}: sent via {', '.join(sent) or 'nothing (no DISCORD_WEBHOOK_URL / ALERT_EMAIL configured)'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
