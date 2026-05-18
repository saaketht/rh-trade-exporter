#!/usr/bin/env python3
"""SPY intraday 5-minute bars from Polygon (massive.com).

Caches one day of bars per file at outputs/spy_intraday/{YYYY-MM-DD}.json.
Used by the calendar's day modal to render a candle chart with premarket H/L
overlay lines and entry/exit markers.

Polygon free-tier constraints (verified by probe — see
plans/calendar-modal-roadmap.md Step 4):
- 5 requests per minute  → 13s sleep between calls
- ~2 years of rolling history (anything older returns 403 NOT_AUTHORIZED)
- ~100-120 bars per response → pagination required for full 04:00-20:00 days
- Pre/post-market bars included (04:00 ET to 20:00 ET when available)

API key is read from .env (MASSIVE_API_KEY=...). Never logged.

Usage:
    python spy_intraday.py                      # default: yesterday + today
    python spy_intraday.py --date 2026-05-13   # one specific day
    python spy_intraday.py --backfill          # all missing weekdays within 2y
    python spy_intraday.py --backfill --limit 30  # max 30 days this run
    python spy_intraday.py --json              # silent mode (cron-friendly)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = SCRIPT_DIR / "outputs"
CACHE_DIR = OUTPUTS_DIR / "spy_intraday"
ENV_FILE = SCRIPT_DIR / ".env"

POLYGON_BASE = "https://api.polygon.io"
RATE_LIMIT_SLEEP = 13  # seconds between requests; 5/min ceiling on free tier
# Polygon free-tier rolling window — strictly less than 2 years back
PLAN_HORIZON_DAYS = 365 * 2


# ───────────────────────────────────────────────
# Auth
# ───────────────────────────────────────────────

def load_polygon_key() -> str | None:
    """Read MASSIVE_API_KEY from .env. Returns None if missing."""
    env = os.environ.get("MASSIVE_API_KEY")
    if env:
        return env.strip()
    if not ENV_FILE.exists():
        return None
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        if k.strip() == "MASSIVE_API_KEY":
            return v.strip().strip('"').strip("'")
    return None


# ───────────────────────────────────────────────
# Fetch
# ───────────────────────────────────────────────

def fetch_day(target: str, api_key: str, log=print) -> dict:
    """Fetch all 5m bars for one date with pagination + rate limiting.

    Returns a payload dict shaped for cache storage:
        - On success:        {date, fetched_at, source, interval, bars: [...]}
        - On out-of-plan:    {date, available: False, reason: "out_of_plan"}
        - On empty (holiday/weekend): {date, available: False, reason: "no_data"}
        - On error:          {date, available: False, reason: "error", message}

    Each pagination hop counts against the rate limit — caller is responsible
    for spacing between *separate dates*, but this function paces internally
    when next_url is hit.
    """
    url = (f"{POLYGON_BASE}/v2/aggs/ticker/SPY/range/5/minute/{target}/{target}"
           f"?adjusted=true&sort=asc&limit=5000&apiKey={api_key}")
    all_bars: list[dict] = []
    hop = 0
    while url:
        if hop > 0:
            log(f"    paginating (next_url, hop #{hop}) — sleeping {RATE_LIMIT_SLEEP}s")
            time.sleep(RATE_LIMIT_SLEEP)
        # Retry once on transport errors (e.g. read timeout). Polygon occasionally
        # drops a request mid-fetch; a single retry after a short backoff recovers
        # almost all of these without polluting the cache with an error stub.
        r = None
        last_err = None
        for attempt in range(2):
            try:
                r = requests.get(url, timeout=30)
                last_err = None
                break
            except requests.RequestException as e:
                last_err = e
                if attempt == 0:
                    log(f"    ⚠ {type(e).__name__}: {str(e)[:80]} — retrying in 5s")
                    time.sleep(5)
        if last_err is not None:
            return {"date": target, "available": False, "reason": "error",
                    "message": str(last_err)}
        if r.status_code == 403:
            return {"date": target, "available": False, "reason": "out_of_plan",
                    "message": r.json().get("message", "")[:200]}
        if r.status_code == 429:
            log(f"    ⚠ 429 rate-limited; sleeping 60s and retrying once")
            time.sleep(60)
            r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return {"date": target, "available": False, "reason": "error",
                    "message": f"HTTP {r.status_code}: {r.text[:200]}"}
        body = r.json()
        results = body.get("results") or []
        for bar in results:
            all_bars.append({
                # Polygon: t = ms since epoch UTC. Store in seconds to match
                # Lightweight Charts' UTCTimestamp format directly.
                "t": int(bar["t"] // 1000),
                "o": bar["o"], "h": bar["h"], "l": bar["l"], "c": bar["c"],
                "v": bar.get("v", 0),
            })
        url = body.get("next_url")
        if url and "apiKey=" not in url:
            url = url + ("&" if "?" in url else "?") + f"apiKey={api_key}"
        hop += 1

    if not all_bars:
        return {"date": target, "available": False, "reason": "no_data"}
    return {
        "date": target,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "polygon",
        "interval": "5m",
        "bars": all_bars,
    }


def write_cache(payload: dict) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{payload['date']}.json"
    path.write_text(json.dumps(payload))
    return path


# ───────────────────────────────────────────────
# Date selection
# ───────────────────────────────────────────────

def weekdays_between(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:  # Mon=0..Fri=4
            yield d
        d += timedelta(days=1)


def missing_dates(start: date, end: date) -> list[date]:
    """Weekdays in [start, end] for which we don't already have a cache file."""
    out = []
    for d in weekdays_between(start, end):
        if not (CACHE_DIR / f"{d.isoformat()}.json").exists():
            out.append(d)
    return out


def default_targets() -> list[date]:
    """Previous trading day + today. On Monday, the previous trading day is
    Friday; on Tue–Fri it's the day before. Today is included only if it's a
    weekday (weekend runs just refresh the prior Friday)."""
    today = date.today()
    prev = today - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    targets = [prev]
    if today.weekday() < 5 and today != prev:
        targets.append(today)
    return targets


# ───────────────────────────────────────────────
# Orchestration
# ───────────────────────────────────────────────

def run(targets: list[date], api_key: str, log=print) -> dict:
    """Fetch each target date, sleeping between *separate dates* to respect
    the 5-req/min ceiling. Returns a summary dict.

    Special-case: Polygon's free tier serves end-of-day data, not real-time.
    Today's bars typically aren't available until after market close + some
    settlement delay. If `target == today` AND the response is out_of_plan or
    error, we DON'T persist the stub — that would block retry on the next run.
    """
    today_iso = date.today().isoformat()
    summary = {"fetched": 0, "out_of_plan": 0, "no_data": 0, "errors": 0,
               "skipped_cached": 0, "deferred": 0, "total": len(targets)}
    for i, d in enumerate(targets):
        ds = d.isoformat()
        cache_path = CACHE_DIR / f"{ds}.json"
        if cache_path.exists():
            log(f"  [{i+1}/{len(targets)}] {ds} cached — skip")
            summary["skipped_cached"] += 1
            continue
        if i > 0:
            log(f"  ⏱ rate limit: sleeping {RATE_LIMIT_SLEEP}s")
            time.sleep(RATE_LIMIT_SLEEP)
        log(f"  [{i+1}/{len(targets)}] fetching {ds}…")
        payload = fetch_day(ds, api_key, log=log)
        reason = payload.get("reason")
        # For TODAY, treat out_of_plan/error as "not yet ready" — skip the cache
        # write so the next run retries. Older dates that come back out_of_plan
        # really are beyond the 2y window; persist them to avoid wasteful retries.
        if ds == today_iso and reason in ("out_of_plan", "error"):
            summary["deferred"] += 1
            log(f"    · today's data not ready yet (deferred for retry)")
            continue
        write_cache(payload)
        if reason == "out_of_plan":
            summary["out_of_plan"] += 1
            log(f"    ✗ out of plan (>2y back)")
        elif reason == "no_data":
            summary["no_data"] += 1
            log(f"    · no data (holiday/weekend)")
        elif reason == "error":
            summary["errors"] += 1
            log(f"    ⚠ error: {payload.get('message', '')[:120]}")
        else:
            summary["fetched"] += 1
            log(f"    ✓ {len(payload['bars'])} bars")
    return summary


def main(as_json: bool = False, mode: str = "default",
         since_iso: str | None = None, date_iso: str | None = None,
         limit: int | None = None) -> None:
    log = (lambda *a, **k: None) if as_json else print
    key = load_polygon_key()
    if not key:
        print("❌ MASSIVE_API_KEY not found in .env or environment.", file=sys.stderr)
        sys.exit(1)

    if mode == "date":
        try:
            d = datetime.strptime(date_iso, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            print(f"❌ bad --date (need YYYY-MM-DD): {date_iso}", file=sys.stderr)
            sys.exit(1)
        # --date forces a re-fetch even if cached. Bypass run()'s skip logic.
        log(f"📈 SPY intraday — forcing fetch of {d.isoformat()}")
        if not as_json:
            cache_path = CACHE_DIR / f"{d.isoformat()}.json"
            if cache_path.exists():
                log(f"  (overwriting existing cache)")
        payload = fetch_day(d.isoformat(), key, log=log)
        write_cache(payload)
        bars = len(payload.get("bars") or [])
        log(f"  ✓ {bars} bars" if bars else f"  · {payload.get('reason')}")
        if as_json:
            print(json.dumps({"date": d.isoformat(), "bars": bars,
                              "reason": payload.get("reason")}))
        return

    if mode == "backfill":
        # Start = strictly more recent than today − 2y (Polygon's exclusive boundary).
        today = date.today()
        floor = today - timedelta(days=PLAN_HORIZON_DAYS - 1)
        start = floor
        if since_iso:
            try:
                start = max(floor, datetime.strptime(since_iso, "%Y-%m-%d").date())
            except ValueError:
                print(f"❌ bad --since: {since_iso}", file=sys.stderr)
                sys.exit(1)
        targets = missing_dates(start, today)
        if limit:
            targets = targets[:limit]
        log(f"📈 SPY intraday backfill — {len(targets)} missing weekdays "
            f"({start} → {today}, ~{len(targets) * RATE_LIMIT_SLEEP / 60:.1f}m wall-time)")
        summary = run(targets, key, log=log)
    else:
        targets = default_targets()
        log(f"📈 SPY intraday — refreshing {[d.isoformat() for d in targets]}")
        # Default mode also bypasses the cache-skip so latest data wins on rerun.
        # Delete any existing cache for these days, then run.
        for d in targets:
            (CACHE_DIR / f"{d.isoformat()}.json").unlink(missing_ok=True)
        summary = run(targets, key, log=log)

    log(f"\n📊 done: {summary}")
    if as_json:
        print(json.dumps(summary))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group()
    g.add_argument("--date", help="Fetch one specific date (YYYY-MM-DD), force overwrite")
    g.add_argument("--backfill", action="store_true",
                   help="Walk all missing weekdays within Polygon's 2y window")
    p.add_argument("--since", help="With --backfill: earliest date (default: 2y floor)")
    p.add_argument("--limit", type=int,
                   help="With --backfill: max days per run (lets you spread across sessions)")
    p.add_argument("--json", action="store_true", help="Silent mode, print summary as JSON")
    args = p.parse_args()
    if args.date:
        main(as_json=args.json, mode="date", date_iso=args.date)
    elif args.backfill:
        main(as_json=args.json, mode="backfill", since_iso=args.since, limit=args.limit)
    else:
        main(as_json=args.json, mode="default")
