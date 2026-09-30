#!/usr/bin/env python3
"""Nightly capture of intraday price bars for the option contracts you traded.

Why: Robinhood's option price history is only reachable for a short time.
Checked 2026-09-30 against RH's /marketdata/options/historicals/ endpoint:
  interval=minute   only with span=day   → today's session only
  interval=5minute  only with span=week  → roughly the last 7 calendar days
  interval=hour     with span=month      → ~3 weeks (too coarse for 30-min analysis)
So today's contracts are captured at 1-minute resolution the same evening, and
any day a failed run missed is caught up at 5-minute while still in reach. The
result is a permanent per-contract price record — what's needed to measure what
a stop at X% or a 30-minute cut would really have saved (the split in
/api/trades/split only has SPY's direction).

Modes:
  default     today (1-minute) + any trading day in the last 6 calendar days whose
              file is missing or incomplete (5-minute; self-heals after a missed
              run or a dead token)
  --date D    refetch one date (still merges: never replaces good bars with worse)
  --json      silent mode, print a JSON summary (cron-friendly)

Output: outputs/option_intraday/{YYYY-MM-DD}.json
  {date, fetched_at, source,
   contracts: [{instrument_id, symbol, type, strike, expiry, interval, bars: [{t,o,h,l,c}]}],
   missing: [{symbol, type, strike, expiry, reason}]}
  t is Unix seconds UTC (same as spy_intraday). Gap-fill ("interpolated") bars
  are dropped — they carry no information.

Contracts come from spy_trades.csv + other_trades.csv (entry day through exit
day) and unmatched_opens.csv (entry day through expiry). Instrument ids come
from .rh_instrument_cache.json, which hood.py maintains.

Exit codes: 0 ok, 2 token rejected (run hood.py's token refresh first), 1 other.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

BASE = Path(__file__).resolve().parent
OUT_DIR = BASE / "outputs"
CAPTURE_DIR = OUT_DIR / "option_intraday"
TOKEN_FILE = BASE / ".rh_token"
INSTRUMENT_CACHE_FILE = BASE / ".rh_instrument_cache.json"
HIST_URL = "https://api.robinhood.com/marketdata/options/historicals/{id}/"
ET = ZoneInfo("America/New_York")
CATCHUP_DAYS = 6   # span=week reaches ~7 calendar days back; the 7th is only partly covered
REQUEST_PAUSE = 0.25


class AuthError(Exception):
    pass


# ──────────────────────────────────────────────
# Parsing helpers
# ──────────────────────────────────────────────

def parse_day(s: str | None) -> date | None:
    """M/D/YYYY or YYYY-MM-DD → date."""
    if not s:
        return None
    s = s.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def contract_key(symbol, expiry: date, typ, strike) -> tuple:
    return ((symbol or "").upper(), expiry.isoformat(), (typ or "").lower(), round(float(strike), 4))


def weekdays(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def contracts_by_date(out_dir: Path, today: date) -> dict[date, set]:
    """Which contracts were held on which trading day."""
    held: dict[date, set] = {}

    def add(d0: date, d1: date, key: tuple):
        for d in weekdays(d0, min(d1, today)):
            held.setdefault(d, set()).add(key)

    for name in ("spy_trades.csv", "other_trades.csv"):
        for r in _read_csv(out_dir / name):
            d0, exp = parse_day(r.get("Date")), parse_day(r.get("Expiry Date"))
            if not d0 or not exp or not r.get("Strike"):
                continue
            d1 = d0
            try:
                tm = (r.get("Entry Time") or "00:00:00").strip()
                entry = datetime.fromisoformat(f"{d0.isoformat()}T{tm if tm.count(':') == 2 else tm + ':00'}")
                d1 = (entry + timedelta(minutes=float(r.get("Hold Time (min)") or 0))).date()
            except ValueError:
                pass
            add(d0, d1, contract_key(r.get("Symbol"), exp, r.get("Type"), r["Strike"]))

    for r in _read_csv(out_dir / "unmatched_opens.csv"):
        d0 = parse_day(r.get("Date"))
        exp = parse_day(r.get("Expiry") or r.get("Expiry Date"))
        if d0 and exp and r.get("Strike"):
            add(d0, exp, contract_key(r.get("Symbol"), exp, r.get("Type"), r["Strike"]))
    return held


def instrument_index(cache_path: Path) -> dict[tuple, str]:
    """(symbol, expiry, type, strike) → instrument id, from hood.py's cache."""
    if not cache_path.exists():
        return {}
    try:
        cache = json.loads(cache_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}
    idx = {}
    for url, v in cache.items():
        if not isinstance(v, dict):
            continue
        exp = parse_day(v.get("expiration_date"))
        iid = v.get("id") or url.rstrip("/").split("/")[-1]
        if exp and v.get("strike_price") and v.get("type"):
            idx[contract_key(v.get("chain_symbol"), exp, v.get("type"), v["strike_price"])] = iid
    return idx


def normalize_bars(payload: dict) -> list[dict]:
    """RH historicals payload → [{t,o,h,l,c}] sorted, interpolated bars dropped."""
    points = payload.get("data_points") or payload.get("bars")
    if points is None and payload.get("results"):
        first = payload["results"][0] or {}
        points = first.get("data_points") or first.get("bars")
    out = []
    for p in points or []:
        if p.get("interpolated"):
            continue
        try:
            t = datetime.fromisoformat(p["begins_at"].replace("Z", "+00:00"))
            out.append({"t": int(t.timestamp()), "o": float(p["open_price"]), "h": float(p["high_price"]),
                        "l": float(p["low_price"]), "c": float(p["close_price"])})
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda b: b["t"])
    return out


def bars_on(bars: list[dict], d: date) -> list[dict]:
    return [b for b in bars if datetime.fromtimestamp(b["t"], ET).date() == d]


# ──────────────────────────────────────────────
# I/O
# ──────────────────────────────────────────────

def load_token(path: Path | None = None) -> str | None:
    path = path or TOKEN_FILE
    if not path.exists():
        return None
    raw = path.read_text().strip()
    if raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    return raw or None


def fetch_bars(instrument_id: str, token: str, interval: str = "5minute", span: str = "week",
               session=requests) -> list[dict]:
    """RH allows minute+day and 5minute+week (and rejects minute+week with a 400)."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    params = {"interval": interval, "span": span, "bounds": "regular"}
    for attempt in range(3):
        r = session.get(HIST_URL.format(id=instrument_id), headers=headers, params=params, timeout=20)
        if r.status_code in (401, 403):
            raise AuthError(f"HTTP {r.status_code}")
        if r.status_code in (429, 500, 502, 503):
            time.sleep(int(r.headers.get("Retry-After", 2 ** attempt)))
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return normalize_bars(r.json())
    return []


def read_day_file(d: date, capture_dir: Path) -> dict | None:
    p = capture_dir / f"{d.isoformat()}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError:
        return None


def captured_keys(doc: dict | None) -> set:
    if not doc:
        return set()
    return {contract_key(c["symbol"], date.fromisoformat(c["expiry"]), c["type"], c["strike"])
            for c in doc.get("contracts", []) if c.get("bars")}


def merge_contracts(old: list[dict], new: list[dict]) -> list[dict]:
    """Per contract, keep whichever version has more bars (never downgrade)."""
    best = {c["instrument_id"]: c for c in old}
    for c in new:
        prev = best.get(c["instrument_id"])
        if prev is None or len(c["bars"]) > len(prev.get("bars", [])):
            best[c["instrument_id"]] = c
    return sorted(best.values(), key=lambda c: (c["symbol"], c["expiry"], c["type"], c["strike"]))


# ──────────────────────────────────────────────
# Main flow
# ──────────────────────────────────────────────

def pick_targets(held: dict[date, set], today: date, capture_dir: Path, force: date | None = None,
                 index: dict | None = None) -> list[date]:
    """Dates to (re)capture: today, plus recent days with a fetchable contract not yet saved."""
    if force:
        return [force] if force in held else []
    out = []
    for d in sorted(held):
        if d < today - timedelta(days=CATCHUP_DAYS) or d > today:
            continue
        want = {k for k in held[d] if index is None or k in index}
        if d == today or not want <= captured_keys(read_day_file(d, capture_dir)):
            out.append(d)
    return out


def run(targets: list[date], held: dict[date, set], index: dict[tuple, str], token: str,
        capture_dir: Path | None = None, today: date | None = None, log=print, fetch=fetch_bars) -> dict:
    capture_dir = capture_dir or CAPTURE_DIR
    today = today or datetime.now(ET).date()
    capture_dir.mkdir(parents=True, exist_ok=True)
    want_today = set(held.get(today, set())) if today in targets else set()
    want_past = {k for d in targets if d != today for k in held.get(d, set())}

    minute: dict[tuple, list] = {}   # today's session, 1-minute
    five: dict[tuple, list] = {}     # last ~week, 5-minute
    keys = sorted(k for k in want_today | want_past if k in index)
    for i, key in enumerate(keys):
        iid = index[key]
        if key in want_today:
            minute[key] = fetch(iid, token, "minute", "day")
        if key in want_past or (key in want_today and not minute.get(key)):
            five[key] = fetch(iid, token, "5minute", "week")
        got = f"{len(minute.get(key, []))} 1m" if key in minute else ""
        got += (", " if got and key in five else "") + (f"{len(five[key])} 5m" if key in five else "")
        log(f"  📈 {key[0]} {key[3]:g}{key[2][0].upper()} {key[1]}: {got} bars")
        if i < len(keys) - 1:
            time.sleep(REQUEST_PAUSE)

    summary = {"dates": {}, "contracts_fetched": len(keys)}
    for d in targets:
        new, missing = [], []
        for key in sorted(held.get(d, set())):
            sym, exp, typ, strike = key
            meta = {"symbol": sym, "type": typ, "strike": strike, "expiry": exp}
            if key not in index:
                missing.append({**meta, "reason": "no instrument id in cache"})
                continue
            bars, interval = [], None
            if d == today and minute.get(key):
                bars, interval = bars_on(minute[key], d), "minute"
            if not bars:
                bars, interval = bars_on(five.get(key, []), d), "5minute"
            if not bars:
                missing.append({**meta, "reason": "no bars returned (out of reach or no trades)"})
                continue
            new.append({"instrument_id": index[key], **meta, "interval": interval, "bars": bars})
        old = read_day_file(d, capture_dir) or {}
        contracts = merge_contracts(old.get("contracts", []), new)
        have = {c["instrument_id"] for c in contracts}
        missing = [m for m in missing if index.get(contract_key(m["symbol"], date.fromisoformat(m["expiry"]), m["type"], m["strike"])) not in have]
        doc = {"date": d.isoformat(), "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "source": "robinhood", "contracts": contracts, "missing": missing}
        (capture_dir / f"{d.isoformat()}.json").write_text(json.dumps(doc))
        n1 = sum(1 for c in contracts if c.get("interval") == "minute")
        summary["dates"][d.isoformat()] = {"contracts": len(contracts), "one_minute": n1, "missing": len(missing)}
        log(f"💾 {d}: {len(contracts)} contracts saved ({n1} at 1-minute), {len(missing)} missing")
    return summary


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--date", help="Refetch one date (YYYY-MM-DD); merges, never downgrades")
    p.add_argument("--json", action="store_true", help="Silent mode, print summary as JSON")
    a = p.parse_args(argv)
    log = (lambda *_: None) if a.json else print

    token = load_token()
    if not token:
        print("❌ No .rh_token — save one first (see hood.py --save-token).", file=sys.stderr)
        return 2
    today = datetime.now(ET).date()
    held = contracts_by_date(OUT_DIR, today)
    index = instrument_index(INSTRUMENT_CACHE_FILE)
    force = date.fromisoformat(a.date) if a.date else None
    targets = pick_targets(held, today, CAPTURE_DIR, force, index)
    if not targets:
        log("✅ Nothing to capture — every recent trading day is already complete.")
        if a.json:
            print(json.dumps({"dates": {}, "contracts_fetched": 0}))
        return 0
    log(f"🗓  Capturing option bars for {', '.join(d.isoformat() for d in targets)}")
    try:
        summary = run(targets, held, index, token, log=log)
    except AuthError as e:
        print(f"❌ Robinhood rejected the token ({e}). Refresh .rh_token, then rerun.", file=sys.stderr)
        return 2
    if a.json:
        print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
