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
  --backfill  history from Massive (Polygon) instead of Robinhood: 1-minute bars WITH
              volume (RH's volume field is always 0) for every traded contract-day in
              Massive's ~2-year window, before today. Needs MASSIVE_API_KEY (.env), not
              the RH token. Free tier = 5 requests/minute, one request per contract-day,
              so ~900 contract-days take ~3.5 hours; resumable (--since D, --limit N).
              Raw responses go to outputs/option_massive/{date}.json (the volume record,
              incl. 07:30–09:30 / 16:00–16:15 prints, and what makes it resumable);
              each day's regular-session bars are then merged into
              outputs/option_intraday/ with the same never-fewer-bars rule.
  --json      silent mode, print a JSON summary (cron-friendly)

Output: outputs/option_intraday/{YYYY-MM-DD}.json
  {date, fetched_at, source,
   contracts: [{instrument_id, symbol, type, strike, expiry, interval, source, bars: [{t,o,h,l,c[,v]}]}],
   missing: [{symbol, type, strike, expiry, reason}]}
  t is Unix seconds UTC (same as spy_intraday). Gap-fill ("interpolated") bars
  are dropped — they carry no information. source is "robinhood" or "massive";
  only Massive bars carry v (contracts traded). Massive has no bar for a minute
  without trades, so an illiquid contract can have fewer 1-minute bars than RH's
  5-minute capture — the merge then keeps RH's. Entries written before
  2026-10-31 have no per-contract source (all Robinhood).

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

import spy_intraday

BASE = Path(__file__).resolve().parent
OUT_DIR = BASE / "outputs"
CAPTURE_DIR = OUT_DIR / "option_intraday"
TOKEN_FILE = BASE / ".rh_token"
INSTRUMENT_CACHE_FILE = BASE / ".rh_instrument_cache.json"
HIST_URL = "https://api.robinhood.com/marketdata/options/historicals/{id}/"
ET = ZoneInfo("America/New_York")
CATCHUP_DAYS = 6   # span=week reaches ~7 calendar days back; the 7th is only partly covered
REQUEST_PAUSE = 0.25
MASSIVE_DIR = OUT_DIR / "option_massive"
MASSIVE_URL = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{d}/{d}"
MASSIVE_PAUSE = 13           # free tier: 5 requests/minute
MASSIVE_HORIZON_DAYS = 729   # free tier: ~2 years back; older dates return 403


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


def regular_session(bars: list[dict]) -> list[dict]:
    """09:30 ≤ t < 16:00 ET — what RH returns with bounds=regular."""
    def hm(b):
        t = datetime.fromtimestamp(b["t"], ET)
        return t.hour * 60 + t.minute
    return [b for b in bars if 9 * 60 + 30 <= hm(b) < 16 * 60]


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


def entry_key(c: dict) -> tuple:
    return contract_key(c["symbol"], date.fromisoformat(c["expiry"]), c["type"], c["strike"])


def merge_contracts(old: list[dict], new: list[dict]) -> list[dict]:
    """Per contract, keep whichever version has more bars (never downgrade).

    Keyed by contract, not instrument id: a Massive entry has no RH id when the
    instrument cache lacks it. This is the merge rule analysis/prereg/PREREG.md
    names for the out-of-sample test — don't change it without an amendment."""
    best = {entry_key(c): c for c in old}
    for c in new:
        prev = best.get(entry_key(c))
        if prev is None or len(c["bars"]) > len(prev.get("bars", [])):
            best[entry_key(c)] = c
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
            new.append({"instrument_id": index[key], **meta, "interval": interval, "source": "robinhood", "bars": bars})
        doc = write_merged(d, capture_dir, new, missing, "robinhood")
        contracts, missing = doc["contracts"], doc["missing"]
        n1 = sum(1 for c in contracts if c.get("interval") == "minute")
        summary["dates"][d.isoformat()] = {"contracts": len(contracts), "one_minute": n1, "missing": len(missing)}
        log(f"💾 {d}: {len(contracts)} contracts saved ({n1} at 1-minute), {len(missing)} missing")
    return summary


def write_merged(d: date, capture_dir: Path, new: list[dict], missing: list[dict], source: str) -> dict:
    """Merge new contract entries into the day file (never fewer bars) and save it.
    A contract that ends up with bars is dropped from `missing`; earlier missing
    entries are kept until something covers them."""
    old = read_day_file(d, capture_dir) or {}
    contracts = merge_contracts(old.get("contracts", []), new)
    have = {entry_key(c) for c in contracts if c.get("bars")}
    seen, kept = set(), []
    for m in missing + old.get("missing", []):
        k = entry_key(m)
        if k not in have and k not in seen:
            seen.add(k)
            kept.append(m)
    sources = {c.get("source", "robinhood") for c in contracts}
    doc = {"date": d.isoformat(), "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "source": sources.pop() if len(sources) == 1 else ("mixed" if sources else source),
           "contracts": contracts, "missing": kept}
    capture_dir.mkdir(parents=True, exist_ok=True)
    (capture_dir / f"{d.isoformat()}.json").write_text(json.dumps(doc))
    return doc


# ──────────────────────────────────────────────
# Massive (Polygon) backfill
# ──────────────────────────────────────────────

def occ_ticker(key: tuple) -> str:
    """(SPY, 2026-10-28, call, 771.0) → O:SPY261028C00771000."""
    sym, exp, typ, strike = key
    return f"O:{sym}{date.fromisoformat(exp):%y%m%d}{typ[0].upper()}{round(strike * 1000):08d}"


def fetch_massive(ticker: str, d: date, api_key: str, session=requests, sleep=time.sleep) -> tuple[str, list]:
    """One contract-day of 1-minute bars → ("ok", bars) | ("no_data", []) | ("out_of_plan", []).
    Raises AuthError on a bad key, RuntimeError when it can't get an answer."""
    url = MASSIVE_URL.format(ticker=ticker, d=d.isoformat())
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": api_key}
    for attempt in range(3):
        try:
            r = session.get(url, params=params, timeout=30)
        except requests.RequestException as e:
            if attempt == 2:
                raise RuntimeError(f"{type(e).__name__}") from None
            sleep(5)
            continue
        if r.status_code == 429:
            sleep(60)
            continue
        if r.status_code == 401:
            raise AuthError("HTTP 401 (check MASSIVE_API_KEY)")
        if r.status_code == 403:
            return "out_of_plan", []
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")
        bars = [{"t": int(b["t"] // 1000), "o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"], "v": b.get("v", 0)}
                for b in r.json().get("results") or []]
        return ("ok" if bars else "no_data"), bars_on(bars, d)
    raise RuntimeError("rate-limited 3 times")


def backfill(held: dict[date, set], index: dict[tuple, str], api_key: str, today: date,
             since: date | None = None, limit: int | None = None, massive_dir: Path | None = None,
             capture_dir: Path | None = None, log=print, fetch=fetch_massive, sleep=time.sleep) -> dict:
    """Fetch every not-yet-fetched contract-day from Massive, then merge each day into
    the capture files. "no_data" answers are recorded so they aren't asked again;
    403s and errors aren't, so a later run retries them."""
    massive_dir = massive_dir or MASSIVE_DIR
    capture_dir = capture_dir or CAPTURE_DIR
    massive_dir.mkdir(parents=True, exist_ok=True)
    since = max(since or date.min, today - timedelta(days=MASSIVE_HORIZON_DAYS))
    days = [d for d in sorted(held) if since <= d < today]

    todo = []
    for d in days:
        doc = read_day_file(d, massive_dir) or {}
        done = {entry_key(c) for c in doc.get("contracts", []) + doc.get("no_data", [])}
        todo += [(d, k) for k in sorted(held[d]) if k not in done]
    if limit is not None:
        todo = todo[:limit]
    log(f"🗓  Massive backfill {since} → {today - timedelta(days=1)}: {len(todo)} contract-days to fetch"
        f" (~{len(todo) * MASSIVE_PAUSE / 3600:.1f}h at 5/min)")

    summary = {"fetched": 0, "bars": 0, "no_data": 0, "out_of_plan": 0, "errors": 0, "dates": 0}
    by_day: dict[date, list] = {}
    for d, k in todo:
        by_day.setdefault(d, []).append(k)
    first = True
    for d in days:
        if d in by_day:
            doc = read_day_file(d, massive_dir) or {"contracts": [], "no_data": []}
            for k in by_day[d]:
                if not first:
                    sleep(MASSIVE_PAUSE)
                first = False
                sym, exp, typ, strike = k
                meta = {"ticker": occ_ticker(k), "symbol": sym, "type": typ, "strike": strike, "expiry": exp}
                try:
                    status, bars = fetch(meta["ticker"], d, api_key)
                except RuntimeError as e:
                    summary["errors"] += 1
                    log(f"  ⚠ {meta['ticker']} {d}: {e} — will retry next run")
                    continue
                summary["fetched"] += 1
                if status == "ok":
                    doc["contracts"].append({**meta, "interval": "minute", "bars": bars})
                    summary["bars"] += len(bars)
                elif status == "no_data":
                    doc["no_data"].append(meta)
                    summary["no_data"] += 1
                else:
                    summary["out_of_plan"] += 1
                log(f"  📈 {meta['ticker']} {d}: {len(bars)} bars" + ("" if status == "ok" else f" ({status})"))
            doc.update(date=d.isoformat(), source="massive",
                       fetched_at=datetime.now(timezone.utc).isoformat(timespec="seconds"))
            (massive_dir / f"{d.isoformat()}.json").write_text(json.dumps(doc))
        if merge_massive_day(d, index, massive_dir, capture_dir):
            summary["dates"] += 1
    return summary


def merge_massive_day(d: date, index: dict[tuple, str], massive_dir: Path, capture_dir: Path) -> bool:
    """Fold a day's Massive bars into its capture file. False when there's nothing to fold.

    Only regular-session bars are folded in. Massive also has 07:30–09:30 and
    16:00–16:15 prints, and counting those would let a Massive series with gaps in
    the session "beat" RH's complete 1-minute capture in the never-fewer merge.
    The raw file keeps every bar."""
    doc = read_day_file(d, massive_dir)
    new = []
    for c in (doc or {}).get("contracts", []):
        bars = regular_session(c["bars"])
        if bars:
            new.append({"instrument_id": index.get(entry_key(c), c["ticker"]),
                        **{f: c[f] for f in ("symbol", "type", "strike", "expiry", "interval")},
                        "source": "massive", "bars": bars})
    if not new:
        return False
    write_merged(d, capture_dir, new, [], "massive")
    return True


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--date", help="Refetch one date (YYYY-MM-DD); merges, never downgrades")
    p.add_argument("--backfill", action="store_true", help="History from Massive: 1-minute bars with volume")
    p.add_argument("--since", help="--backfill: earliest date (YYYY-MM-DD); default = Massive's 2-year window")
    p.add_argument("--limit", type=int, help="--backfill: at most N contract-days this run")
    p.add_argument("--json", action="store_true", help="Silent mode, print summary as JSON")
    a = p.parse_args(argv)
    log = (lambda *_: None) if a.json else print

    if a.backfill:
        key = spy_intraday.load_polygon_key()
        if not key:
            print("❌ No MASSIVE_API_KEY in the environment or .env.", file=sys.stderr)
            return 2
        today = datetime.now(ET).date()
        try:
            summary = backfill(contracts_by_date(OUT_DIR, today), instrument_index(INSTRUMENT_CACHE_FILE), key,
                               today, date.fromisoformat(a.since) if a.since else None, a.limit, log=log)
        except AuthError as e:
            print(f"❌ Massive rejected the key ({e}).", file=sys.stderr)
            return 2
        log(f"✅ {summary['fetched']} fetched ({summary['bars']} bars, {summary['no_data']} with no trades,"
            f" {summary['out_of_plan']} out of plan), {summary['errors']} errors; {summary['dates']} day files merged")
        if a.json:
            print(json.dumps(summary))
        return 1 if summary["errors"] else 0

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
