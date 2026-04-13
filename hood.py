"""
Robinhood Options Trade Exporter
=================================
Pulls options orders via Robinhood's undocumented API, pairs entries/exits (FIFO),
enriches with daily OHLC, intraday VWAP/EMA, VIX, greeks, and outputs CSVs.

OUTPUT FILES (in --output-dir):
  spy_trades.csv         SPY round-trip trades (each close = one row)
  other_trades.csv       Non-SPY round-trip trades
  unmatched_opens.csv    Open positions with no matching close
  cancelled.csv          Cancelled orders
  rejected.csv           Rejected orders
  failed.csv             Failed orders (if any)

SETUP:
  pip install requests yfinance pandas

AUTH TOKEN (priority order):
  1. --token flag          python hood.py --token "Bearer ..."
  2. $RH_TOKEN env var     export RH_TOKEN="..."
  3. .rh_token file        python hood.py --save-token (on first run)
  4. --token-stdin         pbpaste | python hood.py --token-stdin

EXAMPLES:
  python hood.py --token "Bearer ..." --save-token         # first run
  python hood.py --after-date 2026-03-10 --symbol SPY      # daily export
  python hood.py --start 2026-01-01                         # full history
  python hood.py --dump-raw                                 # debug
"""

import argparse
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict, Counter
from pathlib import Path
import time as time_module
import sys
import json
import os

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
BASE_URL = "https://api.robinhood.com"
OPTIONS_ORDERS_URL = f"{BASE_URL}/options/orders/"
ACCOUNTS_URL = f"{BASE_URL}/accounts/"
SCRIPT_DIR = Path(__file__).resolve().parent
ACCT_CACHE_FILE = SCRIPT_DIR / ".rh_accounts.json"
TOKEN_FILE = SCRIPT_DIR / ".rh_token"
INSTRUMENT_CACHE_FILE = SCRIPT_DIR / ".rh_instrument_cache.json"

_instrument_cache = {}


def make_headers(token: str) -> dict:
    return {
        "Authorization": token,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }


def validate_token(raw: str) -> str:
    """Validate token is non-empty and return normalized 'Bearer ...' string."""
    token = raw.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        print("\n❌ Token is empty.")
        sys.exit(1)
    return f"Bearer {token}"


def save_token(token: str):
    """Save bare token (without Bearer prefix) to .rh_token with restrictive permissions."""
    bare = token[7:].strip() if token.startswith("Bearer ") else token.strip()
    TOKEN_FILE.write_text(bare + "\n")
    TOKEN_FILE.chmod(0o600)
    print(f"  💾 Token saved to {TOKEN_FILE}")


def resolve_token(args) -> str:
    """
    Resolve auth token in priority order:
    1. --token flag (explicit always wins)
    2. $RH_TOKEN env var
    3. .rh_token file
    4. --token-stdin (piped input)

    Returns validated 'Bearer ...' string.
    """
    raw = None
    source = None

    if args.token:
        raw = args.token
        source = "--token flag"
    elif os.environ.get("RH_TOKEN"):
        raw = os.environ["RH_TOKEN"]
        source = "$RH_TOKEN env"
    elif TOKEN_FILE.exists():
        raw = TOKEN_FILE.read_text().strip()
        if raw:
            source = str(TOKEN_FILE)
        else:
            raw = None
    elif args.token_stdin:
        if sys.stdin.isatty():
            print("  Paste your token (then Enter):")
        raw = sys.stdin.readline().strip()
        if raw:
            source = "stdin"

    if not raw:
        print("\n❌ No auth token provided. Use one of:")
        print(f"   --token 'Bearer <token>'")
        print(f"   export RH_TOKEN='<token>'")
        print(f"   Save to {TOKEN_FILE}")
        print(f"   echo '<token>' | python hood.py --token-stdin")
        sys.exit(1)

    token = validate_token(raw)
    print(f"  🔑 Token from {source}")

    # Offer to save if not already file-sourced
    if source != str(TOKEN_FILE) and args.save_token:
        save_token(token)

    return token


def get_with_retry(url: str, headers: dict, max_retries: int = 3) -> requests.Response:
    """GET with exponential backoff on 429/5xx."""
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers)
        if resp.status_code in (429, 500, 502, 503):
            wait = int(resp.headers.get("Retry-After", 2 ** attempt))
            print(f"  ⏳ HTTP {resp.status_code} — retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
            time_module.sleep(wait)
            continue
        return resp
    return resp


# ──────────────────────────────────────────────
# ACCOUNT DISCOVERY + CACHING
# ──────────────────────────────────────────────
def load_cached_accounts() -> list[str]:
    """Load previously-saved account numbers from cache file."""
    if ACCT_CACHE_FILE.exists():
        try:
            data = json.loads(ACCT_CACHE_FILE.read_text())
            nums = data.get("account_numbers", [])
            if nums:
                print(f"  📁 Loaded cached account numbers from {ACCT_CACHE_FILE}: {', '.join(nums)}")
                return nums
        except Exception:
            pass
    return []


def save_cached_accounts(account_numbers: list[str]):
    """Cache account numbers for future runs."""
    ACCT_CACHE_FILE.write_text(json.dumps({"account_numbers": account_numbers}, indent=2))
    print(f"  💾 Saved account numbers to {ACCT_CACHE_FILE} for future runs")


def fetch_accounts_from_api(headers: dict) -> list[dict]:
    """Fetch accounts from /accounts/ (may not return cash sub-accounts)."""
    resp = get_with_retry(ACCOUNTS_URL, headers=headers)

    if resp.status_code == 401:
        print("\n❌ AUTH FAILED — token is expired or invalid.")
        print("   Grab a fresh one from DevTools → Network → Authorization header.")
        sys.exit(1)

    resp.raise_for_status()
    accounts = resp.json().get("results", [])

    print(f"  /accounts/ returned {len(accounts)} account(s):")
    for i, acct in enumerate(accounts):
        acct_num = acct.get("account_number", "???")
        acct_type = acct.get("type", "unknown")
        buying_power = acct.get("buying_power", "?")
        print(f"    [{i+1}] {acct_num} ({acct_type}) — buying power: ${float(buying_power):,.2f}")
    print()

    return accounts


def discover_account_numbers(headers: dict, manual_override: str = None) -> list[str]:
    """
    Determine which account numbers to use, in priority order:
    1. Manual override (--account-numbers flag)
    2. Cached from previous run (.rh_accounts.json)
    3. /accounts/ API endpoint (limited — misses cash sub-accounts)

    If manual override is provided, it's cached for future runs.
    """
    # 1. Manual override
    if manual_override:
        nums = [n.strip() for n in manual_override.split(",") if n.strip()]
        print(f"  → Manual override: {', '.join(nums)}")
        save_cached_accounts(nums)
        return nums

    # 2. Try cache — if present, use it directly (skip noisy /accounts/ API call)
    cached = load_cached_accounts()
    if cached:
        print(f"  → Using cached accounts: {', '.join(cached)}")
        return cached

    # 3. Fall back to /accounts/ API (limited — misses cash sub-accounts)
    api_accounts = fetch_accounts_from_api(headers)
    api_nums = [a.get("account_number") for a in api_accounts if a.get("account_number")]

    if api_nums:
        print(f"  → Using API-discovered accounts: {', '.join(api_nums)}")
        if len(api_nums) == 1:
            print(f"  ⚠ Only 1 account found. If you have a cash sub-account, use --account-numbers")
        save_cached_accounts(api_nums)
        return api_nums

    print("  ❌ No accounts found. Use --account-numbers to provide them manually.")
    sys.exit(1)


# ──────────────────────────────────────────────
# API FETCHING
# ──────────────────────────────────────────────
def fetch_all_options_orders(headers: dict, account_numbers: list[str],
                             after_date: str = None, symbol: str = None,
                             filled_only: bool = False) -> list[dict]:
    """Paginate through all options orders with server-side filters.

    Filters:
      after_date  — updated_at[gte] (YYYY-MM-DD, converted to midnight UTC)
      symbol      — chain_symbol (e.g. "SPY")
      filled_only — state=filled
    """
    all_orders = []
    page = 1
    acct_param = ",".join(account_numbers)

    # Build base query params
    params = [f"account_numbers={acct_param}"]
    if after_date:
        params.append(f"updated_at[gte]={after_date}T00:00:00Z")
    if symbol:
        params.append(f"chain_symbol={symbol.upper()}")
    if filled_only:
        params.append("state=filled")

    base_qs = "&".join(params)
    url = f"{OPTIONS_ORDERS_URL}?{base_qs}"

    filters_desc = []
    if after_date:
        filters_desc.append(f"after={after_date}")
    if symbol:
        filters_desc.append(f"symbol={symbol.upper()}")
    if filled_only:
        filters_desc.append("filled only")
    filter_str = f" [{', '.join(filters_desc)}]" if filters_desc else ""
    print(f"  account_numbers={acct_param}{filter_str}\n")

    while url:
        print(f"  Page {page}...")

        # Pagination: RH next URLs may drop our custom params — always re-inject
        if page > 1:
            sep = "&" if "?" in url else "?"
            # Strip any params we manage, then re-add them
            fetch_url = f"{url}{sep}{base_qs}"
        else:
            fetch_url = url

        if page == 1:
            print(f"    URL: {fetch_url}")

        resp = get_with_retry(fetch_url, headers=headers)

        if resp.status_code == 401:
            print("\n❌ AUTH FAILED — token expired.")
            sys.exit(1)
        if resp.status_code != 200:
            print(f"\n❌ HTTP {resp.status_code}: {resp.text[:500]}")
            sys.exit(1)

        data = resp.json()
        all_orders.extend(data.get("results", []))
        url = data.get("next")
        page += 1
        time_module.sleep(0.3)

    # Deduplicate
    seen = set()
    deduped = []
    for o in all_orders:
        oid = o.get("id")
        if oid and oid not in seen:
            seen.add(oid)
            deduped.append(o)

    print(f"\n  → {len(deduped)} unique orders")

    # Diagnostics
    states = defaultdict(int)
    dates = []
    acct_counts = defaultdict(int)
    for o in deduped:
        states[o.get("state", "unknown")] += 1
        created = o.get("created_at", o.get("updated_at", ""))
        if created:
            dates.append(created[:10])
        acct_counts[o.get("account_number", "(unknown)")] += 1
    dates.sort()

    print(f"  States: {dict(states)}")
    if dates:
        print(f"  Date range: {dates[0]} → {dates[-1]}")
    if len(acct_counts) > 1 or (acct_counts and list(acct_counts.keys())[0] != "(unknown)"):
        print(f"  Per-account: {dict(acct_counts)}")
    print()

    return deduped


def load_instrument_cache():
    """Load cached instrument data from disk into memory."""
    global _instrument_cache
    if INSTRUMENT_CACHE_FILE.exists():
        try:
            _instrument_cache = json.loads(INSTRUMENT_CACHE_FILE.read_text())
            print(f"  📁 Loaded {len(_instrument_cache)} cached instruments from {INSTRUMENT_CACHE_FILE.name}")
        except Exception:
            _instrument_cache = {}


def save_instrument_cache():
    """Persist instrument cache to disk."""
    if _instrument_cache:
        INSTRUMENT_CACHE_FILE.write_text(json.dumps(_instrument_cache, indent=2))
        print(f"  💾 Saved {len(_instrument_cache)} instruments to {INSTRUMENT_CACHE_FILE.name}")


def resolve_option_instrument(instrument_url: str, headers: dict) -> dict:
    if instrument_url in _instrument_cache:
        return _instrument_cache[instrument_url]
    resp = get_with_retry(instrument_url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    _instrument_cache[instrument_url] = data
    time_module.sleep(0.1)
    return data


# ──────────────────────────────────────────────
# CLASSIFY ORDERS BY STATE
# ──────────────────────────────────────────────
def classify_orders(orders: list[dict]) -> dict:
    """
    Split orders into buckets by state.
    Returns dict with keys: filled, cancelled, rejected, failed, other
    """
    buckets = defaultdict(list)
    for o in orders:
        state = o.get("state", "unknown").lower()
        if state in ("filled", "confirmed"):
            buckets["filled"].append(o)
        elif state == "cancelled":
            buckets["cancelled"].append(o)
        elif state == "rejected":
            buckets["rejected"].append(o)
        elif state == "failed":
            buckets["failed"].append(o)
        else:
            buckets["other"].append(o)
    return dict(buckets)


# ──────────────────────────────────────────────
# PARSE FILLED EXECUTIONS
# ──────────────────────────────────────────────
def parse_executions(orders: list[dict], headers: dict) -> list[dict]:
    """Flatten filled orders into individual execution records."""
    executions = []
    resolve_count = 0

    for order in orders:
        has_executions = any(len(leg.get("executions", [])) > 0 for leg in order.get("legs", []))
        if not has_executions:
            continue

        order_id = order.get("id")
        for leg in order.get("legs", []):
            position_effect = leg.get("position_effect", "").lower()
            side = leg.get("side", "").lower()
            option_url = leg.get("option", "")

            try:
                if option_url and option_url not in _instrument_cache:
                    resolve_count += 1
                    if resolve_count % 20 == 0:
                        print(f"  Resolving instruments... ({resolve_count})")
                inst = resolve_option_instrument(option_url, headers) if option_url else {}
            except Exception as e:
                print(f"  ⚠ Could not resolve {option_url}: {e}")
                inst = {}

            for exe in leg.get("executions", []):
                ts_str = exe.get("timestamp", "")
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00")) if ts_str else None

                executions.append({
                    "order_id": order_id,
                    "dt": dt,
                    "position_effect": position_effect,
                    "side": side,
                    "quantity": int(float(exe.get("quantity", 0))),
                    "price_per_share": float(exe.get("price", 0)),
                    "option_type": inst.get("type", "").lower(),
                    "strike_price": float(inst.get("strike_price", 0)) if inst.get("strike_price") else 0,
                    "expiration_date": inst.get("expiration_date", ""),
                    "chain_symbol": inst.get("chain_symbol", ""),
                    "option_url": option_url,
                    "account_number": order.get("account_number", ""),
                })

    executions.sort(key=lambda e: e["dt"] or datetime.min.replace(tzinfo=timezone.utc))
    print(f"  → {len(executions)} executions from {len(orders)} filled orders")
    if resolve_count:
        print(f"  → Resolved {resolve_count} instruments")
    print()
    return executions


# ──────────────────────────────────────────────
# PAIR OPENS → CLOSES
# ──────────────────────────────────────────────
def pair_into_trade_rows(executions: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    FIFO pair opens to closes. Each close = one output row.
    Returns (paired_rows, unmatched_open_records).
    """
    by_contract = defaultdict(lambda: {"opens": [], "closes": []})
    for ex in executions:
        key = ex["option_url"]
        if ex["position_effect"] == "open":
            by_contract[key]["opens"].append(dict(ex))
        elif ex["position_effect"] == "close":
            by_contract[key]["closes"].append(dict(ex))

    rows = []
    unmatched_open_records = []
    group_counter = 0
    unmatched_close_count = 0

    for contract_url, groups in sorted(
        by_contract.items(),
        key=lambda x: (x[1]["opens"] or x[1]["closes"])[0]["dt"]
        if (x[1]["opens"] or x[1]["closes"]) else datetime.min.replace(tzinfo=timezone.utc)
    ):
        opens = groups["opens"]
        closes = groups["closes"]

        while opens:
            entry = opens.pop(0)
            entry_qty_remaining = entry["quantity"]
            if entry_qty_remaining <= 0:
                continue

            group_counter += 1
            group_id = f"G{group_counter}"
            entry_price = entry["price_per_share"]
            matched_any = False

            while entry_qty_remaining > 0 and closes:
                exit_ = closes[0]

                if exit_["dt"] and entry["dt"] and exit_["dt"] < entry["dt"]:
                    unmatched_close_count += 1
                    closes.pop(0)
                    continue

                match_qty = min(entry_qty_remaining, exit_["quantity"])
                entry_cost = round(entry_price * match_qty * 100, 2)
                exit_credit = round(exit_["price_per_share"] * match_qty * 100, 2)

                if entry["side"] == "buy":
                    pl = round(exit_credit - entry_cost, 2)
                else:
                    pl = round(entry_cost - exit_credit, 2)

                pl_pct = round((pl / entry_cost) * 100, 6) if entry_cost else 0
                hold_min = round((exit_["dt"] - entry["dt"]).total_seconds() / 60) if entry["dt"] and exit_["dt"] else 0

                entry_dt = entry["dt"]
                exit_dt = exit_["dt"]
                trade_date = entry_dt.date() if entry_dt else None
                exp_str = entry.get("expiration_date", "")
                dte = 0
                if exp_str and trade_date:
                    try:
                        dte = (datetime.strptime(exp_str, "%Y-%m-%d").date() - trade_date).days
                    except ValueError:
                        dte = 0

                rows.append({
                    "entry_dt": entry_dt,
                    "exit_dt": exit_dt,
                    "trade_date": trade_date,
                    "expiry_date": exp_str,
                    "option_type": entry.get("option_type", ""),
                    "quantity": match_qty,
                    "entry_cost": entry_cost,
                    "exit_credit": exit_credit,
                    "pl": pl,
                    "pl_pct": pl_pct,
                    "hold_min": hold_min,
                    "strike_price": entry.get("strike_price"),
                    "chain_symbol": entry.get("chain_symbol", ""),
                    "group_id": group_id,
                    "dte": dte,
                    "account_number": entry.get("account_number", ""),
                })

                matched_any = True
                entry_qty_remaining -= match_qty

                if exit_["quantity"] > match_qty:
                    closes[0] = dict(exit_)
                    closes[0]["quantity"] = exit_["quantity"] - match_qty
                else:
                    closes.pop(0)

            if not matched_any or entry_qty_remaining > 0:
                unmatched_open_records.append({
                    **entry,
                    "unmatched_qty": entry_qty_remaining if entry_qty_remaining > 0 else entry["quantity"],
                    "group_id": group_id,
                })

        for c in closes:
            unmatched_close_count += c["quantity"]

    rows.sort(key=lambda r: (
        r["entry_dt"] or datetime.min.replace(tzinfo=timezone.utc),
        r["exit_dt"] or datetime.min.replace(tzinfo=timezone.utc),
    ))

    print(f"  → {len(rows)} paired trade rows")
    if unmatched_open_records:
        print(f"  → {len(unmatched_open_records)} unmatched open position(s)")
    if unmatched_close_count:
        print(f"  → {unmatched_close_count} orphaned close(s)")
    print()
    return rows, unmatched_open_records


# ──────────────────────────────────────────────
# MARKET DATA
# ──────────────────────────────────────────────
def fetch_rh_intraday(symbol: str, headers: dict) -> list[dict]:
    """Fetch 5-min intraday bars from RH (span=week gives ~5 trading days).
    Returns list of {begins_at, open, high, low, close, volume} dicts."""
    url = f"{BASE_URL}/marketdata/historicals/{symbol}/?interval=5minute&span=week&bounds=regular"
    resp = get_with_retry(url, headers=headers)
    if resp.status_code != 200:
        return []
    bars = resp.json().get("historicals", [])
    return [
        {
            "begins_at": b["begins_at"],
            "close": float(b["close_price"]),
            "high": float(b["high_price"]),
            "low": float(b["low_price"]),
            "volume": int(b.get("volume", 0)),
        }
        for b in bars if int(b.get("volume", 0)) > 0
    ]


def compute_vwap(bars: list[dict], up_to: datetime) -> float | None:
    """Compute VWAP from market open of that day up to the given UTC datetime."""
    if not bars:
        return None
    target_date = up_to.strftime("%Y-%m-%d")
    cum_pv = 0.0
    cum_vol = 0
    for b in bars:
        bar_date = b["begins_at"][:10]
        if bar_date != target_date:
            continue
        bar_dt = datetime.fromisoformat(b["begins_at"].replace("Z", "+00:00"))
        if bar_dt > up_to:
            break
        typical = (b["high"] + b["low"] + b["close"]) / 3
        cum_pv += typical * b["volume"]
        cum_vol += b["volume"]
    return round(cum_pv / cum_vol, 2) if cum_vol > 0 else None


def compute_ema(bars: list[dict], up_to: datetime, period: int = 8) -> float | None:
    """Compute EMA(period) from 5-min close prices on that day up to the given UTC datetime."""
    if not bars:
        return None
    target_date = up_to.strftime("%Y-%m-%d")
    closes = []
    for b in bars:
        if b["begins_at"][:10] != target_date:
            continue
        bar_dt = datetime.fromisoformat(b["begins_at"].replace("Z", "+00:00"))
        if bar_dt > up_to:
            break
        closes.append(b["close"])
    if len(closes) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
    return round(ema, 2)


def fetch_greeks(instrument_urls: list[str], headers: dict) -> dict:
    """Fetch greeks from /marketdata/options/ in batches.
    Returns {instrument_url: {delta, gamma, theta, vega, iv}}."""
    result = {}
    batch_size = 17  # RH limit per call
    for i in range(0, len(instrument_urls), batch_size):
        batch = instrument_urls[i:i + batch_size]
        url = f"{BASE_URL}/marketdata/options/?instruments={','.join(batch)}"
        resp = get_with_retry(url, headers=headers)
        if resp.status_code != 200:
            continue
        for r in resp.json().get("results", []):
            if r is None:
                continue
            inst_url = r.get("instrument", "")
            delta = r.get("delta")
            if delta is not None:
                result[inst_url] = {
                    "delta": round(float(delta), 4),
                    "gamma": round(float(r["gamma"]), 6) if r.get("gamma") else None,
                    "theta": round(float(r["theta"]), 4) if r.get("theta") else None,
                    "vega": round(float(r["vega"]), 4) if r.get("vega") else None,
                    "iv": round(float(r["implied_volatility"]), 4) if r.get("implied_volatility") else None,
                }
        time_module.sleep(0.2)
    return result


def enrich_greeks(executions: list[dict], rows: list[dict], headers: dict):
    """Add delta at entry to paired trade rows. Only useful for same-day (non-expired) contracts.
    Modifies rows in-place."""
    # Collect unique instrument URLs from the open-side executions
    open_urls = set()
    for ex in executions:
        if ex["position_effect"] == "open" and ex.get("option_url"):
            open_urls.add(ex["option_url"])

    if not open_urls:
        return

    greeks = fetch_greeks(list(open_urls), headers)
    filled = sum(1 for url in open_urls if url in greeks)
    print(f"  → Greeks available for {filled}/{len(open_urls)} instruments")

    for r in rows:
        r["delta"] = None

    # Map instrument URLs to rows via (chain_symbol, strike, expiry, type) key
    open_lookup = {}
    for ex in executions:
        if ex["position_effect"] == "open" and ex.get("option_url"):
            key = (ex["chain_symbol"], ex["strike_price"], ex["expiration_date"], ex["option_type"])
            open_lookup[key] = ex["option_url"]

    filled_count = 0
    for r in rows:
        key = (r["chain_symbol"], r.get("strike_price"), r.get("expiry_date"), r.get("option_type"))
        url = open_lookup.get(key)
        if url and url in greeks:
            r["delta"] = greeks[url]["delta"]
            filled_count += 1

    if filled_count:
        print(f"  → Delta filled for {filled_count}/{len(rows)} trades")
    else:
        print(f"  → No delta data (contracts likely expired)")
    print()


def fetch_options_events(headers: dict, account_numbers: list[str]) -> list[dict]:
    """Fetch options events (expiration/exercise/assignment) from RH."""
    acct_param = ",".join(account_numbers)
    url = f"{BASE_URL}/options/events/?account_numbers={acct_param}"
    all_events = []
    while url:
        resp = get_with_retry(url, headers=headers)
        if resp.status_code != 200:
            print(f"  ⚠ Options events: HTTP {resp.status_code}")
            return []
        data = resp.json()
        all_events.extend(data.get("results", []))
        url = data.get("next")
    return all_events


def check_options_events(headers: dict, account_numbers: list[str],
                         unmatched_opens: list[dict], after_date: str = None):
    """Check for exercise/assignment/expiration events and warn about unmatched positions.
    Returns expiration option URLs so caller can resolve expired opens into trade rows."""
    events = fetch_options_events(headers, account_numbers)
    if not events:
        print("  No options events found.\n")
        return set()

    # Filter by date if specified
    if after_date:
        events = [e for e in events if e.get("event_date", "") >= after_date]

    exercises = [e for e in events if e["type"] == "exercise"]
    assignments = [e for e in events if e["type"] == "assignment"]
    expirations = [e for e in events if e["type"] == "expiration"]

    print(f"  Events: {len(expirations)} expirations, {len(exercises)} exercises, {len(assignments)} assignments")

    if exercises:
        print(f"  ⚠ AUTO-EXERCISED positions:")
        for e in exercises:
            print(f"    {e['event_date']} qty={e['quantity']} cash=${e.get('total_cash_amount', '?')} acct={e['account_number']}")

    if assignments:
        print(f"  ⚠ ASSIGNED positions:")
        for e in assignments:
            print(f"    {e['event_date']} qty={e['quantity']} cash=${e.get('total_cash_amount', '?')} acct={e['account_number']}")

    exp_urls = set(e.get("option", "") for e in expirations)

    # Cross-reference: do any unmatched opens match an expiration event?
    if unmatched_opens and expirations:
        matched = [o for o in unmatched_opens if o.get("option_url", "") in exp_urls]
        if matched:
            print(f"  ℹ {len(matched)} unmatched open(s) expired OTM → will be added as $0 exits")

    print()
    return exp_urls


def resolve_expired_opens(unmatched_opens: list[dict], exp_urls: set) -> tuple[list[dict], list[dict]]:
    """Convert unmatched opens that expired OTM into paired trade rows with $0 exit.
    Returns (expired_rows, remaining_unmatched)."""
    expired_rows = []
    remaining = []
    ET = ZoneInfo("America/New_York")

    for rec in unmatched_opens:
        if rec.get("option_url", "") not in exp_urls:
            remaining.append(rec)
            continue

        qty = rec.get("unmatched_qty", rec.get("quantity", 0))
        entry_price = rec.get("price_per_share", 0)
        entry_cost = round(entry_price * qty * 100, 2)
        entry_dt = rec.get("dt")
        trade_date = entry_dt.date() if entry_dt else None

        # Exit at market close (4 PM ET) on expiry date
        exp_str = rec.get("expiration_date", "")
        exit_dt = None
        if exp_str:
            try:
                exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                exit_dt = datetime(exp_date.year, exp_date.month, exp_date.day,
                                   16, 0, 0, tzinfo=ET)
            except ValueError:
                pass

        if rec["side"] == "buy":
            pl = round(0 - entry_cost, 2)
        else:
            pl = round(entry_cost - 0, 2)
        pl_pct = -100.0 if entry_cost else 0

        hold_min = round((exit_dt - entry_dt).total_seconds() / 60) if entry_dt and exit_dt else 0

        dte = 0
        if exp_str and trade_date:
            try:
                dte = (datetime.strptime(exp_str, "%Y-%m-%d").date() - trade_date).days
            except ValueError:
                pass

        expired_rows.append({
            "entry_dt": entry_dt,
            "exit_dt": exit_dt,
            "trade_date": trade_date,
            "expiry_date": exp_str,
            "option_type": rec.get("option_type", ""),
            "quantity": qty,
            "entry_cost": entry_cost,
            "exit_credit": 0,
            "pl": pl,
            "pl_pct": pl_pct,
            "hold_min": hold_min,
            "strike_price": rec.get("strike_price"),
            "chain_symbol": rec.get("chain_symbol", ""),
            "group_id": rec.get("group_id", ""),
            "dte": dte,
            "account_number": rec.get("account_number", ""),
        })

    if expired_rows:
        print(f"  → {len(expired_rows)} expired OTM position(s) resolved as $0 exits")
    return expired_rows, remaining


def fetch_rh_historicals(symbol: str, start: str, end: str, headers: dict) -> dict:
    """Fetch daily OHLC from RH /marketdata/historicals/. Returns {date_str: {Open,High,Low,Close}}."""
    url = f"{BASE_URL}/marketdata/historicals/{symbol}/?interval=day&span=year&bounds=regular"
    resp = get_with_retry(url, headers=headers)
    if resp.status_code != 200:
        print(f"  ⚠ RH historicals for {symbol}: HTTP {resp.status_code}")
        return {}

    data = resp.json()
    result = {}
    for bar in data.get("historicals", []):
        date_key = bar.get("begins_at", "")[:10]
        if date_key and start <= date_key <= end:
            result[date_key] = {
                "Asset Open": round(float(bar["open_price"]), 2),
                "Asset High": round(float(bar["high_price"]), 2),
                "Asset Low": round(float(bar["low_price"]), 2),
                "Asset Close": round(float(bar["close_price"]), 2),
            }
    return result


def fetch_market_data(rows: list[dict], headers: dict) -> dict:
    """Fetch daily OHLC per underlying (via RH) + VIX (via yfinance). Returns {(symbol, date): {...}}."""
    if not rows:
        return {}

    needed = set()
    dates = set()
    for r in rows:
        sym = r.get("chain_symbol", "").upper()
        d = str(r["trade_date"]) if r.get("trade_date") else None
        if sym and d:
            needed.add((sym, d))
            dates.add(d)
    if not needed:
        return {}

    sorted_dates = sorted(dates)
    start = (datetime.strptime(sorted_dates[0], "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")
    end = (datetime.strptime(sorted_dates[-1], "%Y-%m-%d") + timedelta(days=2)).strftime("%Y-%m-%d")

    symbols = sorted(set(sym for sym, _ in needed))
    print(f"  Symbols: {', '.join(symbols)}")

    lookup = {}
    for sym in symbols:
        try:
            bars = fetch_rh_historicals(sym, start, end, headers)
            for date_key, ohlc in bars.items():
                lookup[(sym, date_key)] = ohlc
            print(f"  {sym}: {len(bars)} days from RH")
        except Exception as e:
            print(f"  ⚠ {sym} RH failed ({e}), falling back to yfinance...")
            try:
                df = yf.download(sym, start=start, end=end, progress=False)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                for idx, row in df.iterrows():
                    date_key = idx.strftime("%Y-%m-%d")
                    lookup[(sym, date_key)] = {
                        "Asset Open": round(row["Open"], 2),
                        "Asset High": round(row["High"], 2),
                        "Asset Low": round(row["Low"], 2),
                        "Asset Close": round(row["Close"], 2),
                    }
            except Exception as e2:
                print(f"  ⚠ {sym} yfinance also failed: {e2}")
        time_module.sleep(0.2)

    print(f"  Fetching VIX (yfinance)...")
    try:
        vix = yf.download("^VIX", start=start, end=end, progress=False)
        if isinstance(vix.columns, pd.MultiIndex):
            vix.columns = vix.columns.get_level_values(0)
        for idx, row in vix.iterrows():
            lookup[("^VIX", idx.strftime("%Y-%m-%d"))] = round(row["Close"], 1)
    except Exception as e:
        print(f"  ⚠ VIX: {e}")

    print(f"  → {len(lookup)} data points\n")
    return lookup


def enrich_intraday(rows: list[dict], headers: dict):
    """Add VWAP and 8 EMA to trade rows using RH 5-min intraday bars.
    Only works for trades within the last ~5 trading days (span=week limit).
    Modifies rows in-place."""
    symbols = set(r.get("chain_symbol", "").upper() for r in rows if r.get("chain_symbol"))
    intraday_cache = {}

    for sym in sorted(symbols):
        try:
            bars = fetch_rh_intraday(sym, headers)
            if bars:
                intraday_cache[sym] = bars
                dates = sorted(set(b["begins_at"][:10] for b in bars))
                print(f"  {sym}: {len(bars)} intraday bars ({dates[0]} → {dates[-1]})")
            else:
                print(f"  {sym}: no intraday data available")
        except Exception as e:
            print(f"  ⚠ {sym} intraday failed: {e}")

    filled_count = 0
    for r in rows:
        sym = r.get("chain_symbol", "").upper()
        entry_dt = r.get("entry_dt")
        if not entry_dt or sym not in intraday_cache:
            r["vwap"] = None
            r["ema8"] = None
            continue
        bars = intraday_cache[sym]
        r["vwap"] = compute_vwap(bars, entry_dt)
        r["ema8"] = compute_ema(bars, entry_dt, period=8)
        if r["vwap"] is not None:
            filled_count += 1

    print(f"  → VWAP/EMA filled for {filled_count}/{len(rows)} trades\n")


# ──────────────────────────────────────────────
# TIMEZONE
# ──────────────────────────────────────────────
ET = ZoneInfo("America/New_York")

def to_eastern(dt):
    if dt is None:
        return None
    aware = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    return aware.astimezone(ET)


def fmt_time(dt, style="excel"):
    if dt is None:
        return ""
    et = to_eastern(dt)
    return et.strftime("%H:%M:%S") if style == "excel" else et.strftime("%-I:%M:%S %p")


def fmt_date(d):
    """Format date as M/D/YYYY."""
    if d is None:
        return ""
    if isinstance(d, str):
        try:
            d = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            return d
    return d.strftime("%-m/%-d/%Y")


# ──────────────────────────────────────────────
# BUILD PAIRED TRADE CSV
# ──────────────────────────────────────────────
def build_trade_df(rows: list[dict], market: dict,
                   start_date=None, end_date=None, time_format="excel") -> pd.DataFrame:
    output = []
    cumulative_pl = 0

    for r in rows:
        date_str = str(r["trade_date"]) if r["trade_date"] else ""
        if start_date and date_str < start_date:
            continue
        if end_date and date_str > end_date:
            continue

        sym = r.get("chain_symbol", "").upper()
        mkt = market.get((sym, date_str), {})
        vix = market.get(("^VIX", date_str), "")
        cumulative_pl += r["pl"]

        entry_et = to_eastern(r["entry_dt"]) if r["entry_dt"] else None

        output.append({
            "Trade #": 0,
            "Date": fmt_date(r["trade_date"]),
            "Day": r["entry_dt"].strftime("%a") if r["entry_dt"] else "",
            "Account": r.get("account_number", ""),
            "Symbol": sym,
            "Expiry Date": fmt_date(r["expiry_date"]),
            "Type": r["option_type"].capitalize(),
            "Strike": r.get("strike_price", ""),
            "Qty": r["quantity"],
            "Asset Open": mkt.get("Asset Open", ""),
            "Asset High": mkt.get("Asset High", ""),
            "Asset Low": mkt.get("Asset Low", ""),
            "Asset Close": mkt.get("Asset Close", ""),
            "VWAP": r.get("vwap", ""),
            "8 EMA": r.get("ema8", ""),
            "Entry Time": fmt_time(r["entry_dt"], time_format),
            "Exit Time": fmt_time(r["exit_dt"], time_format),
            "Hold Time (min)": r["hold_min"],
            "Entry Hour": entry_et.hour if entry_et else "",
            "Entry Cost": int(-r["entry_cost"]),
            "Risk ($)": int(r["entry_cost"]),
            "Exit Credit": int(r["exit_credit"]),
            "P/L ($)": int(r["pl"]),
            "Cumulative P/L ($)": int(cumulative_pl),
            "P/L (%)": round(r["pl_pct"], 6),
            "Win/Loss": "WIN" if r["pl"] > 0 else ("LOSS" if r["pl"] < 0 else "BE"),
            "Is Win": 1 if r["pl"] > 0 else 0,
            "VIX": vix,
            "Delta": r.get("delta", ""),
            "Group ID": r["group_id"],
            "DTE": r["dte"],
        })

    for i, row in enumerate(output, start=1):
        row["Trade #"] = i

    return pd.DataFrame(output)


# ──────────────────────────────────────────────
# BUILD NON-TRADE CSVs
# ──────────────────────────────────────────────
def build_order_df(orders: list[dict], headers: dict) -> pd.DataFrame:
    """Build a simple DataFrame for non-filled orders (cancelled, rejected, failed)."""
    rows = []
    for o in orders:
        created = o.get("created_at", "")
        symbol = o.get("chain_symbol", "")
        legs = o.get("legs", [])

        # Get basic info from first leg
        leg_info = ""
        option_type = ""
        strike = ""
        expiry = ""
        if legs:
            leg = legs[0]
            option_url = leg.get("option", "")
            if option_url:
                try:
                    inst = resolve_option_instrument(option_url, headers)
                    option_type = inst.get("type", "").capitalize()
                    strike = inst.get("strike_price", "")
                    expiry = inst.get("expiration_date", "")
                except Exception:
                    pass
            leg_info = f"{leg.get('side', '')} to {leg.get('position_effect', '')}"

        rows.append({
            "Date": created[:10] if created else "",
            "Time": created[11:19] if len(created) > 19 else "",
            "Symbol": symbol,
            "Type": option_type,
            "Strike": strike,
            "Expiry": expiry,
            "Side": leg_info,
            "Qty": o.get("quantity", ""),
            "Price": o.get("price", ""),
            "State": o.get("state", ""),
            "Order ID": o.get("id", ""),
        })

    return pd.DataFrame(rows)


def build_unmatched_opens_df(records: list[dict]) -> pd.DataFrame:
    """Build DataFrame for open positions with no matching close."""
    rows = []
    for r in records:
        entry_et = to_eastern(r.get("dt"))
        trade_date = r["dt"].date() if r.get("dt") else None

        rows.append({
            "Date": fmt_date(trade_date),
            "Account": r.get("account_number", ""),
            "Symbol": r.get("chain_symbol", "").upper(),
            "Type": r.get("option_type", "").capitalize(),
            "Strike": r.get("strike_price", ""),
            "Expiry": r.get("expiration_date", ""),
            "Side": r.get("side", ""),
            "Unmatched Qty": r.get("unmatched_qty", r.get("quantity", "")),
            "Entry Price/Share": r.get("price_per_share", ""),
            "Entry Time": fmt_time(r.get("dt"), "excel"),
            "Group ID": r.get("group_id", ""),
        })

    return pd.DataFrame(rows)


# ──────────────────────────────────────────────
# SUMMARY
# ──────────────────────────────────────────────
def print_trade_summary(df: pd.DataFrame, label: str):
    if len(df) == 0:
        return
    wins = (df["Win/Loss"] == "WIN").sum()
    losses = (df["Win/Loss"] == "LOSS").sum()
    be = (df["Win/Loss"] == "BE").sum()
    total_pl = df["P/L ($)"].sum()
    print(f"     {label}: {len(df)} rows | {wins}W/{losses}L/{be}BE | P/L: ${total_pl:,.0f}")
    print(f"       {df['Date'].iloc[0]} → {df['Date'].iloc[-1]} | {df['Group ID'].nunique()} groups")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Export Robinhood options trades to multiple CSVs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--token", default=None,
                        help='Auth token from browser DevTools (also reads $RH_TOKEN or .rh_token)')
    parser.add_argument("--token-stdin", action="store_true",
                        help="Read token from stdin (e.g. pbpaste | python hood.py --token-stdin)")
    parser.add_argument("--save-token", action="store_true",
                        help="Save the provided token to .rh_token for future runs")
    parser.add_argument("--account-numbers", default=None,
                        help='Comma-separated account numbers (cached after first use)')
    parser.add_argument("--start", default=None,
                        help="Start date YYYY-MM-DD (inclusive, applies to trade CSVs only)")
    parser.add_argument("--end", default=None,
                        help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--after-date", default=None,
                        help="Server-side filter: only orders updated after YYYY-MM-DD (uses updated_at[gte])")
    parser.add_argument("--symbol", default=None,
                        help="Server-side filter: only this underlying symbol (e.g. SPY)")
    parser.add_argument("--filled-only", action="store_true", default=False,
                        help="Server-side filter: only fetch filled orders (auto-enabled unless --dump-raw)")
    parser.add_argument("--output-dir", default="./outputs/",
                        help="Output directory (default: outputs directory)")
    parser.add_argument("--time-format", choices=["excel", "ampm"], default="excel",
                        help="Time format: excel (HH:MM:SS) or ampm (H:MM:SS AM/PM)")
    parser.add_argument("--dump-raw", action="store_true",
                        help="Save raw API JSON to rh_raw_orders.json")
    args = parser.parse_args()

    token = resolve_token(args)
    headers = make_headers(token)

    # ── Quick auth check ──
    print("🔐 Verifying token...", end=" ")
    try:
        resp = requests.get(f"{BASE_URL}/user/", headers=headers, timeout=10)
        if resp.status_code == 200:
            username = resp.json().get("username", "unknown")
            print(f"✅ authenticated as {username}")
        else:
            print(f"❌ token rejected (HTTP {resp.status_code})")
            sys.exit(1)
    except requests.RequestException as e:
        print(f"❌ connection failed: {e}")
        sys.exit(1)
    print()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── 1. Discover accounts ──
    print("🔑 Discovering accounts...\n")
    account_numbers = discover_account_numbers(headers, manual_override=args.account_numbers)
    print()

    # ── Load instrument cache ──
    load_instrument_cache()

    # ── 2. Fetch all orders ──
    # Auto-enable filled_only when not dumping raw (skips cancelled/rejected/failed server-side)
    filled_only = args.filled_only or (not args.dump_raw)

    print("📡 Fetching options orders...\n")
    orders = fetch_all_options_orders(
        headers, account_numbers,
        after_date=args.after_date,
        symbol=args.symbol,
        filled_only=filled_only and not args.dump_raw,
    )

    if args.dump_raw:
        raw_path = out_dir / "rh_raw_orders.json"
        raw_path.write_text(json.dumps(orders, indent=2, default=str))
        print(f"  Raw JSON saved to {raw_path}\n")

    if not orders:
        print("No orders found.")
        sys.exit(0)

    # ── 3. Classify by state ──
    print("📂 Classifying orders...\n")
    buckets = classify_orders(orders)
    for state, items in buckets.items():
        print(f"  {state}: {len(items)}")
    print()

    # ── 4. Parse and pair filled orders ──
    filled = buckets.get("filled", [])
    paired_rows = []
    unmatched_opens = []

    executions = []
    if filled:
        print("🔍 Parsing filled executions...\n")
        executions = parse_executions(filled, headers)
        save_instrument_cache()

        if executions:
            print("🔗 Pairing entries → exits...\n")
            paired_rows, unmatched_opens = pair_into_trade_rows(executions)

    # ── 5. Fetch market data (only for paired trades) ──
    market = {}
    if paired_rows:
        print("📈 Fetching market data...\n")
        market = fetch_market_data(paired_rows, headers)

        print("📊 Computing intraday VWAP + 8 EMA...\n")
        enrich_intraday(paired_rows, headers)

        print("📐 Fetching greeks (delta)...\n")
        enrich_greeks(executions, paired_rows, headers)

    # ── 5b. Check options events (exercise/assignment/expiration) ──
    print("🔔 Checking options events...\n")
    exp_urls = check_options_events(headers, account_numbers, unmatched_opens, after_date=args.after_date)

    # ── 5c. Resolve expired OTM opens as $0 exits ──
    if unmatched_opens and exp_urls:
        expired_rows, unmatched_opens = resolve_expired_opens(unmatched_opens, exp_urls)
        if expired_rows:
            paired_rows.extend(expired_rows)
            # Re-sort by entry time
            paired_rows.sort(key=lambda r: (
                r["entry_dt"] or datetime.min.replace(tzinfo=timezone.utc),
                r["exit_dt"] or datetime.min.replace(tzinfo=timezone.utc),
            ))
            # Fetch market data for newly added expired rows
            expired_market = fetch_market_data(expired_rows, headers)
            market.update(expired_market)
            print()

    # ── 6. Build and save CSVs ──
    print("📝 Writing CSVs...\n")
    files_written = []

    if paired_rows:
        # Split SPY vs non-SPY
        spy_rows = [r for r in paired_rows if r.get("chain_symbol", "").upper() == "SPY"]
        other_rows = [r for r in paired_rows if r.get("chain_symbol", "").upper() != "SPY"]

        if spy_rows:
            df = build_trade_df(spy_rows, market, args.start, args.end, args.time_format)
            if len(df) > 0:
                path = out_dir / "spy_trades.csv"
                df.to_csv(path, index=False)
                files_written.append(("spy_trades.csv", len(df)))
                print_trade_summary(df, "SPY trades")

        if other_rows:
            df = build_trade_df(other_rows, market, args.start, args.end, args.time_format)
            if len(df) > 0:
                path = out_dir / "other_trades.csv"
                df.to_csv(path, index=False)
                files_written.append(("other_trades.csv", len(df)))
                print_trade_summary(df, "Other trades")

    if unmatched_opens:
        df = build_unmatched_opens_df(unmatched_opens)
        path = out_dir / "unmatched_opens.csv"
        df.to_csv(path, index=False)
        files_written.append(("unmatched_opens.csv", len(df)))
        print(f"     Unmatched opens: {len(df)} positions")

    # Non-filled order CSVs
    for state in ("cancelled", "rejected", "failed"):
        items = buckets.get(state, [])
        if items:
            df = build_order_df(items, headers)
            path = out_dir / f"{state}.csv"
            df.to_csv(path, index=False)
            files_written.append((f"{state}.csv", len(df)))
            print(f"     {state.capitalize()}: {len(df)} orders")

    # Summary
    print(f"\n✅ Done! Files written to {out_dir.resolve()}:\n")
    for fname, count in files_written:
        print(f"   📄 {fname} ({count} rows)")

    if not files_written:
        print("   (no output files — check your date filters or account numbers)")


if __name__ == "__main__":
    main()