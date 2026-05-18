#!/usr/bin/env python3
"""
Cash flow summary — pulls all money movement from Robinhood and calculates net P/L.

Uses: bonfire unified transfers, Gold subscription fees, dividends, portfolio equity.
Shares .rh_token with hood.py.
"""

import argparse
import json
import requests
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
TOKEN_FILE = SCRIPT_DIR / ".rh_token"
SYMBOLS_CACHE_FILE = SCRIPT_DIR / ".rh_resolved_symbols.json"

API_BASE = "https://api.robinhood.com"
BONFIRE_BASE = "https://bonfire.robinhood.com"

# Dividend states that count toward cost-basis math. Pending dividends are
# entitlements RH has scheduled but not yet paid — keep them in the events log
# so the UI surfaces them, but exclude from totals until they actually settle.
PAID_DIVIDEND_STATES = {"paid", "reinvested"}


def load_token() -> str:
    if not TOKEN_FILE.exists():
        print("❌ No .rh_token file. Run hood.py --save-token first.")
        sys.exit(1)
    raw = TOKEN_FILE.read_text().strip()
    if raw.lower().startswith("bearer "):
        return raw
    return f"Bearer {raw}"


def headers(token: str) -> dict:
    return {"Authorization": token, "Accept": "application/json", "User-Agent": "Mozilla/5.0"}


def load_resolved_symbols() -> dict:
    """Read URL→symbol cache for dividend instrument lookups."""
    if SYMBOLS_CACHE_FILE.exists():
        try:
            return json.loads(SYMBOLS_CACHE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_resolved_symbols(cache: dict) -> None:
    try:
        SYMBOLS_CACHE_FILE.write_text(json.dumps(cache, indent=2, sort_keys=True))
    except OSError:
        pass


def fetch_instrument_symbol(url: str, hdrs: dict) -> str | None:
    """One-shot resolve of an RH instrument URL → ticker symbol. None on failure."""
    try:
        r = requests.get(url, headers=hdrs, timeout=15)
        if r.status_code != 200:
            return None
        return r.json().get("symbol")
    except (requests.RequestException, ValueError):
        return None


def resolve_dividend_symbols(divs: list, hdrs: dict, log=print) -> None:
    """Mutates each dividend dict to attach `_symbol` (ticker) via cached lookup.
    Misses fall back to empty string; downstream uses instrument UUID as the fallback."""
    cache = load_resolved_symbols()
    unique_urls = {d.get("instrument") for d in divs if d.get("instrument")}
    missing = [u for u in unique_urls if u not in cache]
    if missing:
        log(f"  🔍 resolving {len(missing)} new dividend instrument{'s' if len(missing) > 1 else ''}…")
        for url in missing:
            cache[url] = fetch_instrument_symbol(url, hdrs) or ""
        save_resolved_symbols(cache)
    for d in divs:
        d["_symbol"] = cache.get(d.get("instrument", ""), "") or ""


def paginate(url: str, hdrs: dict) -> list:
    """Fetch all pages from a paginated endpoint."""
    results = []
    while url:
        r = requests.get(url, headers=hdrs)
        if r.status_code != 200:
            print(f"  ⚠️  HTTP {r.status_code} on {url}")
            break
        data = r.json()
        results.extend(data.get("results", []))
        url = data.get("next")
    return results


# ──────────────────────────────────────────────────────────────────────────────
# Backfill helpers (used by --backfill to reconstruct per-day historical snapshots)
# ──────────────────────────────────────────────────────────────────────────────

PERF_HEADERS_EXTRA = {
    "Origin": "https://robinhood.com",
    "Referer": "https://robinhood.com/",
    "X-TimeZone-Id": "America/New_York",
    "X-Hyper-Ex": "enabled",
    "Accept": "*/*",
}


def fetch_portfolio_performance(account_number: str, hdrs: dict) -> list:
    """Fetch the historical equity chart for one account.
    Returns list of (date_iso, equity_dollars) tuples sorted ascending.

    Endpoint reverse-engineered from RH web app: bonfire chart data with
    ~309 points across all-time. Date strings come labeled (e.g. "Jun 8, 2020")
    and dollar amounts as decimal strings inside cursor_data.
    """
    h = {**hdrs, **PERF_HEADERS_EXTRA}
    url = (
        f"{BONFIRE_BASE}/portfolio/performance/{account_number}"
        "?chart_style=PERFORMANCE&chart_type=historical_portfolio"
        "&display_span=all&include_all_hours=true"
    )
    r = requests.get(url, headers=h)
    if r.status_code != 200:
        return []
    d = r.json()
    out = []
    for line in d.get("lines", []) or []:
        for seg in line.get("segments", []) or []:
            for p in seg.get("points", []) or []:
                cd = p.get("cursor_data") or {}
                label = (cd.get("label") or {}).get("value")
                pcd = (cd.get("price_chart_data") or {}).get("dollar_value") or {}
                amt = pcd.get("amount")
                if not label or amt is None:
                    continue
                try:
                    iso = datetime.strptime(label, "%b %d, %Y").strftime("%Y-%m-%d")
                except ValueError:
                    continue
                try:
                    out.append((iso, float(amt)))
                except (TypeError, ValueError):
                    continue
    out.sort()
    return out


def total_equity_by_date(per_account_series: dict) -> dict:
    """Given {account_number: [(date, equity), ...]} (each sorted asc), return
    {date_iso: total_equity_summed_across_accounts}. Carries forward each
    account's last-known equity until the next data point. Accounts with no
    data on/before a date contribute 0 (e.g. cash sub-account before opening).
    """
    if not per_account_series:
        return {}
    all_dates = sorted({d for series in per_account_series.values() for d, _ in series})
    last_known = {acct: 0.0 for acct in per_account_series}
    idx = {acct: 0 for acct in per_account_series}
    out = {}
    for d in all_dates:
        for acct, series in per_account_series.items():
            while idx[acct] < len(series) and series[idx[acct]][0] <= d:
                last_known[acct] = series[idx[acct]][1]
                idx[acct] += 1
        out[d] = round(sum(last_known.values()), 2)
    return out


def collect_dated_cashflows(transfers: list, fees: list, divs: list, refs: list) -> dict:
    """Walk the same data main() iterates, but emit (date, amount) tuples for
    each completed cashflow event so we can compute as-of cumulative totals
    for any historical date.

    Returns dict with keys: deposits, withdrawals, gold, dividends, referrals
    Each value is a list of (date_iso, amount) sorted asc by date.
    """
    deposits, withdrawals = [], []
    for t in transfers:
        if t.get("state") not in ("completed", "submitted"):
            # Only completed transfers count toward historical basis.
            # 'submitted' shows up briefly post-completion; treat as completed.
            if t.get("state") != "completed":
                continue
        try:
            amt = float(t.get("amount", 0))
        except (TypeError, ValueError):
            continue
        direction = t.get("direction", "?")
        transfer_type = t.get("transfer_type", "")
        orig = t.get("originating_account_type", "")
        recv = t.get("receiving_account_type", "")
        date = (t.get("created_at") or "")[:10]
        if not date:
            continue
        # Same classifier as main()
        if transfer_type == "internal" or (orig == "rhs_account" and recv == "rhs_account"):
            continue  # internal, excluded
        if orig and recv:
            if recv == "rhs_account" and orig != "rhs_account":
                deposits.append((date, amt))
            elif orig == "rhs_account" and recv != "rhs_account":
                if direction == "pull":
                    deposits.append((date, amt))
                else:
                    withdrawals.append((date, amt))
        else:
            if direction == "pull":
                deposits.append((date, amt))
            elif direction == "push":
                withdrawals.append((date, amt))

    gold = []
    for f in fees:
        try:
            amt = float(f["amount"])
        except (TypeError, ValueError, KeyError):
            continue
        date = f.get("date")
        if date:
            gold.append((date, amt))

    dividends = []
    for d in divs:
        if d.get("state") == "voided":
            continue
        try:
            amt = float(d["amount"])
        except (TypeError, ValueError, KeyError):
            continue
        date = d.get("payable_date")
        if date:
            dividends.append((date, amt))

    referrals = []
    for ref in refs:
        reward = ref.get("reward") or {}
        date = (ref.get("created_at") or "")[:10]
        if not date:
            continue
        for s in reward.get("stocks") or []:
            if s.get("state") in ("failed", "voided"):
                continue
            try:
                cost = float(s.get("cost_basis", 0))
            except (TypeError, ValueError):
                continue
            referrals.append((date, cost))
        cash_reward = reward.get("cash")
        if cash_reward and cash_reward.get("state") not in ("failed", "voided"):
            try:
                referrals.append((date, float(cash_reward.get("amount", 0))))
            except (TypeError, ValueError):
                pass

    for lst in (deposits, withdrawals, gold, dividends, referrals):
        lst.sort()

    return {
        "deposits": deposits,
        "withdrawals": withdrawals,
        "gold": gold,
        "dividends": dividends,
        "referrals": referrals,
    }


def extract_events(transfers: list, fees: list, divs: list, refs: list) -> list:
    """Emit per-event records for every cashflow line item — transfers, gold fees,
    dividends, and referral grants. Used to power the calendar's day-by-day cashflow
    visualization. Each event has a stable id so re-runs are idempotent.

    Event schema:
        {
          id:             "<kind>:<source_id>",   # stable; safe to dedup on
          kind:           "deposit" | "withdrawal" | "internal"
                          | "gold_fee" | "dividend" | "referral",
          date:           "YYYY-MM-DD",
          amount:         float (always positive; `kind` carries the sign semantics),
          state:          "completed" | "pending" | "submitted" | "failed" | "voided",
          # transfer-only:
          transfer_type:  e.g. "ach", "non_originated_ach", "internal"
          direction:      raw RH field ("pull" | "push"), kept for forensics
          orig_acct_type: e.g. "external_bank_account", "rhs_account"
          recv_acct_type: ditto
          originator:     details.originator_name when present (e.g. "IRS REFUND")
          note:           details.description fallback
          # dividend-only:
          symbol:         underlying that paid
          # referral-only:
          asset:          stock symbol or "CASH"
        }

    Note: transfers in non-completed states are emitted too (with their `state`),
    so the UI can show pending ACH transfers as in-flight. Failed/voided are emitted
    so they can be greyed out — they're real history even if they don't count.
    """
    out: list[dict] = []

    for t in transfers:
        try:
            amt = float(t.get("amount", 0))
        except (TypeError, ValueError):
            continue
        if amt == 0:
            continue
        date = (t.get("created_at") or "")[:10]
        if not date:
            continue
        tid = t.get("id") or ""
        state = t.get("state", "?")
        direction = t.get("direction", "?")
        transfer_type = t.get("transfer_type", "")
        orig = t.get("originating_account_type", "")
        recv = t.get("receiving_account_type", "")
        details = t.get("details") or {}
        originator = details.get("originator_name") or ""
        note = details.get("description") or ""

        # Classify with the same rules main() uses.
        if transfer_type == "internal" or (orig == "rhs_account" and recv == "rhs_account"):
            kind = "internal"
        elif orig and recv:
            if recv == "rhs_account" and orig != "rhs_account":
                kind = "deposit"
            elif orig == "rhs_account" and recv != "rhs_account":
                kind = "deposit" if direction == "pull" else "withdrawal"
            else:
                continue
        else:
            if direction == "pull":
                kind = "deposit"
            elif direction == "push":
                kind = "withdrawal"
            else:
                continue

        # Schema confirmed from /paymenthub/unified_transfers/ probe — see
        # cmd_debug_transfers. Important fields for tilt analysis:
        #   • created_at — full ISO with TZ (submission time)
        #   • updated_at — last state change (≈ completion time once state=completed)
        #   • details.early_access_amount — instantly-tradeable portion of a deposit
        #   • details.expected_landing_datetime — when ACH should clear
        #   • originating_/receiving_transfer_account_info.account_name_title
        #     — human-readable account names like "cash · Individual" vs raw "rhs_account"
        def _to_float(v):
            try: return float(v) if v not in (None, "") else None
            except (TypeError, ValueError): return None

        early_access = _to_float(details.get("early_access_amount")) if isinstance(details, dict) else None
        expected_landing = details.get("expected_landing_datetime") if isinstance(details, dict) else None
        is_instant_eligible = details.get("is_eligible_for_instant_transfer") if isinstance(details, dict) else None
        service_fee = _to_float(t.get("service_fee"))
        orig_info = t.get("originating_transfer_account_info") or {}
        recv_info = t.get("receiving_transfer_account_info") or {}

        out.append({
            "id": f"transfer:{tid}" if tid else f"transfer:{date}:{amt}:{direction}:{kind}",
            "kind": kind,
            "date": date,
            # Full ISO timestamps preserve time-of-day for tilt analysis. `date` stays
            # as YYYY-MM-DD so the calendar's filter-by-date keeps working unchanged.
            "timestamp": t.get("created_at") or "",
            "updated_at": t.get("updated_at") or "",
            "amount": round(amt, 2),
            "state": state,
            "transfer_type": transfer_type,
            "direction": direction,
            "orig_acct_type": orig,
            "recv_acct_type": recv,
            "orig_account_name": orig_info.get("account_name_title") or orig_info.get("account_name_inline") or "",
            "recv_account_name": recv_info.get("account_name_title") or recv_info.get("account_name_inline") or "",
            "originator": originator,
            "note": note,
            # Instant-deposit metadata — only present on originated_ach deposits, but
            # safe to carry as None elsewhere. Tilt detection keys on `early_access_amount`.
            "early_access_amount": round(early_access, 2) if early_access is not None else None,
            "expected_landing_datetime": expected_landing,
            "is_instant_eligible": is_instant_eligible,
            "service_fee": round(service_fee, 2) if service_fee else None,
        })

    for f in fees:
        try:
            amt = float(f.get("amount", 0))
        except (TypeError, ValueError):
            continue
        date = f.get("date")
        if not date or amt == 0:
            continue
        fid = f.get("id") or f"{date}:{amt}"
        out.append({
            "id": f"gold_fee:{fid}",
            "kind": "gold_fee",
            "date": date,
            "amount": round(amt, 2),
            "state": f.get("state", "?"),
        })

    for d in divs:
        try:
            amt = float(d.get("amount", 0))
        except (TypeError, ValueError):
            continue
        date = d.get("payable_date")
        if not date or amt == 0:
            continue
        did = d.get("id") or f"{date}:{amt}"
        instrument_url = d.get("instrument") or ""
        instrument_id = instrument_url.rstrip("/").split("/")[-1] if instrument_url else ""
        # Prefer the resolved ticker (set by resolve_dividend_symbols); fall back
        # to the instrument UUID prefix so something still renders for unresolved.
        symbol = d.get("_symbol") or ""
        out.append({
            "id": f"dividend:{did}",
            "kind": "dividend",
            "date": date,
            "amount": round(amt, 2),
            "state": d.get("state", "?"),
            "symbol": symbol,                    # e.g. "SPY" when resolved, else ""
            "instrument_id": instrument_id,      # UUID fallback for the UI
        })

    for ref in refs:
        reward = ref.get("reward") or {}
        date = (ref.get("created_at") or "")[:10]
        if not date:
            continue
        rid = ref.get("id") or date
        for i, s in enumerate(reward.get("stocks") or []):
            try:
                cost = float(s.get("cost_basis", 0))
            except (TypeError, ValueError):
                continue
            if cost == 0:
                continue
            out.append({
                "id": f"referral:{rid}:stock:{i}",
                "kind": "referral",
                "date": date,
                "amount": round(cost, 2),
                "state": s.get("state", "?"),
                "asset": s.get("symbol", "?"),
            })
        cash_reward = reward.get("cash")
        if cash_reward:
            try:
                cash_amt = float(cash_reward.get("amount", 0))
            except (TypeError, ValueError):
                cash_amt = 0
            if cash_amt:
                out.append({
                    "id": f"referral:{rid}:cash",
                    "kind": "referral",
                    "date": date,
                    "amount": round(cash_amt, 2),
                    "state": cash_reward.get("state", "?"),
                    "asset": "CASH",
                })

    return out


def merge_events_to_jsonl(events: list, path: Path) -> tuple[int, int]:
    """Merge new events into `path` (JSONL), dedup by stable id, sort by date asc.
    Returns (existing_count, written_count).
    """
    existing: dict[str, dict] = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                eid = rec.get("id")
                if eid:
                    existing[eid] = rec
            except json.JSONDecodeError:
                continue
    prior = len(existing)
    for e in events:
        eid = e.get("id")
        if eid:
            existing[eid] = e  # idempotent overwrite — latest classification wins
    merged = sorted(existing.values(), key=lambda r: (r.get("date") or "", r.get("kind") or "", r.get("id") or ""))
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for e in merged:
            f.write(json.dumps(e) + "\n")
    return prior, len(merged)


def build_historical_snapshots(equity_by_date: dict, dated: dict) -> list:
    """For each date with a known equity value, compute the as-of cashflow
    cumulative totals and derive a synthetic snapshot.
    """
    if not equity_by_date:
        return []
    dates = sorted(equity_by_date.keys())

    # Walk dated lists with pointers, accumulating running totals.
    pointers = {k: 0 for k in dated}
    running = {k: 0.0 for k in dated}

    snapshots = []
    for d in dates:
        for k, lst in dated.items():
            while pointers[k] < len(lst) and lst[pointers[k]][0] <= d:
                running[k] += lst[pointers[k]][1]
                pointers[k] += 1

        deposits = running["deposits"]
        withdrawals = running["withdrawals"]
        gold = running["gold"]
        dividends = running["dividends"]
        referrals = running["referrals"]
        net_deposited = deposits - withdrawals
        basis = net_deposited - gold + dividends + referrals
        equity = equity_by_date[d]
        pnl = equity - basis
        pnl_pct = (pnl / deposits * 100) if deposits else 0
        total_return = equity + withdrawals - deposits
        tr_pct = (total_return / deposits * 100) if deposits else 0

        snapshots.append({
            # Use 16:00 ET (20:00 UTC) so timestamp sorts cleanly relative to
            # live snapshots that are mostly from afternoon cron runs.
            "timestamp": f"{d}T20:00:00+00:00",
            "deposits": round(deposits, 2),
            "withdrawals": round(withdrawals, 2),
            "deposits_pending": 0.0,
            "withdrawals_pending": 0.0,
            "net_deposited": round(net_deposited, 2),
            "gold_fees": round(gold, 2),
            "dividends": round(dividends, 2),
            "referral_grants": round(referrals, 2),
            "net_cash_basis": round(basis, 2),
            "current_equity": round(equity, 2),
            "all_time_pnl": round(pnl, 2),
            "all_time_pnl_pct": round(pnl_pct, 1),
            "total_return": round(total_return, 2),
            "total_return_pct": round(tr_pct, 1),
            "synthetic": True,
        })
    return snapshots


def cmd_backfill(as_json: bool = False) -> None:
    """Reconstruct historical per-day portfolio snapshots from RH's web chart
    endpoint plus all dated cashflows. Overwrites cash_flow_historical.jsonl.
    """
    token = load_token()
    hdrs = headers(token)
    log = (lambda *a, **k: None) if as_json else print

    r = requests.get(f"{API_BASE}/user/", headers=hdrs)
    if r.status_code == 401:
        print("❌ Token expired. Grab a fresh one from browser DevTools.", file=sys.stderr)
        sys.exit(1)

    log("⏳ Backfilling historical snapshots from /portfolio/performance/")
    log("=" * 75)

    # Pull all the same source data main() does
    transfers = paginate(f"{BONFIRE_BASE}/paymenthub/unified_transfers/", hdrs)
    fees = paginate(f"{API_BASE}/subscription/subscription_fees/", hdrs)
    divs = paginate(f"{API_BASE}/dividends/", hdrs)
    refs = paginate(f"{API_BASE}/midlands/referral/", hdrs)
    log(f"  pulled {len(transfers)} transfers, {len(fees)} fees, {len(divs)} dividends, {len(refs)} referrals")

    dated = collect_dated_cashflows(transfers, fees, divs, refs)
    log(f"  {len(dated['deposits'])} completed deposits, {len(dated['withdrawals'])} withdrawals")

    # Also persist per-event log — backfill is the natural moment to rebuild it.
    events = extract_events(transfers, fees, divs, refs)
    events_file = SCRIPT_DIR / "outputs" / "cash_flow_events.jsonl"
    prior, total = merge_events_to_jsonl(events, events_file)
    log(f"  📝 events: {total} total ({total - prior:+d} new) → outputs/cash_flow_events.jsonl")

    # Account list
    accts_file = SCRIPT_DIR / ".rh_accounts.json"
    if accts_file.exists():
        account_numbers = json.loads(accts_file.read_text()).get("account_numbers", [])
    else:
        accts = paginate(f"{API_BASE}/accounts/", hdrs)
        account_numbers = [a["account_number"] for a in accts]

    per_account = {}
    for acct_num in account_numbers:
        series = fetch_portfolio_performance(acct_num, hdrs)
        per_account[acct_num] = series
        log(f"  account {acct_num}: {len(series)} historical points"
            + (f" ({series[0][0]} → {series[-1][0]})" if series else ""))

    equity_by_date = total_equity_by_date(per_account)
    snapshots = build_historical_snapshots(equity_by_date, dated)

    out_file = SCRIPT_DIR / "outputs" / "cash_flow_historical.jsonl"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w") as f:
        for s in snapshots:
            f.write(json.dumps(s) + "\n")

    log(f"\n✅ Wrote {len(snapshots)} synthetic snapshots → {out_file.name}")
    if snapshots:
        log(f"   range: {snapshots[0]['timestamp'][:10]} → {snapshots[-1]['timestamp'][:10]}")


def main(as_json=False):
    token = load_token()
    hdrs = headers(token)

    # Suppress all verbose output in JSON mode
    log = (lambda *a, **k: None) if as_json else print

    # ── Validate token ──
    r = requests.get(f"{API_BASE}/user/", headers=hdrs)
    if r.status_code == 401:
        print("❌ Token expired. Grab a fresh one from browser DevTools.", file=sys.stderr)
        sys.exit(1)

    log("🏦 Cash Flow Summary")
    log("=" * 75)

    # ── 1. Unified Transfers (bonfire) ──
    log("\n📤📥 Transfers (bonfire unified)")
    log("-" * 75)
    transfers = paginate(f"{BONFIRE_BASE}/paymenthub/unified_transfers/", hdrs)

    deposits_completed = 0.0
    deposits_pending = 0.0
    withdrawals_completed = 0.0
    withdrawals_pending = 0.0
    internal_total = 0.0

    for t in transfers:
        amt = float(t.get("amount", 0))
        state = t.get("state", "?")
        direction = t.get("direction", "?")
        transfer_type = t.get("transfer_type", "")
        orig = t.get("originating_account_type", "")
        recv = t.get("receiving_account_type", "")
        dt = t.get("created_at", "")[:10]
        details = t.get("details") or {}
        note = details.get("originator_name") or details.get("description") or ""

        # Classify money flow from the user's perspective.
        # Account-type pair is the unambiguous money-flow signal; the top-level
        # `direction` field encodes the originator's verb, not the user's perspective.
        # An IRS tax refund (non_originated_ach, external→rhs_account, direction=push)
        # is a deposit even though direction=push.
        if transfer_type == "internal" or (orig == "rhs_account" and recv == "rhs_account"):
            category = "internal"
        elif orig and recv:
            if recv == "rhs_account" and orig != "rhs_account":
                category = "deposit"
            elif orig == "rhs_account" and recv != "rhs_account":
                category = "deposit" if direction == "pull" else "withdrawal"
            else:
                log(f"  ⚠️  unknown transfer shape: {transfer_type} {direction} {orig}→{recv} ${amt}")
                continue
        else:
            # Fallback when account-type fields are absent: use direction alone.
            if direction == "pull":
                category = "deposit"
            elif direction == "push":
                category = "withdrawal"
            else:
                log(f"  ⚠️  unknown transfer shape: {transfer_type} {direction} ${amt}")
                continue

        if category == "deposit":
            flow = "         ACH → account "
        elif category == "internal":
            flow = "     account → account "
        else:
            flow = "     account → ACH     "

        if state == "failed":
            label = "  ✗"
        elif state == "pending":
            label = "  ⏳"
        elif category == "internal":
            label = "  ↔"
        else:
            label = "  ✓"

        suffix = ""
        if category == "internal":
            suffix = "  (internal, excluded)"
        elif note:
            suffix = f"  [{transfer_type}: {note}]"
        elif transfer_type:
            suffix = f"  [{transfer_type}]"
        log(f"{label} {dt}  {flow}  ${amt:>10,.2f}  {state}{suffix}")

        if state == "failed":
            continue
        if category == "internal":
            internal_total += amt
            continue
        if category == "deposit":
            if state == "pending":
                deposits_pending += amt
            else:
                deposits_completed += amt
        elif category == "withdrawal":
            if state == "pending":
                withdrawals_pending += amt
            else:
                withdrawals_completed += amt

    log(f"\n  Deposits:    ${deposits_completed:>10,.2f} completed   ${deposits_pending:>10,.2f} pending")
    log(f"  Withdrawals: ${withdrawals_completed:>10,.2f} completed   ${withdrawals_pending:>10,.2f} pending")
    if internal_total:
        log(f"  Internal:    ${internal_total:>10,.2f} (excluded — inter-account moves)")

    # ── 2. Gold Fees ──
    log(f"\n💳 Gold Subscription Fees")
    log("-" * 75)
    fees = paginate(f"{API_BASE}/subscription/subscription_fees/", hdrs)

    total_gold = 0.0
    for f in fees:
        amt = float(f["amount"])
        total_gold += amt
        log(f"  {f['date']}  ${amt:>6,.2f}  {f['state']}")

    log(f"\n  Total Gold: ${total_gold:>10,.2f} ({len(fees)} months)")

    # ── 3. Dividends ──
    log(f"\n💰 Dividends")
    log("-" * 75)
    divs = paginate(f"{API_BASE}/dividends/", hdrs)
    # Attach `_symbol` to each dividend so the events log shows tickers, not UUIDs.
    resolve_dividend_symbols(divs, hdrs, log=log)

    total_div = 0.0
    pending_div = 0.0
    for d in divs:
        amt = float(d["amount"])
        state = d["state"]
        sym = d.get("_symbol") or "?"
        if state == "voided":
            log(f"  {d['payable_date']}  {sym:<6} ${amt:>8,.2f}  {state} (not counted)")
            continue
        if state not in PAID_DIVIDEND_STATES:
            # Pending / scheduled future payments: visible in the events log,
            # but EXCLUDED from the running basis until they actually settle.
            # Otherwise pending divs inflate basis and deflate displayed P/L.
            pending_div += amt
            log(f"  {d['payable_date']}  {sym:<6} ${amt:>8,.2f}  {state} (pending — not in basis)")
            continue
        total_div += amt
        log(f"  {d['payable_date']}  {sym:<6} ${amt:>8,.2f}  {state}")

    log(f"\n  Total paid dividends: ${total_div:>10,.2f}"
        + (f"   (+${pending_div:,.2f} pending, excluded)" if pending_div else ""))

    # ── 4. Referral Stock Grants ──
    log(f"\n🎁 Referral Stock Grants")
    log("-" * 75)
    refs = paginate(f"{API_BASE}/midlands/referral/", hdrs)

    total_referral = 0.0
    for ref in refs:
        reward = ref.get("reward", {})
        stocks = reward.get("stocks", [])
        cash_reward = reward.get("cash")
        dt = ref.get("created_at", "")[:10]
        direction = ref.get("direction", "?")
        ref_state = ref.get("state", "?")

        for s in stocks:
            sym = s.get("symbol", "?")
            cost = float(s.get("cost_basis", 0))
            s_state = s.get("state", "?")
            if s_state in ("failed", "voided"):
                log(f"  {dt}  {sym}  ${cost:>8,.2f}  {s_state} (not counted)")
                continue
            total_referral += cost
            log(f"  {dt}  {sym}  ${cost:>8,.2f}  {s_state}")

        if cash_reward:
            cash_amt = float(cash_reward.get("amount", 0))
            c_state = cash_reward.get("state", "?")
            if c_state not in ("failed", "voided"):
                total_referral += cash_amt
                log(f"  {dt}  CASH  ${cash_amt:>8,.2f}  {c_state}")

    log(f"\n  Total referral grants: ${total_referral:>10,.2f}")

    # ── 5. Current Equity (both accounts) ──
    log(f"\n📊 Current Portfolio")
    log("-" * 75)

    # Get account numbers from cache
    accts_file = SCRIPT_DIR / ".rh_accounts.json"
    if accts_file.exists():
        account_numbers = json.loads(accts_file.read_text()).get("account_numbers", [])
    else:
        accts = paginate(f"{API_BASE}/accounts/", hdrs)
        account_numbers = [a["account_number"] for a in accts]

    equity = 0.0
    accounts_breakdown = []
    for acct_num in account_numbers:
        r = requests.get(f"{API_BASE}/accounts/{acct_num}/", headers=hdrs)
        if r.status_code != 200:
            continue
        a = r.json()
        acct_type = a.get("type", "?")
        cash = float(a.get("portfolio_cash", 0))

        # For margin/individual accounts, get portfolio equity (includes positions)
        pr = requests.get(f"{API_BASE}/portfolios/{acct_num}/", headers=hdrs)
        if pr.status_code == 200:
            pd = pr.json()
            eq = float(pd.get("extended_hours_equity", pd.get("equity", 0)))
        else:
            eq = cash  # cash-only account

        equity += eq
        accounts_breakdown.append({
            "type": acct_type,
            "account_number": acct_num,
            "equity": round(eq, 2),
            "cash": round(cash, 2),
        })
        log(f"  {acct_type:<12} ({acct_num}):  ${eq:>10,.2f}")

    log(f"\n  Total equity: ${equity:>10,.2f}")

    # ── 6. Summary ──
    # Pending transfers are baked into basis: RH already debits/credits equity
    # the moment a transfer is initiated, so excluding pending from basis would
    # make all_time_pnl and total_return drift by the size of any in-flight ACH.
    # Pending almost always clears, and on the next run it'll move from pending
    # to completed with no math change.
    deposits_total = deposits_completed + deposits_pending
    withdrawals_total = withdrawals_completed + withdrawals_pending
    net_deposited = deposits_total - withdrawals_total
    cost_basis = net_deposited - total_gold + total_div + total_referral
    pnl = equity - cost_basis
    pnl_pct = (pnl / deposits_total * 100) if deposits_total else 0
    total_return = equity + withdrawals_total - deposits_total
    tr_pct = (total_return / deposits_total * 100) if deposits_total else 0

    # Always append snapshot to JSONL
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "deposits": round(deposits_completed, 2),
        "withdrawals": round(withdrawals_completed, 2),
        "deposits_pending": round(deposits_pending, 2),
        "withdrawals_pending": round(withdrawals_pending, 2),
        "net_deposited": round(net_deposited, 2),
        "gold_fees": round(total_gold, 2),
        "gold_months": len(fees),
        "dividends": round(total_div, 2),
        "referral_grants": round(total_referral, 2),
        "net_cash_basis": round(cost_basis, 2),
        "current_equity": round(equity, 2),
        "all_time_pnl": round(pnl, 2),
        "all_time_pnl_pct": round(pnl_pct, 1),
        "total_return": round(total_return, 2),
        "total_return_pct": round(tr_pct, 1),
        "accounts": accounts_breakdown,
    }
    out_file = SCRIPT_DIR / "outputs" / "cash_flow.jsonl"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "a") as f:
        f.write(json.dumps(entry) + "\n")

    # ── 7. Per-event log (powers calendar day-by-day cashflow visualization) ──
    events = extract_events(transfers, fees, divs, refs)
    events_file = SCRIPT_DIR / "outputs" / "cash_flow_events.jsonl"
    prior, total = merge_events_to_jsonl(events, events_file)
    new_count = total - prior
    log(f"\n📝 Cashflow events: {total} total ({new_count:+d} new) → outputs/cash_flow_events.jsonl")

    if as_json:
        return

    print(f"\n{'=' * 75}")
    print("📋 SUMMARY (pending transfers included in basis)")
    print(f"{'=' * 75}")
    pending_dep_note = f"  (+${deposits_pending:,.2f} pending)" if deposits_pending else ""
    pending_wd_note = f"  (+${withdrawals_pending:,.2f} pending)" if withdrawals_pending else ""
    print(f"  Deposits:           ${deposits_total:>10,.2f}{pending_dep_note}")
    print(f"  Withdrawals:       -${withdrawals_total:>10,.2f}{pending_wd_note}")
    print(f"  Net deposited:      ${net_deposited:>10,.2f}")
    print(f"  Gold fees:         -${total_gold:>10,.2f}")
    print(f"  Dividends:         +${total_div:>10,.2f}")
    print(f"  Referral grants:   +${total_referral:>10,.2f}")
    print(f"  ─────────────────────────────────")
    print(f"  Net cash basis:     ${cost_basis:>10,.2f}")
    print(f"  Current equity:     ${equity:>10,.2f}")
    print(f"  ─────────────────────────────────")
    emoji = "🟢" if pnl >= 0 else "🔴"
    print(f"  {emoji} All-time P/L:     ${pnl:>10,.2f}  ({pnl_pct:+.1f}% on ${deposits_total:,.2f} deposited)")

    tr_emoji = "🟢" if total_return >= 0 else "🔴"
    print(f"  {tr_emoji} Total return:     ${total_return:>10,.2f}  ({tr_pct:+.1f}%)")
    print(f"       (equity + withdrawals - deposits, includes fees/dividends/referrals)")


def cmd_debug_transfers(limit: int = 3) -> None:
    """Dump full raw JSON of the most recent N transfers from RH's unified_transfers endpoint.

    Use this to inspect what fields RH actually returns — useful for figuring out where
    instant-deposit flags, completion timestamps, and other tilt-relevant fields live.
    """
    token = load_token()
    hdrs = headers(token)
    r = requests.get(f"{API_BASE}/user/", headers=hdrs)
    if r.status_code == 401:
        print("❌ Token expired. Run `python hood.py --save-token \"Bearer ...\"` first.", file=sys.stderr)
        sys.exit(1)
    transfers = paginate(f"{BONFIRE_BASE}/paymenthub/unified_transfers/", hdrs)
    if not transfers:
        print("No transfers returned.")
        return
    # Sort newest first by created_at
    transfers.sort(key=lambda t: t.get("created_at", ""), reverse=True)
    sample = transfers[:limit]
    print(f"=== {len(transfers)} total transfers; showing {len(sample)} most recent ===\n")
    for i, t in enumerate(sample, 1):
        print(f"--- Transfer #{i} ({t.get('state', '?')} {t.get('transfer_type', '?')}) ---")
        print(json.dumps(t, indent=2, default=str))
        print()
    # Also list every distinct top-level key seen across ALL transfers — schema map
    all_keys = set()
    for t in transfers:
        all_keys.update(t.keys())
    print(f"=== All top-level keys across all {len(transfers)} transfers ===")
    print(sorted(all_keys))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="Output summary as JSON")
    parser.add_argument("--backfill", action="store_true",
                        help="One-shot: rebuild cash_flow_historical.jsonl from RH's portfolio chart endpoint")
    parser.add_argument("--debug-transfers", action="store_true",
                        help="Dump the full JSON of recent transfers to inspect the schema (instant flags, completion times, etc.)")
    parser.add_argument("--debug-limit", type=int, default=3,
                        help="Number of recent transfers to dump with --debug-transfers (default 3)")
    args = parser.parse_args()
    if args.debug_transfers:
        cmd_debug_transfers(limit=args.debug_limit)
    elif args.backfill:
        cmd_backfill(as_json=args.json)
    else:
        main(as_json=args.json)
