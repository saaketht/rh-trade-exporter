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

API_BASE = "https://api.robinhood.com"
BONFIRE_BASE = "https://bonfire.robinhood.com"


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

    total_div = 0.0
    for d in divs:
        amt = float(d["amount"])
        state = d["state"]
        if state == "voided":
            log(f"  {d['payable_date']}  ${amt:>8,.2f}  {state} (not counted)")
            continue
        total_div += amt
        log(f"  {d['payable_date']}  ${amt:>8,.2f}  {state}")

    log(f"\n  Total dividends: ${total_div:>10,.2f}")

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
    net_deposited = deposits_completed - withdrawals_completed
    net_deposited_with_pending = (deposits_completed + deposits_pending) - (withdrawals_completed + withdrawals_pending)
    cost_basis = net_deposited - total_gold + total_div + total_referral
    pnl = equity - cost_basis
    pnl_pct = (pnl / deposits_completed * 100) if deposits_completed else 0
    total_return = equity + withdrawals_completed - deposits_completed
    tr_pct = (total_return / deposits_completed * 100) if deposits_completed else 0

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

    if as_json:
        return

    print(f"\n{'=' * 75}")
    print("📋 SUMMARY (completed transactions only)")
    print(f"{'=' * 75}")
    print(f"  Deposits:           ${deposits_completed:>10,.2f}")
    print(f"  Withdrawals:       -${withdrawals_completed:>10,.2f}")
    print(f"  Net deposited:      ${net_deposited:>10,.2f}")
    print(f"  Gold fees:         -${total_gold:>10,.2f}")
    print(f"  Dividends:         +${total_div:>10,.2f}")
    print(f"  Referral grants:   +${total_referral:>10,.2f}")
    print(f"  ─────────────────────────────────")
    print(f"  Net cash basis:     ${cost_basis:>10,.2f}")
    print(f"  Current equity:     ${equity:>10,.2f}")
    print(f"  ─────────────────────────────────")
    emoji = "🟢" if pnl >= 0 else "🔴"
    print(f"  {emoji} All-time P/L:     ${pnl:>10,.2f}  ({pnl_pct:+.1f}% on ${deposits_completed:,.2f} deposited)")

    tr_emoji = "🟢" if total_return >= 0 else "🔴"
    print(f"  {tr_emoji} Total return:     ${total_return:>10,.2f}  ({tr_pct:+.1f}%)")
    print(f"       (equity + withdrawals - deposits, includes fees/dividends/referrals)")

    if deposits_pending or withdrawals_pending:
        print(f"\n  ⏳ Pending: +${deposits_pending:,.2f} deposits, -${withdrawals_pending:,.2f} withdrawals")
        future_basis = net_deposited_with_pending - total_gold + total_referral + total_div
        future_pnl = equity - future_basis
        future_deps = deposits_completed + deposits_pending
        future_pct = (future_pnl / future_deps * 100) if future_deps else 0
        emoji2 = "🟢" if future_pnl >= 0 else "🔴"
        print(f"  {emoji2} P/L after pending: ${future_pnl:>10,.2f}  ({future_pct:+.1f}%)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="Output summary as JSON")
    parser.add_argument("--backfill", action="store_true",
                        help="One-shot: rebuild cash_flow_historical.jsonl from RH's portfolio chart endpoint")
    args = parser.parse_args()
    if args.backfill:
        cmd_backfill(as_json=args.json)
    else:
        main(as_json=args.json)
