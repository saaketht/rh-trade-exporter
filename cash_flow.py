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
        dt = t.get("created_at", "")[:10]

        # Skip internal inter-account transfers (margin ↔ cash) — money stays in RH
        is_internal = transfer_type == "internal"

        if direction == "pull":
            flow = "         ACH → account "
        elif is_internal:
            flow = "     account → account "
        else:
            flow = "     account → ACH     "

        if state == "failed":
            label = "  ✗"
        elif state == "pending":
            label = "  ⏳"
        elif is_internal:
            label = "  ↔"
        else:
            label = "  ✓"

        suffix = "  (internal, excluded)" if is_internal else ""
        log(f"{label} {dt}  {flow}  ${amt:>10,.2f}  {state}{suffix}")

        if state == "failed":
            continue
        if is_internal:
            internal_total += amt
            continue
        if direction == "pull":  # deposit
            if state == "pending":
                deposits_pending += amt
            else:
                deposits_completed += amt
        elif direction == "push":  # withdrawal
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

    # --json: emit structured JSON and exit
    if as_json:
        print(json.dumps({
            "deposits": round(deposits_completed, 2),
            "withdrawals": round(withdrawals_completed, 2),
            "net_deposited": round(net_deposited, 2),
            "gold_fees": round(total_gold, 2),
            "dividends": round(total_div, 2),
            "referral_grants": round(total_referral, 2),
            "net_cash_basis": round(cost_basis, 2),
            "current_equity": round(equity, 2),
            "all_time_pnl": round(pnl, 2),
            "all_time_pnl_pct": round(pnl_pct, 1),
            "total_return": round(total_return, 2),
            "total_return_pct": round(tr_pct, 1),
        }))
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
    args = parser.parse_args()
    main(as_json=args.json)
