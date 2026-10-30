"""Nightly health check — posts to Discord only when something is off.

Runs from vps/run.sh after the refresh chain. Checks:
  1. Steps — any non-zero step in outputs/.last_refresh.json, or no refresh today.
  2. Gap days — dates in the last LOOKBACK_DAYS where Robinhood shows filled option
     executions but no CSV row (entry or exit) or open position covers that date.
     This is the Oct 16 2026 case: a dead token, then a fixed one-day window that
     never looked back. Gaps on/after hood's incremental cursor are skipped: the
     next run refetches those by itself.
  3. Option bars — today's outputs/option_intraday file missing while there were
     trades today, or listing contracts it couldn't fetch.

  python health_check.py            # check; post to Discord only if something is off
  python health_check.py --dry-run  # print the report, never post
  python health_check.py --always   # post even when everything is fine (testing)
"""
import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from zoneinfo import ZoneInfo

import hood
import token_watch as tw

BASE = Path(__file__).resolve().parent
OUT = BASE / "outputs"
ET = ZoneInfo("America/New_York")
LOOKBACK_DAYS = 14


def step_issues(state_file: Path, today: str) -> list[str]:
    if not state_file.exists():
        return ["no refresh record (outputs/.last_refresh.json) — has the chain run?"]
    s = json.loads(state_file.read_text())
    issues = []
    ended = datetime.fromisoformat(s["ended_at"]).astimezone(ET).date().isoformat()
    if ended != today:
        issues.append(f"last refresh ended {ended}, not today")
    issues += [f"step `{n}` exited {rc}" for n, rc in s.get("steps", {}).items() if rc != 0]
    return issues


def execution_dates(orders: list[dict]) -> dict[str, int]:
    """ET date → number of filled executions across all legs."""
    out: dict[str, int] = {}
    for o in orders:
        for leg in o.get("legs", []):
            for e in leg.get("executions", []):
                ts = e.get("timestamp")
                if ts:
                    d = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(ET).date().isoformat()
                    out[d] = out.get(d, 0) + 1
    return out


def covered_dates(out_dir: Path) -> set[str]:
    """Dates some CSV row accounts for: trade entries, trade exits, open positions."""
    dates: set[str] = set()
    for name in ("spy_trades.csv", "other_trades.csv"):
        p = out_dir / name
        if not p.exists():
            continue
        df = pd.read_csv(p, usecols=["Date", "Entry Time", "Hold Time (min)"])
        en = pd.to_datetime(df["Date"] + " " + df["Entry Time"].astype(str), errors="coerce")
        ex = en + pd.to_timedelta(pd.to_numeric(df["Hold Time (min)"], errors="coerce").fillna(0), unit="m")
        dates |= set(en.dropna().dt.date.astype(str)) | set(ex.dropna().dt.date.astype(str))
    p = out_dir / "unmatched_opens.csv"
    if p.exists():
        dates |= set(pd.to_datetime(pd.read_csv(p, usecols=["Date"])["Date"], errors="coerce").dropna().dt.date.astype(str))
    return dates


def gap_issues(executions: dict[str, int], covered: set[str], cursor: str | None) -> list[str]:
    gaps = sorted(d for d in executions if d not in covered and (cursor is None or d <= cursor))
    return [f"gap day {d}: {executions[d]} filled executions on Robinhood, no rows in the CSVs "
            f"(fix: `hood.py --after-date {(date.fromisoformat(d) - timedelta(days=1)).isoformat()}`)"
            for d in gaps]


def option_issues(out_dir: Path, today: str, traded_today: bool) -> list[str]:
    p = out_dir / "option_intraday" / f"{today}.json"
    if not p.exists():
        return ["no option bars saved for today"] if traded_today else []
    missing = json.loads(p.read_text()).get("missing") or []
    return [f"option bars missing for {len(missing)} contract(s) today: "
            + ", ".join(f"{m.get('symbol')} {m.get('strike')}{(m.get('type') or '?')[0].upper()} ({m.get('reason')})"
                        for m in missing[:5])] if missing else []


def fetch_executions(since: str) -> dict[str, int]:
    token = tw.read_token()
    if not token:
        raise RuntimeError("no .rh_token")
    accounts = hood.load_cached_accounts()
    if not accounts:
        raise RuntimeError("no cached account numbers (.rh_accounts.json)")
    orders = hood.fetch_all_options_orders(hood.make_headers(f"Bearer {token}"), accounts,
                                           after_date=since, filled_only=True)
    return execution_dates(orders)


def run(today: str | None = None, out_dir: Path = OUT, fetch=fetch_executions) -> list[str]:
    today = today or datetime.now(ET).date().isoformat()
    issues = step_issues(out_dir / ".last_refresh.json", today)
    since = (date.fromisoformat(today) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    covered = covered_dates(out_dir)
    try:
        execs = fetch(since)
        issues += gap_issues({d: n for d, n in execs.items() if d >= since}, covered,
                             hood.determine_incremental_cursor(out_dir))
    # Network/auth/RH shape, or hood's sys.exit on a corrupt account cache: report it,
    # don't crash the cron.
    except (Exception, SystemExit) as e:  # noqa: BLE001
        issues.append(f"couldn't check gap days: {e!r}")
        execs = {}
    issues += option_issues(out_dir, today, traded_today=today in covered or today in execs)
    return issues


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="print the report, never post")
    ap.add_argument("--always", action="store_true", help="post even when everything is fine")
    a = ap.parse_args(argv)

    issues = run()
    body = ("⚠️ **Nightly health** — something needs a look:\n" + "\n".join(f"• {i}" for i in issues)
            if issues else "✅ **Nightly health** — all steps ok, no gap days, option bars saved.")
    print(body)
    if a.dry_run or not (issues or a.always):
        return 0
    sent = tw.notify("RH nightly health", body, tw.env_value("DISCORD_WEBHOOK_URL"), tw.env_value("ALERT_EMAIL"))
    print(f"  → sent via {', '.join(sent) or 'nothing (no DISCORD_WEBHOOK_URL configured)'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
