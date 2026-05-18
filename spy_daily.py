#!/usr/bin/env python3
"""SPY + VIX daily OHLC cache.

Fetches end-of-day OHLC for SPY and VIX from yfinance and persists to
`outputs/spy_daily.json`. The dashboard's Calendar view reads this to show
per-day market context (SPY % change, VIX level) on every cell — even days
the user didn't trade.

Idempotent: each run overwrites the cache with fresh data from the
earliest needed date through today. Designed to be safe to run multiple
times per day (cron + manual via Admin tab).

Range is inferred from outputs/spy_trades.csv (earliest trade date − 7 days)
unless --since is passed. Falls back to a 5-year window if no trade CSV exists.

Usage:
    python spy_daily.py                    # auto-range, refresh full cache
    python spy_daily.py --since 2024-01-01 # explicit start
    python spy_daily.py --json             # silent mode (cron-friendly)
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = SCRIPT_DIR / "outputs"
OUT_FILE = OUTPUTS_DIR / "spy_daily.json"
TRADES_CSV = OUTPUTS_DIR / "spy_trades.csv"


def _earliest_trade_date() -> date | None:
    """Earliest entry-Date in spy_trades.csv, or None if CSV missing/empty."""
    if not TRADES_CSV.exists():
        return None
    try:
        df = pd.read_csv(TRADES_CSV, usecols=["Date"])
        dates = pd.to_datetime(df["Date"], errors="coerce").dropna()
        if not len(dates):
            return None
        return dates.min().date()
    except Exception:
        return None


def _series_from_yf(symbol: str, start: date, end: date) -> dict[str, dict]:
    """Pull daily OHLC for one symbol. Returns {YYYY-MM-DD: {open, high, low, close}}.

    `end` is exclusive in yfinance's API, so we pass end+1 to include today.
    """
    try:
        df = yf.download(symbol, start=start.isoformat(),
                         end=(end + timedelta(days=1)).isoformat(),
                         progress=False, auto_adjust=False)
    except Exception as e:
        print(f"  ⚠ {symbol}: yfinance error: {e}", file=sys.stderr)
        return {}
    if df is None or df.empty:
        return {}
    # yfinance may return MultiIndex columns when ticker is a list — flatten if so
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    out = {}
    for ts, row in df.iterrows():
        d = ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)[:10]
        try:
            o = float(row["Open"]); h = float(row["High"])
            l = float(row["Low"]); c = float(row["Close"])
        except (KeyError, TypeError, ValueError):
            continue
        if pd.isna(o) or pd.isna(c):
            continue
        out[d] = {"open": round(o, 2), "high": round(h, 2),
                  "low": round(l, 2), "close": round(c, 2)}
    return out


def build_payload(since: date | None = None, log=print) -> dict:
    """Returns the full payload that gets written to outputs/spy_daily.json."""
    if since is None:
        ed = _earliest_trade_date()
        if ed:
            since = ed - timedelta(days=7)
            log(f"  range start: {since} (earliest trade − 7d)")
        else:
            since = date.today() - timedelta(days=365 * 5)
            log(f"  range start: {since} (no trade CSV — 5y fallback)")
    today = date.today()
    log(f"  fetching SPY {since} → {today}")
    spy = _series_from_yf("SPY", since, today)
    log(f"  fetching ^VIX {since} → {today}")
    vix = _series_from_yf("^VIX", since, today)
    log(f"  got {len(spy)} SPY days, {len(vix)} VIX days")

    # Combine into a per-date record with prior-close-based % change for SPY.
    all_dates = sorted(set(spy) | set(vix))
    days: list[dict] = []
    prev_spy_close: float | None = None
    for d in all_dates:
        rec: dict = {"date": d}
        s = spy.get(d)
        if s:
            rec["spy_open"]  = s["open"]
            rec["spy_high"] = s["high"]
            rec["spy_low"]  = s["low"]
            rec["spy_close"] = s["close"]
            if prev_spy_close is not None and prev_spy_close > 0:
                rec["spy_pct"] = round((s["close"] - prev_spy_close) / prev_spy_close * 100, 2)
            prev_spy_close = s["close"]
        v = vix.get(d)
        if v:
            rec["vix_close"] = v["close"]
        days.append(rec)

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "range": {"start": (days[0]["date"] if days else None),
                  "end":   (days[-1]["date"] if days else None)},
        "days": days,
    }


def main(as_json: bool = False, since: str | None = None) -> None:
    log = (lambda *a, **k: None) if as_json else print
    log("📈 SPY + VIX daily cache")
    log("=" * 60)
    since_d = None
    if since:
        try:
            since_d = datetime.strptime(since, "%Y-%m-%d").date()
        except ValueError:
            print(f"❌ bad --since (need YYYY-MM-DD): {since}", file=sys.stderr)
            sys.exit(1)
    payload = build_payload(since=since_d, log=log)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps(payload, indent=2))
    log(f"  wrote {len(payload['days'])} days → {OUT_FILE.relative_to(SCRIPT_DIR)}")
    if as_json:
        print(json.dumps({"days": len(payload["days"]),
                          "range": payload["range"],
                          "generated_at": payload["generated_at"]}))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--since", help="YYYY-MM-DD start date (default: earliest trade − 7d)")
    p.add_argument("--json", action="store_true", help="Silent mode, print JSON summary")
    args = p.parse_args()
    main(as_json=args.json, since=args.since)
