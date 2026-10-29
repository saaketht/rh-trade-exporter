"""Disposition-score pre-registration: frozen definitions + runner.

Ported from the 2026-10-05 session scratchpad (common.py / exposure.py / tests.py,
recovered verbatim from transcript 6b201fc4). The definitions here ARE the
pre-registration — see PREREG.md. Do not edit them to make a result come out;
any change must be dated in PREREG.md before the check is run.

  python analysis/prereg/disposition.py replicate   # original Test 1 + 2 on the registered sample
  python analysis/prereg/disposition.py status      # out-of-sample: eligible-day counts only, no outcomes
  python analysis/prereg/disposition.py oos         # the out-of-sample check (refuses until it's due)
"""
import argparse
import glob
import json
import os
import sys
from datetime import date

import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# ── Frozen parameters (PREREG.md) ─────────────────────────────────────────────
STEP_MIN = 5            # interval length; state is read at each interval start
SPY_BAND = 0.05         # $ SPY move inside which an order counts as flat (proxy state)
OPT_BAND = 0.02         # 2% premium move inside which an order counts as flat (option state)
EARLY_MIN = 60          # Test 1 window: first 60 min after the day's first entry
T1_MIN_EACH = 3         # Test 1 eligibility: >= 3 green and >= 3 red early intervals
T2_MIN_EACH = 5         # Test 2 eligibility: >= 5 green and >= 5 red intervals
PASS_RHO = -0.20
PERMS = 10_000
SEED = 7
SPLIT = "2026-07-01"    # halves for the registered sample
REG_END = "2026-10-02"  # registered sample: orders through this date
OOS_START = "2026-10-06"  # first trading day after registration (2026-10-05, evening)
OOS_STEP_MIN = 1        # AMENDMENT 2026-10-29: primary OOS test uses 1-minute intervals (see PREREG.md)
OOS_MIN_DAYS = 20       # run the OOS check once this many days are eligible ...
OOS_EARLIEST = "2026-12-04"  # ... but not before this date
OOS_LATEST = "2027-01-29"    # if still < OOS_FLOOR eligible days here: report inconclusive
OOS_FLOOR = 15
OOS_P = 0.05            # single primary test → no Bonferroni


# ── Data ──────────────────────────────────────────────────────────────────────
def load_orders(root=ROOT):
    """Trade rows (one per closing execution) with entry/exit datetimes and the GROSS P/L
    the original test used (P/L ($), not Net P/L)."""
    df = pd.read_csv(f"{root}/outputs/spy_trades.csv")
    df["en"] = pd.to_datetime(df["Date"] + " " + df["Entry Time"].astype(str))
    df["ex"] = df["en"] + pd.to_timedelta(df["Hold Time (min)"].astype(float), unit="m")
    df["pl"] = df["P/L ($)"].astype(float)
    df["cost"] = df["Entry Cost"].abs().astype(float)
    df["px_in"] = df["cost"] / df["Qty"] / 100
    df["iso"] = df.en.dt.date.astype(str)
    df["exp"] = pd.to_datetime(df["Expiry Date"]).dt.date.astype(str)
    df["ck"] = df["exp"] + "|" + df["Type"].str.lower() + "|" + df["Strike"].astype(float).map(lambda x: f"{x:g}")
    df["sign"] = np.where(df["Type"].str.lower() == "call", 1, -1)
    return df


class Spy:
    """SPY 5-minute bars per day from outputs/spy_intraday/."""
    def __init__(self, root=ROOT):
        self.root, self.cache = root, {}

    def day(self, iso):
        if iso not in self.cache:
            p, s = f"{self.root}/outputs/spy_intraday/{iso}.json", None
            if os.path.exists(p):
                b = json.load(open(p)).get("bars") or []
                if b:
                    idx = pd.to_datetime([x["t"] for x in b], unit="s", utc=True).tz_convert(ET).tz_localize(None)
                    s = pd.DataFrame({"o": [x["o"] for x in b], "c": [x["c"] for x in b]}, index=idx).sort_index()
            self.cache[iso] = s
        return self.cache[iso]

    def at(self, iso, t):
        """Close of the last 5-min bar fully ended by t; else open of the bar containing t."""
        s = self.day(iso)
        if s is None:
            return np.nan
        done = s[s.index + pd.Timedelta(minutes=5) <= t]
        if len(done):
            return done.c.iloc[-1]
        cur = s[s.index <= t]
        return cur.o.iloc[-1] if len(cur) else np.nan


def load_options(root=ROOT):
    """{(iso, ck): (close series, bar minutes)} from outputs/option_intraday/ (any source)."""
    P = {}
    for f in glob.glob(f"{root}/outputs/option_intraday/*.json"):
        j = json.load(open(f))
        for c in j.get("contracts", []):
            if not c.get("bars"):
                continue
            ck = f"{c['expiry']}|{c['type']}|{float(c['strike']):g}"
            idx = pd.to_datetime([b["t"] for b in c["bars"]], unit="s", utc=True).tz_convert(ET).tz_localize(None)
            step = 1 if c.get("interval") == "minute" else 5
            P[(j["date"], ck)] = (pd.Series([b["c"] for b in c["bars"]], index=idx).sort_index(), step)
    return P


# ── States ────────────────────────────────────────────────────────────────────
def spy_state_fn(spy, band=SPY_BAND):
    def state(g, t):
        iso = g.iso.iloc[0]
        a, b = spy.at(iso, g.en.iloc[0]), spy.at(iso, t)
        if np.isnan(a) or np.isnan(b):
            return None
        m = (b - a) * g.sign.iloc[0]
        return "green" if m > band else ("red" if m < -band else "flat")
    return state


def opt_state_fn(P, band=OPT_BAND):
    def state(g, t):
        v = P.get((g.iso.iloc[0], g.ck.iloc[0]))
        if v is None:
            return None
        s, step = v
        done = s[s.index + pd.Timedelta(minutes=step) <= t]
        if not len(done):
            return None
        a, b = g.px_in.iloc[0], done.iloc[-1]
        m = (b - a) / a
        return "green" if m > band else ("red" if m < -band else "flat")
    return state


def intervals(df, state_fn, step=STEP_MIN):
    """One record per order (Group ID) per 5-min interval held. State at the interval
    start (first one `step` minutes after entry); sold = any exit of that order inside
    (t, t+step]."""
    rec = []
    for gid, g in df.groupby("Group ID"):
        t0, tend, exits = g.en.iloc[0], g.ex.max(), list(g.ex)
        t = t0 + pd.Timedelta(minutes=step)
        while t < tend:
            st = state_fn(g, t)
            if st is not None:
                sold = any(t < e <= t + pd.Timedelta(minutes=step) for e in exits)
                rec.append(dict(gid=gid, iso=g.iso.iloc[0], t=t, state=st, sold=sold))
            t += pd.Timedelta(minutes=step)
    return pd.DataFrame(rec, columns=["gid", "iso", "t", "state", "sold"])


# ── Statistics ────────────────────────────────────────────────────────────────
def score(x):
    """D = share of green intervals with a sale − share of red intervals with a sale."""
    g, r = x[x.state == "green"], x[x.state == "red"]
    d = (g.sold.mean() if len(g) else np.nan) - (r.sold.mean() if len(r) else np.nan)
    return pd.Series(dict(ng=len(g), nr=len(r), D=d))


def ret(o):
    return o.pl.sum() / o.cost.sum() if o.cost.sum() > 0 else np.nan


def spearman(a, b):
    return pd.Series(a).rank().corr(pd.Series(b).rank())


def perm_p(a, b, n=PERMS, seed=SEED):
    """One-sided (negative) p from n random shuffles of b's ranks."""
    rng = np.random.default_rng(seed)
    rho = spearman(a, b)
    ra, rb = pd.Series(a).rank().values, pd.Series(b).rank().values
    sims = np.array([np.corrcoef(ra, rng.permutation(rb))[0, 1] for _ in range(n)])
    return rho, (sims <= rho).mean()


def test1_table(df, J, early_min=EARLY_MIN, k=T1_MIN_EACH):
    """Test 1 rows: per eligible day, early-window D and the return on premium of orders
    ENTERED after the window."""
    first = df.groupby("iso").en.min()
    E = J[J.t < J.iso.map(first) + pd.Timedelta(minutes=early_min)]
    rows = []
    for iso, x in E.groupby("iso"):
        s = score(x)
        if s.ng < k or s.nr < k:
            continue
        later = df[(df.iso == iso) & (df.en >= first[iso] + pd.Timedelta(minutes=early_min))]
        if later["Group ID"].nunique() < 1:
            continue
        rows.append(dict(iso=iso, D=s.D, out=ret(later), usd=later.pl.sum()))
    return pd.DataFrame(rows, columns=["iso", "D", "out", "usd"])


def test2_table(df, J, k=T2_MIN_EACH):
    days = sorted(df.iso.unique())
    nxt = {d: days[i + 1] for i, d in enumerate(days[:-1])}
    rows = []
    for iso, x in J.groupby("iso"):
        s = score(x)
        if s.ng < k or s.nr < k or iso not in nxt:
            continue
        o = df[df.iso == nxt[iso]]
        rows.append(dict(iso=iso, D=s.D, out=ret(o), usd=o.pl.sum()))
    return pd.DataFrame(rows, columns=["iso", "D", "out", "usd"])


def report(name, T, p_cut, halves=True):
    rho, p = perm_p(T.D, T.out)
    ok = rho <= PASS_RHO and p < p_cut
    line = f"{name}: n={len(T)} days  rho={rho:+.2f}  one-sided p={p:.3f}"
    if halves:
        h1, h2 = T[T.iso < SPLIT], T[T.iso >= SPLIT]
        r1, r2 = spearman(h1.D, h1.out), spearman(h2.D, h2.out)
        ok = ok and r1 < 0 and r2 < 0
        line += f"  halves: {r1:+.2f} (n={len(h1)}) / {r2:+.2f} (n={len(h2)})"
    print(f"\n{line}  -> {'PASS' if ok else 'FAIL'}")
    if len(T) >= 3:
        T = T.assign(tercile=pd.qcut(T.D.rank(method="first"), 3, labels=["low D", "mid D", "high D"]))
        print(T.groupby("tercile", observed=True).agg(
            days=("D", "size"), D=("D", "mean"), ret_on_premium=("out", "mean"),
            dollars=("usd", "sum"), blowups=("usd", lambda s: (s < -500).sum())).round(3).to_string())
    return dict(n=len(T), rho=rho, p=p, ok=ok)


# ── Commands ──────────────────────────────────────────────────────────────────
def replicate(root=ROOT):
    """The registered tests on the registered sample (orders through REG_END, SPY proxy)."""
    spy = Spy(root)
    df = load_orders(root)
    df = df[(df.iso <= REG_END) & df.iso.map(lambda d: spy.day(d) is not None)]
    J = intervals(df, spy_state_fn(spy))
    r1 = report("TEST 1  early-window D -> rest-of-day return on premium", test1_table(df, J), 0.025)
    r2 = report("TEST 2  today's D -> next trading day return on premium", test2_table(df, J), 0.025)
    return r1, r2


def oos_sample(root=ROOT, through=None):
    """Out-of-sample orders and three interval sets:
    primary  — option-bar state at 1-minute steps (AMENDMENT 2026-10-29)
    opt5     — option-bar state at 5-minute steps (the follow-up as first described; reported)
    spy5     — the registered SPY-proxy state at 5-minute steps (strict replication; reported)"""
    spy, P = Spy(root), load_options(root)
    df = load_orders(root)
    df = df[(df.iso >= OOS_START) & ((df.iso <= through) if through else True)]
    return df, dict(
        primary=intervals(df, opt_state_fn(P), step=OOS_STEP_MIN),
        opt5=intervals(df, opt_state_fn(P), step=STEP_MIN),
        spy5=intervals(df[df.iso.map(lambda d: spy.day(d) is not None)], spy_state_fn(spy), step=STEP_MIN),
    )


def eligible_days(df, J, early_min=EARLY_MIN, k=T1_MIN_EACH):
    """Test 1 eligibility only — counts green/red early intervals; never touches outcomes."""
    if J.empty:
        return []
    first = df.groupby("iso").en.min()
    E = J[J.t < J.iso.map(first) + pd.Timedelta(minutes=early_min)]
    c = E.groupby("iso").state.value_counts().unstack(fill_value=0)
    ok = (c.get("green", 0) >= k) & (c.get("red", 0) >= k)
    later = {iso for iso in first.index if (df[(df.iso == iso) & (df.en >= first[iso] + pd.Timedelta(minutes=early_min))]).shape[0]}
    return sorted(i for i in ok[ok].index if i in later)


def is_due(n, today):
    return today >= OOS_EARLIEST and (n >= OOS_MIN_DAYS or today >= OOS_LATEST)


def status(root=ROOT, today=None, sample=None):
    today = today or date.today().isoformat()
    df, J = sample or oos_sample(root)
    n = len(eligible_days(df, J["primary"]))
    due = is_due(n, today)
    print(f"OOS since {OOS_START}: {df.iso.nunique()} trading days, {n} eligible for Test 1 "
          f"(primary: option-bar state, {OOS_STEP_MIN}-min intervals). "
          f"Reported versions: {len(eligible_days(df, J['opt5']))} eligible at 5-min option state, "
          f"{len(eligible_days(df, J['spy5']))} at the registered 5-min SPY proxy.")
    print(f"Due when n >= {OOS_MIN_DAYS} and today >= {OOS_EARLIEST}, or on {OOS_LATEST} regardless. "
          f"Today {today}: {'DUE' if due else 'not due'}.")
    return n, due


def oos(root=ROOT, today=None, force=False):
    sample = oos_sample(root)
    n, due = status(root, today, sample)
    if not (due or force):
        print("Not due — refusing to compute outcomes. (--force exists only for testing the code path.)")
        return None
    df, J = sample
    T = test1_table(df, J["primary"])
    if len(T) < OOS_FLOOR:
        print(f"\nINCONCLUSIVE: only {len(T)} eligible days (< {OOS_FLOOR}). No pass/fail is reported.")
        return dict(n=len(T), inconclusive=True)
    res = report(f"OOS TEST 1 (PRIMARY, option-bar state, {OOS_STEP_MIN}-min)  early-window D -> "
                 "rest-of-day return on premium", T, OOS_P, halves=False)
    print("\nReported, not tested (expected underpowered — see PREREG.md amendment):")
    for key, label in (("opt5", "option-bar state, 5-min"), ("spy5", "registered SPY proxy, 5-min")):
        t = test1_table(df, J[key])
        if len(t) < OOS_FLOOR:
            print(f"  {label}: {len(t)} eligible days — too few to report a correlation")
        else:
            rho, p = perm_p(t.D, t.out)
            print(f"  {label}: n={len(t)} rho={rho:+.2f} one-sided p={p:.3f}")
    return res


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["replicate", "status", "oos"])
    ap.add_argument("--force", action="store_true", help=argparse.SUPPRESS)
    a = ap.parse_args(argv)
    {"replicate": lambda: replicate(), "status": lambda: status(), "oos": lambda: oos(force=a.force)}[a.cmd]()


if __name__ == "__main__":
    sys.exit(main())
