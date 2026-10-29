# Pre-registration: disposition score predicts the rest of the day

Runner: [`disposition.py`](disposition.py) — its constants and functions are the frozen definitions.
Provenance: [`original/`](original/) holds the 2026-10-05 scripts and pre-registration verbatim,
recovered on 2026-10-29 from that session's transcript after its scratchpad was deleted.
Those scripts point at the old scratchpad and aren't meant to be run.

```
python analysis/prereg/disposition.py replicate   # registered tests on the registered sample
python analysis/prereg/disposition.py status      # out-of-sample eligible-day counts (no outcomes)
python analysis/prereg/disposition.py oos         # the out-of-sample check — refuses until due
```

---

## Part 1 — Registered 2026-10-05 (verbatim, before any outcome was computed)

> Data: spy_trades.csv orders (Group ID), Sep 2025 to Oct 2 2026; SPY 5-min bars as the green/red proxy.
> State: every 5 min after entry, order is green if SPY has moved > $0.05 in its favor since entry
> (calls up, puts down), red if > $0.05 against, else flat. Interval = next 5 minutes; "sold" = any exit
> of that order inside the interval.
> Score D = (share of green intervals with a sale) - (share of red intervals with a sale).
> D > 0 means selling winners faster than losers (disposition).
> Outcome metric: return on premium = sum(P/L) / sum(entry cost) of the orders in the outcome set
> (size-neutral). Dollars and blowups (< -$500) reported as secondary only.
>
> Test 1 - early window predicts the rest of the day
>   Window: first 60 minutes after the day's first entry. D_early from intervals starting in the window.
>   Eligible day: >= 3 green and >= 3 red early intervals, and >= 1 order entered after the window.
>   Outcome: return on premium of orders ENTERED after the window (no overlap with the score).
> Test 2 - today's score predicts the next trading day
>   D_day from all intervals that day. Eligible day: >= 5 green and >= 5 red intervals.
>   Outcome: return on premium of all orders on the next day with trades.
>
> Both: hypothesis is higher D -> worse outcome. Statistic: Spearman rank correlation, one-sided
> p from 10,000 random shuffles. Pass = rho <= -0.20 AND p < 0.025 (two primary tests, Bonferroni)
> AND the same sign in both halves (before / from 2026-07-01).
> Reported but not tested: tercile means; same-day D vs same-day return (circular, reference only);
> 3-day rolling D -> next day.

**Result (2026-10-05):** Test 1 PASS — n=28 days, rho −0.37, one-sided p 0.024, halves −0.37 (n=21) / −0.13 (n=7).
Test 2 FAIL — n=69, rho −0.06. `disposition.py replicate` reproduces both on today's data (2026-10-29),
restricted to orders through 2026-10-02. P/L is the gross `P/L ($)` column, as originally used.

The session closed by naming the follow-up: confirm out-of-sample on the 1-minute option bars the
cron had just started saving (2026-09-30), after about two months. That plan was never written down
as a protocol. Part 2 does that.

---

## Part 2 — Out-of-sample protocol, amended 2026-10-29 (before any OOS outcome was computed)

### What was looked at before writing this
Only **eligibility**: per-day counts of green/red/flat early intervals, and hold-time distributions.
No D values against outcomes, no OOS return or P/L figure, no correlation was computed on
out-of-sample days. As of 2026-10-29, over 17 OOS trading days (2026-10-06 → 2026-10-28):

| Interval state | Eligible days for Test 1 |
|---|---|
| Registered SPY proxy, 5-min | 1 |
| Option-bar state, 5-min (the follow-up as first described) | 0 |
| Option-bar state, **1-min** | 9 |

Why: holding times collapsed. The median hold is 4 min out of sample vs 11 min in the registered sample,
and 51% of orders now close inside 5 minutes (was 29%). A 5-minute sampling grid can't see them. The
9am lockout (2026-09-24) also moved several days' first entry to 10:00 or later.

### Amendment
The **primary** out-of-sample test is Test 1 with one change: **intervals are 1 minute** (state read
every minute after entry; "sold" = any exit of that order inside the next minute). Everything else is
as registered:
- **State:** from the contract's own option bars, which is the follow-up's stated intent. Green if the last
  fully-completed bar's close is > 2% above the entry price per share, red if > 2% below, else flat
  (`OPT_BAND`, as defined in the 2026-10-05 calibration script). 1-minute bars where saved; on days that
  only have 5-minute bars the state holds within each bar. Bars are whatever `outputs/option_intraday/`
  holds at run time (Robinhood capture or a later backfill), using the existing never-replace-with-fewer
  merge.
- **Window, eligibility, outcome, statistic:** unchanged. First 60 min after the day's first entry;
  ≥ 3 green and ≥ 3 red early intervals; ≥ 1 order entered after the window; return on premium (gross)
  of orders entered after the window; Spearman rho with a one-sided p from 10,000 shuffles (seed 7).
- **Sample:** trading days from **2026-10-06** (first day after registration) through the run date.
- **Pass:** rho ≤ −0.20 **and** one-sided p < **0.05**. This is a single primary test, so there's no
  Bonferroni correction. There's no halves rule: the out-of-sample period is a single span.
- **When:** the first run on or after **2026-12-04** with **≥ 20 eligible days**, or on **2027-01-29**
  regardless. With fewer than **15** eligible days at that point, report **inconclusive**, with no pass or
  fail. `disposition.py oos` enforces this and refuses to compute outcomes before it is due.
- **Reported alongside, not tested:** the same Test 1 at 5-minute option-bar state and with the
  registered 5-minute SPY proxy. Both are expected to have too few days.

### What a result means
- **Pass:** an early-session disposition score, at 1-minute resolution, predicts worse return on
  premium for the rest of the day out of sample. It confirms the finding, not the exact registered
  5-minute statistic.
- **Fail:** the 2026-10-05 result doesn't generalize to the current trading style. Drop the score as an
  early warning; don't re-specify it to rescue it.
- **Inconclusive:** too few eligible days. Say so; don't extend the window after the fact.

Any further change to this file must be dated and made before `oos` is first run.
