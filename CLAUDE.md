# CLAUDE.md

## Project Overview

RH Options Trade Exporter — a Python CLI (`hood.py`, ~1700 lines) that pulls options order history from Robinhood's undocumented API and outputs structured CSVs for trading journal backfill, plus `journal_sync.py` which merges those CSVs into an Excel journal, plus a FastAPI dashboard (`server.py`) for visualizing trade data. Built for a day trader running 0DTE SPY options.

## Key Files

- `hood.py` — main script (single file, no package structure)
- `cash_flow.py` — pulls all money movement from Robinhood (transfers, fees, dividends, referrals, equity) and outputs JSONL snapshots
- `journal_sync.py` — merges `outputs/spy_trades.csv` into `spy_0dte_journal.xlsx`, appending only new trades (dedup by Date+Entry Time+Strike+Type+Qty+Exit Time). Preserves existing rows, formulas, and manual-fill columns (AA–AE).
- `fix_group_ids.py` — one-time Group ID repair for existing CSVs (`spy_trades.csv`, `other_trades.csv`, plus `journal_notes.json` keys). Drops legacy `G{n}` rows that have an exact twin (same Date/Symbol/Expiry/Type/Strike/Qty/Entry/Exit/costs/P/L), renames the rest to the stable ID derived from Date + Entry Time + Strike + Type via `hood.make_group_id`, and splits IDs shared across expiries with the same rule as `hood.assign_group_ids`. Dry-run by default; `--apply` backs up to `*.prefixids.bak`; `--dir` targets another outputs folder. Dry-run as of 2026-10-26: gener drops 9 SPY duplicates (−$288) and renames all 78 `other_trades.csv` rows (that file predates stable IDs); local splits the one ET calendar-spread collision. It also reports rows sharing a (Group ID, Exit Time) key — pre-aggregation order fragments it doesn't merge (gener: 34 SPY, 9 other; gener was never regrouped).
- `backfill_history.py` — one-time: extends a trade CSV *backwards* with rows it never had (gener's `spy_trades.csv` starts 2026-04-22; local's goes back to 2025-09-17). Rebuilds the missing rows from a full raw dump (filtered to `state=filled`) through `regroup_trades.reconstruct` — so they get order aggregation, stable IDs, fees and cents — and carries Asset OHLC / VWAP / 8 EMA / VIX / Delta from `--sticky-from` (a CSV that captured them) by (Date, Strike, Type, Entry Time). Rows on/after the target's first date are untouched. Refuses to write if positions differ between the rebuild and the sticky source, any rebuilt row has no sticky match, or the result has duplicate (Group ID, Exit Time) keys. Dry-run by default; `--apply` backs up to `*.prebackfill.bak`. Applied on gener 2026-10-27: +254 rows (P/L −$479.00 both sides, 0 position diffs, 0 misses, every captured value carried); gener's `spy_trades.csv` now starts 2025-09-17.
- `analysis/prereg/` — pre-registered analyses (tracked so the freeze is timestamped by git). `PREREG.md` = the 2026-10-05 disposition-score registration verbatim + the 2026-10-29 out-of-sample amendment (1-minute option-bar intervals; due on/after 2026-12-04 with ≥ 20 eligible days, or 2027-01-29 regardless; < 15 → inconclusive). `disposition.py` holds the frozen definitions: `replicate` reproduces the registered result (Test 1 n=28, rho −0.37), `status` prints eligible-day counts without touching outcomes, `oos` refuses to compute outcomes until due. `original/` = the 2026-10-05 scripts recovered verbatim from that session's transcript (not runnable; provenance). Their printed outputs contain dollar P/L, so they live in the gitignored `plans/` instead. Never edit a frozen constant without a dated amendment in `PREREG.md`.
- `journal_migrate.py` — one-time companion to `regroup_trades.py`. Rebuilds the journal's Trade Log from the regrouped CSV and carries manual columns AA–AE forward keyed by position (Date, Strike, Type); scale-out notes attach to the earliest-exit row. Dry-run by default; `--apply` backs up to `*.prefragfix.bak`, aborts if any note can't be placed. Imports `journal_sync` helpers.
- `regroup_trades.py` — one-time migration that rebuilt the legacy fragmented `spy_trades.csv` / `other_trades.csv` with order-level grouping (see "Executions aggregated by order_id" below). Dry-run by default; `--apply` backs up to `*.prefragfix.bak` then writes, refusing any file where a position would be lost. Carries captured sticky/market columns forward losslessly by exact (Date, Strike, Type, Entry Time) match. For non-SPY contracts left net-open past expiry with no closing execution, `add_expired_otm_closes` synthesizes a $0 expiry close (expired worthless → long loses premium, short keeps credit) so they become realized rows — scoped to non-SPY so the verified SPY data is untouched.
- `server.py` — FastAPI dashboard server (~875 lines), serves API + static dashboard
- `static/index.html` — SPA shell with top nav bar (8 views: Analysis, Calendar, Habits, Positions, Pre-Trade, Trade Log, Portfolio, Admin). Loads TradingView Lightweight Charts + Chart.js + chartjs-adapter-date-fns from CDNs. Exposes `window.loadView` for cross-view navigation (e.g. Calendar → Positions deep-link). Also hosts the global **9am lockout banner** (sticky under the nav on every view, weekdays 09:25–10:00 ET, countdown to 10:00, live 9am-hour stats computed from `/api/trades` at the decision level). Preview any time with `?lockout=preview`. Also hosts the **Net / Gross** P/L toggle (right of the nav tabs) — see "Fees" under Critical Design Decisions. Defines `window.UI` — the one registry of UI-state keys (`UI.get/set`, `UI.KEYS` with store, default and label): **prefs** go to localStorage (candle size, ETH/PM-H/L/Snap toggles, equity-curve mode), **session** state to sessionStorage (intraday chart / cash-flow section expanded — folded on a fresh tab, kept across days and reloads). Older view keys (overlays, spans, KPI groups) still use localStorage directly until the Step 6 Settings page. The view loader runs a fragment's scripts **in order, awaiting each external `<script src>`** (fixes Trade Log's `agGrid is not defined` race), fetches views with `cache: 'no-cache'` so a pulled update isn't masked by a stale fragment, and removes `body > [data-view-owned]` elements on every view change (the calendar's floating `#cal-tip` used to linger as unstyled text). Defines `window.fmtAbsMoney(v)`, the one money formatter every view's P/L helper uses (2 decimals when there are cents, none for whole dollars) — never interpolate a raw P/L number: summed cents carry float noise (`+566.1999999999998`). `/api/trades/daily` also rounds its sums to cents.
- `static/style.css` — shared dark theme CSS
- `static/views/analysis.html` — equity curve (Daily P/L by default, Cumulative toggle; the mode is a `UI` pref re-read on every rerender, and tab handlers use `onclick` so they don't stack), KPIs (Total P/L, **Fees** (explainer shows $/contract round trip and % of gross for the span), Win Rate, Avg Win/Loss, **Profit Factor, Expectancy**, Best/Worst Day, Max Consec Wins/Losses, current-streak pill, plus Tier 1 from `plans/potential-kpis.md`: Payoff / W-L ratio, Avg + Median Daily, Recovery Factor, Largest Winner/Loser ratio), grouped with an explainer modal per KPI, Chart.js breakdowns. **Streaks count per CSV row (one closing execution) in entry-time order, and a break-even row resets both counters** — so they run longer than per-position or per-day streaks (e.g. 13/9 per row vs 6/5 per day as of Oct 2026). Insight narrative includes top-N% concentration sentence. Span selector (1D / 1W / 1M / 3M / 6M / YTD / 1Y / ALL) filters trades by date before all KPIs/charts compute, persisted to `localStorage` (default ALL).
- `static/views/calendar.html` — browsable monthly P/L heatmap + day-detail modal. Day cells show P/L + small SPY % + VIX + cash-flow corner icons (overlay row above grid toggles each, persisted in localStorage). Click any active day → modal with: collapsible Intraday chart (folded by default per session; Lightweight Charts 4.1.7: 5/10/15m toggle defaulting to a saved 10m, ETH / PM H/L / Snap toggles — Snap = magnet to the close, since snapping to O/H/L/C needs v5 — premarket H/L lines, trade markers, ETH dimming, ET-forced axis, `fixLeftEdge/fixRightEdge` so you can't scroll or zoom out past the day's bars), collapsible Cash flow section, leg-detection view chips (Legs/Roundtrips/Fills/By contract/By hour), Carried-positions section for multi-day entries. Arrow keys (←/→) + transparent side bars navigate active days while the modal is open; cross-month auto-syncs grid. With no modal open, ←/→ step through months (stops at the ends; ignored with modifier keys, when a form field has focus, or when the Calendar view isn't mounted). `monthsWithTrades` includes cash-flow-only months but excludes pending/scheduled states.
- `static/views/habits.html` — top section **Where the Money Comes From**: three-group P/L split from `/api/trades/split` (tiles, stacked monthly Chart.js bars + net line, 30-min rescue rate, recent flagged orders). Then the **What Works** behavior cards (recent window vs baseline), then pattern-over-time cards (Overtrading, Edge, Blowups, Chase, Position Sizing, Put vs Call, Morning vs Afternoon). Split out of Calendar so the calendar stays focused on the heatmap.
- `static/views/positions.html` — currently-open positions from `unmatched_opens.csv` with computed days-held and DTE remaining. Active vs expired sections, sorted nearest-expiry-first. Calendar's Carried-positions rows deep-link here via `#positions:<contract_key>` with scroll-to + yellow flash highlight. Live mark prices deferred (see roadmap).
- `static/views/pretrade.html` — pre-trade reference (day of week / VIX / hour sub-tabs) plus an interactive guardrail checklist at the top, keyed per ET trading day in localStorage so it resets each session.
- `static/views/tradelog.html` — ag-Grid trade table with trades/daily toggle, editable journal notes, Fees column after P/L.
- `static/views/portfolio.html` — account-level equity / P/L / income view backed by `cash_flow.jsonl` (live snapshots only; the synthetic-historical backfill was removed). Clickable KPIs open a modal with formula + breakdown for each metric (current equity → per-account split; all-time P/L, total return, net deposited, lifetime income → derivation rows). Span selector (1D / 1W / 1M / 3M / 6M / YTD / 1Y / 5Y / ALL) filters the chart data client-side, default 1Y, persisted to `localStorage`. 1D special-cases pre-dedup snapshots (so multi-snapshot days from manual + cron runs show intra-day variation); falls back to last 2 daily points if today has <2 snapshots. Time-axis tick unit auto-adapts (`hour` for 1D, `day` for 1W–1M, `week` for 1M–3M, `month` for 6M–2Y, `year` beyond). Charts use a Chart.js time-scale x-axis with `chartjs-adapter-date-fns` (loaded once in `index.html`).
- `static/views/admin.html` — token health + on-demand fetch UI. Sections: (1) RH token status with traffic-light + countdown to JWT expiry + "Probe RH" button; (2) paste-to-update form that accepts raw JWT, `Bearer <jwt>`, `Authorization: Bearer <jwt>` line, or full cURL paste; (3) on-demand run buttons. Top rows: **Pull from gener** (`vps/pull_data.sh` — the usual action on local, since gener is the source of truth) and **Daily refresh** (`daily_refresh.py`, the same chain gener's cron runs; per-step status mirrors below by parsing `[STEP] <name> START|END exit=N` markers; exit 3 shows as "⚠ partial"). Daily refresh button is auto-disabled when the RH token traffic light is red (`applyTokenGate`) — individual rows stay enabled so spy_* can run without an RH token. Individual rows below for surgical retry: `hood`, `cash_flow`, `spy_intraday`, `spy_daily`, `spy_intraday_back` (spy_intraday --backfill). Polls `/api/admin/run/{job_id}` every 2s while a job is running. Refreshes token status after a job finishes. On view load, `restoreFromRecentRuns()` reattaches to any in-flight job or paints the final state of a run that ended within the last 5 minutes — so a page reload mid-run or right after completion doesn't lose context.
- `static/sandbox/leg-layout.html` — standalone sandbox for testing layout variants of the leg-row (A/B/C grid templates × treatment toggles for dd-note alignment, urgency huddle, 🌙 placement, P/L %). Not part of the SPA; opened directly via `/static/sandbox/leg-layout.html` for visual A/B comparison before porting to the real calendar.
- `static/sandbox/kpi-layout.html` — standalone sandbox with two tabs: (1) Analysis KPI tile layout variants (A grouped + collapsible / B headline+collapsible / C fixed 5×3 compact), with toggles for combo tiles (Best+Worst Day, Avg+Median Daily, Max-W+Max-L streaks, Avg-Win+Avg-Loss), current-streak conditional tile (active/winning/none), label style (full/short), density (comfortable/compact), and click-to-modal demo. Group headers in Variant A persist collapse state to localStorage. (2) pf-modal close-button redesign (broken-baseline reproduces the bug / M1 width-fix-only / M2 width-fix+polish / M3 explicit-flex-header with split scroll body) with width-preset (440/560/680), long-content toggle, M3 head-divider toggle, and viewport-simulator (desktop/tablet 780/mobile 400). Open at `/static/sandbox/kpi-layout.html` before porting the chosen variants to analysis.html / portfolio.html.
- `tests/test_hood.py` — pytest suite for hood.py pure logic (token, account cache, FIFO pairing, Group ID assignment + collisions, VWAP/EMA, formatting, DataFrame building, incremental cursor, sticky-column merge)
- `tests/test_server.py` — pytest suite for server.py (auth, CSV parsing, all API endpoints incl. positions, spy_daily, spy_intraday, cash-flow events, three-group trades split; routes; column mapping; exit_date bucketing)
- `tests/test_cash_flow.py` — pytest suite for cash_flow.py (token, pagination, transfer categorization, summary math, JSONL output, **extract_events**, **merge_events_to_jsonl**, **resolve_dividend_symbols**, **pending-dividend exclusion**)
- `tests/test_spy_daily.py` — pytest suite for spy_daily.py (yfinance mocked: payload assembly, pct-change, range inference, cache I/O)
- `tests/test_spy_intraday.py` — pytest suite for spy_intraday.py (Polygon mocked: token, pagination, 403/429/timeout handling, rate-limit pacing, weekday selection, missing-dates dedup, cache I/O)
- `tests/test_option_intraday.py` — option_intraday.py: contract selection (0DTE, overnight holds, open positions, other symbols), instrument index, bar parsing (interpolated dropped, ET day split), fetch params + 401/404, catch-up targets, 1-minute today with 5-minute fallback, never-downgrade merge. Massive backfill: OCC tickers, ms→s + volume, 403/401/429/500 handling, resume (answers recorded, errors/403 retried), 2-year window/today/limit, merge keeps more bars and clears `missing`, only regular-session bars merged. Autouse guard fails any real network call.
- `tests/test_token_watch.py` — token_watch.py: JWT exp decode, probe, status evaluation, alert de-dup/repeat/recovery rules, undelivered alerts retried, Discord + mail delivery, end-to-end main with state file. Autouse network guard.
- `tests/test_fix_group_ids.py` — fix_group_ids.py: twin drop + renumber/cumulative, near-twin kept, orphan rename, derived ID equals `hood.make_group_id`, calendar-spread split (incl. both legs starting as G rows), dry-run writes nothing, notes follow renames / conflicts kept.
- `tests/test_backfill_history.py` — backfill_history.backfill: only pre-target rows added, sticky carried, fees present, renumber/cumulative; refuses on position mismatch or unmatched rebuilt row; other symbols kept out.
- `tests/test_pull_data.py` — runs the real `vps/pull_data.sh` with an `rsync` shim that maps `gener:` to a temp dir: mirror + backup of replaced files, journal notes and `*.bak` untouched, cash-flow union/gener-wins merge, dry run changes nothing. Skips without bash + rsync.
- `tests/test_disposition_prereg.py` — tamper guard on the frozen pre-registration constants, interval/state/score mechanics on synthetic orders, eligibility works without P/L columns (so `status` can't peek), the due rule, and `oos` refusing before it's due.
- `tests/test_run_sh.py` — runs the real `vps/run.sh` + `daily_refresh.py` against a stub `.venv/bin/python` (runs daily_refresh for real, logs every step): chain order then health_check, hood bare (cursor applies), hood failure fatal (no later steps, no health check), later failures non-fatal (chain continues, exit-3 path, `.last_refresh.json` records it), a missing `.env` / alert key doesn't abort. Skips without bash.
- `tests/test_health_check.py` — ET execution dates, covered dates (entries, overnight exits, open positions), gap days skip covered and post-cursor dates, step/option issues, all-clear run, RH failure (incl. `SystemExit`) reported not raised, posting rules (quiet when clear, `--always`, `--dry-run`). Network and Discord guarded.
- `tests/test_journal_sync.py` — pytest suite for journal_sync.py (dedup key incl. equal-size scale-out sync, trend coercion, DV strip, append smoke test against synthetic workbook)
- `tests/test_legs.mjs` — node test for the Calendar day-modal leg detection. Slices the real `buildLegs` + per-event render block out of `static/views/calendar.html` by anchor strings and exercises them: scale-out vs close classification + outcomes, and that full-close rows drop the `(lock gains/cut losses)` parenthetical while scale-out rows keep it. `tests/test_legs_render.py` is a pytest shim that shells out to node so `pytest tests/` covers it (skips if node is absent).
- `plans/` — markdown design docs that survive across sessions. **Gitignored** (as are `static/sandbox/` and `archive/`), so these exist only on the local machine — not versioned, not on gener. Start with `roadmap.md` (v2, 2026-10-26: phased plan + doc-gap list; the June version is `roadmap-2026-06-17.md`). Also: `live-monitor-design.md` (spec only — 30-min stopwatch per open position), `data-topology.md` (local vs VPS source of truth — **decided 2026-10-27: gener is the source of truth**, local mirrors it via `vps/pull_data.sh`), `portfolio-reconstruction.md`, `loop-since-4-15.md` (May findings that seeded the 30-min rule), `ROBINHOOD_API.md` (endpoint notes), `fastapi.md`, `nice_to_know.txt` (early dashboard review notes), `next_chat_prompt.md` (superseded by `journal_sync.py`). Notable: `architecture-v2.md` (modal abstraction, persistence, CSS strategy, productionization), `calendar-modal-roadmap.md` (4-step calendar plan, all shipped), `potential-kpis.md` (queue of KPI enrichment options), `resume_framing.md` (how to talk about this project externally), `admin-concurrency-notes.md` (lock model, what races on collision, vps/run.sh vs daily_refresh.py drift risk, log retention, UI reload edge cases), `trading-review-2026-09-10.md` (distilled Sep 11–Oct 21 2026 review: three-group split, oil test, deposit-loop timeline, tooling built, RH option-historicals limits, VPS facts, open items)
- `.github/workflows/test.yml` — GitHub Actions CI (runs tests on push/PR to main)
- `vps/run.sh` — cron wrapper: runs `daily_refresh.py --quiet` (the shared chain), alerts via Discord webhook + email if hood fails (`DISCORD_WEBHOOK_URL` / `ALERT_EMAIL` from the crontab env, falling back to `.env`), then `health_check.py`. Exit 3 from the chain (a later step failed) is not an alert by itself — health_check reports it. hood runs **bare** (no `--after-date` / `--symbol`) so its incremental cursor applies; until 2026-10-26 it passed a fixed `--after-date <yesterday> --symbol SPY`, so a failed run before a weekend lost the Friday (Oct 16 2026). The cursor only heals *trailing* gaps; an older hole needs a one-time `hood.py --after-date <day before>` — health_check names these. The `.env` alert-key lookup ends in `|| true`: without it, a missing `.env` or `ALERT_EMAIL` key failed under `set -euo pipefail` and killed the run silently before the log header.
- `vps/pull_data.sh` — one-way pull gener → local `outputs/` (rsync over the `gener` SSH alias; `RH_VPS` / `RH_VPS_DIR` override). Never writes to the VPS, never deletes local files; anything it replaces goes to `outputs/.prepull/<timestamp>/`. Excludes `*.bak`, raw dumps, `.admin_jobs/`, token-monitor state and `journal_notes.json`. `cash_flow.jsonl` is merged by timestamp and `cash_flow_events.jsonl` by event id (gener wins) instead of replaced, because local dashboard refreshes write their own snapshots (local had 193, gener 103, zero overlap, as of 2026-10-27). `--dry-run` shows changes. Uses `${DRY[@]+...}` because macOS bash 3.2 errors on an empty array under `set -u`.
- `vps/rh-trade-exporter.service` — systemd unit for the dashboard server
- `vps/hosting-guide.md` — domain + nginx + HTTPS setup guide
- `vps/logrotate.conf` — logrotate config for VPS cron log
- `README.md` — user-facing documentation
- `.rh_token` — saved auth token (chmod 600, gitignored)
- `.server_token` — dashboard auth token (chmod 600, gitignored)
- `.rh_accounts.json` — cached account numbers (gitignored)
- `.rh_instrument_cache.json` — persisted option instrument data (gitignored)
- `.rh_resolved_symbols.json` — URL→ticker cache for dividend instrument lookups (gitignored). Populated by `cash_flow.py::resolve_dividend_symbols`; one HTTP call per unique instrument URL, then cache hits forever. Lets the calendar's cash-flow rows show "SPY" instead of an instrument UUID.
- `.env` — `MASSIVE_API_KEY=...` for Polygon (spy_intraday.py reads this). Gitignored.
- `outputs/journal_notes.json` — trade journal notes keyed by Group ID
- `outputs/cash_flow.jsonl` — append-only portfolio snapshots from each `cash_flow.py` run
- `outputs/cash_flow_historical.jsonl` — **DEPRECATED/orphaned.** Was the synthetic per-day backfill from RH's chart endpoint; the `--backfill` command and the `/api/cash-flow` merge that read it were removed (chart endpoint gave span-relative, non-anchored values). Any existing file on disk is no longer read or written; safe to delete. Replacement: `plans/portfolio-reconstruction.md`.
- `outputs/cash_flow_events.jsonl` — per-event log of every transfer / Gold fee / dividend / referral, written by **every** `cash_flow.py` run. Idempotent merge by stable id (`transfer:<rh_id>`, `gold_fee:<id>`, `dividend:<id>`, `referral:<id>:stock:N` / `referral:<id>:cash`). Sorted by date asc. Schema: `{id, kind, date, amount, state, ...kind-specific fields}` where `kind ∈ {deposit, withdrawal, internal, gold_fee, dividend, referral}`. Powers the calendar's day-cell flow icons (↓ deposit / ↑ withdrawal / ↔ internal) and the modal's Cash flow section. Pending and failed/voided rows are kept so the UI can show in-flight ACH or grey out historical voids.
- `outputs/option_intraday/{YYYY-MM-DD}.json` — per-day option price bars from `option_intraday.py`, one entry per contract. Schema: `{date, fetched_at, source: "robinhood"|"massive"|"mixed", contracts: [{instrument_id, symbol, type, strike, expiry, interval: "minute"|"5minute", source, bars: [{t,o,h,l,c[,v]}]}], missing: [{symbol, type, strike, expiry, reason}]}`, `t` = Unix seconds UTC, regular session only. Gap-fill bars are dropped. Per-contract `source` was added 2026-10-31 (absent = Robinhood); only Massive entries carry `v`. `analysis/prereg/disposition.py` reads these files, so the merge rule (per contract, keep the version with more bars) is part of the pre-registration.
- `outputs/option_massive/{YYYY-MM-DD}.json` — raw Massive answers from `option_intraday.py --backfill`: `{date, fetched_at, source: "massive", contracts: [{ticker, symbol, type, strike, expiry, interval: "minute", bars: [{t,o,h,l,c,v}]}], no_data: [...]}`. Every bar Massive returned, including 07:30–09:30 and 16:00–16:15 prints. This is the **volume record**: on days where RH's 1-minute capture won the merge (RH-captured days since 2026-09-30), volume lives only here. `no_data` = Massive answered with no trades, recorded so it isn't asked again.
- `outputs/.last_refresh.json` — per-step exit codes of the last `daily_refresh.py` run, read by health_check.
- `outputs/.token_watch.json` / `outputs/.token_status` — token_watch.py state (status, token fingerprint, last alert time) and the one-line status the login banner prints.
- `outputs/spy_daily.json` — daily OHLC cache for SPY + VIX, rebuilt fresh on each `spy_daily.py` run (overwritten, not append). Powers the Calendar's per-cell market context overlay (SPY % change, VIX level color-banded). Schema: `{generated_at, range: {start, end}, days: [{date, spy_open, spy_high, spy_low, spy_close, spy_pct, vix_close}]}`. Range auto-inferred from earliest trade date in `spy_trades.csv` minus 7 days; falls back to 5-year window if no trades exist.
- `spy_daily.py` — fetches yfinance daily for `SPY` and `^VIX`, writes the cache above. Idempotent, cron-friendly (`--json` silent mode). Called nightly via `vps/run.sh` after hood.py + cash_flow.py, and on-demand via Admin → Run.
- `outputs/spy_intraday/{YYYY-MM-DD}.json` — 5-minute SPY bars from Polygon (free tier via `MASSIVE_API_KEY` in `.env`). One file per trading day. Schema: `{date, fetched_at, source, interval: "5m", bars: [{t, o, h, l, c, v}]}` where `t` is **Unix seconds UTC** (Lightweight Charts format). For dates outside Polygon's 2y window or with no data (weekends/holidays), the stub `{date, available: false, reason}` is written instead — prevents repeated fetches of known-unavailable days. **Exception:** today's date is NEVER cached as `out_of_plan` (Polygon's free tier serves end-of-day data, so today comes back as 403 until market close + settlement; we defer the write so the next run retries). Powers the calendar day modal's intraday candle chart.
- `spy_intraday.py` — Polygon free tier client. Reads `MASSIVE_API_KEY` from `.env`, paginates per day (~2 hops for 04:00–20:00 bars), sleeps 13s between distinct dates (5 req/min ceiling). Modes: default (previous trading day + today — walks back through weekends so a Monday run fills Friday + Monday), `--date YYYY-MM-DD` (force a single date, overwrites cache), `--backfill` (walks all missing weekdays inside the 2y window, optional `--since` and `--limit`). Called nightly via `vps/run.sh`, on-demand via Admin → Run.
- `option_intraday.py` — nightly capture of option price bars for the contracts you traded, before Robinhood's retention drops them. RH's `/marketdata/options/historicals/{id}/` only allows `minute`+`span=day` (today only) and `5minute`+`span=week` (~7 calendar days back); `hour`+`span=month` reaches ~3 weeks. So today's contracts are saved at 1-minute, and any day in the last 6 calendar days that a failed run missed is caught up at 5-minute. Contracts come from spy/other trades (entry through exit day) + unmatched opens (entry through expiry); instrument ids from `.rh_instrument_cache.json`. Merges per contract and never replaces bars with fewer. **`--backfill`** pulls history from **Massive** (Polygon, `MASSIVE_API_KEY`, no RH token needed) instead: 1-minute bars **with volume** (RH's `volume` is always 0) for every traded contract-day in Massive's ~2-year window, excluding today. One request per contract-day at 5/min (13s apart), so the first full run is ~925 contract-days ≈ 3.5 h; it's resumable (raw answers cached in `outputs/option_massive/`; 403s and errors aren't cached, so the next run retries them) with `--since D` / `--limit N`. Each day's **regular-session** Massive bars are merged into `option_intraday/` with the same never-fewer rule. Massive skips minutes with no trades, so RH's complete 1-minute capture usually wins where it exists (10/28 0DTE: RH 390 vs Massive 376 in-session); Massive fills days before 2026-09-30 and 5-minute catch-up days. Closes agree with RH to about $0.01 on liquid contracts. The OCC ticker is built from the contract (`O:SPY261028C00771000`). **Caveat:** if the only run on a day happens before the close (e.g. an Admin refresh at 15:50), that day's file stays truncated: `pick_targets` only checks that each contract is present, not that its bars reach 16:00, and after today the 1-minute span is gone (local 10/2 and 10/5 are like this; the VPS copies are complete). Exit 2 on a rejected token. Runs in `vps/run.sh` right after hood.py (non-fatal) and last in `daily_refresh.py`.
- `token_watch.py` — Robinhood token monitor for the VPS cron. Reads the JWT `exp` for the countdown and asks RH `GET /user/` (authoritative). Statuses: missing / rejected / expired / expiring (<24h) / ok / unknown (network blip, no alert). Alerts via Discord (`DISCORD_WEBHOOK_URL`) and `mail` (`ALERT_EMAIL`), read from env then `.env`; de-duplicated via `outputs/.token_watch.json` (bad status: on change then every 12h; expiring: once per token; one "recovered" note after a fresh token). An alert that reached no channel isn't marked handled — the previous status/fingerprint are kept so the next run re-decides it. On gener `DISCORD_WEBHOOK_URL` lives only in the crontab, so a manual `token_watch.py` from an SSH shell can't send; before 2026-10-27 such a run silently consumed the one-shot "recovered" alert (that's why the Oct 27 token swap got no Discord note). Writes `outputs/.token_status` for the SSH login banner. `--dry-run` evaluates without alerting or saving. `--banner` is the SSH-login mode: live probe with a 4s timeout, prints the status (+ fix command when bad), never alerts and never touches alert state — login shows the truth now, while cron stays the thing that warns you on days you don't log in. `--test-alert` sends a test message through the configured channels (exit 1 if nothing is configured).
- `vps/login_banner.sh` — at SSH login runs `token_watch.py --banner` (live check, capped at 6s with `timeout`); if that fails, falls back to the cached `outputs/.token_status` (marked "cached", with the fix command when red/yellow and a warning if >2h stale). Installed by one line in `~/.bashrc` (see Deployment).
- `daily_refresh.py` — **the one definition of the nightly chain**, shared by `vps/run.sh` (cron) and the Admin "Daily refresh" button so they can't drift: hood → option_intraday → cash_flow → spy_daily → spy_intraday. hood is fatal (aborts — nothing runs on stale data); the rest are non-fatal (failure recorded, chain continues). Exit 0 = all ok, 3 = hood ok but a later step failed, else hood's own code (hood only uses 0/1). `--quiet` passes `--json` to steps that support it (cron). Emits `[STEP] <name> START|END exit=N` markers plus `ABORTED at` / `DONE WITH FAILURES:` / `ALL DONE`, and writes `outputs/.last_refresh.json` (`{started_at, ended_at, steps: {name: rc}}`) for health_check. Uses `.venv/bin/python` when present, else `sys.executable`.
- `health_check.py` — runs after the chain in `vps/run.sh`; posts to Discord **only when something is off** (`--always` posts an all-clear, `--dry-run` never posts). Checks: (1) non-zero steps or no refresh today, from `.last_refresh.json`; (2) **gap days** — dates in the last 14 days where RH shows filled option executions (one `/options/orders/?state=filled` fetch via hood's helpers) but no CSV row's entry or exit date, and no open position, covers them; dates after hood's incremental cursor are skipped since the next run refetches them; each gap names its fix (`hood.py --after-date <day before>`); (3) today's option bars missing while there were trades, or contracts in the file's `missing` list. RH/auth failures (incl. hood's `sys.exit` on a corrupt account cache) are reported as an issue, never crash the cron. Reuses `token_watch.notify` / `env_value`.
- `spy_0dte_journal.xlsx` — the user's Excel trading journal (Trade Log sheet is what journal_sync writes into; Dashboard + Setup Analysis sheets are untouched)
- `spy_0dte_journal_updated.xlsx` — journal_sync output (original untouched unless `--in-place`)

## How It Works

1. User grabs a Bearer token from browser DevTools (in practice lasts several days; `token_watch.py` reads the real `exp`. `.rh_token` may carry a `Bearer ` prefix — strip it before probing)
2. Script hits Robinhood's private REST API with server-side filters (`state=filled`, `chain_symbol`, `updated_at[gte]`)
3. Fetches options orders, resolves instruments (cached to disk), extracts executions, FIFO-pairs opens→closes
4. Each **closing execution** = one output row (not each round-trip)
5. Enriches with: daily OHLC (RH historicals, yfinance fallback), VIX (yfinance), intraday VWAP + 8 EMA (RH 5-min bars), greeks/delta (RH marketdata), options events (exercise/assignment/expiration warnings)
6. Expired-OTM opens: `check_options_events` collects RH `expiration` events and `resolve_expired_opens` turns matching unmatched opens into $0-exit trade rows on ingest (so this is not only a `regroup_trades.py` thing; the regroup synthesis covers non-SPY contracts with no expiration event)
7. Outputs separate CSVs: spy_trades, other_trades, unmatched_opens, cancelled, rejected, failed

## Critical Design Decisions

**One row per exit, not per round-trip.** If user opens 3 contracts and closes 2 then 1, that's 2 rows with the same Group ID and entry time but different exit times/quantities. Do not change this without asking.

**Entry Cost is negative, Exit Credit is positive.** Entry Cost = -(price_per_share × qty × 100).

**Executions aggregated by order_id before FIFO pairing.** RH reports one order as multiple partial-fill executions (often within seconds at slightly different prices). `aggregate_executions_by_order` (hood.py) collapses fills sharing `(order_id, option_url, position_effect)` into one lot — quantity summed, price volume-weighted (P/L-preserving), timestamp = earliest fill (the stable Group ID anchor) — *before* `pair_into_trade_rows`. Without it, one trading decision fragments into several rows with distinct Group IDs (~25% inflation, polluting every Analysis KPI). Genuine scale-outs and averaging-down stay separate (they're separate orders). The legacy fragmented CSVs were migrated once via `regroup_trades.py`; new data is correct on ingest.

**FIFO pairing by contract URL.** Can mismatch if the same exact contract is opened, closed, then re-opened same day.

**No third-party Robinhood libraries.** Only `requests`, `yfinance`, `pandas`, and stdlib (`zoneinfo`). Auth is a manually-copied Bearer token.

**Single file.** Keep it that way unless it pushes well past 1500 lines.

**Stable Group IDs.** Format: `YYYY-MM-DD-HHMMSS-{strike}{C|P}` (ET entry time). Derived from trade data so re-runs produce identical IDs — journal_sync and any other downstream consumer can trust them across runs. `hood.make_group_id` builds the base; `hood.assign_group_ids` sets it on every open lot before FIFO pairing and closes the base format's holes: (1) contracts sharing a base (calendar spread — same strike/type/second, different expiries) → nearest expiry keeps the plain ID, later ones get `-YYYYMMDD` (`-{symbol}` if same expiry), so existing 0DTE IDs never change; (2) two orders on one contract in the same second → `-2`, `-3` in order_id order; (3) no timestamp → `ORD-{order_id}`. The old fallback was a `G{counter}` that changed every run; pre-Apr-24-2026 CSV rows still carry those until `fix_group_ids.py --apply` is run (also needed for the one existing collision, `2023-12-08-093421-13.5C`). Note `parse_executions` stores a missing strike as `0`, not None.

**"A trade" means different units in different views.** Analysis, Pre-Trade, Trade Log, Calendar cells and most Habits cards count CSV **rows** (closing executions); the lockout banner, Chase card and probe-before-size count **Group IDs** (opening orders); the Calendar day modal counts **positions/legs** (`buildLegs`). Roughly 809 rows / 766 orders / 642 positions as of Sep 24 2026. Streaks, win rate, avg loss and some Habits verdicts (Overtrading, Cut losses early) change with the unit. Name the unit before quoting any per-trade stat. Unifying this is roadmap Phase 3 #1.

**Fees: `Fees ($)` and `Net P/L ($)` columns; `P/L ($)` stays gross.** RH reports fees per order (`regulatory_fees` + `contract_fees`; `net_amount` = `processed_premium` ± fees to the cent, verified on all 771 orders in the raw dump). `parse_executions` spreads each order's fees evenly over every contract it filled across all legs (`fee_per_contract`); each paired row gets (open lot's + close lot's per-contract fee) × matched qty, in cents. Expired-worthless exits carry the entry fee only (both `resolve_expired_opens` and `regroup_trades.py`'s synthesized closes). Reconciled on the raw dump: $82.95 charged = $82.91 in rows + $0.04 on the open lot. Rates: $0.04 per contract per side since Sep 2025 ($0.02 in Dec 2025) with sub-cent regulatory variation. Gross `P/L ($)`, `P/L (%)`, Win/Loss and the journal are unchanged. Rows written before 2026-10-27 have blank fee columns until refetched (`hood.py --full` or a regroup). **The dashboard shows net by default** (since 2026-10-27): every trade endpoint takes `basis=net|gross` (default net); `server.py::_read_trades` swaps `pl` for `net_pl` and recomputes `wl`, `is_win`, `pl_pct` and `cumulative_pl`, keeping the gross value as `pl_gross`; rows without fee data keep gross. The nav's Net / Gross toggle (localStorage `plBasis`) makes `apiFetch` append the param to every `/api/trades*` and `/api/summary` call and re-renders the view, so views never branch on it. Robinhood's own "Realized profit" is **gross** (e.g. 6× 767C 10/27: $600 → $900 = +$300.00, while the account moved $899.76 − $600.24 = $299.52), so Gross mode matches the RH app's P/L and Net matches cash. Net vs gross matters beyond dollars: about a dozen tiny gross wins are net losses (win rate 58.0% gross vs 57.1% net, Oct 2026). Gold fees are separate (account-level, `cash_flow.py`). Money columns (Entry Cost, Risk, Exit Credit, P/L, Cumulative P/L) are stored in cents, rounded, since 2026-10-27. They used to be `int()`-truncated: a close order filled at several prices and FIFO-split across lots gives fractional dollars (14 rows on gener, e.g. −$0.14 stored as $0 while Win/Loss said LOSS).

**Incremental by default.** Bare `python hood.py` derives the fetch cursor from existing CSVs: `min(max(Date) across spy_trades + other_trades, min(Date) in unmatched_opens) - 1 day`. The `unmatched_opens` floor guarantees multi-DTE positions get refetched; if `unmatched_opens.csv` exists but can't be read (or has unparseable dates) the cursor returns None → full fetch, because a partial window would rewrite that file without the older opens. A corrupt `.rh_accounts.json` exits instead of falling back to `/accounts/` (which would miss the cash sub-account and overwrite the cache). `--full` opts out. New rows are merged into existing CSVs (dedup key = Group ID + Exit Time).

**Sticky columns on merge.** On CSV merge, Delta / VWAP / 8 EMA carry forward from the old row if the new row has a blank value. Reason: RH `/marketdata/options/` returns null for expired contracts, and RH 5-min bars only go back ~5 trading days. Without stickiness, re-runs would erase point-in-time data captured earlier.

**VWAP / 8 EMA are categorical.** Emitted as `Above` / `Below` / `At` / `N/A` (vs underlying at entry, ±$0.05 tolerance). The journal's `Trend Aligned?` formula expects these strings, not raw prices.

**Daily OHLC fallback.** Primary: RH `span=year`. Secondary: yfinance daily. Tertiary: synthesize from the intraday 5-min bars already fetched for VWAP/EMA (covers today's trades before RH's evening reconciliation populates the daily endpoint).

**Cash-flow transfer classification keys off account-type pair, not `direction`.** `bonfire/paymenthub/unified_transfers/` returns `direction=push|pull` from the *originator*'s perspective, which lies for inbound third-party credits (e.g. an IRS tax refund is `non_originated_ach, direction=push, external→rhs_account` — yet money flows IN). The classifier in [cash_flow.py](cash_flow.py) uses `originating_account_type → receiving_account_type` as the primary signal: `*→rhs_account` from non-rhs is a **deposit**; `rhs_account→*` to non-rhs uses `direction` (`pull`=deposit, `push`=withdrawal); `rhs_account ↔ rhs_account` is **internal** and excluded; unknown shapes log a warning and are skipped (never silently miscounted). Verbose log shows `transfer_type` + `details.originator_name` so future surprises are visible at a glance.

**Cash-flow snapshots are live-only; the synthetic chart-backfill was removed.** `outputs/cash_flow.jsonl` is append-only — every `python cash_flow.py` run adds a snapshot. `/api/cash-flow` serves these and only these (one per UTC date, latest-timestamp wins). The old synthetic backfill (`cash_flow.py --backfill` → `cash_flow_historical.jsonl`, scraped from RH's `bonfire/portfolio/performance/` chart endpoint) was **removed** because that endpoint returns a *span-relative return line*, not stable absolute account value — span-dependent and not reconcilable with live equity (its first point never anchored to true basis). A robust replacement via transaction reconstruction (options + stock orders + cash flows, marked to historical closes) is planned — see `plans/portfolio-reconstruction.md`. Until then the Portfolio curve starts at the first live snapshot; the Analysis tab's realized-P/L equity curve already covers performance-over-time back to the earliest trade.

**Option II — close-date P/L attribution.** `/api/trades/daily` and the Calendar's day modal bucket trades by **exit_date**, not entry_date. For 0DTE this is a no-op. For 1DTE+ holds, the −$10 from a 740C opened 5/6 and closed 5/7 lands on 5/7's bucket where it was actually realized. `server.py::_compute_exit_date` derives the exit date from entry_date + entry_time + hold_time_min. Every `/api/trades` row now carries `exit_date`. Frontend filters by `(t.exit_date || t.date) === dateStr`. Cumulative P/L is recomputed in exit-date order — the CSV's stored `Cumulative P/L ($)` column was entry-date-assumed and would be wrong here.

**Leg detection (frontend).** Inside the Calendar day modal, [static/views/calendar.html](static/views/calendar.html)::`buildLegs` reconstructs the trader's actual decisions from broker-fragmented executions. A "leg" = continuous holding period for one (strike, type) pair, split at flatlines (net qty returns to zero). Within a leg, events are classified as `open / add (avg-down/up/flat) / scale-out / close`. Same-price opens within 60s are auto-merged (broker order-fragmentation, not a real add). Same-price/same-time closes are merged the same way. The detector handles multi-day events via full ISO datetime sorting; per-leg auto-adapts: intraday → theta-band-by-hour tokens; multi-session → `📅 NDTE at entry` instead. Limits: legs key on (strike, type) only — no expiry — and nothing links multi-leg packages (RH tags them `opening_strategy: "straddle"` / `"custom"` on the order, but hood.py drops that field and the order id), so straddles show as two unrelated legs and 1-lot ladders at changing prices as separate adds.

**Pending dividends are excluded from cost basis.** `cash_flow.py::main()` only counts `PAID_DIVIDEND_STATES = {"paid", "reinvested"}` into `total_div`. RH schedules future dividend payments as `pending`, and counting them would inflate `net_cash_basis` → deflate displayed all-time P/L. Pending dividends still appear in `cash_flow_events.jsonl` so the calendar surfaces them (RH does the same in its app), they just don't pollute the basis snapshot. `voided` is also excluded.

**Polygon free tier — today's data is deferred, not cached.** Polygon's free tier serves end-of-day data; intraday queries for *today* during market hours return 403 NOT_AUTHORIZED until after market close + settlement. If we wrote the `out_of_plan` stub for today, the next run would skip it (the cache says "known unavailable"). `spy_intraday.py::run()` special-cases `target == today.isoformat()` and skips the cache write so the next run retries. Older `out_of_plan` dates ARE persisted (they're permanently beyond the 2y window).

**Calendar month keys are zero-padded (`YYYY-MM`) before sorting.** `monthsWithTrades` is a plain string sort; unpadded keys put Oct–Dec between Jan and Feb (fixed 2026-10-02).

**Calendar modal arrow-key + side-bar navigation.** Active days = union of trade entry/exit dates + cash-flow event dates. Sorted ascending. `←` / `→` arrow keys + transparent 44px nav bars flanking the modal jump prev/next. Cross-month navigation auto-syncs the calendar grid behind the modal via `currentMonthIdx`. Keydown listener is removed + re-added on each Calendar IIFE run via `window.__calArrowNavListener` to prevent stacking across nav.

**`monthsWithTrades` includes cash-flow-only months but with state filter.** Built from union of trade days + completed cash-flow event dates. Explicitly excludes `pending / failed / voided / scheduled` states so RH's future-scheduled dividend pays and recurring-deposit shimmers don't pad the calendar with empty months. Cash-flow-only months exist purely in this frontend variable — no leak into Analysis, Pre-Trade, Trade Log, or Portfolio (those query their own data sources unchanged).

**Admin endpoints write `.rh_token` and spawn subprocesses; auth is the existing dashboard token.** `POST /api/admin/token` validates against RH `/user/` before persisting (atomic write with `tempfile + os.replace`, `chmod 0o600`). `POST /api/admin/run` only accepts hardcoded script names (`ADMIN_SCRIPTS` whitelist in [server.py](server.py)) and rejects 409 if any job is currently running. Job state lives in an in-memory dict on the FastAPI module, protected by a threading lock — single-worker uvicorn assumed (matches `vps/rh-trade-exporter.service`). If uvicorn restarts mid-run, the subprocess is orphaned and the job is lost from the table; the cron backstop covers worst-case data freshness. JWT decode for `token-status` is unverified (`base64` only, no signature check) — the token was already issued by RH and we just want the `exp` claim; the authority on validity is RH itself via the optional `?probe=true` flag.

## Robinhood API Endpoints (Undocumented)

All reverse-engineered from browser network traffic and robin-stocks source:

| Endpoint | Purpose | Notes |
|---|---|---|
| `GET /accounts/` | List accounts | Broken for cash sub-accounts |
| `GET /options/orders/` | Paginated options orders | Supports `account_numbers`, `updated_at[gte]`, `state=filled`, `chain_symbol`. Orders carry `opening_strategy` / `closing_strategy` / `strategy` (`long_call`, `short_put`…, `straddle` for same-strike combos, `custom` for split strikes) and fee fields (`regulatory_fees`, `contract_fees`, `net_amount` = `processed_premium` ± fees). hood.py uses the fees; the strategy labels are still ignored |
| `GET /marketdata/options/historicals/{id}/` | Option price bars | `minute`+`span=day` (today only), `5minute`+`span=week` (~7 days), `hour`+`span=month` (~3 weeks). Used by `option_intraday.py` |
| `GET /options/instruments/{id}/` | Contract details | type, strike, expiry, chain_symbol |
| `GET /options/events/` | Exercise/assignment/expiration | `account_numbers` param, paginated |
| `GET /marketdata/historicals/{sym}/` | Daily or intraday OHLC | `interval=day&span=year` or `interval=5minute&span=week` |
| `GET /marketdata/options/` | Greeks (delta, gamma, etc.) | `instruments=URL1,URL2,...`, batch ≤17, point-in-time only |
| `GET /portfolios/{acct}/` | Current portfolio snapshot | `equity`, `extended_hours_equity`, `market_value` |
| ~~`GET bonfire.robinhood.com/portfolio/performance/{acct}`~~ | **No longer used** (was the synthetic backfill source) | Returned a span-relative *return* line, not absolute account value — removed. If re-needed, it's `?chart_style=PERFORMANCE&chart_type=historical_portfolio&display_span=all` with `Origin`/`X-Hyper-Ex` headers, `lines[].segments[].points[].cursor_data`. |
| `GET /accounts/{acct}/` | Account details | `type` (margin/cash), `portfolio_cash` |
| `GET /subscription/subscription_fees/` | Gold fees ledger | Paginated, monthly entries |
| `GET /dividends/` | Dividends paid | Paginated, includes `voided` state |
| `GET /midlands/referral/` | Referral stock/cash grants | Paginated, nested `reward.stocks[]` + `reward.cash` |
| `GET /user/` | Auth check (200 vs 401) | Used as a token-validity ping |
| `GET https://bonfire.robinhood.com/paymenthub/unified_transfers/` | All transfers (ACH, internal, non-originated) | Different host (`bonfire`). Categorize by `originating/receiving_account_type` pair; `direction` lies for inbound credits. |

**Pagination:** `next` URLs don't preserve custom query params — must re-inject on every page.

## CSV Output Columns

Trade #, Date, Day, Account, Symbol, Expiry Date, Type, Strike, Qty, Asset Open/High/Low/Close, VWAP, 8 EMA, Entry Time, Exit Time, Hold Time (min), Entry Hour, Entry Cost, Risk ($), Exit Credit, P/L ($), Cumulative P/L ($), P/L (%), Win/Loss, Is Win, VIX, Delta, Group ID, DTE, Fees ($), Net P/L ($)

## Data Source Limitations

- **VWAP / 8 EMA**: RH 5-min bars only go back ~5 trading days (`span=week`). yfinance 5-min gap-fills up to ~60 days. Older trades get `N/A`.
- **Delta**: Point-in-time only. Useful for same-day exports before expiry; null for expired contracts. Sticky-column merge preserves previously-captured values.
- **VIX**: Still from yfinance (`^VIX` not on RH).
- **Daily OHLC**: RH historicals (`span=year`), then yfinance daily, then synthesized from intraday 5-min bars for today's session.

## Known Issues

- FIFO pairing breaks if user re-opens the same exact contract after closing it same day
- RH `/accounts/` API does not return cash sub-accounts — must use `--account-numbers` manually on first run (cached after)
- Rows written before 2026-10-27 keep truncated whole-dollar money columns until rebuilt (regroup or `--full`); the gap is tiny (gener: 14 rows, $0.98) but can mislabel a near-zero row as BE
- An expired RH token stops the whole nightly chain (gener: 401s on Sep 29, Sep 30, Oct 16, Oct 26 2026). Today's 1-minute option bars are only fetchable the same day, so a missed day drops to 5-minute catch-up
- Legacy `G{n}` IDs and the ET calendar-spread collision remain in existing CSVs until `fix_group_ids.py --apply` runs (new data is correct on ingest)
- `option_intraday.py` files stay truncated if the day's only run was before the close (only possible from a local pre-close Admin run now that gener is the source of truth). `--backfill` refills them from Massive only if they're contract-days it hasn't fetched yet, and Massive wins only where it has more regular-session bars
- All open items are tracked, phased, in `plans/roadmap.md`

## Journal Sync

`journal_sync.py` appends new CSV trades into `spy_0dte_journal.xlsx` without touching existing rows.

**Usage:**

```bash
python journal_sync.py                    # reads outputs/spy_trades.csv, writes spy_0dte_journal_updated.xlsx
python journal_sync.py --in-place         # overwrite spy_0dte_journal.xlsx directly
python journal_sync.py --fetch            # auto-run hood.py first with --after-date <journal's max date>
python journal_sync.py --journal <path> --csv <path> --output <path>
```

**Key behaviors:**

- **Dedup key is (Date, Entry Time, Strike, Type, Qty, Exit Time)** — NOT Group ID. Format-independent. Exit Time was added 2026-10-27: without it, once the first of two equal-size scale-outs was in the journal the second was skipped forever. Every existing journal row has an exit time, so the change re-matches all 384 rows (verified).
- **Write scope is row-bounded**: every write targets `start_row + i` where `start_row = last_row + 1`. Existing rows are never touched.
- **Formulas re-emitted per row** (row-substituted from row 2 patterns): O (Trend Aligned), R (Hold Time), S (Entry Hour), V (P/L $), W (Cumulative P/L), X (P/L %), Y (Win/Loss), Z (Is Win), AG (Risk $), AH (R-Multiple).
- **Manual-fill columns AA–AE** (Setup / Trigger / Exit Reason / Rules Followed / Notes) are left blank on new rows. AE carries the yellow fill + borders from row 2. AA, AB, AC, AE use left-aligned wrap-text; everything else is center-aligned. Row height is 31.
- **Per-column number formats** are explicit (`COLUMN_FORMATS` dict). Do NOT copy formats from row 2 blindly — row 2 of the journal has junk formats on D, G, N.
- **Excel table range** (`Table2`) is extended to the new last row so appended rows are part of the formatted table.
- **Stale DV strip**: the source journal had data validations leaked onto rows ≥150 for columns C, E, K, L, AB from prior row-insert shifts. `strip_stale_dvs` removes them. AA (Setup) is intentionally preserved — room for a future setup-type dropdown.
- **Safety diff**: after save (unless `--in-place`), rows 1..last_row are compared cell-by-cell between original and output. Aborts with exit 2 on any difference.

## Dashboard Server

`server.py` is a lightweight FastAPI app that serves the trade dashboard and API endpoints.

**Auth:** Bearer token from `.server_token` file. Supports `Authorization: Bearer <token>` header or `?token=<token>` query param. No token file = auth disabled (local dev).

**API Endpoints:**

| Route | Source | Returns |
|---|---|---|
| `GET /api/trades?symbol=SPY&basis=net` | `spy_trades.csv` + `other_trades.csv` | Trade list as JSON array. `basis=net` (default) / `gross` — applies to `/api/trades/daily`, `/api/trades/split` and `/api/summary` too; 400 on any other value. Net rows carry `pl_gross`; all rows carry `fees` / `net_pl` from the CSV. |
| `GET /api/trades/daily` | `spy_trades.csv` | Daily aggregates: date, pl, num_trades, wins, cumulative_pl, vix |
| `GET /api/trades/split` | `spy_trades.csv` + `spy_intraday/*.json` | Three-group split of SPY opening orders (Group ID), first match wins: `entry_leak` (9am-hour entry, or re-entry ≤10 min after a red exit), `hold_leak` (held >30 min while SPY was against the position at +30m, from 5-min bar opens — underlying direction, not option marks), `clean`. Returns `{groups, months (by exit month), rescue: {underwater_at_30, ended_green}, hold_check_missing, orders}`. `server.py::compute_pl_split`; bar files memoized by mtime. Derived 2026-09-29 and validated out-of-sample (pre-July rules held post-July). |
| `GET /api/trades/open` | `unmatched_opens.csv` | Raw open positions (used by the deprecated path; Positions tab uses `/api/positions` instead) |
| `GET /api/positions` | `unmatched_opens.csv` | Open positions enriched with `days_held`, `dte_remaining`, `expired` flag, and a stable `contract_key` for cross-view linking. Sorted nearest-expiry-first. Live mark prices deferred — see [plans/architecture-v2.md](plans/architecture-v2.md). |
| `GET /api/cash-flow` | `cash_flow.jsonl` | Live portfolio snapshots, one per UTC date (latest-timestamp wins). Synthetic-historical merge removed. |
| `GET /api/cash-flow/events?date=YYYY-MM-DD` | `cash_flow_events.jsonl` | Per-event log (transfers, fees, dividends, referrals); optional `date` filter. Sorted by date asc, then kind. |
| `GET /api/spy/daily` | `spy_daily.json` | SPY + VIX daily OHLC for the calendar overlay. Returns `{generated_at, range, days}`. Empty `{days: []}` when cache hasn't been built. |
| `GET /api/spy/intraday/{YYYY-MM-DD}` | `spy_intraday/{date}.json` | 5-minute SPY bars for one date. Returns either `{date, bars, source, interval}` on success or `{date, available: false, reason: "not_cached"\|"out_of_plan"\|"no_data"\|"corrupt"\|"error", bars: []}`. The 400 response covers malformed dates and path-traversal attempts. |
| `GET /api/admin/token-status?probe=true|false` | `.rh_token` (JWT decode) + optionally `GET https://api.robinhood.com/user/` | `{valid, exp, expires_in_seconds, masked, probed, probe_ok?, probe_status?}` |
| `POST /api/admin/token` | Body `{token: "..."}` (raw JWT, `Bearer <jwt>`, header line, or full cURL paste) | Validates against RH `/user/`, atomic-writes `.rh_token` with chmod 600, returns refreshed status. 400 on RH-rejection or unparseable input. |
| `POST /api/admin/run` | Body `{script: ...}` | Spawns the named script via `subprocess.Popen`, stdout+stderr → `outputs/.admin_jobs/{job_id}.log`. 409 if a job is already running. Whitelist: `hood`, `cash_flow`, `spy_daily`, `spy_intraday`, `spy_intraday_back` (spy_intraday --backfill), `option_intraday`, `token_watch`, `daily_refresh` (the shared chain: hood → option_intraday → cash_flow → spy_daily → spy_intraday; only hood fatal), `pull_data` (`bash vps/pull_data.sh`). Watcher thread flips state to `done`/`failed` on exit. `_prune_old_job_logs()` runs on every spawn, keeping only the 50 most recent `*.log` files by mtime (`JOBS_LOG_KEEP` in server.py). |
| `GET /api/admin/run/{job_id}` | In-memory job table + log file | `{state, started_at, ended_at, exit_code, log_tail (last 32KB)}`. UI polls every 2s. 32KB tail is sized to comfortably fit a full daily_refresh run's output including all eight `[STEP]` markers, so cold-restore on page reload paints child rows correctly. |
| `GET /api/admin/runs` | In-memory job table | Recent jobs (newest first, max 20). No log content. |
| `GET /api/summary` | All files | Quick stats: total trades, P/L, win rate |
| `GET /api/notes` | `journal_notes.json` | Journal notes keyed by Group ID |
| `POST /api/notes` | → `journal_notes.json` | Save/update a journal note |
| `GET /dashboard` | `static/index.html` | App shell (auth checked) |
| `GET /static/{path}` | `static/` | CSS, view fragments (no auth) |

**CSV column mapping:** `P/L ($)` → `pl`, `Risk ($)` → `risk`, `8 EMA` → `ema8`, etc. Dates normalized from `M/D/YYYY` to `YYYY-MM-DD`.

**Journal notes:** Simple JSON file at `outputs/journal_notes.json`, keyed by Group ID. Edited via the Trade Log view's ag-Grid.

**Cash flow snapshot schema** (`outputs/cash_flow.jsonl` lines):
```
timestamp, deposits, withdrawals, deposits_pending, withdrawals_pending,
net_deposited, gold_fees, gold_months, dividends, referral_grants,
net_cash_basis, current_equity, all_time_pnl, all_time_pnl_pct,
total_return, total_return_pct,
accounts: [{type, account_number, equity, cash}, ...]
```
Older live snapshots (pre-2026-05) lack `accounts`, `gold_months`, and `*_pending` fields — the dashboard handles missing fields gracefully.

## Deployment

**Deployed 2026-09-30 at `f6e2d32`:** crontab fixed (`DISCORD_WEBHOOK_URL`) with the hourly token_watch line, login banner in `~/.bashrc`, fresh token saved.

**VPS state as first checked 2026-09-30 (`ssh gener`, repo at `/home/gener/rh-trade-exporter`):** cron only — no uvicorn process, `rh-trade-exporter.service` not installed, nginx inactive, so the dashboard/Admin page is not reachable there. Repo was 1 commit behind GitHub (at `93b3ba9`). The crontab defined the webhook as `DISCORD_WEBHOOD_URL` (typo) so run.sh's failure alerts never fired; `ALERT_EMAIL` unset. Python 3.11, timezone America/New_York.

**Email alerts never leave gener (checked 2026-10-26).** `ALERT_EMAIL` is now set in the crontab, but `mail` (bsd-mailx) hands off to exim4 configured `dc_eximconfig_configtype='local'`, which bounces every external address ("Mailing to remote domains not supported", 13 bounces in `/var/mail/gener` since Oct 1). `mail` exits 0 once exim queues the message, so `token_watch.log` still says "sent via discord, email". **Decision 2026-10-26: Discord-only.** `ALERT_EMAIL` removed from gener's crontab rather than fixing exim. If email is ever wanted: gener is a GCE VM (europe-west4) and GCE blocks outbound port 25, so it needs exim as a smarthost on port 587 (e.g. `smtp.gmail.com::587` + an app password in `/etc/exim4/passwd.client`), not "internet site".

**Token monitor + login banner on gener** (after pulling):
```
# crontab -e
DISCORD_WEBHOOK_URL=...            # the variable name run.sh and token_watch.py read
15 7-21 * * * cd /home/gener/rh-trade-exporter && .venv/bin/python token_watch.py >> token_watch.log 2>&1
# ~/.bashrc
[[ $- == *i* ]] && [ -f ~/rh-trade-exporter/vps/login_banner.sh ] && bash ~/rh-trade-exporter/vps/login_banner.sh
```
Replace a dead token over SSH without it landing in shell history: `cd ~/rh-trade-exporter && read -rs T && printf '%s\n' "${T#Bearer }" > .rh_token && chmod 600 .rh_token`.


- **VPS**: Cloned on Ubuntu VPS (set timezone to America/New_York, or adjust cron hours for UTC)
- **Dashboard** (intended; **not installed on gener** as of 2026-09-30 — see VPS state above): `vps/rh-trade-exporter.service` runs uvicorn on `127.0.0.1:8000`, nginx reverse proxy with HTTPS
- **Cron**: `vps/run.sh` runs daily at 4:05 PM ET (weekdays only) — `daily_refresh.py` (hood → option_intraday → cash_flow → spy_daily → spy_intraday) then `health_check.py`; plus the hourly `token_watch.py` line
- **Log rotation**: `vps/logrotate.conf` → `/etc/logrotate.d/rh-trade-exporter` (weekly, 4 weeks retained)
- **CI**: GitHub Actions runs `pytest tests/` on push to main and on PRs
- **Alerting**: Discord on hood.py failure (run.sh), token problems (token_watch, hourly) and anything health_check finds (gap days, failed steps, missing option bars). Discord-only in practice — see the email note above

## Possible Future Work

- Discord bot integration (same VPS) for trade notifications / alerts
- ~~`zoneinfo` for proper timezone handling~~ ✅ done
- ~~Tests + CI~~ ✅ done
- ~~VPS cron scheduling~~ ✅ done
- ~~Dashboard server~~ ✅ done
- ~~Direct XLSX merge into user's spreadsheet template~~ ✅ done (`journal_sync.py`)
- Portfolio historicals for P/L reconciliation — chart-scrape backfill removed (span-relative/unreliable); robust transaction-reconstruction planned in `plans/portfolio-reconstruction.md`

## Commands

```bash
pip install -r requirements.txt

# First run (MUST pass --account-numbers if you have a cash sub-account)
python hood.py --token "Bearer ..." --save-token --account-numbers "XXXXX,YYYYY"

# Incremental daily export (default — cursor derived from existing CSVs)
python hood.py

# Explicit date window
python hood.py --after-date 2026-03-12 --symbol SPY

# Full refetch (ignores cursor)
python hood.py --full

# Merge new CSV trades into the Excel journal
python journal_sync.py                    # → spy_0dte_journal_updated.xlsx
python journal_sync.py --in-place         # overwrite original
python journal_sync.py --fetch            # auto-run hood.py first

# Cash flow + portfolio
python cash_flow.py                       # appends a new snapshot to cash_flow.jsonl
python cash_flow.py --json                # silent mode (cron-friendly), still writes JSONL
# (cash_flow.py --backfill removed — chart endpoint was span-relative/unreliable; see plans/portfolio-reconstruction.md)
# (or use the dashboard's Admin tab → "Run" buttons to do these without SSH)

# Option bars with volume from Massive (resumable; ~3.5 h for the first full run)
python option_intraday.py --backfill
python option_intraday.py --backfill --since 2026-09-01 --limit 50

# Debug
python hood.py --dump-raw

# Run all tests
pytest tests/ -v

# Start dashboard (local dev, no auth)
uvicorn server:app --reload
# Then visit http://localhost:8000/dashboard

# Start dashboard with auth
echo "my-secret-token" > .server_token
uvicorn server:app --reload
# Visit http://localhost:8000/dashboard?token=my-secret-token
```

## Style Notes

- Single-file script, emoji prefixes for progress sections
- User prefers direct, no-bullshit communication
