# CLAUDE.md

## Project Overview

RH Options Trade Exporter — a Python CLI (`hood.py`, ~1500 lines) that pulls options order history from Robinhood's undocumented API and outputs structured CSVs for trading journal backfill, plus `journal_sync.py` which merges those CSVs into an Excel journal, plus a FastAPI dashboard (`server.py`) for visualizing trade data. Built for a day trader running 0DTE SPY options.

## Key Files

- `hood.py` — main script (single file, no package structure)
- `cash_flow.py` — pulls all money movement from Robinhood (transfers, fees, dividends, referrals, equity) and outputs JSONL snapshots
- `journal_sync.py` — merges `outputs/spy_trades.csv` into `spy_0dte_journal.xlsx`, appending only new trades (dedup by Date+Entry Time+Strike+Type+Qty). Preserves existing rows, formulas, and manual-fill columns (AA–AE).
- `regroup_trades.py` — one-time migration that rebuilt the legacy fragmented `spy_trades.csv` / `other_trades.csv` with order-level grouping (see "Executions aggregated by order_id" below). Dry-run by default; `--apply` backs up to `*.prefragfix.bak` then writes, refusing any file where a position would be lost. Carries captured sticky/market columns forward losslessly by exact (Date, Strike, Type, Entry Time) match. For non-SPY contracts left net-open past expiry with no closing execution, `add_expired_otm_closes` synthesizes a $0 expiry close (expired worthless → long loses premium, short keeps credit) so they become realized rows — scoped to non-SPY so the verified SPY data is untouched.
- `server.py` — FastAPI dashboard server (~200 lines), serves API + static dashboard
- `static/index.html` — SPA shell with top nav bar (8 views: Analysis, Calendar, Habits, Positions, Pre-Trade, Trade Log, Portfolio, Admin). Loads TradingView Lightweight Charts + Chart.js + chartjs-adapter-date-fns from CDNs. Exposes `window.loadView` for cross-view navigation (e.g. Calendar → Positions deep-link). Also hosts the global **9am lockout banner** (sticky under the nav on every view, weekdays 09:25–10:00 ET, countdown to 10:00, live 9am-hour stats computed from `/api/trades` at the decision level). Preview any time with `?lockout=preview`.
- `static/style.css` — shared dark theme CSS
- `static/views/analysis.html` — equity curve (cumulative/daily toggle), KPIs (Total P/L, Win Rate, Avg Win/Loss, **Profit Factor, Expectancy**, Best/Worst Day, Max Consec Wins/Losses), Chart.js breakdowns. Insight narrative includes top-N% concentration sentence. Span selector (1D / 1W / 1M / 3M / 6M / YTD / 1Y / ALL) filters trades by date before all KPIs/charts compute, persisted to `localStorage` (default ALL).
- `static/views/calendar.html` — browsable monthly P/L heatmap + day-detail modal. Day cells show P/L + small SPY % + VIX + cash-flow corner icons (overlay row above grid toggles each, persisted in localStorage). Click any active day → modal with: collapsible Intraday chart (Lightweight Charts: 5/10/15m toggle, premarket H/L lines, trade markers, ETH dimming, ET-forced axis), collapsible Cash flow section, leg-detection view chips (Legs/Roundtrips/Fills/By contract/By hour), Carried-positions section for multi-day entries. Arrow keys (←/→) + transparent side bars navigate active days; cross-month auto-syncs grid. `monthsWithTrades` includes cash-flow-only months but excludes pending/scheduled states.
- `static/views/habits.html` — top section **Where the Money Comes From**: three-group P/L split from `/api/trades/split` (tiles, stacked monthly Chart.js bars + net line, 30-min rescue rate, recent flagged orders). Then the **What Works** behavior cards (recent window vs baseline), then pattern-over-time cards (Overtrading, Edge, Blowups, Chase, Position Sizing, Put vs Call, Morning vs Afternoon). Split out of Calendar so the calendar stays focused on the heatmap.
- `static/views/positions.html` — currently-open positions from `unmatched_opens.csv` with computed days-held and DTE remaining. Active vs expired sections, sorted nearest-expiry-first. Calendar's Carried-positions rows deep-link here via `#positions:<contract_key>` with scroll-to + yellow flash highlight. Live mark prices deferred (see roadmap).
- `static/views/pretrade.html` — pre-trade reference (day of week / VIX / hour sub-tabs)
- `static/views/tradelog.html` — ag-Grid trade table with trades/daily toggle, editable journal notes
- `static/views/portfolio.html` — account-level equity / P/L / income view backed by `cash_flow.jsonl` (live snapshots only; the synthetic-historical backfill was removed). Clickable KPIs open a modal with formula + breakdown for each metric (current equity → per-account split; all-time P/L, total return, net deposited, lifetime income → derivation rows). Span selector (1D / 1W / 1M / 3M / 6M / YTD / 1Y / 5Y / ALL) filters the chart data client-side, default 1Y, persisted to `localStorage`. 1D special-cases pre-dedup snapshots (so multi-snapshot days from manual + cron runs show intra-day variation); falls back to last 2 daily points if today has <2 snapshots. Time-axis tick unit auto-adapts (`hour` for 1D, `day` for 1W–1M, `week` for 1M–3M, `month` for 6M–2Y, `year` beyond). Charts use a Chart.js time-scale x-axis with `chartjs-adapter-date-fns` (loaded once in `index.html`).
- `static/views/admin.html` — token health + on-demand fetch UI. Sections: (1) RH token status with traffic-light + countdown to JWT expiry + "Probe RH" button; (2) paste-to-update form that accepts raw JWT, `Bearer <jwt>`, `Authorization: Bearer <jwt>` line, or full cURL paste; (3) on-demand run buttons. Top run row is the **Daily refresh** chain (hood → cash_flow → spy_intraday → spy_daily, abort on first failure, per-step status mirrors below by parsing `[STEP] <name> START|END exit=N` markers from the shared log stream). Daily refresh button is auto-disabled when the RH token traffic light is red (`applyTokenGate`) — individual rows stay enabled so spy_* can run without an RH token. Individual rows below for surgical retry: `hood`, `cash_flow`, `spy_intraday`, `spy_daily`, `spy_intraday_back` (spy_intraday --backfill). Polls `/api/admin/run/{job_id}` every 2s while a job is running. Refreshes token status after a job finishes. On view load, `restoreFromRecentRuns()` reattaches to any in-flight job or paints the final state of a run that ended within the last 5 minutes — so a page reload mid-run or right after completion doesn't lose context.
- `static/sandbox/leg-layout.html` — standalone sandbox for testing layout variants of the leg-row (A/B/C grid templates × treatment toggles for dd-note alignment, urgency huddle, 🌙 placement, P/L %). Not part of the SPA; opened directly via `/static/sandbox/leg-layout.html` for visual A/B comparison before porting to the real calendar.
- `static/sandbox/kpi-layout.html` — standalone sandbox with two tabs: (1) Analysis KPI tile layout variants (A grouped + collapsible / B headline+collapsible / C fixed 5×3 compact), with toggles for combo tiles (Best+Worst Day, Avg+Median Daily, Max-W+Max-L streaks, Avg-Win+Avg-Loss), current-streak conditional tile (active/winning/none), label style (full/short), density (comfortable/compact), and click-to-modal demo. Group headers in Variant A persist collapse state to localStorage. (2) pf-modal close-button redesign (broken-baseline reproduces the bug / M1 width-fix-only / M2 width-fix+polish / M3 explicit-flex-header with split scroll body) with width-preset (440/560/680), long-content toggle, M3 head-divider toggle, and viewport-simulator (desktop/tablet 780/mobile 400). Open at `/static/sandbox/kpi-layout.html` before porting the chosen variants to analysis.html / portfolio.html.
- `tests/test_hood.py` — pytest suite for hood.py pure logic (token, FIFO pairing, VWAP/EMA, formatting, DataFrame building, incremental cursor, sticky-column merge)
- `tests/test_server.py` — pytest suite for server.py (auth, CSV parsing, all API endpoints incl. positions, spy_daily, spy_intraday, cash-flow events, three-group trades split; routes; column mapping; exit_date bucketing)
- `tests/test_cash_flow.py` — pytest suite for cash_flow.py (token, pagination, transfer categorization, summary math, JSONL output, **extract_events**, **merge_events_to_jsonl**, **resolve_dividend_symbols**, **pending-dividend exclusion**)
- `tests/test_spy_daily.py` — pytest suite for spy_daily.py (yfinance mocked: payload assembly, pct-change, range inference, cache I/O)
- `tests/test_spy_intraday.py` — pytest suite for spy_intraday.py (Polygon mocked: token, pagination, 403/429/timeout handling, rate-limit pacing, weekday selection, missing-dates dedup, cache I/O)
- `tests/test_option_intraday.py` — option_intraday.py: contract selection (0DTE, overnight holds, open positions, other symbols), instrument index, bar parsing (interpolated dropped, ET day split), fetch params + 401/404, catch-up targets, 1-minute today with 5-minute fallback, never-downgrade merge. Autouse guard fails any real network call.
- `tests/test_token_watch.py` — token_watch.py: JWT exp decode, probe, status evaluation, alert de-dup/repeat/recovery rules, Discord + mail delivery, end-to-end main with state file. Autouse network guard.
- `tests/test_journal_sync.py` — pytest suite for journal_sync.py (dedup key, trend coercion, DV strip, append smoke test against synthetic workbook)
- `tests/test_legs.mjs` — node test for the Calendar day-modal leg detection. Slices the real `buildLegs` + per-event render block out of `static/views/calendar.html` by anchor strings and exercises them: scale-out vs close classification + outcomes, and that full-close rows drop the `(lock gains/cut losses)` parenthetical while scale-out rows keep it. `tests/test_legs_render.py` is a pytest shim that shells out to node so `pytest tests/` covers it (skips if node is absent).
- `plans/` — markdown design docs that survive across sessions. Notable: `architecture-v2.md` (modal abstraction, persistence, CSS strategy, productionization), `calendar-modal-roadmap.md` (4-step calendar plan, all shipped), `potential-kpis.md` (queue of KPI enrichment options), `resume_framing.md` (how to talk about this project externally), `admin-concurrency-notes.md` (lock model, what races on collision, vps/run.sh vs daily_refresh.py drift risk, log retention, UI reload edge cases)
- `.github/workflows/test.yml` — GitHub Actions CI (runs tests on push/PR to main)
- `vps/run.sh` — cron wrapper script. Runs hood.py → option_intraday.py → cash_flow.py → spy_daily.py → spy_intraday.py sequentially. hood.py failure is fatal + alerts via Discord webhook + email (`DISCORD_WEBHOOK_URL` / `ALERT_EMAIL` from the crontab env, falling back to `.env`). Others are non-fatal (logged but don't trigger alerts).
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
- `outputs/option_intraday/{YYYY-MM-DD}.json` — per-day option price bars from `option_intraday.py`. Schema: `{date, fetched_at, source, contracts: [{instrument_id, symbol, type, strike, expiry, interval: "minute"|"5minute", bars: [{t,o,h,l,c}]}], missing: [{symbol, type, strike, expiry, reason}]}`, `t` = Unix seconds UTC. Gap-fill bars are dropped.
- `outputs/.token_watch.json` / `outputs/.token_status` — token_watch.py state (status, token fingerprint, last alert time) and the one-line status the login banner prints.
- `outputs/spy_daily.json` — daily OHLC cache for SPY + VIX, rebuilt fresh on each `spy_daily.py` run (overwritten, not append). Powers the Calendar's per-cell market context overlay (SPY % change, VIX level color-banded). Schema: `{generated_at, range: {start, end}, days: [{date, spy_open, spy_high, spy_low, spy_close, spy_pct, vix_close}]}`. Range auto-inferred from earliest trade date in `spy_trades.csv` minus 7 days; falls back to 5-year window if no trades exist.
- `spy_daily.py` — fetches yfinance daily for `SPY` and `^VIX`, writes the cache above. Idempotent, cron-friendly (`--json` silent mode). Called nightly via `vps/run.sh` after hood.py + cash_flow.py, and on-demand via Admin → Run.
- `outputs/spy_intraday/{YYYY-MM-DD}.json` — 5-minute SPY bars from Polygon (free tier via `MASSIVE_API_KEY` in `.env`). One file per trading day. Schema: `{date, fetched_at, source, interval: "5m", bars: [{t, o, h, l, c, v}]}` where `t` is **Unix seconds UTC** (Lightweight Charts format). For dates outside Polygon's 2y window or with no data (weekends/holidays), the stub `{date, available: false, reason}` is written instead — prevents repeated fetches of known-unavailable days. **Exception:** today's date is NEVER cached as `out_of_plan` (Polygon's free tier serves end-of-day data, so today comes back as 403 until market close + settlement; we defer the write so the next run retries). Powers the calendar day modal's intraday candle chart.
- `spy_intraday.py` — Polygon free tier client. Reads `MASSIVE_API_KEY` from `.env`, paginates per day (~2 hops for 04:00–20:00 bars), sleeps 13s between distinct dates (5 req/min ceiling). Modes: default (previous trading day + today — walks back through weekends so a Monday run fills Friday + Monday), `--date YYYY-MM-DD` (force a single date, overwrites cache), `--backfill` (walks all missing weekdays inside the 2y window, optional `--since` and `--limit`). Called nightly via `vps/run.sh`, on-demand via Admin → Run.
- `option_intraday.py` — nightly capture of option price bars for the contracts you traded, before Robinhood's retention drops them. RH's `/marketdata/options/historicals/{id}/` only allows `minute`+`span=day` (today only) and `5minute`+`span=week` (~7 calendar days back); `hour`+`span=month` reaches ~3 weeks. So today's contracts are saved at 1-minute, and any day in the last 6 calendar days that a failed run missed is caught up at 5-minute. Contracts come from spy/other trades (entry through exit day) + unmatched opens (entry through expiry); instrument ids from `.rh_instrument_cache.json`. Merges per contract and never replaces bars with fewer. Exit 2 on a rejected token. Runs in `vps/run.sh` right after hood.py (non-fatal) and last in `daily_refresh.py`.
- `token_watch.py` — Robinhood token monitor for the VPS cron. Reads the JWT `exp` for the countdown and asks RH `GET /user/` (authoritative). Statuses: missing / rejected / expired / expiring (<24h) / ok / unknown (network blip, no alert). Alerts via Discord (`DISCORD_WEBHOOK_URL`) and `mail` (`ALERT_EMAIL`), read from env then `.env`; de-duplicated via `outputs/.token_watch.json` (bad status: on change then every 12h; expiring: once per token; one "recovered" note after a fresh token). Writes `outputs/.token_status` for the SSH login banner. `--dry-run` evaluates without alerting or saving. `--banner` is the SSH-login mode: live probe with a 4s timeout, prints the status (+ fix command when bad), never alerts and never touches alert state — login shows the truth now, while cron stays the thing that warns you on days you don't log in. `--test-alert` sends a test message through the configured channels (exit 1 if nothing is configured).
- `vps/login_banner.sh` — at SSH login runs `token_watch.py --banner` (live check, capped at 6s with `timeout`); if that fails, falls back to the cached `outputs/.token_status` (marked "cached", with the fix command when red/yellow and a warning if >2h stale). Installed by one line in `~/.bashrc` (see Deployment).
- `daily_refresh.py` — chained runner invoked by the Admin "Daily refresh" button. Runs hood → cash_flow → spy_intraday → spy_daily → option_intraday sequentially (option capture last so it can never block the core refresh), aborts on first failure. Emits `[STEP]` markers in stdout so the UI can mirror per-step status into the individual run rows below.
- `spy_0dte_journal.xlsx` — the user's Excel trading journal (Trade Log sheet is what journal_sync writes into; Dashboard + Setup Analysis sheets are untouched)
- `spy_0dte_journal_updated.xlsx` — journal_sync output (original untouched unless `--in-place`)

## How It Works

1. User grabs a Bearer token from browser DevTools (expires ~24hrs)
2. Script hits Robinhood's private REST API with server-side filters (`state=filled`, `chain_symbol`, `updated_at[gte]`)
3. Fetches options orders, resolves instruments (cached to disk), extracts executions, FIFO-pairs opens→closes
4. Each **closing execution** = one output row (not each round-trip)
5. Enriches with: daily OHLC (RH historicals, yfinance fallback), VIX (yfinance), intraday VWAP + 8 EMA (RH 5-min bars), greeks/delta (RH marketdata), options events (exercise/assignment/expiration warnings)
6. Outputs separate CSVs: spy_trades, other_trades, unmatched_opens, cancelled, rejected, failed

## Critical Design Decisions

**One row per exit, not per round-trip.** If user opens 3 contracts and closes 2 then 1, that's 2 rows with the same Group ID and entry time but different exit times/quantities. Do not change this without asking.

**Entry Cost is negative, Exit Credit is positive.** Entry Cost = -(price_per_share × qty × 100).

**Executions aggregated by order_id before FIFO pairing.** RH reports one order as multiple partial-fill executions (often within seconds at slightly different prices). `aggregate_executions_by_order` (hood.py) collapses fills sharing `(order_id, option_url, position_effect)` into one lot — quantity summed, price volume-weighted (P/L-preserving), timestamp = earliest fill (the stable Group ID anchor) — *before* `pair_into_trade_rows`. Without it, one trading decision fragments into several rows with distinct Group IDs (~25% inflation, polluting every Analysis KPI). Genuine scale-outs and averaging-down stay separate (they're separate orders). The legacy fragmented CSVs were migrated once via `regroup_trades.py`; new data is correct on ingest.

**FIFO pairing by contract URL.** Can mismatch if the same exact contract is opened, closed, then re-opened same day.

**No third-party Robinhood libraries.** Only `requests`, `yfinance`, `pandas`, and stdlib (`zoneinfo`). Auth is a manually-copied Bearer token.

**Single file.** Keep it that way unless it pushes well past 1500 lines.

**Stable Group IDs.** Format: `YYYY-MM-DD-HHMMSS-{strike}{C|P}` (ET entry time). Derived from trade data so re-runs produce identical IDs — journal_sync and any other downstream consumer can trust them across runs.

**Incremental by default.** Bare `python hood.py` derives the fetch cursor from existing CSVs: `min(max(Date) across spy_trades + other_trades, min(Date) in unmatched_opens) - 1 day`. The `unmatched_opens` floor guarantees multi-DTE positions get refetched. `--full` opts out. New rows are merged into existing CSVs (dedup key = Group ID + Exit Time).

**Sticky columns on merge.** On CSV merge, Delta / VWAP / 8 EMA carry forward from the old row if the new row has a blank value. Reason: RH `/marketdata/options/` returns null for expired contracts, and RH 5-min bars only go back ~5 trading days. Without stickiness, re-runs would erase point-in-time data captured earlier.

**VWAP / 8 EMA are categorical.** Emitted as `Above` / `Below` / `At` / `N/A` (vs underlying at entry, ±$0.05 tolerance). The journal's `Trend Aligned?` formula expects these strings, not raw prices.

**Daily OHLC fallback.** Primary: RH `span=year`. Secondary: yfinance daily. Tertiary: synthesize from the intraday 5-min bars already fetched for VWAP/EMA (covers today's trades before RH's evening reconciliation populates the daily endpoint).

**Cash-flow transfer classification keys off account-type pair, not `direction`.** `bonfire/paymenthub/unified_transfers/` returns `direction=push|pull` from the *originator*'s perspective, which lies for inbound third-party credits (e.g. an IRS tax refund is `non_originated_ach, direction=push, external→rhs_account` — yet money flows IN). The classifier in [cash_flow.py](cash_flow.py) uses `originating_account_type → receiving_account_type` as the primary signal: `*→rhs_account` from non-rhs is a **deposit**; `rhs_account→*` to non-rhs uses `direction` (`pull`=deposit, `push`=withdrawal); `rhs_account ↔ rhs_account` is **internal** and excluded; unknown shapes log a warning and are skipped (never silently miscounted). Verbose log shows `transfer_type` + `details.originator_name` so future surprises are visible at a glance.

**Cash-flow snapshots are live-only; the synthetic chart-backfill was removed.** `outputs/cash_flow.jsonl` is append-only — every `python cash_flow.py` run adds a snapshot. `/api/cash-flow` serves these and only these (one per UTC date, latest-timestamp wins). The old synthetic backfill (`cash_flow.py --backfill` → `cash_flow_historical.jsonl`, scraped from RH's `bonfire/portfolio/performance/` chart endpoint) was **removed** because that endpoint returns a *span-relative return line*, not stable absolute account value — span-dependent and not reconcilable with live equity (its first point never anchored to true basis). A robust replacement via transaction reconstruction (options + stock orders + cash flows, marked to historical closes) is planned — see `plans/portfolio-reconstruction.md`. Until then the Portfolio curve starts at the first live snapshot; the Analysis tab's realized-P/L equity curve already covers performance-over-time back to the earliest trade.

**Option II — close-date P/L attribution.** `/api/trades/daily` and the Calendar's day modal bucket trades by **exit_date**, not entry_date. For 0DTE this is a no-op. For 1DTE+ holds, the −$10 from a 740C opened 5/6 and closed 5/7 lands on 5/7's bucket where it was actually realized. `server.py::_compute_exit_date` derives the exit date from entry_date + entry_time + hold_time_min. Every `/api/trades` row now carries `exit_date`. Frontend filters by `(t.exit_date || t.date) === dateStr`. Cumulative P/L is recomputed in exit-date order — the CSV's stored `Cumulative P/L ($)` column was entry-date-assumed and would be wrong here.

**Leg detection (frontend).** Inside the Calendar day modal, [static/views/calendar.html](static/views/calendar.html)::`buildLegs` reconstructs the trader's actual decisions from broker-fragmented executions. A "leg" = continuous holding period for one (strike, type) pair, split at flatlines (net qty returns to zero). Within a leg, events are classified as `open / add (avg-down/up/flat) / scale-out / close`. Same-price opens within 60s are auto-merged (broker order-fragmentation, not a real add). Same-price/same-time closes are merged the same way. The detector handles multi-day events via full ISO datetime sorting; per-leg auto-adapts: intraday → theta-band-by-hour tokens; multi-session → `📅 NDTE at entry` instead.

**Pending dividends are excluded from cost basis.** `cash_flow.py::main()` only counts `PAID_DIVIDEND_STATES = {"paid", "reinvested"}` into `total_div`. RH schedules future dividend payments as `pending`, and counting them would inflate `net_cash_basis` → deflate displayed all-time P/L. Pending dividends still appear in `cash_flow_events.jsonl` so the calendar surfaces them (RH does the same in its app), they just don't pollute the basis snapshot. `voided` is also excluded.

**Polygon free tier — today's data is deferred, not cached.** Polygon's free tier serves end-of-day data; intraday queries for *today* during market hours return 403 NOT_AUTHORIZED until after market close + settlement. If we wrote the `out_of_plan` stub for today, the next run would skip it (the cache says "known unavailable"). `spy_intraday.py::run()` special-cases `target == today.isoformat()` and skips the cache write so the next run retries. Older `out_of_plan` dates ARE persisted (they're permanently beyond the 2y window).

**Calendar modal arrow-key + side-bar navigation.** Active days = union of trade entry/exit dates + cash-flow event dates. Sorted ascending. `←` / `→` arrow keys + transparent 44px nav bars flanking the modal jump prev/next. Cross-month navigation auto-syncs the calendar grid behind the modal via `currentMonthIdx`. Keydown listener is removed + re-added on each Calendar IIFE run via `window.__calArrowNavListener` to prevent stacking across nav.

**`monthsWithTrades` includes cash-flow-only months but with state filter.** Built from union of trade days + completed cash-flow event dates. Explicitly excludes `pending / failed / voided / scheduled` states so RH's future-scheduled dividend pays and recurring-deposit shimmers don't pad the calendar with empty months. Cash-flow-only months exist purely in this frontend variable — no leak into Analysis, Pre-Trade, Trade Log, or Portfolio (those query their own data sources unchanged).

**Admin endpoints write `.rh_token` and spawn subprocesses; auth is the existing dashboard token.** `POST /api/admin/token` validates against RH `/user/` before persisting (atomic write with `tempfile + os.replace`, `chmod 0o600`). `POST /api/admin/run` only accepts hardcoded script names (`ADMIN_SCRIPTS` whitelist in [server.py](server.py)) and rejects 409 if any job is currently running. Job state lives in an in-memory dict on the FastAPI module, protected by a threading lock — single-worker uvicorn assumed (matches `vps/rh-trade-exporter.service`). If uvicorn restarts mid-run, the subprocess is orphaned and the job is lost from the table; the cron backstop covers worst-case data freshness. JWT decode for `token-status` is unverified (`base64` only, no signature check) — the token was already issued by RH and we just want the `exp` claim; the authority on validity is RH itself via the optional `?probe=true` flag.

## Robinhood API Endpoints (Undocumented)

All reverse-engineered from browser network traffic and robin-stocks source:

| Endpoint | Purpose | Notes |
|---|---|---|
| `GET /accounts/` | List accounts | Broken for cash sub-accounts |
| `GET /options/orders/` | Paginated options orders | Supports `account_numbers`, `updated_at[gte]`, `state=filled`, `chain_symbol` |
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

Trade #, Date, Day, Account, Symbol, Expiry Date, Type, Strike, Qty, Asset Open/High/Low/Close, VWAP, 8 EMA, Entry Time, Exit Time, Hold Time (min), Entry Hour, Entry Cost, Risk ($), Exit Credit, P/L ($), Cumulative P/L ($), P/L (%), Win/Loss, Is Win, VIX, Delta, Group ID, DTE

## Data Source Limitations

- **VWAP / 8 EMA**: RH 5-min bars only go back ~5 trading days (`span=week`). yfinance 5-min gap-fills up to ~60 days. Older trades get `N/A`.
- **Delta**: Point-in-time only. Useful for same-day exports before expiry; null for expired contracts. Sticky-column merge preserves previously-captured values.
- **VIX**: Still from yfinance (`^VIX` not on RH).
- **Daily OHLC**: RH historicals (`span=year`), then yfinance daily, then synthesized from intraday 5-min bars for today's session.

## Known Issues

- FIFO pairing breaks if user re-opens the same exact contract after closing it same day
- RH `/accounts/` API does not return cash sub-accounts — must use `--account-numbers` manually on first run (cached after)
- Rounding to int dollars can lose precision on small trades

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

- **Dedup key is (Date, Entry Time, Strike, Type, Qty)** — NOT Group ID. Format-independent.
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
| `GET /api/trades?symbol=SPY` | `spy_trades.csv` + `other_trades.csv` | Trade list as JSON array |
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
| `POST /api/admin/run` | Body `{script: ...}` | Spawns the named script via `subprocess.Popen`, stdout+stderr → `outputs/.admin_jobs/{job_id}.log`. 409 if a job is already running. Whitelist: `hood`, `cash_flow`, `spy_daily`, `spy_intraday`, `spy_intraday_back` (spy_intraday --backfill), `option_intraday`, `token_watch`, `daily_refresh` (chained: hood → cash_flow → spy_intraday → spy_daily → option_intraday, abort on first failure). Watcher thread flips state to `done`/`failed` on exit. `_prune_old_job_logs()` runs on every spawn, keeping only the 50 most recent `*.log` files by mtime (`JOBS_LOG_KEEP` in server.py). |
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
- **Dashboard**: `vps/rh-trade-exporter.service` runs uvicorn on `127.0.0.1:8000`, nginx reverse proxy with HTTPS
- **Cron**: `vps/run.sh` runs daily at 4:05 PM ET (weekdays only) — runs both `hood.py` and `cash_flow.py`
- **Log rotation**: `vps/logrotate.conf` → `/etc/logrotate.d/rh-trade-exporter` (weekly, 4 weeks retained)
- **CI**: GitHub Actions runs `pytest tests/` on push to main and on PRs
- **Alerting**: Discord webhook + email failsafe on hood.py failure

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
