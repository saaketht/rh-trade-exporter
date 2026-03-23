# Robinhood Options Trade Exporter

Export your Robinhood options trade history to structured CSVs — with execution timestamps to the second, automatic open/close pairing, underlying asset OHLC, and VIX data. Designed for day traders who maintain trading journals and need granular data that Robinhood's UI and PDF confirmations don't provide.

## Why This Exists

Robinhood has no aggregated trade export. You can download individual trade confirmations (one PDF per trading day, no CSV option), but there's no way to get a consolidated history across multiple days. The PDFs also omit execution times beyond the minute, which matters for scalps and 0DTE options.

This script talks directly to Robinhood's API using a session token from your browser — no third-party auth libraries, no stored credentials, no dependencies you can't audit in 5 minutes.

## Output Files

The script produces up to six CSVs in your output directory:

| File | Description |
|---|---|
| `spy_trades.csv` | SPY round-trip trades — each row is one closing execution |
| `other_trades.csv` | Non-SPY round-trip trades (same format) |
| `unmatched_opens.csv` | Open positions with no matching close |
| `cancelled.csv` | Cancelled orders with instrument details |
| `rejected.csv` | Rejected orders |
| `failed.csv` | Failed orders (if any) |

### Trade CSV Columns

| Column | Source | Description |
|---|---|---|
| Trade # | Auto | Sequential row number |
| Date | RH API | Trade date (M/D/YYYY) |
| Day | Derived | Day of week |
| Account | RH API | Account number |
| Symbol | RH API | Underlying ticker |
| Expiry Date | RH API | Option expiration date |
| Type | RH API | Call or Put |
| Strike | RH API | Option strike price |
| Qty | RH API | Number of contracts |
| Asset Open/High/Low/Close | RH API | Underlying daily OHLC (yfinance fallback) |
| VWAP | RH API | Volume-weighted average price at entry (5-min bars, last ~5 days only) |
| 8 EMA | RH API | 8-period EMA at entry (5-min bars, requires ≥40 min after open) |
| Entry Time | RH API | Execution timestamp (to the second), converted to ET |
| Exit Time | RH API | Closing execution timestamp |
| Hold Time (min) | Derived | Minutes between entry and exit |
| Entry Hour | Derived | Hour of entry (ET) |
| Entry Cost | RH API | Premium paid (negative) |
| Risk ($) | Derived | abs(Entry Cost) — capital at risk |
| Exit Credit | RH API | Premium received (positive) |
| P/L ($) | Derived | Profit/loss in dollars |
| Cumulative P/L ($) | Derived | Running total |
| P/L (%) | Derived | Return on entry cost |
| Win/Loss | Derived | WIN, LOSS, or BE |
| Is Win | Derived | 1 or 0 |
| VIX | yfinance | VIX close for trade date |
| Delta | RH API | Option delta at time of export (same-day only, null for expired) |
| Group ID | Derived | Links rows from the same opening position |
| DTE | Derived | Days to expiration at entry |

### Trade Pairing Logic

Each **closing execution** produces its own row. If you open 3 contracts and close 2 then 1, that generates 2 rows sharing the same Group ID, entry time, and per-contract entry cost. Pairing is FIFO per contract.

## Setup

### Requirements

- Python 3.10+
- Three packages (no Robinhood-specific libraries):

```bash
pip install requests yfinance pandas
```

### Getting Your Auth Token

1. Open [robinhood.com](https://robinhood.com) in your browser
2. Open DevTools (`F12`) → **Network** tab
3. Navigate around (e.g., click into your positions or order history)
4. Find any request to `api.robinhood.com`
5. Click it → **Headers** tab → copy the full `Authorization` value

It looks like: `Bearer <random-string>`

The token expires in ~24 hours.

### Token Storage

The script resolves your token in this priority order:

| Priority | Method | How |
|---|---|---|
| 1 | `--token` flag | Pass directly on the command line |
| 2 | `$RH_TOKEN` env var | `export RH_TOKEN='rh\|...'` |
| 3 | `.rh_token` file | Saved with `--save-token` (recommended) |
| 4 | `--token-stdin` | Pipe or paste from clipboard |

**Recommended: save to file on first run, then forget about it.**

```bash
# First time — grab token from DevTools, save it
python hood.py --token "Bearer ..." --save-token

# Every run after — no token needed (reads .rh_token)
python hood.py --start 2026-03-01
```

The token file is created with `chmod 600` (owner-read-only). When it expires (~24 hours), grab a fresh one and run with `--token "..." --save-token` again to overwrite.

**Other ways to pass the token:**

```bash
# Paste from macOS clipboard (never touches shell history or disk)
pbpaste | python hood.py --token-stdin

# Env var (useful for scripting, but goes in shell history unless you use read -s)
read -s RH_TOKEN && export RH_TOKEN
python hood.py --start 2026-03-01

# Direct flag (convenient, but visible in shell history and ps)
python hood.py --token "Bearer ..."
```

### Multiple Accounts (Margin + Cash)

Robinhood's `/accounts/` API endpoint often does not return cash sub-accounts. If you have both a margin and a cash account, the script will only see your margin account by default.

**To fix this, pass both account numbers on your first run:**

```bash
python hood.py --token "Bearer ..." --account-numbers "111111111,222222222"
```

The script caches them to `.rh_accounts.json`, so subsequent runs auto-detect both accounts without the flag.

**To find your cash account number:**
- Check the Robinhood app → Settings → Account Info
- Or open DevTools → Network tab and look for any request URL containing `account_numbers=`

## Usage

### Basic Export (All Accounts, All Dates)

```bash
python hood.py --token "Bearer ..." --save-token
```

### Subsequent Runs (Token Saved)

```bash
python hood.py
python hood.py --start 2026-01-01
```

### First Run with Multiple Accounts

```bash
python hood.py --token "Bearer ..." --save-token --account-numbers "963889571,123456789"
```

### Date Filtering

```bash
# Client-side: fetches all orders, filters output
python hood.py --start 2026-01-01
python hood.py --start 2026-02-01 --end 2026-02-15

# Server-side: only fetches orders updated after this date (faster)
python hood.py --after-date 2026-03-10

# Combine: server-side fetch since March, output only SPY
python hood.py --after-date 2026-03-01 --symbol SPY
```

### Custom Output Directory

```bash
python hood.py --output-dir ./exports
```

### Debug Mode

```bash
python hood.py --dump-raw
```

Saves the raw API response to `rh_raw_orders.json` for inspection.

### All Flags

| Flag | Default | Description |
|---|---|---|
| `--token` | None | Auth token from browser (also reads `$RH_TOKEN` or `.rh_token`) |
| `--token-stdin` | Off | Read token from stdin (e.g. `pbpaste \| python hood.py --token-stdin`) |
| `--save-token` | Off | Save token to `.rh_token` (chmod 600) for future runs |
| `--account-numbers` | Auto-detected | Comma-separated account numbers |
| `--start` | None | Start date filter (YYYY-MM-DD, inclusive, client-side) |
| `--end` | None | End date filter (YYYY-MM-DD, inclusive, client-side) |
| `--after-date` | None | Server-side filter: only orders updated after date (YYYY-MM-DD) |
| `--symbol` | None | Server-side filter: only this underlying (e.g. `SPY`) |
| `--filled-only` | Auto | Server-side filter: only filled orders (auto-enabled unless `--dump-raw`) |
| `--output-dir` | `./outputs/` | Directory for output CSVs |
| `--time-format` | `excel` | `excel` (HH:MM:SS) or `ampm` (H:MM:SS AM/PM) |
| `--dump-raw` | Off | Save raw JSON for debugging (disables server-side state filter) |

## Security

This script does **not** use any third-party Robinhood libraries. Authentication is handled by you — you copy a session token from your browser. The script only makes GET requests (reads data, never writes).

**What the script accesses (all GET, read-only):**
- `GET /accounts/` — list your accounts
- `GET /options/orders/` — your options order history
- `GET /options/instruments/{id}/` — option contract details (strike, expiry, type)
- `GET /options/events/` — exercise/assignment/expiration events
- `GET /marketdata/historicals/{sym}/` — daily and intraday price bars
- `GET /marketdata/options/` — greeks (delta, gamma, etc.)

**Token handling:**
- `--save-token` writes to `.rh_token` with `chmod 600` (owner-read-only)
- `--token-stdin` and `$RH_TOKEN` avoid putting the token in shell history
- The `--token` flag works but is visible in shell history and `ps` output
- The script never sends your token anywhere except `api.robinhood.com`

**Recommendations:**
- Use `--save-token` on first run, then omit `--token` on subsequent runs
- Don't commit `.rh_token` or tokens to version control
- Log out of the browser session after exporting (invalidates the token)
- The token expires in ~24 hours regardless

## Troubleshooting

**"AUTH FAILED — token is expired or invalid"**
Grab a fresh token from your browser. Tokens expire after ~24 hours.

**Only seeing trades from one account**
Use `--account-numbers` to pass both account numbers. See the [Multiple Accounts](#multiple-accounts-margin--cash) section.

**Missing recent trades**
Run with `--dump-raw` and check `rh_raw_orders.json`. Look for orders with unexpected states or missing execution data.

**"⚠ X open contract(s) with no matching close"**
These are positions you opened but haven't closed yet (or closed before the export's date range). They appear in `unmatched_opens.csv`.

**yfinance errors ("possibly delisted")**
Some older tickers may have been delisted or renamed. The script continues — those rows will have empty OHLC columns. Daily OHLC primarily comes from Robinhood's historicals API; yfinance is the fallback.

**VWAP / 8 EMA columns are blank for older trades**
Intraday 5-minute bars from Robinhood only go back ~5 trading days. Trades older than that will have empty VWAP and 8 EMA columns.

**Delta column is blank**
Delta is point-in-time. It only populates for same-day exports run before contracts expire. For 0DTE, run the script before 4pm ET.

## License

MIT