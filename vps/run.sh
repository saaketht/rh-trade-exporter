#!/usr/bin/env bash
# Cron wrapper — runs the nightly chain (daily_refresh.py), alerts if hood fails,
# then runs health_check.py, which posts to Discord only when something is off.
# Usage: crontab -e → 5 16 * * 1-5 /path/to/rh-trade-exporter/vps/run.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIR="$(dirname "$SCRIPT_DIR")"
cd "$DIR"

LOG="$DIR/cron.log"
# Alert settings: crontab env first, then .env (gitignored) as a fallback.
# `|| true`: a missing .env or key must not trip `set -euo pipefail` (grep exits 1/2),
# which would kill the run silently before the log header is written.
env_file_value() { grep -E "^$1=" "$DIR/.env" 2>/dev/null | head -1 | cut -d= -f2- | tr -d "\"'" || true; }
DISCORD_WEBHOOK="${DISCORD_WEBHOOK_URL:-$(env_file_value DISCORD_WEBHOOK_URL)}"
EMAIL="${ALERT_EMAIL:-$(env_file_value ALERT_EMAIL)}"

echo "===== $(date '+%Y-%m-%d %H:%M:%S') =====" >> "$LOG"

# The chain itself lives in daily_refresh.py (shared with the admin panel's
# "Daily refresh" button): hood → option_intraday → cash_flow → spy_daily →
# spy_intraday. hood runs bare, so its incremental cursor refetches any day a
# failed run missed. Exit 0 = all ok, 3 = hood ok but a later step failed,
# anything else = hood failed (fatal).
set +e
.venv/bin/python -u daily_refresh.py --quiet >> "$LOG" 2>&1
RC=$?
set -e

if [ "$RC" -ne 0 ] && [ "$RC" -ne 3 ]; then
    echo "❌ hood.py failed (exit $RC) — skipping the rest of the chain" >> "$LOG"
    TAIL=$(tail -20 "$LOG")

    # Discord webhook
    if [ -n "$DISCORD_WEBHOOK" ]; then
        MSG="⚠️ **hood.py failed** (exit $RC)\n\`\`\`\n${TAIL:0:1800}\n\`\`\`"
        curl -s -H "Content-Type: application/json" \
            -d "{\"content\": \"$MSG\"}" \
            "$DISCORD_WEBHOOK" > /dev/null 2>&1 || true
    fi

    # Email failsafe
    if [ -n "$EMAIL" ]; then
        echo "$TAIL" | mail -s "hood.py failed (exit $RC)" "$EMAIL" 2>/dev/null || true
    fi

    exit $RC
fi

if [ "$RC" -eq 0 ]; then
    echo "✅ refresh: all steps ok" >> "$LOG"
else
    echo "⚠️ refresh: hood ok, a later step failed (see [STEP] lines above)" >> "$LOG"
fi

# Gap days, failed steps, missing option bars → Discord, only when something is off.
.venv/bin/python health_check.py >> "$LOG" 2>&1 || echo "⚠️ health_check.py failed (non-fatal)" >> "$LOG"

exit 0
