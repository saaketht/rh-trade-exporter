#!/usr/bin/env bash
# Cron wrapper — runs hood.py and sends alerts on failure.
# Usage: crontab -e → 5 16 * * 1-5 /path/to/rh-trade-exporter/vps/run.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIR="$(dirname "$SCRIPT_DIR")"
cd "$DIR"

LOG="$DIR/cron.log"
DISCORD_WEBHOOK="${DISCORD_WEBHOOK_URL:-}"
EMAIL="${ALERT_EMAIL:-}"

echo "===== $(date '+%Y-%m-%d %H:%M:%S') =====" >> "$LOG"

# Run the export
if .venv/bin/python hood.py --after-date "$(date -d '-1 day' +%Y-%m-%d)" --symbol SPY >> "$LOG" 2>&1; then
    echo "✅ hood.py success" >> "$LOG"
else
    EXIT_CODE=$?
    TAIL=$(tail -20 "$LOG")

    # Discord webhook
    if [ -n "$DISCORD_WEBHOOK" ]; then
        MSG="⚠️ **hood.py failed** (exit $EXIT_CODE)\n\`\`\`\n${TAIL:0:1800}\n\`\`\`"
        curl -s -H "Content-Type: application/json" \
            -d "{\"content\": \"$MSG\"}" \
            "$DISCORD_WEBHOOK" > /dev/null 2>&1 || true
    fi

    # Email failsafe
    if [ -n "$EMAIL" ]; then
        echo "$TAIL" | mail -s "hood.py failed (exit $EXIT_CODE)" "$EMAIL" 2>/dev/null || true
    fi

    exit $EXIT_CODE
fi

# Run cash flow snapshot
if .venv/bin/python cash_flow.py --json >> "$LOG" 2>&1; then
    echo "✅ cash_flow.py success" >> "$LOG"
else
    echo "⚠️ cash_flow.py failed (non-fatal)" >> "$LOG"
fi

exit 0
