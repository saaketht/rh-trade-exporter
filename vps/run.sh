#!/usr/bin/env bash
# Cron wrapper — runs hood.py and sends alerts on failure.
# Usage: crontab -e → 5 16 * * 1-5 /home/gener/rh-trade-exporter/run.sh

set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

LOG="$DIR/cron.log"
DISCORD_WEBHOOK="${DISCORD_WEBHOOK_URL:-}"
EMAIL="${ALERT_EMAIL:-}"

echo "===== $(date '+%Y-%m-%d %H:%M:%S') =====" >> "$LOG"

# Run the export
if .venv/bin/python hood.py --after-date "$(date -d '-1 day' +%Y-%m-%d)" --symbol SPY >> "$LOG" 2>&1; then
    echo "✅ Success" >> "$LOG"
    exit 0
fi

EXIT_CODE=$?
TAIL=$(tail -20 "$LOG")

# Discord webhook
if [ -n "$DISCORD_WEBHOOK" ]; then
    # Truncate to Discord's 2000 char limit
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
