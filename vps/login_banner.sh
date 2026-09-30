#!/usr/bin/env bash
# Prints the Robinhood token status at SSH login.
# Live check first (token_watch.py --banner: asks RH, never alerts, never touches alert state),
# capped at 6s; falls back to the status the hourly cron last wrote.
# Install once on gener — append to ~/.bashrc:
#   [[ $- == *i* ]] && [ -f ~/rh-trade-exporter/vps/login_banner.sh ] && bash ~/rh-trade-exporter/vps/login_banner.sh
DIR="$(cd "$(dirname "$0")/.." && pwd)"
PY="$DIR/.venv/bin/python"

if [ -x "$PY" ] && [ -f "$DIR/token_watch.py" ]; then
    if command -v timeout >/dev/null 2>&1; then
        out="$(cd "$DIR" && timeout 6 "$PY" token_watch.py --banner 2>/dev/null)"
    else
        out="$(cd "$DIR" && "$PY" token_watch.py --banner 2>/dev/null)"
    fi
    if [ -n "$out" ]; then
        echo "$out"
        exit 0
    fi
fi

# Fallback: last status written by the hourly cron.
F="$DIR/outputs/.token_status"
if [ ! -f "$F" ]; then
    echo "RH token: live check failed and no cached status yet (is the token_watch cron running?)"
    exit 0
fi
echo "$(cat "$F")  (cached — live check failed)"
age=$(( $(date +%s) - $(stat -c %Y "$F" 2>/dev/null || stat -f %m "$F") ))
if [ "$age" -gt 7200 ]; then
    echo "  (cached status is $((age / 3600))h old — is the token_watch cron running?)"
fi
case "$(head -c 4 "$F")" in
    "🔴"*|"🟡"*) echo "  Fix: cd ~/rh-trade-exporter && read -rs T && printf '%s\n' \"\${T#Bearer }\" > .rh_token && chmod 600 .rh_token" ;;
esac
