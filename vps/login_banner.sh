#!/usr/bin/env bash
# Prints the Robinhood token status at SSH login (written hourly by token_watch.py).
# Install once on gener — append to ~/.bashrc:
#   [[ $- == *i* ]] && [ -f ~/rh-trade-exporter/vps/login_banner.sh ] && bash ~/rh-trade-exporter/vps/login_banner.sh
DIR="$(cd "$(dirname "$0")/.." && pwd)"
F="$DIR/outputs/.token_status"
if [ ! -f "$F" ]; then
    echo "RH token: no status yet — token_watch.py hasn't run (check crontab)."
    exit 0
fi
cat "$F"
age=$(( $(date +%s) - $(stat -c %Y "$F" 2>/dev/null || stat -f %m "$F") ))
if [ "$age" -gt 7200 ]; then
    echo "  (status is $((age / 3600))h old — is the token_watch cron running?)"
fi
case "$(head -c 4 "$F")" in
    "🔴"*|"🟡"*) echo "  Fix: cd ~/rh-trade-exporter && read -rs T && printf '%s\n' \"\${T#Bearer }\" > .rh_token && chmod 600 .rh_token" ;;
esac
