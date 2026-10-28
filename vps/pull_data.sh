#!/usr/bin/env bash
# One-way data pull: gener (source of truth) → this machine's outputs/.
# Never writes to the VPS. Local files that would be overwritten are moved to
# outputs/.prepull/<timestamp>/ first, and nothing local is deleted.
#
# The two cash-flow logs are merged, not replaced: local dashboard refreshes
# add their own snapshots, so cash_flow.jsonl is unioned by timestamp and
# cash_flow_events.jsonl by event id (gener's copy wins on a conflict).
#
# Usage: vps/pull_data.sh            # pull
#        vps/pull_data.sh --dry-run  # show what would change
# Override the host / remote repo path with RH_VPS / RH_VPS_DIR.

set -euo pipefail

HOST="${RH_VPS:-gener}"
REMOTE_DIR="${RH_VPS_DIR:-rh-trade-exporter}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
STAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP="$DIR/outputs/.prepull/$STAMP"
PY="$DIR/.venv/bin/python"

DRY=()
[ "${1:-}" = "--dry-run" ] && DRY=(-n)

# Excluded: backups, raw dumps (large, machine-specific), admin job logs, the
# VPS's own token-monitor state, journal notes (edited wherever the dashboard
# runs), and the cash-flow logs (merged below).
# ${DRY[@]+...}: macOS bash 3.2 treats an empty array as unbound under `set -u`.
rsync -a -i ${DRY[@]+"${DRY[@]}"} -b --backup-dir="$BACKUP" \
    --exclude='*.bak' --exclude='rh_raw_orders*.json' --exclude='.admin_jobs/' \
    --exclude='.token_watch.json' --exclude='.token_status' --exclude='.prepull/' \
    --exclude='journal_notes.json' --exclude='cash_flow*.jsonl' \
    "$HOST:$REMOTE_DIR/outputs/" "$DIR/outputs/"

INCOMING="$(mktemp -d)"
trap 'rm -rf "$INCOMING"' EXIT
rsync -a "$HOST:$REMOTE_DIR/outputs/cash_flow.jsonl" "$HOST:$REMOTE_DIR/outputs/cash_flow_events.jsonl" "$INCOMING/"

"$PY" - "$DIR" "$INCOMING" "$BACKUP" "${#DRY[@]}" <<'EOF'
import json, shutil, sys
from pathlib import Path
root, incoming, backup, dry = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4] != "0"
sys.path.insert(0, str(root))
import cash_flow

def lines(p):
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()] if p.exists() else []

def keep(p):
    if p.exists():
        backup.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, backup / p.name)

snap_local, snap_in = root / "outputs/cash_flow.jsonl", incoming / "cash_flow.jsonl"
have = {r["timestamp"]: r for r in lines(snap_local)}
new = [r for r in lines(snap_in) if r["timestamp"] not in have]
print(f"cash_flow.jsonl: {len(have)} local + {len(new)} from gener")
if new and not dry:
    keep(snap_local)
    have.update({r["timestamp"]: r for r in new})
    snap_local.write_text("".join(json.dumps(have[t]) + "\n" for t in sorted(have)))

ev_local, ev_in = root / "outputs/cash_flow_events.jsonl", incoming / "cash_flow_events.jsonl"
local_ids, gener_ev = {r.get("id") for r in lines(ev_local)}, lines(ev_in)
print(f"cash_flow_events.jsonl: {len(local_ids)} local, {sum(e.get('id') not in local_ids for e in gener_ev)} new from gener")
if gener_ev and not dry:
    keep(ev_local)
    cash_flow.merge_events_to_jsonl(gener_ev, ev_local)
EOF

if [ ${#DRY[@]} -eq 0 ]; then
    echo "✅ pulled $HOST:$REMOTE_DIR/outputs → outputs/ (replaced files kept in outputs/.prepull/$STAMP)"
fi
