"""One-time migration: repair Group IDs in existing trade CSVs.

Two kinds of bad IDs exist in history (hood.py::assign_group_ids prevents both
going forward):

1. "G{n}" counter IDs — written by hood.py before stable IDs (40d3172, Apr 24
   2026). They change every run, so later runs re-added the same trades under
   stable IDs and the G rows stayed as duplicates (gener: 9 rows, +$288).
   A G row with an exact twin is dropped; one without is renamed to the stable
   ID derived from its Date + Entry Time + Strike + Type.
2. Collisions — the stable ID has no expiry, so legs of a calendar spread
   (same strike/type/second, different expiries) share one ID and even one
   (Group ID, Exit Time) merge key. The nearest expiry keeps the plain ID and
   the others get "-YYYYMMDD", the same rule hood.py now applies on ingest.

journal_notes.json keys follow the renames. unmatched_opens.csv is rebuilt on
every hood.py run, so it isn't touched.

Dry-run by default (prints what would change, writes nothing).
Pass --apply to back up to *.prefixids.bak and overwrite.

  python fix_group_ids.py                 # dry run on outputs/
  python fix_group_ids.py --apply
"""
import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

import hood

TRADE_FILES = ("spy_trades.csv", "other_trades.csv")
# A G row duplicates a stable row only if the trade itself is identical.
TWIN_COLS = ["Date", "Symbol", "Expiry Date", "Type", "Strike", "Qty",
             "Entry Time", "Exit Time", "Entry Cost", "Exit Credit", "P/L ($)"]
G_ID = r"^G\d+$"


def _key(row) -> tuple:
    return tuple(str(row[c]) for c in TWIN_COLS)


def derived_id(row) -> str | None:
    """Stable ID from CSV columns; None if the row has no usable entry time."""
    try:
        et = datetime.strptime(f"{pd.to_datetime(row['Date']).date()} {row['Entry Time']}",
                               "%Y-%m-%d %H:%M:%S").replace(tzinfo=hood.ET)
    except (ValueError, TypeError):
        return None
    return hood.make_group_id(et, row["Strike"], row["Type"])


def fix_fallback_ids(df: pd.DataFrame, log: list) -> tuple[pd.DataFrame, dict]:
    """Drop G rows that have a twin, rename the rest. Returns (df, {old_id: new_id})."""
    is_g = df["Group ID"].astype(str).str.match(G_ID)
    twins = {_key(r): r["Group ID"] for _, r in df[~is_g].iterrows()}
    renames, drop = {}, []
    for idx, row in df[is_g].iterrows():
        twin = twins.get(_key(row))
        if twin:
            drop.append(idx)
            renames[row["Group ID"]] = twin
            log.append(f"  drop  {row['Group ID']:>5}  {row['Date']} {row['Strike']:g}{row['Type'][0]} "
                       f"{row['Entry Time']}→{row['Exit Time']}  P/L {row['P/L ($)']:+}  (twin {twin})")
            continue
        new = derived_id(row)
        if new is None:
            log.append(f"  KEEP  {row['Group ID']:>5}  {row['Date']} — no entry time to derive an ID from")
            continue
        df.at[idx, "Group ID"] = new
        renames[row["Group ID"]] = new
        log.append(f"  rename {row['Group ID']} → {new}")
    return df.drop(index=drop), renames


def fix_collisions(df: pd.DataFrame, log: list) -> tuple[pd.DataFrame, dict]:
    """Suffix the later legs of IDs shared across expiries/symbols. Returns (df, renames)."""
    renames = {}
    for gid, grp in df.groupby("Group ID"):
        legs = grp[["Expiry Date", "Symbol"]].drop_duplicates()
        if len(legs) < 2:
            continue
        legs = legs.assign(_exp=pd.to_datetime(legs["Expiry Date"])).sort_values(["_exp", "Symbol"])
        first = legs.iloc[0]
        for _, leg in legs.iloc[1:].iterrows():
            suffix = (hood.expiry_suffix(leg["Expiry Date"]) if leg["_exp"] != first["_exp"]
                      else f"-{leg['Symbol']}")
            new = gid + suffix
            mask = ((df["Group ID"] == gid) & (df["Expiry Date"] == leg["Expiry Date"])
                    & (df["Symbol"] == leg["Symbol"]))
            df.loc[mask, "Group ID"] = new
            renames.setdefault(gid, []).append(new)
            log.append(f"  split {gid}: {leg['Symbol']} exp {leg['Expiry Date']} ({mask.sum()} rows) → {new}")
    return df, renames


def move_notes(notes: dict, fallback_renames: dict, log: list) -> dict:
    """Re-key notes for renamed/dropped G IDs. Collision renames keep the note on
    the plain ID (the nearest-expiry leg), so they need no move."""
    for old, new in fallback_renames.items():
        if old not in notes:
            continue
        if new in notes:
            log.append(f"  note conflict: both {old} and {new} have notes — keeping both, merge by hand")
            continue
        notes[new] = notes.pop(old)
        log.append(f"  note  {old} → {new}")
    return notes


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", default=str(hood.SCRIPT_DIR / "outputs"), help="outputs directory")
    ap.add_argument("--apply", action="store_true", help="back up and write (default: dry run)")
    args = ap.parse_args()
    out = Path(args.dir)

    all_fallback, changed = {}, {}
    for name in TRADE_FILES:
        path = out / name
        if not path.exists():
            continue
        df = pd.read_csv(path)
        log: list[str] = []
        before_rows, before_pl = len(df), df["P/L ($)"].sum()
        df, fallback = fix_fallback_ids(df, log)
        df, _ = fix_collisions(df, log)
        all_fallback.update(fallback)
        print(f"📄 {name}: {before_rows} → {len(df)} rows, P/L ${before_pl:,.0f} → ${df['P/L ($)'].sum():,.0f}")
        print("\n".join(log) if log else "  nothing to fix")
        frag = int(df.duplicated(["Group ID", "Exit Time"]).sum())
        if frag:
            print(f"  ℹ {frag} rows share a (Group ID, Exit Time) with another row: order fragments from "
                  f"before order_id aggregation. Totals are right, row counts aren't — regroup_trades.py fixes that.")
        if log:
            changed[path] = hood.finalize_trade_df(df.reset_index(drop=True))

    notes_path = out / "journal_notes.json"
    notes = None
    if notes_path.exists() and all_fallback:
        nlog: list[str] = []
        notes = move_notes(json.loads(notes_path.read_text()), all_fallback, nlog)
        print("📝 journal_notes.json:\n" + ("\n".join(nlog) if nlog else "  no notes on renamed IDs"))
        if not nlog:
            notes = None

    if not changed:
        print("\n✅ Nothing to fix.")
        return
    if not args.apply:
        print("\nDry run — nothing written. Re-run with --apply to back up and write.")
        return
    for path, df in changed.items():
        shutil.copy2(path, path.with_name(path.name + ".prefixids.bak"))
        df.to_csv(path, index=False)
        print(f"💾 wrote {path.name} (backup: {path.name}.prefixids.bak)")
    if notes is not None:
        shutil.copy2(notes_path, notes_path.with_name(notes_path.name + ".prefixids.bak"))
        notes_path.write_text(json.dumps(notes, indent=2))
        print("💾 wrote journal_notes.json (backup: journal_notes.json.prefixids.bak)")


if __name__ == "__main__":
    main()
