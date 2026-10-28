"""One-time backfill: extend a trade CSV backwards with history it never had.

gener's spy_trades.csv starts 2026-04-22 (its first cron run); local's goes back
to 2025-09-17. To make gener the source of truth, the missing early rows are
rebuilt from a complete raw-orders dump with the current pipeline (order
aggregation, stable Group IDs, fees, cents) — the same reconstruction
regroup_trades.py uses — and the point-in-time columns that can no longer be
fetched (Asset OHLC, VWAP, 8 EMA, VIX, Delta) are carried from a CSV that
captured them, matched by (Date, Strike, Type, Entry Time).

Rows on/after the target's first date are left exactly as they are.

Safety: refuses to write if the rebuilt rows and the sticky source disagree on
which positions exist in the backfill range, or if any rebuilt row finds no
sticky match. Dry-run by default; --apply backs up to *.prebackfill.bak.

  python backfill_history.py --raw dump_filled.json --sticky-from local_spy_trades.csv
  python backfill_history.py --raw dump_filled.json --sticky-from local_spy_trades.csv --apply
"""
import argparse
import shutil
import sys
from datetime import date
from pathlib import Path

import pandas as pd

import hood
import regroup_trades as rg


def backfill(target: pd.DataFrame, rebuilt_rows: list, sticky: pd.DataFrame,
             symbol_is_spy: bool = True) -> tuple[pd.DataFrame, dict]:
    """Return (combined_df, report). Pure — no I/O."""
    before = pd.to_datetime(target["Date"]).min()
    rows = [r for r in rebuilt_rows
            if ((r.get("chain_symbol") or "").upper() == "SPY") == symbol_is_spy
            and pd.Timestamp(r["trade_date"]) < before]
    new = hood.build_trade_df(rows, {})

    sdates = pd.to_datetime(sticky["Date"])
    src = sticky[sdates < before]
    lut = {}
    for _, s in src.iterrows():
        lut.setdefault(rg._old_key(s), s)
    misses = 0
    for idx in new.index:
        k = (str(new.at[idx, "Date"]), float(new.at[idx, "Strike"]),
             str(new.at[idx, "Type"]).lower()[0], str(new.at[idx, "Entry Time"]))
        m = lut.get(k)
        if m is None:
            misses += 1
            continue
        for col in rg.STICKY:
            if col in src.columns and not hood._is_blank(m[col]):
                new.at[idx, col] = m[col]

    pos = lambda df: {rg._old_key(r)[:3] for _, r in df.iterrows()}
    src_pos, new_pos = pos(src), pos(new)
    report = {
        "before": before.date(),
        "rows": len(new), "sticky_rows": len(src),
        "pl_rebuilt": round(float(new["P/L ($)"].sum()), 2) if len(new) else 0.0,
        "pl_sticky": round(float(src["P/L ($)"].sum()), 2) if len(src) else 0.0,
        "missing_from_rebuilt": sorted(src_pos - new_pos), "rebuilt_only": sorted(new_pos - src_pos),
        "misses": misses,
        "sticky_filled": {c: int(sum(not hood._is_blank(v) for v in new[c])) for c in rg.STICKY if c in new},
    }
    report["ok"] = not (report["missing_from_rebuilt"] or report["rebuilt_only"] or misses)

    combined = hood.finalize_trade_df(pd.concat([new, target], ignore_index=True))
    report["dup_keys"] = int(combined.duplicated(["Group ID", "Exit Time"]).sum())
    report["ok"] = report["ok"] and report["dup_keys"] == 0
    return combined, report


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw", required=True, help="complete raw orders dump, filtered to state=filled")
    ap.add_argument("--sticky-from", required=True, help="CSV holding captured OHLC/VWAP/EMA/VIX/Delta")
    ap.add_argument("--output-dir", default="outputs")
    ap.add_argument("--file", default="spy_trades.csv")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    path = Path(args.output_dir) / args.file
    target = pd.read_csv(path)
    sticky = pd.read_csv(args.sticky_from)
    hood.load_instrument_cache()

    class _A:
        token = None; save_token = False; account_numbers = None
    try:
        headers = hood.make_headers(hood.resolve_token(_A()))
    except SystemExit:
        headers = {}   # no token: instruments must come from the cache
    rows = rg.reconstruct(Path(args.raw), headers, date.today())

    combined, rep = backfill(target, rows, sticky, symbol_is_spy=args.file.startswith("spy"))
    print(f"\n📄 {args.file}: adding {rep['rows']} rows before {rep['before']} "
          f"(sticky source has {rep['sticky_rows']} rows there)")
    print(f"   P/L of added rows: rebuilt ${rep['pl_rebuilt']:,.2f} vs sticky source ${rep['pl_sticky']:,.2f}")
    print(f"   positions missing from rebuilt: {len(rep['missing_from_rebuilt'])} {rep['missing_from_rebuilt'][:5]}")
    print(f"   positions only in rebuilt:      {len(rep['rebuilt_only'])} {rep['rebuilt_only'][:5]}")
    print(f"   rows with no sticky match: {rep['misses']} | duplicate (Group ID, Exit Time): {rep['dup_keys']}")
    print("   sticky values filled: " + ", ".join(f"{k} {v}" for k, v in rep["sticky_filled"].items()))
    print(f"   result: {len(target)} → {len(combined)} rows, "
          f"P/L ${target['P/L ($)'].sum():,.2f} → ${combined['P/L ($)'].sum():,.2f}")

    if not rep["ok"]:
        print("\n⚠ checks failed — NOT writing.")
        sys.exit(1)
    if not args.apply:
        print("\nDry run — nothing written. Re-run with --apply to back up and write.")
        return
    bak = path.with_name(path.name + ".prebackfill.bak")
    shutil.copy2(path, bak)
    combined.to_csv(path, index=False)
    print(f"\n💾 wrote {path} (backup: {bak.name})")


if __name__ == "__main__":
    main()
