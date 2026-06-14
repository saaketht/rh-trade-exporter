#!/usr/bin/env python3
"""One-time migration: regroup spy_trades.csv / other_trades.csv by order_id.

The legacy CSVs were built by pairing at the *execution* level, so one order
that Robinhood filled in several partial executions fragmented into multiple
rows with distinct Group IDs. hood.py now aggregates by order_id on ingest, so
new data is correct going forward — this script rebuilds the *existing* history.

It reconstructs clean rows from a complete raw-orders dump (parse -> aggregate
-> pair), scopes each output to its existing file's date range, and carries the
expensive point-in-time columns (Asset OHLC, VWAP, 8 EMA, VIX, Delta) forward
from the current CSV by an exact (Date, Strike, Type, Entry Time) match. Those
values are per-day or per-entry-instant and identical across a position's
fragments, so the carry is exact — nothing captured is lost.

Dry-run by default (prints a verification report, writes nothing).
Pass --apply to back up the originals (*.bak) and overwrite.

  python regroup_trades.py --raw outputs/rh_raw_orders_full.json          # dry run
  python regroup_trades.py --raw outputs/rh_raw_orders_full.json --apply  # write
"""
import argparse
import json
import shutil
import sys
from pathlib import Path

import pandas as pd
from zoneinfo import ZoneInfo

import hood

STICKY = ["Asset Open", "Asset High", "Asset Low", "Asset Close",
          "VWAP", "8 EMA", "VIX", "Delta"]
ET = ZoneInfo("America/New_York")


def _et(dt):
    return dt.astimezone(ET) if dt else None


def _fmt_d(ts):
    d = pd.Timestamp(ts)
    return f"{d.month}/{d.day}/{d.year}"


def _fmt_t(dt):
    e = _et(dt)
    return e.strftime("%H:%M:%S") if e else ""


def _old_key(row):
    return (str(row["Date"]), float(row["Strike"]),
            str(row["Type"]).lower()[0], str(row["Entry Time"]))


def _new_key(r):
    return (_fmt_d(r["trade_date"]), float(r["strike_price"]),
            (r["option_type"] or "")[0], _fmt_t(r["entry_dt"]))


def reconstruct(raw_path: Path, headers: dict):
    """Full raw dump -> clean paired rows (all symbols, all dates).

    `headers` lets parse_executions resolve any option instruments missing
    from the local cache (needed for non-SPY contracts); SPY instruments are
    already cached, so SPY reconstruction is identical with or without auth.
    """
    orders = json.loads(raw_path.read_text())
    execs = hood.parse_executions(orders, headers)
    hood.save_instrument_cache()
    execs = hood.aggregate_executions_by_order(execs)
    rows, _ = hood.pair_into_trade_rows(execs)
    return rows


def regroup_file(name: str, want_spy: bool, all_rows: list, out_dir: Path, apply: bool):
    path = out_dir / name
    if not path.exists():
        print(f"  [skip] {name} does not exist")
        return
    old = pd.read_csv(path)
    old_min = pd.to_datetime(old["Date"]).min()
    old_max = pd.to_datetime(old["Date"]).max()

    # filter to this file's symbol set + date scope
    def is_spy(r):
        return (r.get("chain_symbol", "") or "").upper() == "SPY"
    rows = [r for r in all_rows
            if is_spy(r) == want_spy
            and old_min <= pd.Timestamp(r["trade_date"]) <= old_max]

    new_df = hood.build_trade_df(rows, {})  # identity/derived cols; sticky blank

    # carry-forward sticky/market columns from the old CSV
    lut = {}
    for _, orow in old.iterrows():
        lut.setdefault(_old_key(orow), orow)
    misses = 0
    for idx in new_df.index:
        nrow = new_df.loc[idx]
        k = (str(nrow["Date"]), float(nrow["Strike"]),
             str(nrow["Type"]).lower()[0], str(nrow["Entry Time"]))
        m = lut.get(k)
        if m is None:
            misses += 1
            continue
        for col in STICKY:
            v = m[col]
            if pd.notna(v) and str(v).strip().lower() not in ("", "nan"):
                new_df.at[idx, col] = v

    # sort / renumber / cumulative via the existing merge machinery (empty target)
    import tempfile
    tmp = Path(tempfile.mkdtemp()) / name
    final = hood.merge_trade_csv(tmp, new_df)

    # ── verification ──
    old_pl = int(old["P/L ($)"].sum())
    new_pl = int(final["P/L ($)"].sum())
    op = {(_old_key(r)[:3]) for _, r in old.iterrows()}
    npset = {(_old_key(r)[:3]) for _, r in final.iterrows()}
    pos_ok = not (op - npset) and not (npset - op)
    plnote = "rounding" if pos_ok else "POSITIONS DIFFER — not just rounding"
    print(f"\n  {name}: {len(old)} -> {len(final)} rows "
          f"(removed {len(old) - len(final)} fragmentation extras)")
    print(f"    P/L: old=${old_pl}  new=${new_pl}  diff=${new_pl - old_pl} ({plnote})")
    print(f"    positions: missing-from-new={len(op - npset)}  new-only={len(npset - op)}")
    print(f"    carry-forward misses: {misses}")
    # count real values (treat ''/nan as blank, like hood._is_blank)
    def real(series):
        return int(sum(0 if hood._is_blank(v) else 1 for v in series))
    for col in STICKY:
        print(f"      {col:<12} old={real(old[col]):>3}  new={real(final[col]):>3}")

    if misses or (op - npset) or (npset - op):
        print(f"    ⚠ anomaly in {name} — NOT writing this file")
        return

    if apply:
        bak = path.with_suffix(path.suffix + ".prefragfix.bak")
        if not bak.exists():
            shutil.copy(path, bak)
            print(f"    backed up -> {bak.name}")
        final.to_csv(path, index=False)
        print(f"    ✅ wrote {path.name}")
    else:
        print(f"    (dry run — pass --apply to write)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="outputs/rh_raw_orders_full.json",
                    help="complete raw orders dump (all symbols)")
    ap.add_argument("--output-dir", default="outputs")
    ap.add_argument("--apply", action="store_true", help="back up + overwrite (default: dry run)")
    args = ap.parse_args()

    raw_path = Path(args.raw)
    if not raw_path.exists():
        print(f"Raw dump not found: {raw_path}")
        sys.exit(1)
    out_dir = Path(args.output_dir)

    hood.load_instrument_cache()

    class _A:
        token = None; save_token = False; account_numbers = None
    headers = hood.make_headers(hood.resolve_token(_A()))

    print(f"Reconstructing from {raw_path}...")
    all_rows = reconstruct(raw_path, headers)
    print(f"  {len(all_rows)} clean paired rows reconstructed")

    regroup_file("spy_trades.csv", True, all_rows, out_dir, args.apply)
    regroup_file("other_trades.csv", False, all_rows, out_dir, args.apply)


if __name__ == "__main__":
    main()
