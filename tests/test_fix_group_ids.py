"""Tests for fix_group_ids.py — G{n} duplicate removal, renames, collision splits."""

import json
import sys

import pandas as pd

import fix_group_ids as fg
import hood

COLS = ["Trade #", "Date", "Symbol", "Expiry Date", "Type", "Strike", "Qty", "Entry Time",
        "Exit Time", "Entry Cost", "Exit Credit", "P/L ($)", "Cumulative P/L ($)", "Delta", "Group ID"]


def row(gid, date="4/22/2026", strike=702.0, typ="Put", entry="09:34:43", exit_="10:16:08",
        pl=-15, expiry="4/22/2026", symbol="SPY"):
    return {"Trade #": 0, "Date": date, "Symbol": symbol, "Expiry Date": expiry, "Type": typ,
            "Strike": strike, "Qty": 1, "Entry Time": entry, "Exit Time": exit_,
            "Entry Cost": -28, "Exit Credit": -28 + pl, "P/L ($)": pl,
            "Cumulative P/L ($)": 0, "Delta": None, "Group ID": gid}


def write(tmp_path, name, rows):
    pd.DataFrame(rows, columns=COLS).to_csv(tmp_path / name, index=False)


def run(tmp_path, monkeypatch, *flags):
    monkeypatch.setattr(sys, "argv", ["fix_group_ids.py", "--dir", str(tmp_path), *flags])
    fg.main()


def test_drops_g_row_with_exact_twin(tmp_path, monkeypatch):
    write(tmp_path, "spy_trades.csv", [row("G1"), row("2026-04-22-093443-702P"),
                                       row("2026-04-22-103044-710P", strike=710.0, entry="10:30:44", pl=120)])
    run(tmp_path, monkeypatch, "--apply")
    df = pd.read_csv(tmp_path / "spy_trades.csv")
    assert list(df["Group ID"]) == ["2026-04-22-093443-702P", "2026-04-22-103044-710P"]
    assert list(df["Trade #"]) == [1, 2]
    assert list(df["Cumulative P/L ($)"]) == [-15, 105]      # recomputed without the duplicate
    assert (tmp_path / "spy_trades.csv.prefixids.bak").exists()


def test_g_row_differing_in_pl_is_not_a_twin(tmp_path, monkeypatch):
    # Same contract and times but different P/L → a different trade; rename, don't drop.
    write(tmp_path, "spy_trades.csv", [row("G1", pl=-15), row("2026-04-22-093443-702P", pl=-20)])
    run(tmp_path, monkeypatch, "--apply")
    assert len(pd.read_csv(tmp_path / "spy_trades.csv")) == 2


def test_orphan_g_row_renamed_to_derived_id(tmp_path, monkeypatch):
    write(tmp_path, "other_trades.csv", [row("G1", date="3/15/2021", strike=20.0, typ="Call",
                                             entry="12:30:55", expiry="3/19/2021", symbol="ET")])
    run(tmp_path, monkeypatch, "--apply")
    assert list(pd.read_csv(tmp_path / "other_trades.csv")["Group ID"]) == ["2021-03-15-123055-20C"]


def test_derived_id_matches_hood(tmp_path):
    # The migration and the ingest path must produce the same ID for the same trade.
    from datetime import datetime
    r = pd.Series(row("G1", date="12/8/2023", strike=13.5, typ="Call", entry="09:34:21"))
    dt = datetime(2023, 12, 8, 9, 34, 21, tzinfo=hood.ET)
    assert fg.derived_id(r) == hood.make_group_id(dt, 13.5, "call") == "2023-12-08-093421-13.5C"


def test_splits_calendar_spread_collision(tmp_path, monkeypatch):
    gid = "2023-12-08-093421-13.5C"
    write(tmp_path, "other_trades.csv", [
        row(gid, date="12/8/2023", strike=13.5, typ="Call", entry="09:34:21", exit_="10:36:15",
            pl=9, expiry="1/5/2024", symbol="ET"),
        row(gid, date="12/8/2023", strike=13.5, typ="Call", entry="09:34:21", exit_="10:36:15",
            pl=5, expiry="12/29/2023", symbol="ET"),
    ])
    run(tmp_path, monkeypatch, "--apply")
    df = pd.read_csv(tmp_path / "other_trades.csv")
    by_exp = dict(zip(df["Expiry Date"], df["Group ID"]))
    assert by_exp == {"12/29/2023": gid, "1/5/2024": gid + "-20240105"}


def test_two_g_legs_of_a_spread_end_up_distinct(tmp_path, monkeypatch):
    # gener's shape: both calendar legs were G rows → rename to the same base, then split.
    kw = dict(date="12/8/2023", strike=13.5, typ="Call", entry="09:34:21", exit_="10:36:15", symbol="ET")
    write(tmp_path, "other_trades.csv", [row("G15", expiry="12/29/2023", pl=5, **kw),
                                         row("G16", expiry="1/5/2024", pl=9, **kw)])
    run(tmp_path, monkeypatch, "--apply")
    ids = sorted(pd.read_csv(tmp_path / "other_trades.csv")["Group ID"])
    assert ids == ["2023-12-08-093421-13.5C", "2023-12-08-093421-13.5C-20240105"]


def test_dry_run_writes_nothing(tmp_path, monkeypatch, capsys):
    write(tmp_path, "spy_trades.csv", [row("G1"), row("2026-04-22-093443-702P")])
    before = (tmp_path / "spy_trades.csv").read_text()
    run(tmp_path, monkeypatch)
    assert (tmp_path / "spy_trades.csv").read_text() == before
    assert not (tmp_path / "spy_trades.csv.prefixids.bak").exists()
    assert "Dry run" in capsys.readouterr().out


def test_notes_follow_renames(tmp_path, monkeypatch):
    write(tmp_path, "spy_trades.csv", [row("G1"), row("2026-04-22-093443-702P")])
    (tmp_path / "journal_notes.json").write_text(json.dumps({"G1": "chased the open"}))
    run(tmp_path, monkeypatch, "--apply")
    assert json.loads((tmp_path / "journal_notes.json").read_text()) == {
        "2026-04-22-093443-702P": "chased the open"}


def test_note_conflict_keeps_both(tmp_path, monkeypatch):
    write(tmp_path, "spy_trades.csv", [row("G1"), row("2026-04-22-093443-702P")])
    notes = {"G1": "old", "2026-04-22-093443-702P": "new"}
    (tmp_path / "journal_notes.json").write_text(json.dumps(notes))
    run(tmp_path, monkeypatch, "--apply")
    assert json.loads((tmp_path / "journal_notes.json").read_text()) == notes


def test_clean_file_is_untouched(tmp_path, monkeypatch, capsys):
    write(tmp_path, "spy_trades.csv", [row("2026-04-22-093443-702P")])
    run(tmp_path, monkeypatch, "--apply")
    assert not (tmp_path / "spy_trades.csv.prefixids.bak").exists()
    assert "Nothing to fix" in capsys.readouterr().out
