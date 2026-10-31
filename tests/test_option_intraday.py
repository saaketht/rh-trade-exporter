"""Tests for option_intraday.py — contract selection, bar parsing, merge, capture flow."""

import csv
import json
from datetime import date, datetime, timezone

import pytest

import option_intraday as oi
from option_intraday import ET


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Any real HTTP call from these tests is a bug (it would use the real token)."""
    def refuse(*a, **k):
        raise AssertionError("test attempted a real network call")
    monkeypatch.setattr(oi.requests, "get", refuse)
    monkeypatch.setattr(oi.requests, "post", refuse)


def write_csv(path, headers, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)


TRADE_HEADERS = ["Date", "Symbol", "Expiry Date", "Type", "Strike", "Entry Time", "Hold Time (min)"]


def trade(d, exp, typ, strike, time="10:00:00", hold=10, sym="SPY"):
    return {"Date": d, "Symbol": sym, "Expiry Date": exp, "Type": typ, "Strike": strike,
            "Entry Time": time, "Hold Time (min)": hold}


def ts(day: str, hm: str) -> int:
    return int(datetime.fromisoformat(f"{day}T{hm}:00").replace(tzinfo=ET).timestamp())


def rh_point(day, hm, price, interpolated=False):
    iso = datetime.fromtimestamp(ts(day, hm), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    p = {"begins_at": iso, "open_price": str(price), "high_price": str(price + 0.1),
         "low_price": str(price - 0.1), "close_price": str(price), "session": "reg"}
    if interpolated:
        p["interpolated"] = True
    return p


KEY_763C = ("SPY", "2026-09-29", "call", 763.0)


# ── contract selection ─────────────────────────

class TestContractsByDate:
    def test_0dte_trade_lands_on_its_day(self, tmp_path):
        write_csv(tmp_path / "spy_trades.csv", TRADE_HEADERS, [trade("9/29/2026", "9/29/2026", "Call", "763.0")])
        held = oi.contracts_by_date(tmp_path, date(2026, 9, 30))
        assert held == {date(2026, 9, 29): {KEY_763C}}

    def test_overnight_hold_spans_both_days(self, tmp_path):
        # Entered Tue 15:40, held 1097 min → closed Wed morning.
        write_csv(tmp_path / "spy_trades.csv", TRADE_HEADERS,
                  [trade("9/8/2026", "9/9/2026", "Put", "762.0", time="15:40:26", hold=1097)])
        held = oi.contracts_by_date(tmp_path, date(2026, 9, 30))
        assert set(held) == {date(2026, 9, 8), date(2026, 9, 9)}

    def test_open_position_held_through_expiry_skips_weekend(self, tmp_path):
        write_csv(tmp_path / "unmatched_opens.csv", ["Date", "Symbol", "Type", "Strike", "Expiry"],
                  [{"Date": "9/25/2026", "Symbol": "SPY", "Type": "Call", "Strike": "770", "Expiry": "2026-09-29"}])
        held = oi.contracts_by_date(tmp_path, date(2026, 9, 30))
        assert sorted(held) == [date(2026, 9, 25), date(2026, 9, 28), date(2026, 9, 29)]

    def test_open_position_capped_at_today(self, tmp_path):
        write_csv(tmp_path / "unmatched_opens.csv", ["Date", "Symbol", "Type", "Strike", "Expiry"],
                  [{"Date": "9/29/2026", "Symbol": "SPY", "Type": "Call", "Strike": "770", "Expiry": "2026-10-16"}])
        held = oi.contracts_by_date(tmp_path, date(2026, 9, 30))
        assert max(held) == date(2026, 9, 30)

    def test_other_symbols_included(self, tmp_path):
        write_csv(tmp_path / "other_trades.csv", TRADE_HEADERS,
                  [trade("9/29/2026", "10/17/2026", "Call", "55", sym="HIMS")])
        held = oi.contracts_by_date(tmp_path, date(2026, 9, 30))
        assert held[date(2026, 9, 29)] == {("HIMS", "2026-10-17", "call", 55.0)}


class TestInstrumentIndex:
    def test_maps_cache_entries(self, tmp_path):
        p = tmp_path / "cache.json"
        p.write_text(json.dumps({
            "https://api.robinhood.com/options/instruments/abc/": {
                "id": "abc", "chain_symbol": "SPY", "expiration_date": "2026-09-29",
                "type": "call", "strike_price": "763.0000"},
            "https://api.robinhood.com/options/instruments/noid/": {
                "chain_symbol": "SPY", "expiration_date": "2026-09-29", "type": "put", "strike_price": "760.0000"},
        }))
        idx = oi.instrument_index(p)
        assert idx[KEY_763C] == "abc"
        assert idx[("SPY", "2026-09-29", "put", 760.0)] == "noid"   # id falls back to the URL tail

    def test_missing_or_corrupt_cache(self, tmp_path):
        assert oi.instrument_index(tmp_path / "nope.json") == {}
        (tmp_path / "bad.json").write_text("{not json")
        assert oi.instrument_index(tmp_path / "bad.json") == {}


# ── bar parsing ─────────────────────────────────

class TestNormalizeBars:
    def test_drops_interpolated_and_sorts(self):
        payload = {"data_points": [rh_point("2026-09-29", "10:05", 1.2),
                                   rh_point("2026-09-29", "10:00", 1.0),
                                   rh_point("2026-09-29", "10:10", 1.2, interpolated=True)]}
        bars = oi.normalize_bars(payload)
        assert [b["o"] for b in bars] == [1.0, 1.2]
        assert bars[0]["t"] == ts("2026-09-29", "10:00")

    def test_accepts_results_wrapper_and_bars_key(self):
        payload = {"results": [{"bars": [rh_point("2026-09-29", "10:00", 1.0)]}]}
        assert len(oi.normalize_bars(payload)) == 1

    def test_bars_on_splits_by_eastern_day(self):
        bars = [{"t": ts("2026-09-28", "15:55")}, {"t": ts("2026-09-29", "09:30")}]
        assert oi.bars_on(bars, date(2026, 9, 29)) == [bars[1]]


class TestFetchBars:
    class Resp:
        def __init__(self, code, body=None):
            self.status_code, self._body, self.headers = code, body or {}, {}

        def json(self):
            return self._body

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(self.status_code)

    class Session:
        def __init__(self, resp):
            self.resp, self.calls = resp, []

        def get(self, url, **kw):
            self.calls.append((url, kw))
            return self.resp

    def test_auth_error_raises(self):
        with pytest.raises(oi.AuthError):
            oi.fetch_bars("abc", "tok", session=self.Session(self.Resp(401)))

    def test_requests_5minute_regular_session(self):
        s = self.Session(self.Resp(200, {"data_points": [rh_point("2026-09-29", "10:00", 1.0)]}))
        bars = oi.fetch_bars("abc", "tok", "5minute", "week", session=s)
        url, kw = s.calls[0]
        assert url.endswith("/marketdata/options/historicals/abc/")
        assert kw["params"] == {"interval": "5minute", "span": "week", "bounds": "regular"}
        assert kw["headers"]["Authorization"] == "Bearer tok"
        assert len(bars) == 1

    def test_404_is_empty_not_error(self):
        assert oi.fetch_bars("abc", "tok", session=self.Session(self.Resp(404))) == []


# ── merge + targets + run ───────────────────────

class TestMergeAndTargets:
    def test_merge_never_downgrades(self):
        old = [{"instrument_id": "a", "symbol": "SPY", "expiry": "2026-09-29", "type": "call", "strike": 763.0,
                "bars": [1, 2, 3]}]
        new = [{"instrument_id": "a", "symbol": "SPY", "expiry": "2026-09-29", "type": "call", "strike": 763.0,
                "bars": [1]}]
        assert oi.merge_contracts(old, new)[0]["bars"] == [1, 2, 3]

    def test_targets_skip_complete_days_but_always_include_today(self, tmp_path):
        held = {date(2026, 9, 28): {KEY_763C}, date(2026, 9, 29): {KEY_763C}, date(2026, 9, 30): {KEY_763C}}
        (tmp_path / "2026-09-28.json").write_text(json.dumps({"contracts": [
            {"instrument_id": "a", "symbol": "SPY", "expiry": "2026-09-29", "type": "call", "strike": 763.0, "bars": [1]}]}))
        got = oi.pick_targets(held, date(2026, 9, 30), tmp_path)
        assert got == [date(2026, 9, 29), date(2026, 9, 30)]

    def test_targets_ignore_days_older_than_catchup_window(self, tmp_path):
        held = {date(2026, 9, 1): {KEY_763C}}
        assert oi.pick_targets(held, date(2026, 9, 30), tmp_path) == []

    def test_unfetchable_contracts_dont_keep_a_day_open(self, tmp_path):
        held = {date(2026, 9, 29): {KEY_763C}}
        assert oi.pick_targets(held, date(2026, 9, 30), tmp_path, index={}) == []

    def test_force_date(self, tmp_path):
        held = {date(2026, 9, 1): {KEY_763C}}
        assert oi.pick_targets(held, date(2026, 9, 30), tmp_path, force=date(2026, 9, 1)) == [date(2026, 9, 1)]


class TestRun:
    def test_writes_day_file_with_bars_and_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(oi, "REQUEST_PAUSE", 0)
        other = ("SPY", "2026-09-29", "put", 760.0)
        held = {date(2026, 9, 29): {KEY_763C, other}}
        index = {KEY_763C: "abc"}   # the put has no instrument id
        bars = [{"t": ts("2026-09-29", "10:00"), "o": 1, "h": 1, "l": 1, "c": 1},
                {"t": ts("2026-09-28", "15:55"), "o": 9, "h": 9, "l": 9, "c": 9}]   # other day: excluded
        calls = []

        def fake_fetch(iid, token, interval, span):
            calls.append((iid, interval, span))
            return bars

        summary = oi.run([date(2026, 9, 29)], held, index, "tok", capture_dir=tmp_path,
                         today=date(2026, 9, 30), log=lambda *_: None, fetch=fake_fetch)
        doc = json.loads((tmp_path / "2026-09-29.json").read_text())
        assert calls == [("abc", "5minute", "week")]
        assert [c["instrument_id"] for c in doc["contracts"]] == ["abc"]
        assert len(doc["contracts"][0]["bars"]) == 1
        assert doc["contracts"][0]["interval"] == "5minute"
        assert doc["missing"] == [{"symbol": "SPY", "type": "put", "strike": 760.0, "expiry": "2026-09-29",
                                   "reason": "no instrument id in cache"}]
        assert summary["dates"]["2026-09-29"] == {"contracts": 1, "one_minute": 0, "missing": 1}

    def test_today_captured_at_one_minute(self, tmp_path, monkeypatch):
        monkeypatch.setattr(oi, "REQUEST_PAUSE", 0)
        calls = []
        one_min = [{"t": ts("2026-09-30", "10:00"), "o": 1, "h": 1, "l": 1, "c": 1},
                   {"t": ts("2026-09-30", "10:01"), "o": 1, "h": 1, "l": 1, "c": 1}]
        s = oi.run([date(2026, 9, 30)], {date(2026, 9, 30): {KEY_763C}}, {KEY_763C: "abc"}, "tok",
                   capture_dir=tmp_path, today=date(2026, 9, 30), log=lambda *_: None,
                   fetch=lambda iid, tok, iv, span: calls.append((iv, span)) or one_min)
        assert calls == [("minute", "day")]
        doc = json.loads((tmp_path / "2026-09-30.json").read_text())
        assert doc["contracts"][0]["interval"] == "minute" and len(doc["contracts"][0]["bars"]) == 2
        assert s["dates"]["2026-09-30"]["one_minute"] == 1

    def test_today_falls_back_to_five_minute(self, tmp_path, monkeypatch):
        monkeypatch.setattr(oi, "REQUEST_PAUSE", 0)
        five = [{"t": ts("2026-09-30", "10:00"), "o": 1, "h": 1, "l": 1, "c": 1}]
        calls = []
        oi.run([date(2026, 9, 30)], {date(2026, 9, 30): {KEY_763C}}, {KEY_763C: "abc"}, "tok",
               capture_dir=tmp_path, today=date(2026, 9, 30), log=lambda *_: None,
               fetch=lambda iid, tok, iv, span: calls.append(iv) or ([] if iv == "minute" else five))
        assert calls == ["minute", "5minute"]
        doc = json.loads((tmp_path / "2026-09-30.json").read_text())
        assert doc["contracts"][0]["interval"] == "5minute"

    def test_aged_out_refetch_keeps_earlier_capture(self, tmp_path, monkeypatch):
        monkeypatch.setattr(oi, "REQUEST_PAUSE", 0)
        held = {date(2026, 9, 29): {KEY_763C}}
        good = [{"t": ts("2026-09-29", "10:00"), "o": 1, "h": 1, "l": 1, "c": 1}]
        oi.run([date(2026, 9, 29)], held, {KEY_763C: "abc"}, "tok", capture_dir=tmp_path,
               log=lambda *_: None, fetch=lambda *a: good)
        oi.run([date(2026, 9, 29)], held, {KEY_763C: "abc"}, "tok", capture_dir=tmp_path,
               log=lambda *_: None, fetch=lambda *a: [])
        doc = json.loads((tmp_path / "2026-09-29.json").read_text())
        assert len(doc["contracts"][0]["bars"]) == 1
        assert doc["missing"] == []


class TestMain:
    def test_no_token_exits_2(self, tmp_path, monkeypatch):
        monkeypatch.setattr(oi, "TOKEN_FILE", tmp_path / ".rh_token")
        assert oi.main(["--json"]) == 2

    def test_auth_error_exits_2(self, tmp_path, monkeypatch):
        (tmp_path / ".rh_token").write_text("Bearer tok\n")
        monkeypatch.setattr(oi, "TOKEN_FILE", tmp_path / ".rh_token")
        monkeypatch.setattr(oi, "OUT_DIR", tmp_path)
        monkeypatch.setattr(oi, "INSTRUMENT_CACHE_FILE", tmp_path / "cache.json")
        monkeypatch.setattr(oi, "pick_targets", lambda *a, **k: [date(2026, 9, 29)])

        def rejected(*a, **k):
            raise oi.AuthError("HTTP 401")
        monkeypatch.setattr(oi, "run", rejected)
        assert oi.main(["--json"]) == 2

    def test_nothing_to_do_exits_0(self, tmp_path, monkeypatch, capsys):
        (tmp_path / ".rh_token").write_text("tok\n")
        monkeypatch.setattr(oi, "TOKEN_FILE", tmp_path / ".rh_token")
        monkeypatch.setattr(oi, "OUT_DIR", tmp_path)
        monkeypatch.setattr(oi, "INSTRUMENT_CACHE_FILE", tmp_path / "cache.json")
        assert oi.main(["--json"]) == 0
        assert json.loads(capsys.readouterr().out) == {"dates": {}, "contracts_fetched": 0}


# ── Massive backfill ────────────────────────────

SPY_C = ("SPY", "2026-10-28", "call", 771.0)
SPY_P = ("SPY", "2026-10-28", "put", 765.0)
D = date(2026, 10, 28)


def mbars(n, day="2026-10-28", start="09:30"):
    t0 = ts(day, start)
    return [{"t": t0 + 60 * i, "o": 1.0, "h": 1.1, "l": 0.9, "c": 1.0, "v": 10} for i in range(n)]


def rh_entry(key, n, interval="minute"):
    sym, exp, typ, strike = key
    return {"instrument_id": "rh-" + typ, "symbol": sym, "type": typ, "strike": strike, "expiry": exp,
            "interval": interval, "bars": mbars(n)}


class TestMassive:
    def test_occ_ticker(self):
        assert oi.occ_ticker(SPY_C) == "O:SPY261028C00771000"
        assert oi.occ_ticker(("ET", "2024-01-19", "put", 13.5)) == "O:ET240119P00013500"

    class Resp:
        def __init__(self, status, body=None):
            self.status_code, self._body = status, body or {}

        def json(self):
            return self._body

    class Session:
        def __init__(self, *resps):
            self.resps, self.calls = list(resps), []

        def get(self, url, **kw):
            self.calls.append((url, kw))
            return self.resps.pop(0)

    def test_fetch_parses_ms_and_volume(self):
        t_ms = ts("2026-10-28", "09:30") * 1000
        s = self.Session(self.Resp(200, {"results": [{"t": t_ms, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 42}]}))
        status, bars = oi.fetch_massive("O:SPY261028C00771000", D, "key", session=s)
        assert status == "ok" and bars == [{"t": t_ms // 1000, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 42}]
        url, kw = s.calls[0]
        assert url.endswith("/O:SPY261028C00771000/range/1/minute/2026-10-28/2026-10-28")
        assert kw["params"]["apiKey"] == "key"

    def test_fetch_statuses(self):
        assert oi.fetch_massive("x", D, "k", session=self.Session(self.Resp(200, {"results": []}))) == ("no_data", [])
        assert oi.fetch_massive("x", D, "k", session=self.Session(self.Resp(403))) == ("out_of_plan", [])
        with pytest.raises(oi.AuthError):
            oi.fetch_massive("x", D, "k", session=self.Session(self.Resp(401)))
        with pytest.raises(RuntimeError):
            oi.fetch_massive("x", D, "k", session=self.Session(self.Resp(500)))

    def test_fetch_waits_out_a_429(self):
        slept = []
        s = self.Session(self.Resp(429), self.Resp(200, {"results": []}))
        assert oi.fetch_massive("x", D, "k", session=s, sleep=slept.append) == ("no_data", [])
        assert slept == [60]

    def run_backfill(self, tmp_path, held, answers, **kw):
        calls = []

        def fetch(ticker, d, key):
            calls.append((ticker, d))
            a = answers[ticker]
            if isinstance(a, Exception):
                raise a
            return a
        summary = oi.backfill(held, kw.pop("index", {}), "key", kw.pop("today", date(2026, 10, 31)),
                              massive_dir=tmp_path / "m", capture_dir=tmp_path / "c",
                              log=lambda *_: None, fetch=fetch, sleep=lambda s: None, **kw)
        return summary, calls

    def test_backfill_records_and_resumes(self, tmp_path):
        held = {D: {SPY_C, SPY_P}}
        answers = {"O:SPY261028C00771000": ("ok", mbars(5)), "O:SPY261028P00765000": ("no_data", [])}
        summary, calls = self.run_backfill(tmp_path, held, answers)
        assert len(calls) == 2 and summary["no_data"] == 1 and summary["bars"] == 5
        raw = json.loads((tmp_path / "m" / "2026-10-28.json").read_text())
        assert raw["contracts"][0]["bars"][0]["v"] == 10 and raw["no_data"][0]["type"] == "put"
        cap = json.loads((tmp_path / "c" / "2026-10-28.json").read_text())
        (c,) = cap["contracts"]
        assert c["source"] == "massive" and c["interval"] == "minute" and c["instrument_id"] == "O:SPY261028C00771000"
        _, calls = self.run_backfill(tmp_path, held, answers)
        assert calls == []                      # both answers recorded: nothing asked twice

    def test_errors_and_403_are_retried_next_run(self, tmp_path):
        held = {D: {SPY_C, SPY_P}}
        answers = {"O:SPY261028C00771000": RuntimeError("HTTP 500"), "O:SPY261028P00765000": ("out_of_plan", [])}
        summary, _ = self.run_backfill(tmp_path, held, answers)
        assert summary["errors"] == 1 and summary["out_of_plan"] == 1
        _, calls = self.run_backfill(tmp_path, held, answers)
        assert len(calls) == 2

    def test_window_today_and_limit(self, tmp_path):
        old, today = date(2024, 1, 5), date(2026, 10, 31)
        held = {old: {SPY_C}, D: {SPY_C, SPY_P}, today: {SPY_C}}
        answers = {"O:SPY261028C00771000": ("ok", mbars(1)), "O:SPY261028P00765000": ("ok", mbars(1))}
        _, calls = self.run_backfill(tmp_path, held, answers, limit=1)
        assert calls == [("O:SPY261028C00771000", D)]   # outside 2y and today skipped; limit honored

    def test_merge_keeps_more_bars_and_clears_missing(self, tmp_path):
        (tmp_path / "c").mkdir()
        (tmp_path / "c" / "2026-10-28.json").write_text(json.dumps({
            "date": "2026-10-28", "source": "robinhood",
            "contracts": [rh_entry(SPY_C, 390), rh_entry(SPY_P, 50, "5minute")],
            "missing": [{"symbol": "SPY", "type": "call", "strike": 770.0, "expiry": "2026-10-28", "reason": "x"}]}))
        key770 = ("SPY", "2026-10-28", "call", 770.0)
        held = {D: {SPY_C, SPY_P, key770}}
        answers = {"O:SPY261028C00771000": ("ok", mbars(380)),     # fewer than RH's 1-minute → RH kept
                   "O:SPY261028P00765000": ("ok", mbars(200)),     # more than RH's 5-minute → Massive
                   "O:SPY261028C00770000": ("ok", mbars(30))}      # was missing → now covered
        self.run_backfill(tmp_path, held, answers, index={SPY_P: "rh-put"})
        cap = json.loads((tmp_path / "c" / "2026-10-28.json").read_text())
        by = {(c["type"], c["strike"]): c for c in cap["contracts"]}
        assert by[("call", 771.0)].get("source", "robinhood") == "robinhood" and len(by[("call", 771.0)]["bars"]) == 390
        assert by[("put", 765.0)]["source"] == "massive" and by[("put", 765.0)]["instrument_id"] == "rh-put"
        assert ("call", 770.0) in by and cap["missing"] == []
        assert cap["source"] == "mixed"

    def test_only_regular_session_is_merged(self, tmp_path):
        (tmp_path / "c").mkdir()
        (tmp_path / "c" / "2026-10-28.json").write_text(json.dumps({
            "date": "2026-10-28", "contracts": [rh_entry(SPY_C, 390)], "missing": []}))
        # 120 premarket + 380 session + 14 after the close: more bars in total, fewer in the session
        bars = mbars(120, start="07:30") + mbars(380) + mbars(14, start="16:00")
        self.run_backfill(tmp_path, {D: {SPY_C}}, {"O:SPY261028C00771000": ("ok", bars)})
        cap = json.loads((tmp_path / "c" / "2026-10-28.json").read_text())
        assert len(cap["contracts"][0]["bars"]) == 390 and "source" not in cap["contracts"][0]
        raw = json.loads((tmp_path / "m" / "2026-10-28.json").read_text())
        assert len(raw["contracts"][0]["bars"]) == 514          # raw keeps everything

    def test_main_backfill_needs_key(self, monkeypatch):
        monkeypatch.setattr(oi.spy_intraday, "load_polygon_key", lambda: None)
        assert oi.main(["--backfill", "--json"]) == 2
