"""Tests for token_watch.py — JWT exp decode, status evaluation, alert de-duplication, delivery."""

import base64
import json

import pytest

import token_watch as tw


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Any real HTTP call from these tests is a bug (it would use the real token)."""
    def refuse(*a, **k):
        raise AssertionError("test attempted a real network call")
    monkeypatch.setattr(tw.requests, "get", refuse)
    monkeypatch.setattr(tw.requests, "post", refuse)

NOW = 1_790_000_000.0   # fixed "now" (late Sep 2026)
H = 3600


def jwt(exp=None):
    enc = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()
    payload = {"exp": exp} if exp is not None else {}
    return f"{enc({'alg': 'RS256'})}.{enc(payload)}.sig"


class TestTokenBasics:
    def test_decode_exp(self):
        assert tw.decode_exp(jwt(int(NOW))) == int(NOW)

    def test_decode_exp_garbage(self):
        assert tw.decode_exp("not-a-jwt") is None
        assert tw.decode_exp(jwt()) is None

    def test_read_token_strips_bearer(self, tmp_path):
        p = tmp_path / ".rh_token"
        p.write_text("Bearer abc.def.ghi\n")
        assert tw.read_token(p) == "abc.def.ghi"
        assert tw.read_token(tmp_path / "missing") is None

    def test_fingerprint_is_stable_and_short(self):
        assert tw.fingerprint("x") == tw.fingerprint("x") and len(tw.fingerprint("x")) == 12
        assert tw.fingerprint(None) is None


class TestProbe:
    class Resp:
        def __init__(self, code):
            self.status_code = code

    class Session:
        def __init__(self, code=None, exc=None):
            self.code, self.exc = code, exc

        def get(self, *a, **k):
            if self.exc:
                raise self.exc
            return TestProbe.Resp(self.code)

    def test_ok(self):
        assert tw.probe("t", self.Session(200)) == ("ok", 200)

    def test_rejected(self):
        assert tw.probe("t", self.Session(401)) == ("rejected", 401)

    def test_network_error(self):
        assert tw.probe("t", self.Session(exc=tw.requests.ConnectionError())) == ("error", None)


class TestEvaluate:
    def test_missing(self):
        assert tw.evaluate(None, NOW, None)["status"] == "missing"

    def test_rejected_wins_even_with_future_exp(self):
        cur = tw.evaluate(jwt(int(NOW + 48 * H)), NOW, ("rejected", 401))
        assert cur["status"] == "rejected"
        assert "revoked early" in cur["detail"]

    def test_expired(self):
        assert tw.evaluate(jwt(int(NOW - H)), NOW, ("ok", 200))["status"] == "expired"

    def test_expiring_within_window(self):
        cur = tw.evaluate(jwt(int(NOW + 5 * H)), NOW, ("ok", 200), warn_hours=24)
        assert cur["status"] == "expiring" and cur["hours_left"] == 5.0

    def test_ok(self):
        assert tw.evaluate(jwt(int(NOW + 60 * H)), NOW, ("ok", 200))["status"] == "ok"

    def test_network_error_is_unknown_not_alarm(self):
        assert tw.evaluate(jwt(int(NOW + 60 * H)), NOW, ("error", None))["status"] == "unknown"

    def test_no_probe_falls_back_to_exp(self):
        assert tw.evaluate(jwt(int(NOW + 60 * H)), NOW, None)["status"] == "ok"


class TestDecide:
    def cur(self, status):
        return {"status": status}

    def test_first_run_ok_is_quiet(self):
        assert tw.decide(None, self.cur("ok"), "fp", NOW) is None

    def test_first_run_bad_alerts(self):
        assert tw.decide(None, self.cur("rejected"), "fp", NOW) == "alert"

    def test_same_bad_status_suppressed_until_repeat(self):
        prev = {"status": "rejected", "fingerprint": "fp", "last_alert_at": NOW - 2 * H}
        assert tw.decide(prev, self.cur("rejected"), "fp", NOW, repeat_hours=12) is None
        prev["last_alert_at"] = NOW - 13 * H
        assert tw.decide(prev, self.cur("rejected"), "fp", NOW, repeat_hours=12) == "alert"

    def test_undelivered_bad_alert_retries(self):
        prev = {"status": "rejected", "fingerprint": "fp", "last_alert_at": None}
        assert tw.decide(prev, self.cur("rejected"), "fp", NOW) == "alert"

    def test_bad_status_change_alerts(self):
        prev = {"status": "expired", "fingerprint": "fp", "last_alert_at": NOW - H}
        assert tw.decide(prev, self.cur("rejected"), "fp", NOW) == "alert"

    def test_expiring_alerts_once_per_token(self):
        assert tw.decide({"status": "ok", "fingerprint": "fp"}, self.cur("expiring"), "fp", NOW) == "alert"
        prev = {"status": "expiring", "fingerprint": "fp", "last_alert_at": NOW - 20 * H}
        assert tw.decide(prev, self.cur("expiring"), "fp", NOW) is None
        assert tw.decide(prev, self.cur("expiring"), "new-fp", NOW) == "alert"

    def test_recovery_after_bad(self):
        prev = {"status": "rejected", "fingerprint": "old", "last_alert_at": NOW - H}
        assert tw.decide(prev, self.cur("ok"), "new", NOW) == "recovered"

    def test_recovery_after_expiring_needs_new_token(self):
        prev = {"status": "expiring", "fingerprint": "fp", "last_alert_at": NOW - H}
        assert tw.decide(prev, self.cur("ok"), "fp", NOW) is None
        assert tw.decide(prev, self.cur("ok"), "new", NOW) == "recovered"

    def test_unknown_is_quiet(self):
        assert tw.decide({"status": "ok", "fingerprint": "fp"}, self.cur("unknown"), "fp", NOW) is None


class TestMessagesAndDelivery:
    def test_bad_message_includes_fix(self):
        subj, body = tw.message("alert", {"status": "rejected", "detail": "Robinhood rejected it (HTTP 401)",
                                          "exp": None, "hours_left": None}, NOW)
        assert "rejected" in subj and ".rh_token" in body

    def test_status_line_icon(self):
        line = tw.status_line({"status": "ok", "detail": "accepted by Robinhood", "exp": int(NOW + 60 * H)}, NOW)
        assert line.startswith("🟢 RH token OK")

    def test_env_value_prefers_environment_then_file(self, tmp_path, monkeypatch):
        env = tmp_path / ".env"
        env.write_text('ALERT_EMAIL="me@example.com"\nOTHER=1\n')
        monkeypatch.delenv("ALERT_EMAIL", raising=False)
        assert tw.env_value("ALERT_EMAIL", env) == "me@example.com"
        monkeypatch.setenv("ALERT_EMAIL", "env@example.com")
        assert tw.env_value("ALERT_EMAIL", env) == "env@example.com"
        assert tw.env_value("MISSING_KEY", env) == ""

    def test_notify_discord_and_email(self):
        posts, runs = [], []

        class S:
            def post(self, url, json=None, timeout=None):
                posts.append((url, json))
                return type("R", (), {"status_code": 204})()

        sent = tw.notify("subj", "body", "https://hook", "me@x.com", session=S(),
                         run=lambda cmd, **kw: runs.append((cmd, kw["input"])))
        assert sent == ["discord", "email"]
        assert posts == [("https://hook", {"content": "body"})]
        assert runs == [(["mail", "-s", "subj", "me@x.com"], "body")]

    def test_notify_nothing_configured(self):
        assert tw.notify("s", "b", "", "") == []


class TestMain:
    @pytest.fixture
    def paths(self, tmp_path, monkeypatch):
        monkeypatch.setattr(tw, "TOKEN_FILE", tmp_path / ".rh_token")
        monkeypatch.setattr(tw, "STATE_FILE", tmp_path / "outputs" / ".token_watch.json")
        monkeypatch.setattr(tw, "STATUS_FILE", tmp_path / "outputs" / ".token_status")
        monkeypatch.setattr(tw, "ENV_FILE", tmp_path / ".env")
        monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
        monkeypatch.delenv("ALERT_EMAIL", raising=False)
        return tmp_path

    def test_rejected_token_alerts_once_then_waits(self, paths, monkeypatch):
        (paths / ".rh_token").write_text(jwt(int(9e9)))
        (paths / ".env").write_text("DISCORD_WEBHOOK_URL=https://hook\n")
        monkeypatch.setattr(tw, "probe", lambda t: ("rejected", 401))
        sent = []
        monkeypatch.setattr(tw, "notify", lambda s, b, w, e: sent.append(s) or ["discord"])
        tw.main([])
        tw.main([])
        assert sent == ["RH token rejected"]
        assert (paths / "outputs" / ".token_status").read_text().startswith("🔴 RH token REJECTED")
        assert json.loads((paths / "outputs" / ".token_watch.json").read_text())["status"] == "rejected"

    def test_dry_run_writes_nothing(self, paths, monkeypatch):
        monkeypatch.setattr(tw, "probe", lambda t: ("rejected", 401))
        tw.main(["--dry-run"])
        assert not (paths / "outputs").exists()

    def test_banner_is_read_only_and_never_alerts(self, paths, monkeypatch, capsys):
        (paths / ".rh_token").write_text(jwt(int(9e9)))
        timeouts = []
        monkeypatch.setattr(tw, "probe", lambda t, timeout=15: timeouts.append(timeout) or ("rejected", 401))
        monkeypatch.setattr(tw, "notify", lambda *a: pytest.fail("banner must not alert"))
        assert tw.main(["--banner"]) == 0
        out = capsys.readouterr().out.splitlines()
        assert out[0].startswith("🔴 RH token REJECTED") and out[1].startswith("  Fix: ")
        assert timeouts == [4]
        assert not (paths / "outputs").exists()

    def test_banner_ok_has_no_fix_line(self, paths, monkeypatch, capsys):
        (paths / ".rh_token").write_text(jwt(int(9e9)))
        monkeypatch.setattr(tw, "probe", lambda t, timeout=15: ("ok", 200))
        tw.main(["--banner"])
        out = capsys.readouterr().out.splitlines()
        assert len(out) == 1 and out[0].startswith("🟢 RH token OK")

    def test_test_alert(self, paths, monkeypatch, capsys):
        (paths / ".env").write_text("DISCORD_WEBHOOK_URL=https://hook\n")
        got = []
        monkeypatch.setattr(tw, "notify", lambda s, b, w, e: got.append((s, w)) or ["discord"])
        assert tw.main(["--test-alert"]) == 0
        assert got == [("RH token monitor test", "https://hook")]
        assert "discord" in capsys.readouterr().out

    def test_test_alert_unconfigured_exits_1(self, paths, monkeypatch):
        assert tw.main(["--test-alert"]) == 1

