"""Default fetch fingerprint (chrome145), DL_IMPERSONATE override, h2-reset
fallback to the NEXT family, per-domain pins, Playwright UAs (2026-11-12,
decision 10). 19 test items (15 functions, one parametrized x5). Destined for tests/test_fingerprint.py. Mocks only; the one
subprocess test imports V6 with an env var and makes no network call.
"""
import os
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

SCRAPERS = Path(__file__).resolve().parent.parent / "scrapers"
sys.path.insert(0, str(SCRAPERS))
import dealledger_scraper_v6 as v6  # noqa: E402


class FakeCurlError(Exception):
    def __init__(self, msg, code):
        super().__init__(msg)
        self.code = code


H2_RESET = "Failed to perform, curl: (92) HTTP/2 stream 1 reset by server (error 0x1 PROTOCOL_ERROR)."


class Resp:
    def __init__(self, status=200, text="<html>ok</html>"):
        self.status_code = status
        self.text = text


@pytest.fixture
def fetcher(monkeypatch):
    monkeypatch.setattr(v6, "HAS_CURL_CFFI", True)
    monkeypatch.setattr(v6.time, "sleep", lambda *_: None)
    f = v6.PageFetcher()
    f.session = mock.Mock()
    monkeypatch.setattr(f, "_rewarm", lambda *a, **k: None)
    return f


def _imps(session):
    return [c.kwargs.get("impersonate") for c in session.get.call_args_list]


# ── default + env override ─────────────────────────────────────────────────

def test_default_is_chrome145():
    assert v6.DEFAULT_IMPERSONATE == "chrome145"


@pytest.mark.skipif(not v6.HAS_CURL_CFFI, reason="curl_cffi not installed")
def test_default_and_fallback_are_known_to_installed_curl_cffi():
    assert v6._known_impersonate(v6.DEFAULT_IMPERSONATE)
    assert v6._known_impersonate(v6._reset_fallback_for(v6.DEFAULT_IMPERSONATE))
    assert v6._known_impersonate("chrome131")          # the documented rollback value


def test_resolve_without_env_uses_default():
    assert v6._resolve_impersonate("") == ("chrome145", "default")


def test_resolve_valid_env_wins(monkeypatch):
    monkeypatch.setattr(v6, "_known_impersonate", lambda n: n == "chrome131")
    assert v6._resolve_impersonate("chrome131") == ("chrome131", "DL_IMPERSONATE")


def test_resolve_invalid_env_is_loud_and_uses_default(monkeypatch, capsys):
    monkeypatch.setattr(v6, "_known_impersonate", lambda n: False)
    imp, src = v6._resolve_impersonate("chrome999")
    assert imp == "chrome145" and "invalid" in src
    assert "DL_IMPERSONATE='chrome999'" in capsys.readouterr().out


def test_new_session_uses_resolved_target(monkeypatch):
    sess = mock.Mock()
    monkeypatch.setattr(v6, "HAS_CURL_CFFI", True)
    monkeypatch.setattr(v6, "IMPERSONATE", "safari2601")
    monkeypatch.setattr(v6, "cffi_requests", mock.Mock(Session=sess), raising=False)
    v6.PageFetcher._new_session()
    sess.assert_called_once_with(impersonate="safari2601")


@pytest.mark.skipif(not v6.HAS_CURL_CFFI, reason="curl_cffi not installed")
def test_env_override_applies_at_import():
    env = dict(os.environ, DL_IMPERSONATE="chrome131",
               SUPABASE_SERVICE_KEY="", SUPABASE_SERVICE_ROLE_KEY="")
    code = ("import dealledger_scraper_v6 as v; "
            "print('RESULT', v.IMPERSONATE, v.IMPERSONATE_SOURCE, "
            "v.PageFetcher.RESET_FALLBACK_IMPERSONATE)")
    out = subprocess.run([sys.executable, "-c", code], env=env, cwd=str(SCRAPERS),
                         capture_output=True, text=True, timeout=120).stdout
    line = [l for l in out.splitlines() if l.startswith("RESULT")][-1]
    assert line == "RESULT chrome131 DL_IMPERSONATE safari2601"


# ── fallback goes to the next family ──────────────────────────────────────

@pytest.mark.parametrize("primary,fallback", [
    ("chrome145", "safari2601"), ("chrome131", "safari2601"), ("edge101", "safari2601"),
    ("safari2601", "chrome145"), ("firefox147", "chrome145"),
])
def test_fallback_is_a_different_family(primary, fallback):
    assert v6._reset_fallback_for(primary) == fallback
    assert v6._reset_fallback_for(primary)[:5] != primary[:5]


def test_class_fallback_differs_from_default():
    assert v6.PageFetcher.RESET_FALLBACK_IMPERSONATE != v6.IMPERSONATE


def test_reset_retries_once_with_next_family(fetcher):
    fetcher.session.get.side_effect = [FakeCurlError(H2_RESET, 92), Resp(200, "BODY")]
    assert fetcher._fetch_http("https://laundromat-broker.com/listings.html") == "BODY"
    assert _imps(fetcher.session) == [None, v6._reset_fallback_for(v6.IMPERSONATE)]


# ── per-domain pin (regression narrowing) ─────────────────────────────────

def test_pinned_domain_sends_pin_and_others_do_not(fetcher, monkeypatch):
    monkeypatch.setattr(v6, "IMPERSONATE_DOMAIN_OVERRIDES", {"pinned.example": "chrome131"})
    fetcher.session.get.side_effect = [Resp(200, "A"), Resp(200, "B"), Resp(200, "C")]
    fetcher._fetch_http("https://pinned.example/listings")
    fetcher._fetch_http("https://www.pinned.example/listings")      # www stripped by _bare
    fetcher._fetch_http("https://other.example/listings")
    assert _imps(fetcher.session) == ["chrome131", "chrome131", None]


def test_pinned_domain_reset_falls_back_from_the_pin(fetcher, monkeypatch):
    monkeypatch.setattr(v6, "IMPERSONATE_DOMAIN_OVERRIDES", {"pinned.example": "safari2601"})
    fetcher.session.get.side_effect = [FakeCurlError(H2_RESET, 92), Resp(200, "OK"), Resp(200, "P2")]
    assert fetcher._fetch_http("https://pinned.example/l") == "OK"
    assert fetcher._fetch_http("https://pinned.example/l?p=2") == "P2"
    assert _imps(fetcher.session) == ["safari2601", "chrome145", "chrome145"]


# ── visibility + Playwright UAs ───────────────────────────────────────────

def test_banner_names_fingerprint_source_and_fallback(monkeypatch):
    monkeypatch.setattr(v6, "HAS_CURL_CFFI", True)
    b = v6.fingerprint_banner()
    assert v6.IMPERSONATE in b and v6.IMPERSONATE_SOURCE in b
    assert v6._reset_fallback_for(v6.IMPERSONATE) in b
    assert "\n" not in b


def test_banner_without_curl_cffi(monkeypatch):
    monkeypatch.setattr(v6, "HAS_CURL_CFFI", False)
    assert "plain requests" in v6.fingerprint_banner()


def test_user_agents_current_and_same_shape():
    uas = v6.USER_AGENTS
    assert len(uas) == 5
    assert not any("Chrome/120" in u or "Firefox/121" in u or "Version/17" in u for u in uas)
    chrome = [u for u in uas if "Chrome/" in u]
    assert len(chrome) == 3 and all("Chrome/145.0.0.0" in u for u in chrome)
    assert any("Macintosh" in u for u in chrome) and any("Windows" in u for u in chrome) \
        and any("Linux" in u for u in chrome)
    assert any("Version/26" in u and "Safari/605" in u for u in uas)
    assert any("Firefox/147.0" in u for u in uas)
