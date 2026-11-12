"""run_specialized honours broker_block (2026-11-11). Offline: every DB call
is mocked; nothing reads or writes Supabase. scrapers/ is APPENDED to
sys.path so PYTHONPATH (a patched copy) takes precedence."""
import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import run_specialized as rs  # noqa: E402


@pytest.fixture
def fake_registry(monkeypatch):
    calls = []

    def mk(name):
        def fn():
            calls.append(name)
            return []
        return fn

    reg = {
        "wp_blocked_example_com": {"domain": "blocked-example.com", "account": "1",
                                   "display_name": "Blocked", "wp_rest": True,
                                   "fn": mk("wp_blocked_example_com")},
        "wp_ok_example_com": {"domain": "ok-example.com", "account": "2",
                              "display_name": "Ok", "wp_rest": True,
                              "fn": mk("wp_ok_example_com")},
        "franchise": {"domain": "franchise-example.com", "account": "3",
                      "display_name": "Franchise", "fn": mk("franchise")},
    }
    monkeypatch.setattr(rs, "BROKERS", reg)
    # Any accidental DB call fails the test.
    for f in ("expire_stale_runs", "start_run", "upsert_listings"):
        monkeypatch.setattr(rs, f, lambda *a, **k: pytest.fail("DB call in test"))
    monkeypatch.setattr(rs, "finish_run", lambda *a, **k: None)
    monkeypatch.setattr(rs, "EMPTY_RETRY_WAIT", 0)
    monkeypatch.setattr(rs.time, "sleep", lambda *_: None)
    return calls


def test_blocked_domain_is_never_crawled(fake_registry, monkeypatch):
    monkeypatch.setattr(rs, "load_broker_block", lambda: {"blocked-example.com"})
    rs.run(None, dry_run=True)
    assert "wp_blocked_example_com" not in fake_registry
    assert fake_registry == ["wp_ok_example_com", "franchise"]


def test_block_matches_www_and_subdomains():
    blocked = {"trufortebusinessgroup.com"}
    assert rs.is_block_listed("trufortebusinessgroup.com", blocked)
    assert rs.is_block_listed("www.trufortebusinessgroup.com", blocked)
    assert rs.is_block_listed("listings.trufortebusinessgroup.com", blocked)
    assert not rs.is_block_listed("nottrufortebusinessgroup.com", blocked)
    assert not rs.is_block_listed("", blocked)


def test_unreadable_block_list_skips_wp_rest_and_fails_loud(fake_registry, monkeypatch, caplog):
    monkeypatch.setattr(rs, "SUPABASE_SERVICE_KEY", "test-key-not-real")
    monkeypatch.setattr(rs, "expire_stale_runs", lambda *a, **k: 0)
    monkeypatch.setattr(rs, "start_run", lambda *a, **k: None)
    monkeypatch.setattr(rs, "load_broker_block", lambda: None)
    with pytest.raises(SystemExit) as e:
        rs.run(None, dry_run=False)
    assert e.value.code == 4
    # Neither WP-REST broker ran; the hard-coded franchise scraper still did
    # (it returned 0 listings, so nothing reached upsert_listings).
    assert fake_registry == ["franchise", "franchise"]   # EMPTY retry once
    assert "broker_block NOT read" in caplog.text


def test_load_broker_block_none_without_key(monkeypatch):
    monkeypatch.setattr(rs, "SUPABASE_SERVICE_KEY", "")
    assert rs.load_broker_block() is None


def test_load_broker_block_none_on_http_error(monkeypatch):
    monkeypatch.setattr(rs, "SUPABASE_SERVICE_KEY", "test-key-not-real")

    class R:
        def raise_for_status(self):
            raise RuntimeError("HTTP 401")
    monkeypatch.setattr(rs.http_requests, "get", lambda *a, **k: R())
    assert rs.load_broker_block() is None


def test_load_broker_block_parses_rows(monkeypatch):
    monkeypatch.setattr(rs, "SUPABASE_SERVICE_KEY", "test-key-not-real")

    class R:
        def raise_for_status(self):
            pass

        def json(self):
            return [{"broker_domain": "WWW.TruForteBusinessGroup.com "}, {"broker_domain": None}]
    monkeypatch.setattr(rs.http_requests, "get", lambda *a, **k: R())
    assert rs.load_broker_block() == {"trufortebusinessgroup.com"}
