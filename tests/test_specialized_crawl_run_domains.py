"""Per-domain crawl_run rows for specialized brokers (J3, 2026-11-12).

Bodner's registry domain is bodnergroup.com but every listing lands on
execbb.com; VR's land on ~30 office domains. v_dom_direct / absence_candidates
join crawl_run on broker_domain, so each domain the rows land on needs its own
closed 'ok' row. No network, no DB: the writer is mocked.
"""
import logging
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("SCRAPERS", os.path.join(HERE, "..", "scrapers")))

import crawl_run_log as crl  # noqa: E402
import run_specialized as rs  # noqa: E402


class Resp:
    def __init__(self, status=201, body=None, text=""):
        self.status_code, self._body, self.text = status, body, text
        self.ok = 200 <= status < 300

    def json(self):
        return self._body


@pytest.fixture
def writer(monkeypatch):
    """Mock crawl_run REST: records POSTed bodies, no prior ok runs."""
    posts = []
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-key-not-real")

    def post(url, headers=None, json=None, timeout=None, **k):
        assert url.endswith("/rest/v1/crawl_run")
        posts.append(json)
        return Resp(201, [{"id": len(posts), **json}])

    monkeypatch.setattr(crl.requests, "post", post)
    monkeypatch.setattr(crl.requests, "get", lambda *a, **k: Resp(200, []))
    monkeypatch.setattr(crl.requests, "patch", lambda *a, **k: pytest.fail("domain rows must not PATCH"))
    return posts


# ── pure ────────────────────────────────────────────────────────────────────
def test_registry_domain_never_counted_twice():
    got = crl.domain_counts_excluding(
        {"bodnergroup.com": 3, "www.BodnerGroup.com": 2, "execbb.com": 10,
         "www.execbb.com": 5, "vrdallas.com": 0, None: 4}, "www.bodnergroup.com")
    assert got == {"execbb.com": 15}


# ── writer ──────────────────────────────────────────────────────────────────
def test_two_domains_two_closed_ok_rows(writer):
    ids, failed = crl.record_domain_runs(
        "specialized", "vrbusinessbrokers.com",
        {"vrdallas.com": 12, "bizbizbiz.com": 30, "vrbusinessbrokers.com": 7})
    assert failed == []
    assert sorted(ids) == ["bizbizbiz.com", "vrdallas.com"]
    by = {p["broker_domain"]: p for p in writer}
    assert set(by) == {"bizbizbiz.com", "vrdallas.com"}          # no registry row
    assert by["bizbizbiz.com"]["listings_seen"] == 30
    assert by["vrdallas.com"]["listings_seen"] == 12
    for p in writer:                                            # written closed, never 'running'
        assert p["status"] == "ok" and p["finished_at"] and p["source"] == "specialized"


def test_truncated_domain_recorded_partial(writer, monkeypatch):
    monkeypatch.setattr(crl.requests, "get",
                        lambda *a, **k: Resp(200, [{"listings_seen": 1085}] * 3))
    ids, failed = crl.record_domain_runs("specialized", "bodnergroup.com", {"execbb.com": 40})
    assert writer[0]["status"] == "partial" and "TRUNCATED" in writer[0]["error"]


def test_refused_insert_is_logged_and_reported(monkeypatch, caplog):
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-key-not-real")
    monkeypatch.setattr(crl.requests, "get", lambda *a, **k: Resp(200, []))
    monkeypatch.setattr(crl.requests, "post",
                        lambda *a, **k: Resp(400, {"message": "x"}, '{"message":"check constraint"}'))
    with caplog.at_level(logging.ERROR):
        ids, failed = crl.record_domain_runs("specialized", "bodnergroup.com", {"execbb.com": 9})
    assert ids == {} and failed == ["execbb.com"]
    assert any("NOT written" in r.message and "execbb.com" in r.message
               and r.levelno == logging.ERROR for r in caplog.records)


def test_exception_is_logged_never_raised(monkeypatch, caplog):
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-key-not-real")
    monkeypatch.setattr(crl.requests, "get", lambda *a, **k: Resp(200, []))

    def boom(*a, **k):
        raise ConnectionError("down")
    monkeypatch.setattr(crl.requests, "post", boom)
    with caplog.at_level(logging.ERROR):
        assert crl.record_domain_runs("specialized", "x.com", {"y.com": 1}) == ({}, ["y.com"])
    assert any("NOT written" in r.message for r in caplog.records)


# ── runner wiring ───────────────────────────────────────────────────────────
LISTINGS = (
    [{"listing_url": f"https://execbb.com/buyer/sub/listing.asp?listingid={i}", "title": f"Biz {i}"}
     for i in range(5)]
    + [{"listing_url": f"https://www.vrdallas.com/l/{i}", "title": f"Shop {i}"} for i in range(3)]
)


@pytest.fixture
def runner(monkeypatch):
    calls = {"start": [], "finish": [], "domain": []}
    monkeypatch.setattr(rs, "BROKERS", {"multi": {
        "domain": "bodnergroup.com", "account": "1", "display_name": "Multi",
        "fn": lambda: list(LISTINGS)}})
    monkeypatch.setattr(rs, "SUPABASE_SERVICE_KEY", "test-key-not-real")
    monkeypatch.setattr(rs, "expire_stale_runs", lambda *a, **k: 0)
    monkeypatch.setattr(rs, "load_broker_block", lambda: set())
    monkeypatch.setattr(rs, "_displayed_count_for", lambda *a, **k: {})
    monkeypatch.setattr(rs, "start_run", lambda s, d: calls["start"].append(d) or 77)
    monkeypatch.setattr(rs, "finish_run", lambda rid, st, **k: calls["finish"].append((rid, st, k)))

    def fake_upsert(listings, display_name=None, domain_counts=None):
        bd = rs.domain_breakdown(listings)
        if domain_counts is not None:
            domain_counts.update(bd)
        return sum(bd.values())
    monkeypatch.setattr(rs, "upsert_listings", fake_upsert)
    monkeypatch.setattr(rs, "record_domain_runs",
                        lambda src, reg, dc: calls["domain"].append((src, reg, dict(dc)))
                        or ({d: 1 for d in rs.domain_counts_excluding(dc, reg)}, []))
    return calls


def test_runner_logs_registry_row_plus_each_domain(runner):
    rs.run(["multi"], dry_run=False)
    assert runner["start"] == ["bodnergroup.com"]
    assert runner["finish"][0][1] == "ok" and runner["finish"][0][2]["listings_seen"] == 8
    (src, reg, dc), = runner["domain"]
    assert src == "specialized" and reg == "bodnergroup.com"
    assert crl.domain_counts_excluding(dc, reg) == {"execbb.com": 5, "vrdallas.com": 3}


def test_dry_run_writes_no_rows(runner, monkeypatch, caplog):
    monkeypatch.setattr(rs, "upsert_listings", lambda *a, **k: pytest.fail("dry run upserted"))
    with caplog.at_level(logging.INFO):
        rs.run(["multi"], dry_run=True)
    assert runner["start"] == [] and runner["domain"] == []
    assert all(rid is None for rid, *_ in runner["finish"])     # finish_run(None) is a no-op
    assert any("Would log crawl_run" in r.message and "execbb.com" in r.message
               for r in caplog.records)


def test_runner_reports_failed_domain_rows(runner, monkeypatch, caplog):
    monkeypatch.setattr(rs, "record_domain_runs", lambda src, reg, dc: ({}, ["execbb.com"]))
    with caplog.at_level(logging.ERROR):
        rs.run(["multi"], dry_run=False)
    assert any("NOT written" in r.message and "multi:execbb.com" in r.message
               for r in caplog.records)


def test_upsert_counts_only_accepted_rows(monkeypatch):
    """domain_counts reflects rows the server accepted, not rows attempted."""
    monkeypatch.setattr(rs, "SUPABASE_SERVICE_KEY", "test-key-not-real")
    monkeypatch.setattr(rs.time, "sleep", lambda *_: None)
    monkeypatch.setattr(rs.http_requests, "get", lambda *a, **k: Resp(200, []))

    def post(url, headers=None, json=None, timeout=None):
        bad = any("vrdallas" in r["url"] and r["url"].endswith("/2") for r in json)
        return Resp(400 if bad else 201, [], "bad row")
    monkeypatch.setattr(rs.http_requests, "post", post)
    dc = {}
    n = rs.upsert_listings(list(LISTINGS), display_name="Multi", domain_counts=dc)
    assert n == 7 and dc == {"execbb.com": 5, "vrdallas.com": 2}
