"""crawl_run.active_seen / sold_seen (b-active-sold, 2026-11-25).

listings_seen for specialized brokers counts every row upserted, sold and
inactive included. These pin: the split is computed the way upsert dedupes,
the columns are sent when present, and a missing column never blocks a close.
No network, no DB.
"""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("SCRAPERS", os.path.join(HERE, "..", "scrapers")))

import crawl_run_log as crl  # noqa: E402
import run_specialized as rs  # noqa: E402


class Resp:
    def __init__(self, status=200, body=None, text=""):
        self.status_code, self._body, self.text = status, body, text
        self.ok = 200 <= status < 300

    def json(self):
        return self._body


def L(path, status=None, host="example.com"):
    d = {"listing_url": f"https://{host}/{path}"}
    if status is not None:
        d["status"] = status
    return d


def test_status_breakdown_mixed():
    listings = [L("a"), L("b", "active"), L("c", "sold"), L("d", "sold"),
                L("e", "inactive"), L("a"),                 # dup of a
                L("f", "DROP"), {"title": "no url"},        # never written
                L("g", "sold", host="other.com")]
    tot, per = rs.status_breakdown(listings)
    assert tot == {"active": 2, "sold": 3, "other": 1}
    assert per == {"example.com": (2, 2), "other.com": (0, 1)}


def test_status_breakdown_empty():
    assert rs.status_breakdown([]) == ({"active": 0, "sold": 0, "other": 0}, {})


@pytest.fixture
def key(monkeypatch):
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test")
    monkeypatch.setattr(crl, "_truncation_check", lambda *a: ("ok", None))


def test_finish_run_sends_columns(monkeypatch, key):
    calls = []
    monkeypatch.setattr(crl.requests, "patch",
                        lambda url, headers=None, params=None, json=None, timeout=None:
                        calls.append(json) or Resp(204))
    crl.finish_run(1, "ok", 423, 423, active_seen=51, sold_seen=372)
    assert len(calls) == 1
    assert calls[0]["active_seen"] == 51 and calls[0]["sold_seen"] == 372
    assert calls[0]["listings_seen"] == 423


def test_finish_run_unchanged_without_split(monkeypatch, key):
    calls = []
    monkeypatch.setattr(crl.requests, "patch",
                        lambda url, headers=None, params=None, json=None, timeout=None:
                        calls.append(json) or Resp(204))
    crl.finish_run(1, "ok", 10, 10)
    assert "active_seen" not in calls[0] and "sold_seen" not in calls[0]


def test_finish_run_retries_without_split_keeps_displayed(monkeypatch, key, caplog):
    calls = []

    def patch(url, headers=None, params=None, json=None, timeout=None):
        calls.append(json)
        return Resp(400, text="PGRST204") if "active_seen" in json else Resp(204)

    monkeypatch.setattr(crl.requests, "patch", patch)
    with caplog.at_level("WARNING"):
        crl.finish_run(1, "ok", 78, 78, displayed_count=6, displayed_source="stated",
                       compared_count=6, active_seen=6, sold_seen=72)
    assert len(calls) == 2
    assert "active_seen" not in calls[1] and calls[1]["broker_displayed_count"] == 6
    assert calls[1]["status"] == "ok" and calls[1]["listings_seen"] == 78
    assert "retrying without them" in caplog.text


def test_finish_run_all_columns_missing_still_closes(monkeypatch, key):
    calls = []

    def patch(url, headers=None, params=None, json=None, timeout=None):
        calls.append(json)
        bad = "active_seen" in json or "broker_displayed_count" in json
        return Resp(400, text="x") if bad else Resp(204)

    monkeypatch.setattr(crl.requests, "patch", patch)
    crl.finish_run(1, "ok", 78, 78, displayed_count=6, displayed_source="stated",
                   active_seen=6, sold_seen=72)
    assert len(calls) == 3
    assert set(calls[2]) == {"status", "finished_at", "urls_fetched", "listings_seen", "error"}


def _post_writer(monkeypatch, reject_split):
    posts = []
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test")

    def post(url, headers=None, json=None, timeout=None, **k):
        posts.append(json)
        if reject_split and "active_seen" in json:
            return Resp(400, text="PGRST204")
        return Resp(201, [{"id": len(posts)}])

    monkeypatch.setattr(crl.requests, "post", post)
    monkeypatch.setattr(crl.requests, "get", lambda *a, **k: Resp(200, []))
    return posts


def test_domain_rows_carry_split(monkeypatch):
    posts = _post_writer(monkeypatch, reject_split=False)
    ids, failed = crl.record_domain_runs(
        "specialized", "reg.com", {"a.com": 5, "www.b.com": 3, "reg.com": 9},
        status_counts={"a.com": (4, 1), "B.com": (3, 0), "reg.com": (9, 0)})
    assert failed == [] and len(posts) == 2
    by = {p["broker_domain"]: p for p in posts}
    assert (by["a.com"]["active_seen"], by["a.com"]["sold_seen"]) == (4, 1)
    assert (by["b.com"]["active_seen"], by["b.com"]["sold_seen"]) == (3, 0)


def test_domain_row_retries_without_split(monkeypatch):
    posts = _post_writer(monkeypatch, reject_split=True)
    ids, failed = crl.record_domain_runs("specialized", "reg.com", {"a.com": 5},
                                         status_counts={"a.com": (4, 1)})
    assert failed == [] and ids == {"a.com": 2}
    assert "active_seen" in posts[0] and "active_seen" not in posts[1]


def test_domain_row_without_status_counts_unchanged(monkeypatch):
    posts = _post_writer(monkeypatch, reject_split=False)
    crl.record_domain_runs("specialized", "reg.com", {"a.com": 5})
    assert "active_seen" not in posts[0]
