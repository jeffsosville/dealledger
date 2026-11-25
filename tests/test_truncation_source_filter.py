"""Truncation baseline must compare only crawl_run rows of the same source
(V6 junk counts on office domains must not judge a specialized run)."""
import os
import sys

SCRAPERS = os.environ.get("SCRAPERS") or os.path.join(os.path.dirname(__file__), "..", "scrapers")
sys.path.insert(0, SCRAPERS)
import crawl_run_log as crl  # noqa: E402


class Resp:
    def __init__(self, data, ok=True):
        self._d, self.ok, self.status_code, self.text = data, ok, 200, ""

    def json(self):
        return self._d


def _setup(monkeypatch, run_row, baseline_rows=()):
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-key-not-real")
    calls = []

    def get(url, headers=None, params=None, timeout=None):
        calls.append(dict(params))
        if "id" in params and params["id"].startswith("eq."):
            return Resp(run_row)
        return Resp(list(baseline_rows))
    monkeypatch.setattr(crl.requests, "get", get)
    return calls


def test_recent_ok_counts_filters_by_source(monkeypatch):
    calls = _setup(monkeypatch, [], [{"listings_seen": 2}])
    assert crl._recent_ok_counts("vrsavannah.com", "k", "specialized_vr") == [2]
    assert calls[0]["source"] == "eq.specialized_vr"


def test_recent_ok_counts_no_source_unchanged(monkeypatch):
    calls = _setup(monkeypatch, [])
    crl._recent_ok_counts("x.com", "k")
    assert "source" not in calls[0]


def test_record_domain_run_passes_source_and_ignores_other_source(monkeypatch):
    calls = _setup(monkeypatch, [])           # server returns [] for same-source: no baseline
    posted = []
    monkeypatch.setattr(crl.requests, "post",
                        lambda *a, **k: posted.append(k["json"]) or Resp([{"id": 5}]))
    assert crl.record_domain_run("specialized_vr", "vrsavannah.com", 2) == 5
    assert calls[0]["source"] == "eq.specialized_vr"
    assert posted[0]["status"] == "ok"


def test_truncation_check_filters_by_row_source(monkeypatch):
    calls = _setup(monkeypatch,
                   [{"broker_domain": "vrvgpi.com", "source": "specialized_vr"}],
                   [{"listings_seen": 2}])
    assert crl._truncation_check(9, 2, "k", None) == ("ok", None)
    assert calls[0]["select"] == "broker_domain,source"
    assert calls[1]["source"] == "eq.specialized_vr"


def test_truncation_check_still_flags_same_source_truncation(monkeypatch):
    _setup(monkeypatch, [{"broker_domain": "tworld.com", "source": "specialized_tworld"}],
           [{"listings_seen": 900}])
    status, err = crl._truncation_check(9, 10, "k", None)
    assert status == "partial" and "TRUNCATED" in err
