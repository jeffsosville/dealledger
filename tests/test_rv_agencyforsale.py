"""Tests for scrapers/rv_agencyforsale.py (2026-11-24, rv-agencyforsale).
Must-fail: Premium read as price, sold rows read active, "Allstate Agency" titles.
Must-pass: unsold price, pending label, pagination, existing-id stability."""
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
import rv_agencyforsale as a  # noqa: E402
import rv_cards_a  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "rv")
UNSOLD_URL = "https://agencyforsale.com/listings?search%5Bstatus%5D=unsold"


def _read(n):
    with open(os.path.join(FIX, n), encoding="utf-8") as fh:
        return fh.read()


def _row(base_url, name, title=None, detail_url=None, asking_price=None, cash_flow=None,
         revenue=None, city=None, state=None, description=None):
    return {"id": detail_url, "title": title, "asking_price": asking_price, "cash_flow": cash_flow,
            "revenue": revenue, "state": state, "url": detail_url or base_url, "description": description}


CTX = SimpleNamespace(make_row=_row, norm_url=lambda u: u,
                      state_from_location=lambda loc: "WA" if "Washington" in (loc or "") else None)


class Fetcher:
    def __init__(self):
        self.calls = []

    def _fetch_http(self, url, **kw):
        self.calls.append(url)
        if "/listings/" in url and "?" not in url:
            return _read("afs_detail.html")
        if "status%5D=unsold" in url:
            return _read("afs_unsold_p3.html") if "page=2" in url else _read("afs_unsold_p1.html")
        if "status%5D=pending" in url:
            return _read("afs_pending_p1.html")
        if "status%5D=sold" in url:
            return _read("afs_sold_p1.html") if "page=1" in url else "<html></html>"
        raise AssertionError(url)


def _run(monkeypatch):
    monkeypatch.setattr(a, "PAUSE", 0)
    f = Fetcher()
    rows, drops = rv_cards_a.read_index(UNSOLD_URL, _read("afs_unsold_p1.html"), "afs", f, CTX)
    return rows, f


def test_scope_is_the_list_page_only():
    assert a.in_scope(UNSOLD_URL) and a.in_scope("https://agencyforsale.com/listings")
    assert not a.in_scope("https://agencyforsale.com/listings/7616aa0a-a9a7-4200-81a1-f1a37040ff2b")


def test_card_price_is_listing_price_not_premium():
    cards = a.parse_cards(_read("afs_unsold_p1.html"))
    assert len(cards) == 25
    c = cards[0]
    assert c["price_text"] == "$999,000" and c["premium"] == "$4,745,504"   # the old bug: price 4,745,504
    assert a.parse_total(_read("afs_unsold_p1.html")) == (25, 66)
    assert a.parse_total(_read("afs_pending_p1.html")) == (3, 3)
    assert a.parse_total(_read("afs_unsold_p3.html")) == (66, 66)


def test_rows_status_price_revenue_and_ids(monkeypatch):
    rows, f = _run(monkeypatch)
    by = {r["url"]: r for r in rows}
    unsold = [r for r in rows if r["status"] == "active" and not r.get("broker_status_text")]
    pending = [r for r in rows if r.get("broker_status_text") == "Sale Pending"]
    sold = [r for r in rows if r["status"] == "sold"]
    assert (len(unsold), len(pending)) == (41, 3)          # 25 + 16 across two pages, 3 pending
    assert len(sold) == 21                                  # 25 cards, the site repeats 4 -> 21 distinct
    assert all(r["status"] == "active" for r in pending)    # pending stays active with the label
    assert all(r["broker_status_text"] == "Sold!" and r["asking_price"] is None for r in sold)
    assert all(r["revenue"] is None for r in rows)          # premium is not revenue
    assert all(r["asking_price"] for r in unsold)
    ever = by["https://agencyforsale.com/listings/7616aa0a-a9a7-4200-81a1-f1a37040ff2b"]
    assert ever["asking_price"] == 999_000 and ever["title"] == "ELITE Allstate Agency in Everett, WA"
    assert "Written premium: $4,745,504." in ever["description"]
    assert "Annual revenue: $643,605." in ever["description"]
    assert ever["state"] == "WA"
    # identity is the listing's own URL
    assert all(r["url"].startswith("https://agencyforsale.com/listings/") for r in rows)
    assert len({r["url"] for r in rows}) == len(rows)


def test_consultant_recorded_including_none(monkeypatch):
    rows, _ = _run(monkeypatch)
    assert any("Consultant: none named." in r["description"] for r in rows)
    assert any("Consultant: Justin Slocum." in r["description"] for r in rows)


def test_request_volume_is_bounded(monkeypatch):
    _, f = _run(monkeypatch)
    lists = [u for u in f.calls if "?" in u]
    assert len(lists) <= 3 * a.MAX_PAGES
    assert len(lists) == 4                  # unsold p2 (p1 reused), pending p1, sold p1 + p2 (empty ends it)


def test_most_final_status_wins():
    found = {}
    card = {"href": "/listings/x"}
    for st in ("unsold", "sold", "pending"):
        prev = found.get("x")
        if prev is None or a._RANK[st] > a._RANK[prev]:
            found["x"] = st
    assert found["x"] == "sold"
