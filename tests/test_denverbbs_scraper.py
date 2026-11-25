import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

from denverbbs_scraper import parse_records, row_url, detail_url  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures")


def _read(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return f.read()


# ---------- denverbbs ----------

def _denver():
    return parse_records(json.loads(_read("denverbbs_crm.json")))


def _by(rows, prefix):
    return next(r for r in rows if r["title"].startswith(prefix))


def test_denver_statuses_and_labels():
    rows, st = _denver()
    assert _by(rows, "Welding and Fab")["status"] == "active"
    assert _by(rows, "Welding and Fab")["broker_status_text"] == "under LOI"
    assert _by(rows, "Downtown Denver Indian")["broker_status_text"] == "under-contract"
    assert _by(rows, "Brewpub")["status"] == "active"
    assert "broker_status_text" not in _by(rows, "Brewpub")
    sold = _by(rows, "Liquor License Renewal")
    assert sold["status"] == "sold" and sold["broker_status_text"] == "sold"
    assert st["unknown_status"] == {}


def test_denver_property_only_is_skipped_and_reported_not_lost():
    rows, st = _denver()
    assert len(st["re_only"]) == 1 and "Industrial Real Estate" in st["re_only"][0]
    assert not any("Industrial Real Estate" in r["title"] for r in rows)


def test_denver_prices_come_from_business_price_not_real_estate():
    rows, _ = _denver()
    bingo = _by(rows, "Established and Profitable Bingo")
    assert bingo["price"] == 649995            # businessOpportunity, not realEstate 2,399,995
    assert bingo["revenue"] == 717992 and bingo["cash_flow"] == 180000
    assert _by(rows, "Turnkey, Profitable Indian")["price"] == 314499   # realEstate "1" ignored
    assert _by(rows, "Business Valuation for Flooring")["price"] is None  # 0 = none stated


def test_denver_identity_is_the_crm_id_and_unique():
    rows, _ = _denver()
    urls = [r["listing_url"] for r in rows]
    assert len(set(urls)) == len(urls)
    assert all(u.startswith("https://denverbbs.com/buy-a-business/#dl-") for u in urls)
    rec = json.loads(_read("denverbbs_crm.json"))["business"][0]
    assert row_url(rec).endswith(rec["_id"])
    assert detail_url(rec) == "https://denverbbs.salestreamcrm.com/buy-a-business/" + rec["slug"]


def test_denver_identity_survives_price_and_title_change():
    payload = json.loads(_read("denverbbs_crm.json"))
    before = {r["listing_url"] for r in parse_records(payload)[0]}
    for e in payload["business"]:
        if e.get("businessOpportunity"):
            e["businessOpportunity"] += 5
        e["title"] += " (reduced)"
        e["slug"] += "-2"
    after = {r["listing_url"] for r in parse_records(payload)[0]}
    assert before == after


def test_denver_state_from_name_and_blank_state_ok():
    rows, _ = _denver()
    assert _by(rows, "Brewpub")["state"] == "CO"
    assert _by(rows, "Fencing Company")["state"] is None   # CRM list is empty


def test_denver_empty_payload_is_an_error_not_zero_listings():
    with pytest.raises(ValueError):
        parse_records({"status": "fail", "message": {"error": ["x"]}})


def test_denver_duplicate_ids_collapse():
    payload = json.loads(_read("denverbbs_crm.json"))
    payload["business"].append(dict(payload["business"][0]))
    rows, st = parse_records(payload)
    assert len(rows) == len(payload["business"]) - 1 - len(st["re_only"])
