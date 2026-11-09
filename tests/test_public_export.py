"""Public export + verification gate, offline (no database).
Run: python3 tests/test_public_export.py"""
import csv
import json
import sys
import tempfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import export_public_listings as ex  # noqa: E402
import verify_public_export as vf  # noqa: E402
from public_scope import host_denied, host_of, listing_number  # noqa: E402

TODAY = date(2026, 11, 10)


def test_listing_number_matches_database():
    # Values computed by Postgres: 900000000 + ('x'||substr(md5(id),1,7))::bit(28)::bigint
    assert listing_number("spec:81b92ae4c21c14e7") == 1018150679
    assert listing_number("c5b50f856f31e1eb") == 1037617042


def test_scope_is_host_based():
    assert host_denied(host_of("https://www.bizbuysell.com/Business-Opportunity/x/123/"))
    assert host_denied(host_of("https://app.quietlight.com/listings/1"))
    assert host_denied(host_of("https://broker.com/x"), blocked={"broker.com"})
    # A broker whose own domain contains a marketplace word is fine.
    assert not host_denied(host_of("https://businessesforsalehamptonroads.com/listing/1"))
    assert not host_denied(host_of("https://fcbb.com/food/x"))


def src(i, **kw):
    r = {"id": i, "title": "T", "asking_price": 100000, "cash_flow": None, "revenue": None, "city": None,
         "state": "NY", "vertical": "other", "broker_name": "B", "broker_domain": "b.com",
         "url": f"https://b.com/{i}"}
    r.update(kw)
    return r


def test_export_rows_take_age_from_listing_age_v2():
    ages = {listing_number("a"): {"age_basis": "window", "age_date": "2026-10-20", "listed_after": "2026-10-10"},
            listing_number("b"): {"age_basis": "observed", "age_date": "2026-05-01", "listed_after": "2026-04-01"}}
    a, b, c = src("a"), src("b"), src("c")
    assert ex.apply_age(a, ages) and a["age_basis"] == "window" and a["listed_after"] == "2026-10-10"
    assert ex.apply_age(b, ages) and b["listed_after"] is None   # only windows carry listed_after
    assert not ex.apply_age(c, ages)                             # no age yet: held for tomorrow
    assert a["dealledger_url"] == f"https://dealledger.org/listing/{listing_number('a')}"


def test_dedupe_one_row_per_listing_key():
    rec = {"listing_key": "k", "row_ids": ["a", "b"], "last_seen": "2026-11-09"}
    out = ex.dedupe([src("a"), src("b")], {"a": rec, "b": rec})
    assert [r["id"] for r in out] == ["a"] and out[0]["last_seen"] == "2026-11-09"


# ---- verification gate against a fake database --------------------------
class FakeDb:
    def __init__(self, direct, ages, page, exits, blocked=(), site=20000):
        self.direct, self.ages, self.page, self.exits = direct, ages, page, exits
        self.blocked, self.site = blocked, site

    def all(self, table, select, order):
        return [{"broker_domain": d} for d in self.blocked]

    def by_values(self, table, col, values, select, chunk=0):
        vs = set(values)
        if table == "listings_direct":
            return [r for r in self.direct if r["id"] in vs]
        if table == "listing_age_v2":
            return [dict(a, listing_number=n) for n, a in self.ages.items() if n in vs]
        if table == "mv_listings_page_v2":
            return [{"listing_number": n, "dom_basis": a["age_basis"], "estimated_listed_date": a["age_date"],
                     "listed_after": a["listed_after"]} for n, a in self.page.items() if n in vs]
        if table == "v_listings_left_observation":
            return [{"id": i} for i in self.exits if i in vs]
        raise AssertionError(table)

    def get(self, table, params):
        return [{"listings_on_site": self.site}]


def write_export(folder, rows, left):
    def w(name, fields, rs):
        with open(folder / name, "w", newline="") as f:
            d = csv.DictWriter(f, fieldnames=fields)
            d.writeheader()
            d.writerows(rs)
    w("listings.csv", ex.FIELDS, rows)
    w("listings_left_observation.csv", ex.LEFT_FIELDS, left)
    (folder / "listings.json").write_text(json.dumps(rows))
    (folder / "meta.json").write_text(json.dumps({"rows": len(rows), "left_observation_rows": len(left),
                                                  "generated_at": TODAY.isoformat() + "T15:55:00+00:00"}))


def scenario(n=16000):
    rows, direct, ages = [], [], {}
    for i in range(n):
        rid = f"r{i}"
        r = src(rid)
        ages[listing_number(rid)] = {"age_basis": "observed", "age_date": "2026-06-01", "listed_after": None}
        ex.apply_age(r, ages)
        r["last_seen"] = "2026-11-09"
        rows.append({k: r.get(k) for k in ex.FIELDS})
        direct.append({"id": rid, "status": "active", "published": True, "source": "broker_direct",
                       "url": r["url"], "broker_domain": "b.com"})
    lrow = src("gone")
    ages[listing_number("gone")] = {"age_basis": "in_db", "age_date": "2026-04-01", "listed_after": None}
    ex.apply_age(lrow, ages)
    lrow.update(last_seen="2026-10-30", left_observation_on="2026-11-09",
                left_observation_reason="absent from 3 complete crawls of the broker's site")
    left = [{k: lrow.get(k) for k in ex.LEFT_FIELDS}]
    direct.append({"id": "gone", "status": "inactive", "published": False, "source": "broker_direct",
                   "url": lrow["url"], "broker_domain": "b.com"})
    return rows, left, direct, ages


def run(rows, left, db):
    with tempfile.TemporaryDirectory() as t:
        write_export(Path(t), rows, left)
        rep, _ = vf.verify(t, db, today=TODAY)
        return {c["check"]: c for c in rep.checks}, rep.passed


def test_clean_export_passes():
    rows, left, direct, ages = scenario()
    checks, ok = run(rows, left, FakeDb(direct, ages, dict(ages), {"gone"}, site=16500))
    assert ok, [c for c in checks.values() if not c["passed"]]


def test_gate_catches_bad_exports():
    rows, left, direct, ages = scenario()
    rows[0]["url"] = "https://www.bizbuysell.com/Business-Opportunity/x/1/"          # marketplace
    direct[0]["url"] = rows[0]["url"]
    direct[1]["status"] = "quarantined_platform"                                     # no longer active
    rows[2]["age_basis"] = "window"                                                  # not what the site shows
    page = dict(ages)
    page[int(rows[3]["listing_number"])] = {"age_basis": "in_db", "age_date": "2026-03-01", "listed_after": None}
    left[0]["left_observation_reason"] = "sold"
    checks, ok = run(rows, left, FakeDb(direct, ages, page, set(), site=16500))
    assert not ok
    for code in ("P2", "P3", "A1", "A2", "A3", "L1"):
        assert not checks[code]["passed"], code


def test_gate_rejects_description_column_and_low_counts():
    assert vf.check_columns(ex.FIELDS + ["description"], ex.FIELDS)
    rows, left, direct, ages = scenario(n=100)
    checks, ok = run(rows, left, FakeDb(direct, ages, dict(ages), {"gone"}, site=21000))
    assert not ok and not checks["S1"]["passed"]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
