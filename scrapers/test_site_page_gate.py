#!/usr/bin/env python3
"""
test_site_page_gate.py — principle 8 check for is_site_page() and
listing_url_pattern_filter() (junk_filter.py, added 2026-10-05).

1. Static cases: real listings that MUST pass, site pages that MUST be caught.
2. Live check (when SUPABASE_URL / SUPABASE_SERVICE_KEY are set):
     - every published, active listings_direct row is a must-pass; any it
       rejects is printed as a false positive
     - every row quarantined as 'quarantined_nonlisting' on 2026-10-05
       (quarantine_log) is a must-catch; the catch rate is reported

Usage:
    python3 scrapers/test_site_page_gate.py           # static only offline
    python3 scrapers/test_site_page_gate.py --live    # + live pull
"""
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))
from junk_filter import is_site_page, listing_url_pattern_filter  # noqa: E402

MUST_PASS = [
    ("Lender Pre-Qualified Metalworking Co. w/RE", "https://murphybusiness.com/business-brokerage/detail/23845/lender-pre-qualified-metalworking-co-w-re"),
    ("Food Brokerage Business", "https://thetransitiongroup.biz/listing/food-brokerage-business/"),
    ("Medical Imaging Center in West Texas", "https://intemedior.com/listing/medical-imaging-center-in-west-texas/"),
    ("Construction", "https://murphybusiness.com/business-brokerage/detail/22907/construction"),
    ("Established Janitorial & Commercial Cleaning Company with Recurring Revenue", "https://resolutionep.com/blog/established-cleaning-company-m22n6"),
    ("Bagel Shop w/ Drive Thru and Beer & Wine License", "https://bizbizbiz.com/?post_type=listing&p=567768"),
    ("Asset Sale Opportunity - Distillery Operation - 11940 RW", "https://www.calhouncompanies.com/find-a-business?page=5"),
    ("Cape Cod, MA, ATM route for sale- $139,000 - ATM Brokerage", "https://atmbrokerage.com/atm-route-for-sale/cape-cod-ma-atm-route-for-sale-139000/"),
    ("Own the Leading Business Brokerage Brand for the Burlington Area", "https://www.tworld.com/agents/aaronbrownlee/listings/own-the-leading-business-brokerage-brand-for-the-burlington-area"),
    ("Turnkey Crumbl Cookies Franchise Opportunity", "https://example-broker.com/listings/turnkey-crumbl-cookies"),
    ("Established Real Estate & Property Management Brokerage in Middle TN", "https://nashbb.com/directory/established-real-estate-property-management-brokerage-in-middle-tn/"),
]
MUST_CATCH = [
    ("business valuation Florida", "https://valorembrokers.com/business-valuations/"),
    ("Business Broker Miami", "https://valorembrokers.com/business-broker-miami/"),
    ("Business Brokerage & M&A Services in Florida", "https://valorembrokers.com/services/"),
    ("Let'stalk. Which best describes you?", "https://valorembrokers.com/schedule-a-call/"),
    ("Valorem Business Brokers", "https://www.linkedin.com/company/valorembusinessbrokers/"),
    ("[email protected]", "https://valorembrokers.com/cdn-cgi/l/email-protection#abc"),
    ("Medical Practices", "https://ibainc.com/industries-served/medical-practices/"),
    ("Recognizing the Warning Signs in Your Business", "https://intemedior.com/recognizing-the-warning-signs-in-your-business/"),
    ("Total Business Brokers | Sell Your Business US | Business Valuation", "https://totalbusinessbrokers.com/"),
    ("$80,000,000 $1,030,000 $1,520,000", "https://morganandwestfield.com/buy/businesses-for-sale"),
    ("How Buyers Value a Distribution Business", "https://horizonmaa.com/insights/how-buyers-value-a-distribution-business/"),
    ("Copyright © 2026 Panhandle Business Brokers - All Rights Reserved.", "https://panhandlebb.com/listings/"),
    ("Contact Us - Business Brokerage Services, LLC", "https://denverbbs.com/contact-us/"),
    ("Business Brokerage Transaction Terms", "https://somebroker.com/listing-terms/x"),
]


def static():
    fails = 0
    for t, u in MUST_PASS:
        bad = is_site_page(t, u)
        print(f"[{'FAIL' if bad else 'PASS'}] must-pass  {t[:60]}")
        fails += bad
    for t, u in MUST_CATCH:
        ok = is_site_page(t, u)
        print(f"[{'PASS' if ok else 'FAIL'}] must-catch {t[:60]}")
        fails += (not ok)
    # set-level: intemedior-shaped broker
    cards = [
        {"title": "Medical Imaging Center", "url": "https://intemedior.com/listing/medical-imaging-center/"},
        {"title": "Stone Quarry", "url": "https://intemedior.com/listing/stone-quarry/"},
        {"title": "What Buyers Really Want", "url": "https://intemedior.com/what-buyers-really-want/"},
        {"title": "Inline card", "url": "https://intemedior.com/our-business-listings"},
    ]
    kept, dropped = listing_url_pattern_filter(cards, "https://intemedior.com/our-business-listings")
    ok = len(kept) == 3 and len(dropped) == 1 and dropped[0]["title"] == "What Buyers Really Want"
    print(f"[{'PASS' if ok else 'FAIL'}] pattern filter drops the off-pattern page, keeps inline card")
    fails += (not ok)
    # set-level: broker with no URL pattern keeps everything
    cards2 = [{"title": "A", "url": "https://x.com/a-cafe/"}, {"title": "B", "url": "https://x.com/b-salon/"}]
    k2, d2 = listing_url_pattern_filter(cards2)
    ok = len(k2) == 2 and not d2
    print(f"[{'PASS' if ok else 'FAIL'}] no URL pattern -> nothing dropped")
    fails += (not ok)
    return fails


def _get(path, params):
    import requests
    base = os.environ["SUPABASE_URL"].rstrip("/")
    key = os.environ["SUPABASE_SERVICE_KEY"]
    h = {"apikey": key, "Authorization": f"Bearer {key}"}
    out, start = [], 0
    while True:
        r = requests.get(f"{base}/rest/v1/{path}", params=params,
                         headers={**h, "Range": f"{start}-{start + 999}"}, timeout=60)
        r.raise_for_status()
        rows = r.json()
        out += rows
        if len(rows) < 1000:
            return out
        start += 1000


def live():
    good = _get("listings_direct", {"select": "title,url,broker_domain",
                                     "status": "eq.active", "published": "eq.true"})
    fp = [(r["broker_domain"], r["title"], r["url"]) for r in good
          if is_site_page(r.get("title"), r.get("url") or "")]
    print(f"\nLIVE must-pass: {len(good)} published active rows, {len(fp)} rejected "
          f"({100 * len(fp) / max(1, len(good)):.2f}%)")
    for d, c in Counter(x[0] for x in fp).most_common(15):
        print(f"   {c:>4}  {d}")
    for d, t, u in fp[:40]:
        print(f"   FP  {t[:60]!r}  {u}")
    junk = _get("quarantine_log", {"select": "title,url,reason",
                                    "new_status": "eq.quarantined_nonlisting"})
    caught = [r for r in junk if is_site_page(r.get("title"), r.get("url") or "")]
    print(f"\nLIVE must-catch: {len(junk)} quarantined rows, {len(caught)} caught by is_site_page "
          f"({100 * len(caught) / max(1, len(junk)):.0f}%) — the rest rely on the set-level "
          f"pattern filter or the existing junk gate")
    missed = Counter(r["reason"] for r in junk if r not in caught)
    print("   missed by reason:", dict(missed))
    return len(fp)


if __name__ == "__main__":
    f = static()
    print(f"\nstatic: {'ALL PASS' if not f else f'{f} FAILURE(S)'}")
    if "--live" in sys.argv:
        live()
    sys.exit(1 if f else 0)
