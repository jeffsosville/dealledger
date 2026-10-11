"""Fixtures from live detail pages (Oct 10 2026). Run: python3 scripts/test_enrich_revenue.py"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from enrich_revenue import extract, looks_like_listing

CASES = [
    ("tworld.com",
     '<h2>Listing Details</h2><div>Price: $375,000</div><ul><li>Down Payment: <span class="font-bold">$37,500</span></li>'
     '<li>Sales: <span class="font-bold">$942,361</span></li><li>Seller\'s Discretionary Earnings: '
     '<span class="font-bold">$163,179</span></li></ul>', 942361, 163179),
    ("murphybusiness.com",
     '<li><b>Listed Price :</b> <span> $135,000</span></li><li><b>Total Sales :</b> <span> $110,035</span></li>'
     '<li><b>Discretionary Earnings :</b> <span> $53,229</span></li>', 110035, 53229),
    ("vestedbb.com",
     '<td>Asking Price</td><td>$190,000</td><tr><td class="odd">Gross Revenues</td><td class="even">\t\t$495,000\n </td>'
     '<td class="odd">Lease Remaining</td><td>Register Free to View</td></tr><tr><td class="odd">Owner’s Cash Flow</td>'
     '<td class="even">$84,000</td></tr>', 495000, 84000),
    ("wesellrestaurants.com",
     '<div class="row amount-box"><p class="amount-title mb-0">$<!-- -->599,000</p><p class="amount-subtitle mb-0">Listing #<!-- -->44479</p>'
     '<p class="amount-title mb-0">$<!-- -->1,569,831</p><p class="amount-subtitle mb-0">Sales</p>'
     '<p class="amount-title mb-0">$<!-- -->147,757</p><p class="amount-subtitle mb-0">Owner Benefit</p></div>', 1569831, 147757),
    ("routesforsale.net",
     '<h3>Price:</h3><p>This route is currently grossing an estimated $707,546 per year in sales and nets $98,270!</p>', 707546, None),
    ("routesforsale.net",
     '<h3>Price:</h3><p>Currently this route averages $16,021 per week in sales or $833,085 per year. Commission varies depending on the items sold.</p>', 833085, None),
    ("linkbusiness.com",
     '<p>Listing: #NYC00065</p><label>Sales:</label> Refer to Broker <p>Cash Flow:<strong>$80,000</strong></p>'
     '<p>Gross Revenue:<strong>$500,000</strong></p>', 500000, 80000),
]

fails = 0
for dom, html, rev, cf in CASES:
    got = extract(html, dom)
    ok = got["revenue"] == rev and (cf is None or got["cash_flow"] == cf) and looks_like_listing(dom, html)
    fails += not ok
    print(("ok  " if ok else "FAIL"), dom, got)
assert not looks_like_listing("tworld.com", "<title>Find a Business</title><div>Search results</div>")
print("non-listing page rejected: ok")
sys.exit(1 if fails else 0)
