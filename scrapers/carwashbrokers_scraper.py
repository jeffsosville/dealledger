"""
carwashbrokers_scraper.py — carwashbrokers.com (Car Wash Brokers, Inc.).

Rebased 2026-11-20 from claude/staging/loop/2026-11-13/l2-carwash.patch, which
put this class in specialized_scrapers.py next to a GoldStarScraper that never
reached main. Same code, own file (the feldman / lbrokers pattern), so it does
not depend on the unapplied loop-2 stack. Notes: l2-carwash-notes.md.

WHY A SPECIALIZED SCRAPER
The site is a Caffeine / Internet Computer single-page app: its HTML is a
2.9 KB shell and V6 has written 0 rows since 2026-03-27 (FETCH_ERROR on every
run since). The page's own JS reads /env.json (backend_canister_id,
backend_host) and calls the canister's `listListings` query with an unsigned
anonymous envelope. This does exactly that (scrapers/icp_query.py): no tokens,
no forged headers, no signatures.

WHAT THIS EMITS
Per-listing URL: the SPA route /listings/<id> (a real page, not synthetic).
Identity = md5(url) via format_listing. The canister holds 20 listings: ids
0-11 are the site builder's sample set (stock photos '/listings/<name>.jpg'
that do not match their cities, names such as "Lone Star Self-Serve Wash
Portfolio"); ids 13-20 carry the broker's own photos under /assets/listings/.
`exclude_stock_photos` (default True) keeps only the latter; a land-only
parcel ("Fully Entitled Land for Car Wash Development") is dropped (land-only
is out of scope).
"""

import re
from typing import Dict, List

from curl_cffi import requests

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        import hashlib
        return {"listing_id": hashlib.md5(url.encode()).hexdigest(),
                "listing_url": url, "broker_account": broker_account, **kw}

DOMAIN = "carwashbrokers.com"

_US_STATE_CODES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY", "louisiana": "LA",
    "maine": "ME", "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}


class CaffeineIcpListingsScraper:
    """run_specialized entry: CaffeineIcpListingsScraper().scrape(account)."""

    LIST_ARG = b"DIDL\x01\x6c\x00\x01\x00"      # candid: an empty ListingFilter record (every field opt)
    FIELD_NAMES = ["id", "title", "askingPrice", "city", "createdAt", "description", "washType", "summary",
                   "state", "photo", "offeringMemorandum", "financialHighlights", "locationDetails",
                   "fullServiceTunnel", "express", "selfServe"]
    _LAND_RE = re.compile(r"^(?:fully entitled )?land\b|\bland for\b", re.I)

    def __init__(self, site_host: str = "www.carwashbrokers.com", exclude_stock_photos: bool = True):
        self.site_host = site_host.strip().strip("/")
        self.exclude_stock_photos = exclude_stock_photos
        self.session = requests.Session(impersonate="chrome120")
        self.stats: Dict = {}

    def _env(self) -> Dict:
        r = self.session.get(f"https://{self.site_host}/env.json", timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"{self.site_host}/env.json: HTTP {r.status_code}")
        return r.json()

    def _fetch_raw(self) -> bytes:
        import icp_query
        env = self._env()
        return icp_query.query(env.get("backend_host") or "https://icp-api.io", env["backend_canister_id"],
                               "listListings", self.LIST_ARG)

    @classmethod
    def parse(cls, raw: bytes, broker_account: str, site_host: str = "www.carwashbrokers.com",
              exclude_stock_photos: bool = True):
        import icp_query
        rows = icp_query.candid_decode(raw, cls.FIELD_NAMES)[0]
        out, skipped = [], {"stock_photo": 0, "land": 0}
        for l in rows:
            photo = l.get("photo") or ""
            if exclude_stock_photos and photo.startswith("/listings/"):
                skipped["stock_photo"] += 1
                continue
            if cls._LAND_RE.search(l.get("title") or ""):
                skipped["land"] += 1
                continue
            state = (l.get("state") or "").strip()
            wash = next(iter(l.get("washType") or {}), None)
            out.append(format_listing(
                url=f"https://{site_host}/listings/{l['id']}", broker_account=broker_account,
                title=(l.get("title") or "").strip()[:300],
                price=float(l["askingPrice"]) if l.get("askingPrice") else None,
                location=f"{l.get('city', '')}, {state}".strip(", "), city=(l.get("city") or None),
                state=_US_STATE_CODES.get(state.lower(), state or None),
                description=l.get("summary") or l.get("description"), business_type=wash,
            ))
        return out, skipped, len(rows)

    def scrape(self, broker_account: str = "carwashbrokers", verbose: bool = True) -> List[Dict]:
        out, skipped, n = self.parse(self._fetch_raw(), broker_account, self.site_host, self.exclude_stock_photos)
        self.stats = {"canister_listings": n, "kept": len(out), **{f"skipped_{k}": v for k, v in skipped.items()}}
        if verbose:
            print(f"[carwashbrokers] {len(out)} listings from {self.site_host} {self.stats}")
        return out
