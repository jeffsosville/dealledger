"""
ere_property_scraper.py

Essential Real Estate (ERE) property-theme scraper.

Some business brokers run a WordPress real-estate theme (reputedbrokerage.com:
the G5 Beyot theme + the "essential-real-estate" plugin). Every business is a
`property` post, and the For Sale / For Lease / Not Available split is the
`property-status` taxonomy.

Why not WPRestScraper: the plugin registers the `property` post type and its
taxonomies WITHOUT show_in_rest. On 2026-11-11 /wp-json/wp/v2/types listed no
`property` type, /wp-json/wp/v2/property and /properties returned
rest_no_route, /wp-json/wp/v2/taxonomies listed only category/post_tag/
nav_menu/wp_pattern_category, and the /wp-json/ route index had no ERE/houzez
namespace. There is no REST endpoint to call.

What the site does publish (principle 9: ask the site):
  - wp-sitemap.xml lists the taxonomies property-status, property-state,
    property-city, property-label ... and their terms;
  - each term has a server-side-filtered archive, e.g.
    /property-status/for-sale/page/N/, with the term's own count in its
    heading ("For Sale (379)");
  - each card in the archive carries the WordPress post id
    (data-property-id), the permalink, title, price and price postfix.

So status comes from WHICH status archive a post appears in (the server's
taxonomy filter), not from parsing badges, and state/city come from which
property-state / property-city archive it appears in (the cards carry no
address).

Rows (format_listing shape):
  for-sale             -> status 'active'
  not-available        -> status 'inactive' (explicit, so the writer's status
                          guard retires a row that was active before)
  for-lease only       -> dropped (a lease is not a business for sale)
  a post in both for-sale and for-lease -> active (it IS offered for sale)

Revision 2026-11-12 (owner decision 7, from-chat 23:50 UTC 2026-11-11):
  - The card's property-label badge (Active / Under Contract / Sold / Expired /
    On Hold) is the broker's own label. A for-sale post labelled Sold or
    Expired -> status 'sold' with the label verbatim in broker_status_text
    (Expired -> 'sold' too, per the owner). Under Contract / On Hold stay
    active with the label verbatim. A Coming Soon label is not emitted.
  - property-type Lands and Apartment / multifamily are real estate, not an
    operating business (CRE rule): not emitted, counted in stats.
  - A card whose own excerpt states "Asking (Price): $X" that disagrees with
    the card's header price by more than 50% keeps the header price and gets a
    `price_conflict` note (one log line each). The writer drops the field.

Only cards inside the main archive container (.ere-archive-property) are
read. The search form's Min Price / Max Price <select>s and the sidebar's
"recent properties" carousel (which shows For Lease items on the For Sale
archive) can therefore never become rows: the V6 generic path wrote the
price dropdowns as two listings (quarantined_nonlisting, 2026-11).
"""

import re
import time
from collections import Counter
from html import unescape as html_unescape
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from specialized_scrapers import format_listing


US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new-hampshire": "NH", "new-jersey": "NJ",
    "new-mexico": "NM", "new-york": "NY", "north-carolina": "NC",
    "north-dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode-island": "RI", "south-carolina": "SC",
    "south-dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west-virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district-of-columbia": "DC",
}

_MONEY = re.compile(r"\$?\s*(\d[\d,]*(?:\.\d+)?)\s*([kKmM](?:il+ion)?)?\b")

# --- broker label -> status -------------------------------------------------
# One vocabulary for every writer (principle 15): scrapers/listing_status.py
# (job 3, decisions 5/6/7) when it is importable. The local table below is the
# fallback and holds the same answers for every label this theme uses
# (Active / Under Contract / Sold / Expired / On Hold, plus Coming Soon).
try:
    from listing_status import lookup_status as _shared_lookup, DROP, more_final as _more_final
except Exception:                                   # listing_status.py not applied yet
    _shared_lookup = None
    DROP = "DROP"
    _RANK = {"active": 0, "inactive": 1, DROP: 2, "sold": 3}

    def _more_final(a, b):
        return a if _RANK.get(a, 0) >= _RANK.get(b, 0) else b

_LOCAL_LABELS = {                                   # normalised label -> (status, keep verbatim label)
    "active": ("active", False), "for sale": ("active", False),
    "sold": ("sold", True),
    "under contract": ("active", True), "pending": ("active", True),
    "sale pending": ("active", True), "under loi": ("active", True),
    "has accepted offer": ("active", True), "off market": ("active", True),
    "on hold": ("inactive", True), "expired": ("inactive", True),
    "not available": ("inactive", True),
    "coming soon": (DROP, True), "for lease": (DROP, True),
}
# reputedbrokerage: the broker's "Expired" label means sold (owner decision 7)
_LOCAL_DOMAIN_OVERRIDES = {"reputedbrokerage.com": {"expired": "sold"}}


def label_status(label: Optional[str], domain: Optional[str] = None) -> Optional[Tuple[str, Optional[str]]]:
    """(status, verbatim label or None) for a card label; None when unknown.
    status is 'active' | 'inactive' | 'sold' | DROP."""
    if not label or not label.strip():
        return None
    if _shared_lookup is not None:
        return _shared_lookup(label, domain)
    norm = re.sub(r"[\s\-_/]+", " ", re.sub(r"[!:*()\[\].,]+", " ", label.lower())).strip()
    d = (domain or "").lower()
    d = d[4:] if d.startswith("www.") else d
    over = _LOCAL_DOMAIN_OVERRIDES.get(d, {})
    if norm in over:
        return over[norm], re.sub(r"\s+", " ", label).strip()
    hit = _LOCAL_LABELS.get(norm)
    if hit is None:
        return None
    return hit[0], (re.sub(r"\s+", " ", label).strip() if hit[1] else None)


# --- CRE rule: real estate, not an operating business -------------------------
# property-type names (archive heading / card type link), lower-cased.
CRE_TYPE_RE = re.compile(r"^(lands?|apartments?|multi-?family|duplex|triplex|fourplex)$|multi-?family|apartment",
                         re.I)


def is_cre_type(types) -> bool:
    """True when EVERY property-type on the card is a land / apartment /
    multifamily type. A card also typed as an operating business (Gas Stations,
    Motels/Hotels ...) is kept."""
    ts = [t for t in (types or []) if t]
    return bool(ts) and all(CRE_TYPE_RE.search(t.strip()) for t in ts)


# --- override: an operating business the broker typed as Lands ---------------
# Owner decision 5 (2026-11-11 21:35 UTC): gas stations, truck stops, motels,
# hotels, liquor stores, RV parks ... sold as operating businesses WITH their
# land are in scope; the CRE rule is for building/land-only. On this site the
# broker files some operating gas stations under "Lands" and titles them
# "Land Approved for Gas Station in TX" / "Land for GS 45min from Houston".
# A title-term match alone is NOT enough here: on 2026-11-12, 8 pure-land
# posts named "gas station" / "truck stop" in the title ("Land For Sale
# Approved for Truck Stop", "Land In Dallas Approved for Gas Station"), and
# one operating station is titled "Land for GS ...". So a CRE-typed card is kept
# when EITHER
#   (a) its title names an operating business and does not describe land
#       ("Motel on 5 Acres" mistyped as Lands), OR
#   (b) its own excerpt reports CURRENT operating figures (inside / fuel /
#       restaurant sales, fuel gallons per month) and is not a projection
#       ("IMST" / "Feasibility Projections" = projected numbers on approved-land posts).
BUSINESS_TERM_RE = re.compile(
    r"\b(gas[\s-]?stations?|gs|truck[\s-]?stops?|c[\s-]?stores?|convenience[\s-]?stores?|"
    r"motels?|hotels?|inns?|car[\s-]?wash(?:es)?|laundromats?|rv[\s-]?parks?|"
    r"liquor[\s-]?stores?|restaurants?|bars?|daycares?|day[\s-]?cares?|travel[\s-]?centers?)\b", re.I)
LAND_WORD_RE = re.compile(
    r"\b(land|lots?|acres?|approved|pre-?approved|vacant|unimproved|unzoned|entitled|zoned|"
    r"site|pads?|plots?)\b", re.I)
OPERATING_FIGURES_RE = re.compile(
    r"\b(?:inside|fuel|restaurant|store)\s+sales?\s*[:\-]?\s*~?\s*\$?\s*\d[\d,.]*"
    r"|\bfuel\s+volume\b|\d[\d,]*\s*gallons?\s*(?:gas\s*)?(?:/|per)\s*month", re.I)
PROJECTION_RE = re.compile(r"\bIMST\b|\bprojections?\b|\bprojected\b|\bfeasibility\b|\bpro[\s-]?forma\b", re.I)


def cre_override(title: Optional[str], excerpt: Optional[str]) -> Optional[str]:
    """Reason string when a Lands/Apartment-typed card is an operating
    business and must be emitted; None for land / multifamily only."""
    t, ex = title or "", excerpt or ""
    m = BUSINESS_TERM_RE.search(t)
    if m and not LAND_WORD_RE.search(t):
        return f"title names a business ('{m.group(0)}')"
    if OPERATING_FIGURES_RE.search(ex) and not PROJECTION_RE.search(ex):
        f = OPERATING_FIGURES_RE.search(ex).group(0)
        return f"excerpt reports operating figures ('{f.strip()}')"
    return None


# --- header price vs "Asking Price: $X" in the card's own excerpt -----------
_ASKING = re.compile(
    r"asking(?:\s+price)?\s*[:\-–]?\s*\$\s*(\d+(?:,\d{3})*(?:\.\d+)?)(?![\d,]*\d)(?:\s*(million|mil|mm|m|k)\b)?", re.I)
# no \b after the number: excerpts glue lines together ("$9,200,000Gross Annual ...")
# The archive excerpt is cut at ~55 words and ends "[…]": "Asking Price: $9,200, […]"
# is $9,200,000 cut off, not $9,200.
_CUT = re.compile(r"^,\s*(\[\s*(…|\.\.\.|&hellip;)\s*\]|…|\.\.\.)?\s*$")
PRICE_CONFLICT_RATIO = 1.5      # larger / smaller > 1.5  ==  "disagree by more than 50%"


def asking_in_text(text: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
    """First 'Asking Price: $550K' / 'Asking $4.5 million' in text -> (value, matched text)."""
    if not text:
        return None, None
    m = _ASKING.search(text)
    if not m:
        return None, None
    if not m.group(2) and _CUT.match(text[m.end(1):]):
        return None, None                        # number truncated by the excerpt
    try:
        v = float(m.group(1).replace(",", ""))
    except ValueError:
        return None, None
    suf = (m.group(2) or "").lower()
    if suf in ("million", "mil", "mm", "m"):
        v *= 1_000_000
    elif suf == "k":
        v *= 1_000
    return (v if v > 0 else None), m.group(0).strip()


def price_conflict(header_price: Optional[float], excerpt: Optional[str]) -> Optional[str]:
    """Note when the excerpt's asking price disagrees with the header price by >50%."""
    d, said = asking_in_text(excerpt)
    if not header_price or not d:
        return None
    hi, lo = max(header_price, d), min(header_price, d)
    if hi / lo <= PRICE_CONFLICT_RATIO:
        return None
    return (f"header price ${header_price:,.0f} vs description '{said}' (${d:,.0f}); "
            f"header kept as asking_price")


def parse_ere_price(raw: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
    """'$2,990,000' -> (2990000.0, None)
    '$6,500,000 / Net Income of Casino: $35 k' -> (6500000.0, 'Net Income of Casino: $35 k')
    'Call for price' / '' / None -> (None, None) or (None, note)

    Text after the first '/' is the ERE "price postfix": a note, never the
    price. Only the part before '/' is parsed."""
    if not raw:
        return None, None
    text = re.sub(r"\s+", " ", html_unescape(str(raw))).strip()
    main, _, note = text.partition("/")
    note = note.strip() or None
    m = _MONEY.search(main)
    if not m:
        return None, note
    try:
        val = float(m.group(1).replace(",", ""))
    except ValueError:
        return None, note
    suf = (m.group(2) or "").lower()
    if suf.startswith("m"):
        val *= 1_000_000
    elif suf.startswith("k"):
        val *= 1_000
    return (val if val > 0 else None), note


def parse_archive(html: str) -> Tuple[Optional[int], Optional[str], List[Dict]]:
    """(heading_count, heading_name, cards) for one ERE archive page.

    Cards are read ONLY from the main archive container. Each card:
    post_id, url, title, price_text (main part), price_note (postfix),
    status_badges, label_badges."""
    soup = BeautifulSoup(html, "html.parser")
    archive = soup.select_one("div.ere-archive-property")
    if archive is None:
        return None, None, []

    count, name = None, None
    h2 = archive.select_one(".ere-heading h2")
    if h2 is not None:
        sub = h2.find("sub")
        if sub is not None:
            m = re.search(r"\((\d+)\)", sub.get_text())
            count = int(m.group(1)) if m else None
            sub.extract()
        name = re.sub(r"\s+", " ", h2.get_text()).strip() or None

    cards = []
    for wrap in archive.select("div.ere-item-wrap"):
        link = wrap.select_one("h2.property-title a[href]") or wrap.select_one("a.property-link[href]")
        if link is None:
            continue
        url = link["href"].strip()
        # A property permalink is /property/<slug>/ — nothing else is a listing.
        if not re.search(r"/property/[^/?#]+/?$", url):
            continue
        pid_el = wrap.select_one("[data-property-id]")
        post_id = pid_el.get("data-property-id") if pid_el is not None else None
        title = re.sub(r"\s+", " ", link.get_text() or link.get("title") or "").strip()

        price_text, price_note = None, None
        pe = wrap.select_one(".property-price")
        if pe is not None:
            post = pe.select_one(".property-price-postfix")
            if post is not None:
                price_note = re.sub(r"\s+", " ", post.get_text()).strip().lstrip("/").strip() or None
                post.extract()
            price_text = re.sub(r"\s+", " ", pe.get_text()).strip() or None

        def _badges(kind):
            return [re.sub(r"\s+", " ", b.get_text()).strip().lower()
                    for b in wrap.select(f".ere__term-{kind}")]

        label_text = None
        lb = wrap.select_one(".ere__term-label")
        if lb is not None:
            label_text = re.sub(r"\s+", " ", lb.get_text()).strip() or None
        types = [re.sub(r"\s+", " ", a.get_text()).strip()
                 for a in wrap.select(".property-type-list a")]
        ex = wrap.select_one(".property-excerpt")
        excerpt = re.sub(r"\s+", " ", ex.get_text(" ")).strip() if ex is not None else None

        cards.append({
            "post_id": post_id,
            "url": url,
            "title": title,
            "price_text": price_text,
            "price_note": price_note,
            "status_badges": _badges("status"),
            "label_badges": _badges("label"),
            "label_text": label_text,          # verbatim, as the card shows it
            "types": [t for t in types if t],  # property-type link text(s) on the card
            "excerpt": excerpt or None,        # the card's own (truncated) excerpt
        })
    return count, name, cards


class EREPropertyScraper:
    """
        EREPropertyScraper("www.reputedbrokerage.com").scrape(account)

    One pass = the property-status archives (status), then the property-state
    and property-city archives (location). Term lists come from the site's own
    wp-sitemap so new cities/states are picked up without a code change.
    """

    ACTIVE_TERMS = ("for-sale",)
    RETIRE_TERMS = ("not-available",)      # emitted as status 'inactive'
    SKIP_TERMS = ("for-lease",)            # never emitted

    def __init__(self, domain: str, page_delay: float = 2.0, max_pages: int = 80,
                 session=None):
        self.domain = domain.strip().strip("/")
        self.base = f"https://{self.domain}"
        self.page_delay = page_delay
        self.max_pages = max_pages
        self.session = session or self._make_session()
        self.stats: Dict = {}

    @staticmethod
    def _make_session():
        import requests as _std
        s = _std.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X "
                          "10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36"})
        return s

    def _get(self, url: str):
        r = self.session.get(url, timeout=30)
        time.sleep(self.page_delay)
        return r

    def _terms(self, taxonomy: str) -> List[str]:
        """Term slugs for a taxonomy from wp-sitemap-taxonomies-<tax>-1.xml."""
        r = self._get(f"{self.base}/wp-sitemap-taxonomies-{taxonomy}-1.xml")
        if r.status_code != 200:
            return []
        return re.findall(rf"<loc>[^<]*/{re.escape(taxonomy)}/([^/<]+)/?</loc>", r.text)

    def _archive(self, taxonomy: str, term: str) -> Tuple[Optional[int], Optional[str], Dict[str, Dict]]:
        """All cards of one term archive, keyed by post id (url if no id)."""
        found: Dict[str, Dict] = {}
        heading, name = None, None
        for page in range(1, self.max_pages + 1):
            url = f"{self.base}/{taxonomy}/{term}/" + (f"page/{page}/" if page > 1 else "")
            r = self._get(url)
            if r.status_code == 404:
                break
            if r.status_code != 200:
                raise RuntimeError(f"{url} -> HTTP {r.status_code}")
            count, nm, cards = parse_archive(r.text)
            if page == 1:
                heading, name = count, nm
            new = 0
            for c in cards:
                k = c["post_id"] or c["url"]
                if k not in found:
                    found[k] = c
                    new += 1
            if not new:
                break                       # pagination did not advance (principle 7)
            if heading is not None and len(found) >= heading:
                break
        return heading, name, found

    def scrape(self, broker_account: str, verbose: bool = True) -> List[Dict]:
        status_terms = self._terms("property-status") or list(
            self.ACTIVE_TERMS + self.RETIRE_TERMS + self.SKIP_TERMS)
        by_status: Dict[str, Dict[str, Dict]] = {}
        site_counts: Dict[str, Optional[int]] = {}
        for term in status_terms:
            heading, _, cards = self._archive("property-status", term)
            by_status[term] = cards
            site_counts[term] = heading
            if verbose:
                print(f"[ERE:{self.domain}] property-status/{term}: site says {heading}, "
                      f"collected {len(cards)}")

        # location: state and city archives
        state_of: Dict[str, str] = {}
        city_of: Dict[str, str] = {}
        for term in self._terms("property-state"):
            _, _, cards = self._archive("property-state", term)
            abbr = US_STATES.get(term.lower())
            if abbr:
                for k in cards:
                    state_of.setdefault(k, abbr)
        for term in self._terms("property-city"):
            # the theme ships a literal 'none' city term (motel/hotel pages
            # show "City/Town None") — that is no city
            if term.lower() in ("none", "others", "other", "n-a", "na"):
                continue
            _, name, cards = self._archive("property-city", term)
            if name and name.strip().lower() not in ("none", "others", "other"):
                for k in cards:
                    city_of.setdefault(k, name)
        # business type: property-type archives (Gas Station, Motels/Hotels,
        # Apartment ...). Carried as business_type -> listings_direct.category.
        type_of: Dict[str, str] = {}
        for term in self._terms("property-type"):
            _, name, cards = self._archive("property-type", term)
            if name:
                for k in cards:
                    type_of.setdefault(k, name)

        active = set()
        for t in self.ACTIVE_TERMS:
            active |= set(by_status.get(t, {}))
        retire = set()
        for t in self.RETIRE_TERMS:
            retire |= set(by_status.get(t, {}))
        retire -= active

        cards_by_key: Dict[str, Dict] = {}
        for term in status_terms:
            for k, c in by_status[term].items():
                cards_by_key.setdefault(k, c)

        listings = []
        dropped_cre: Counter = Counter()
        dropped_label: Counter = Counter()
        label_sold: Counter = Counter()
        conflicts: List[Dict] = []
        cre_for_sale = 0
        label_dropped_for_sale = 0
        cre_overrides: List[Dict] = []
        label_moved: Counter = Counter()
        unknown_label: Counter = Counter()
        for k in sorted(active | retire, key=str):
            c = cards_by_key[k]
            types = list(c.get("types") or [])
            if type_of.get(k) and type_of[k] not in types:
                types.append(type_of[k])
            override = cre_override(c.get("title"), c.get("excerpt")) if is_cre_type(types) else None
            if override:
                cre_overrides.append({"url": c.get("url"), "title": c.get("title"),
                                      "types": types, "reason": override})
                print(f"[ERE:{self.domain}] CRE OVERRIDE (emitted): {types} '{c.get('title')}' "
                      f"{c.get('url')}: {override}")
            elif is_cre_type(types):
                dropped_cre[" / ".join(types)] += 1
                if k in active:
                    cre_for_sale += 1
                if verbose:
                    print(f"[ERE:{self.domain}] CRE (not emitted): {types} '{c.get('title')}' {c.get('url')}")
                continue
            label = (c.get("label_text") or "").strip()
            base = "active" if k in active else "inactive"
            ls = label_status(label, self.domain)
            status, status_text = base, None
            if ls is not None:
                if ls[0] == DROP:
                    dropped_label[label] += 1
                    if k in active:
                        label_dropped_for_sale += 1
                    if verbose:
                        print(f"[ERE:{self.domain}] label '{label}' (not emitted): {c.get('url')}")
                    continue
                status = _more_final(base, ls[0])   # a Sold label beats the For Sale term
                status_text = ls[1]
            elif label and label.lower() != "active":
                unknown_label[label] += 1           # carried verbatim, status unchanged
                status_text = label
            if base == "active" and status != "active":
                label_moved[f"{label} -> {status}"] += 1
                if status == "sold":
                    label_sold[label] += 1
            rec = self.card_to_listing(c, broker_account, status=status,
                                       state=state_of.get(k), city=city_of.get(k),
                                       business_type=type_of.get(k))
            if not rec:
                continue
            if status_text:
                rec["broker_status_text"] = status_text      # verbatim ("Sold", "Under Contract" ...)
                rec["status_source"] = "card_label"
            if rec.get("price_conflict"):
                conflicts.append({"url": rec["listing_url"], "title": rec.get("title"),
                                  "price": rec.get("price"), "note": rec["price_conflict"]})
                print(f"[ERE:{self.domain}] PRICE_CONFLICT {rec['listing_url']}: {rec['price_conflict']}")
            listings.append(rec)

        n_status = Counter(l["status"] for l in listings)
        self.stats = {
            "site_counts": site_counts,
            "collected": {t: len(v) for t, v in by_status.items()},
            "unique_posts": len(cards_by_key),
            "emitted_active": n_status.get("active", 0),
            "emitted_sold": n_status.get("sold", 0),
            "emitted_inactive": n_status.get("inactive", 0),
            "sold_by_label": dict(label_sold),
            "for_sale_moved_by_label": dict(label_moved),
            "unknown_labels": dict(unknown_label),
            "status_vocabulary": "listing_status.py" if _shared_lookup is not None else "local fallback",
            "dropped_lease_only": len(set(cards_by_key) - active - retire),
            "dropped_cre": sum(dropped_cre.values()),
            "dropped_cre_by_type": dict(dropped_cre),
            "cre_overrides": cre_overrides,
            "dropped_label": dict(dropped_label),
            "price_conflicts": conflicts,
            "with_price": sum(1 for l in listings if l.get("price")),
            "price_not_disclosed": sum(1 for l in listings if l.get("price_not_disclosed")),
            "with_state": sum(1 for l in listings if l.get("state")),
            "with_city": sum(1 for l in listings if l.get("city")),
            "with_type": sum(1 for l in listings if l.get("business_type")),
            "active_by_type": dict(sorted(
                Counter(
                    l.get("business_type") or "-" for l in listings
                    if l["status"] == "active").items())),
            "active_by_label": dict(sorted(
                Counter(
                    l.get("broker_status_text") or "Active" for l in listings
                    if l["status"] == "active").items())),
        }
        for t in self.ACTIVE_TERMS:
            exp, got = site_counts.get(t), len(by_status.get(t, {}))
            if exp is not None and got != exp:
                print(f"[ERE:{self.domain}] WARNING {t}: site says {exp}, collected {got}")
        self.stats["dropped_cre_for_sale"] = cre_for_sale
        self.stats["dropped_label_for_sale"] = label_dropped_for_sale
        # one line that must add up: for-sale = active + sold + CRE + label-dropped
        print(f"[ERE:{self.domain}] for-sale {len(active)} = active {self.stats['emitted_active']}"
              f" + moved by label {sum(label_moved.values())} {dict(label_moved)}"
              f" + CRE not emitted {cre_for_sale} + label-dropped {label_dropped_for_sale};"
              f" CRE overrides emitted {len(cre_overrides)};"
              f" CRE by type {dict(dropped_cre)}; price conflicts {len(conflicts)};"
              f" vocabulary {self.stats['status_vocabulary']}")
        if verbose:
            print(f"[ERE:{self.domain}] {self.stats}")
        return listings

    @staticmethod
    def card_to_listing(card: Dict, broker_account: str, status: str = "active",
                        state: Optional[str] = None, city: Optional[str] = None,
                        business_type: Optional[str] = None) -> Optional[Dict]:
        url = card.get("url") or ""
        if not re.search(r"/property/[^/?#]+/?$", url):
            return None
        title = card.get("title") or None
        price, note_from_main = parse_ere_price(card.get("price_text"))
        note = card.get("price_note") or note_from_main
        rec = format_listing(
            url=url, broker_account=broker_account, title=title,
            price=price, price_text=card.get("price_text"),
            city=city, state=state, business_type=business_type,
            location=", ".join(x for x in (city, state) if x) or None,
            status=status)
        # Extra fields. upsert_listings() builds its row dict explicitly, so these
        # are NOT written to listings_direct today; they are carried for the
        # snapshot/dry-run and for a later column if one is added.
        rec["price_note"] = note
        rec["price_not_disclosed"] = price is None
        rec["broker_listing_id"] = card.get("post_id")
        rec["broker_label"] = (card.get("label_badges") or [None])[0]
        rec["price_conflict"] = price_conflict(price, card.get("excerpt"))
        return rec
