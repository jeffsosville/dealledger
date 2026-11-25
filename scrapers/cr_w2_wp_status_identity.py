"""
cr_w2_wp_status_identity.py  (count-readings job, worker W2, 2026-11-23)

Broker-specific V6 helpers, kept out of the shared scraper (principle 13):

  * hscbrokers.com       status from the detail URL path
                         /property-listings/<status>/<slug>/ (url_path_status),
                         and the index-page card that is not a listing.
  * negociosenflorida.com  API handler: own WordPress listing cards only
                         (div.wp-listings-shortcode), stops before the IDX /
                         MLS block, price from the card's price field only.
  * saintlouisgroup.com + 47 other brokers: remap_superseded_ids(), the
                         write-path fix for "active row stale, superseded twin
                         updated daily" (trg_listings_direct_lock_superseded).
  * arthurberry.com      API handler: the 4 category pages, one table row per
                         listing, liquor-license-only / CRE / land excluded.

Pure functions plus two V6 handlers; no DB, no SQL. The handlers are given
V6's _make_api_listing, so this module never imports V6.
"""
from __future__ import annotations

import re
from typing import Callable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

try:
    from listing_status import DROP
except Exception:                      # listing_status not importable
    DROP = "DROP"


def _bare(domain: str) -> str:
    return (domain or "").lower().removeprefix("www.")


# ── hscbrokers.com: status in the URL path ──────────────────────────────────
# /<section>/<status>/<slug>/ . Allowlisted per domain: a path segment named
# "sold" is ordinary text on other sites (other WP sites with the same shape
# are listed in the W2 report for the conductor to review, not guessed here).
URL_PATH_STATUS_DOMAINS = {"hscbrokers.com"}
_PATH_STATUS = {
    "active": ("active", None),
    "pending": ("active", "Pending"),          # decision 6: still listed, label kept
    "under-contract": ("active", "Under Contract"),
    "sold": ("sold", "Sold"),                  # broker_marked sold, verbatim
    "leased": (DROP, "Leased"),                # a lease is not a listing
}


def _path_parts(url: str) -> List[str]:
    try:
        return [s for s in urlparse(url or "").path.split("/") if s]
    except Exception:
        return []


def url_path_status(url: str, domain: Optional[str] = None) -> Optional[Tuple[str, Optional[str]]]:
    """(status, label) from /<section>/<status>/<slug>/ on an allowlisted
    broker, else None. The status segment has to sit between the section and a
    slug, so /property-listings/ (the index) and a bare
    /property-listings/sold/ carry no status of their own."""
    if _bare(domain or urlparse(url or "").netloc) not in URL_PATH_STATUS_DOMAINS:
        return None
    parts = _path_parts(url)
    if len(parts) < 3:
        return None
    return _PATH_STATUS.get(parts[1].lower())


def is_index_card(url: Optional[str], domain: Optional[str] = None) -> bool:
    """True for a card whose link is the broker's index itself
    (hscbrokers.com: a 'View Property Listings' button was stored as a
    listing with the index page as its URL)."""
    if _bare(domain or urlparse(url or "").netloc) not in URL_PATH_STATUS_DOMAINS:
        return False
    return len(_path_parts(url or "")) <= 1


# ── shared money parsing for the two handlers ───────────────────────────────
_MONEY = re.compile(r"^\$?\s*(\d{1,3}(?:,\d{3})+|\d{1,3}(?:\.\d{3})+|\d+)(?:\.\d{1,2})?\s*\*?$")


def parse_field_money(text: Optional[str]) -> Optional[float]:
    """A price from ONE price field's text, or None. Accepts '$120,000',
    ' 2,099,000' (no $), '$600.000' (dot thousands), '$280000', '$1,875,000*'.
    Words ('A CONVENIR', 'Undisclosed', 'Contact Agent') and anything with
    more than one figure return None - never guess."""
    t = re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()
    if not t:
        return None
    t = re.sub(r"(?i)\s*plus\s+inventory\s*$", "", t).strip()
    m = _MONEY.match(t)
    if not m:
        return None
    digits = m.group(1)
    v = float(digits.replace(",", "").replace(".", "")) if re.search(r"[.,]\d{3}$", digits) \
        else float(digits)
    return v if v >= 1000 else None


# ── negociosenflorida.com ───────────────────────────────────────────────────
NEGOCIOS_DOMAIN = "negociosenflorida.com"
NEGOCIOS_SUFFIX = re.compile(r"\s*[-–—|]\s*Negocios\s+En\s+Florida\s*$", re.I)
# Ads / info pages / building-only posts that live in the same post type
# (held 2026-11-22 in private.negocios_hold_20261122).
NEGOCIOS_NON_LISTING_SLUGS = (
    "ha-pensado-vender-su-negocio",
    "la-visa-americana",
    "es-usted-dueno-de-una-cafeteria",
    "edificio-comercial-en-venta-en-miami-springs",
)
_NEGOCIOS_STATUS = (
    # (regex on the card's status label, status, keep the label)
    (re.compile(r"^\s*vendid[oa]\b", re.I), "sold", True),
    (re.compile(r"contrato|pendiente|oferta", re.I), "active", True),
)


def strip_negocios_suffix(title: str) -> str:
    return NEGOCIOS_SUFFIX.sub("", title or "").strip()


def negocios_non_listing(title: Optional[str], url: Optional[str]) -> bool:
    """True for the ad / info / building-only posts (junk gate)."""
    path = (urlparse(url or "").path or "").lower()
    if any(f"/listings/{s}" in path for s in NEGOCIOS_NON_LISTING_SLUGS):
        return True
    t = (title or "").lower()
    return bool(re.search(r"ha pensado vender|la visa americana|es usted due[nñ]o de", t))


def parse_negocios_cards(html: str) -> Tuple[List[dict], int]:
    """(cards, held_out) from the broker's OWN listing cards.

    Only div.wp-listings-shortcode > div.listing-wrap is read. The Miami
    Association of Realtors IDX/MLS widget that follows it (div.sidx-container,
    'New Listing - N hours on site', SQFT) is a different subtree and is
    never selected; MLS is never a source. Price comes from span.listing-price
    only, never from the title ('$670K Ventas' in a title is sales).
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cards, held, seen = [], 0, set()
    for wrap in soup.select("div.wp-listings-shortcode div.listing-wrap"):
        a = wrap.select_one("h3.listing-title a[href]")
        if a is None:
            continue
        url = a["href"].strip()
        title = strip_negocios_suffix(a.get_text(" ", strip=True))
        if not title or url in seen:
            continue
        seen.add(url)
        if negocios_non_listing(title, url):
            held += 1
            continue
        price_el = wrap.select_one("span.listing-price")
        st_el = wrap.select_one("span.listing-status")
        type_el = wrap.select_one("span.listing-property-type")
        st_text = st_el.get_text(" ", strip=True) if st_el else ""
        status, label = "active", None
        for rx, s, keep in _NEGOCIOS_STATUS:
            if rx.search(st_text):
                status, label = s, (st_text if keep else None)
                break
        price_text = price_el.get_text(" ", strip=True) if price_el else ""
        cards.append({
            "url": url, "title": title, "status": status, "status_label": label,
            "price": parse_field_money(price_text),
            "price_raw": price_text or None,
            "business_type": type_el.get_text(" ", strip=True) if type_el else None,
        })
    return cards, held


def make_negocios_handler(make_listing: Callable) -> Callable:
    def handler(url, html, broker_name, fetcher):
        host = _bare(urlparse(url).netloc)
        if host != NEGOCIOS_DOMAIN and not host.endswith("." + NEGOCIOS_DOMAIN):
            return None                     # en. / pt. language subdomains too
        cards, held = parse_negocios_cards(html)
        if not cards:
            return None
        out = []
        for c in cards:
            rec = make_listing(url, broker_name, title=c["title"], detail_url=c["url"],
                               asking_price=c["price"], state="FL",
                               description=" ".join(x for x in (c["title"], c["business_type"]) if x))
            rec["broker_domain"] = NEGOCIOS_DOMAIN
            rec["status"] = c["status"]
            rec["status_source"] = "card_status_field"
            rec["broker_status_text"] = c["status_label"]
            if c["price"] is None and c["price_raw"]:
                rec["broker_price_text"] = c["price_raw"]
            out.append(rec)
        print(f"   🗂  negociosenflorida own cards: {len(out)} (held out {held} ad/info/building "
              f"posts); IDX/MLS block not read, no pagination")
        return out
    return handler


# ── arthurberry.com ─────────────────────────────────────────────────────────
ARTHURBERRY_DOMAIN = "arthurberry.com"
_AB_CATEGORY_PATH = re.compile(r"^/businesses-for-sale/([a-z0-9-]+)/?$", re.I)
_AB_NDA = re.compile(r"non-disclosure-agreement", re.I)
_AB_LAND = re.compile(r"\b(?:land|acres?|\d+(?:\.\d+)?\s*ac)\b|mixed[\s-]*use\s+(?:project|development)", re.I)
_AB_REF = re.compile(r"\bREF\s*#?\s*([A-Z]{1,3}\d{3,6})\b")


def ab_category_urls(html: str, base_url: str) -> List[str]:
    """The category pages linked from the Businesses For Sale pages, in order."""
    soup = BeautifulSoup(html or "", "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        u = urljoin(base_url, a["href"]).split("#")[0].split("?")[0]
        p = urlparse(u)
        if _bare(p.netloc) == ARTHURBERRY_DOMAIN and _AB_CATEGORY_PATH.match(p.path) \
                and not u.endswith("/businesses-for-sale/"):
            u = u if u.endswith("/") else u + "/"
            if u not in out:
                out.append(u)
    return out


def _ab_status(td0) -> Tuple[str, Optional[str]]:
    imgs = " ".join((i.get("src") or "") + " " + (i.get("alt") or "") for i in td0.find_all("img")).lower()
    date = td0.get_text(" ", strip=True)
    if "sold" in imgs:
        return "sold", ("Sold " + date).strip()
    if "pending" in imgs:
        return "active", "Pending"
    return "active", None


def ab_exclusion(title: str, subtype: str, href: str) -> Optional[str]:
    """Reason a row is not an operating business, else None. A restaurant sold
    WITH its liquor license has the restaurant as its subtype and stays in."""
    if re.fullmatch(r"\s*liquor\s+licen[sc]es?\s*", subtype or "", re.I):
        return "liquor-license-only"
    if re.search(r"/commercial-listings/", href or "", re.I):
        return "commercial-real-estate"
    if _AB_LAND.search(title or ""):
        return "land"
    return None


def parse_arthurberry_rows(html: str, page_url: str = "https://arthurberry.com/businesses-for-sale/") -> Tuple[List[dict], dict]:
    """(rows, dropped_by_reason) from one category page's listing table."""
    soup = BeautifulSoup(html or "", "html.parser")
    raw = []
    for tr in soup.select("tr[data-row_id]"):
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue
        link = None
        for a in tds[1].find_all("a", href=True):
            h = a["href"].strip()
            if h.lower().startswith("mailto:") or re.search(r"request\s+info", a.get_text(" ", strip=True), re.I):
                continue
            link = a
            break
        if link is None:
            continue
        title = re.sub(r"\s+", " ", link.get_text(" ", strip=True)).strip()
        if not title:
            continue
        href = urljoin(page_url, link["href"].strip())
        status, label = _ab_status(tds[0])
        type_lines = [s.strip() for s in tds[4].get_text("\n", strip=True).split("\n") if s.strip()]
        subtype = type_lines[0] if type_lines else ""
        m = _AB_REF.search(tds[4].get_text(" ", strip=True))
        price_cell = tds[2].get_text(" ", strip=True)
        loc = re.sub(r"\s+", " ", tds[5].get_text(", ", strip=True)).strip(", ")
        raw.append({
            "title": title, "href": href, "status": status, "status_label": label,
            "price": parse_field_money(price_cell.split("New Price!")[0].split("Reduced!")[0].strip()),
            "price_cell": price_cell,
            "revenue": parse_field_money(tds[3].get_text(" ", strip=True)),
            "subtype": subtype, "ref": m.group(1) if m else None, "location": loc,
            "price_note": next((n for n in ("New Price!", "Reduced!") if n in price_cell), None),
        })
    # A shared NDA page (/non-disclosure-agreement) is not an identity.
    from collections import Counter
    shared = {h for h, n in Counter(r["href"] for r in raw).items()
              if n > 1 and _AB_NDA.search(h)}
    rows, dropped = [], {}
    for r in raw:
        why = ab_exclusion(r["title"], r["subtype"], r["href"])
        if why:
            dropped[why] = dropped.get(why, 0) + 1
            continue
        r["detail_url"] = None if r["href"] in shared else r["href"]
        rows.append(r)
    return rows, dropped


def make_arthurberry_handler(make_listing: Callable) -> Callable:
    def handler(url, html, broker_name, fetcher):
        if _bare(urlparse(url).netloc) != ARTHURBERRY_DOMAIN:
            return None
        if "data-row_id" not in (html or "") and "/businesses-for-sale/" not in (html or ""):
            return None
        base = "https://arthurberry.com/businesses-for-sale/"
        cats = ab_category_urls(html, base)
        pages = [(url, html)] if "data-row_id" in (html or "") else []
        for cu in cats:
            if cu.rstrip("/") == url.split("?")[0].rstrip("/"):
                continue
            try:
                chtml, _ = fetcher.fetch(cu)
            except Exception as exc:
                print(f"   ⚠️  arthurberry category {cu}: {exc}")
                continue
            pages.append((cu, chtml))
        by_key, dropped_all = {}, {}
        for pu, ph in pages:
            rows, dropped = parse_arthurberry_rows(ph, pu)
            for k, v in dropped.items():
                dropped_all[k] = dropped_all.get(k, 0) + v
            for r in rows:
                key = r["ref"] or r["href"]
                by_key.setdefault(key, r)       # same row listed under two categories
        out = []
        for r in by_key.values():
            st = re.search(r"\b(?:ID|Idaho)\b", r["location"]) and "ID" or None
            rec = make_listing(base, broker_name, title=r["title"], detail_url=r["detail_url"],
                               asking_price=r["price"], revenue=r["revenue"], state=st,
                               description=" ".join(x for x in (r["title"], r["subtype"], r["location"]) if x))
            rec["broker_domain"] = ARTHURBERRY_DOMAIN
            rec["status"] = r["status"]
            rec["status_source"] = "index_status_image"
            rec["broker_status_text"] = r["status_label"]
            if r["ref"]:
                rec["broker_listing_ref"] = r["ref"]
            if r["price_note"]:
                rec["broker_note"] = r["price_note"]
            if r["price"] is None and r["price_cell"]:
                rec["broker_price_text"] = r["price_cell"]
            out.append(rec)
        sold = sum(1 for r in out if r["status"] == "sold")
        print(f"   🗂  arthurberry: {len(cats)} category page(s), {len(out)} distinct rows "
              f"(sold {sold}, active {len(out) - sold}); excluded {dropped_all or 'none'}")
        return out or None
    return handler


def v6_handlers(make_listing: Callable) -> list:
    """[(signature, handler)] for V6's API_HANDLERS registry."""
    return [
        (NEGOCIOS_DOMAIN, make_negocios_handler(make_listing)),
        (ARTHURBERRY_DOMAIN, make_arthurberry_handler(make_listing)),
    ]


# ── superseded twin: write to the live row, not the pinned one ──────────────
# trg_listings_direct_lock_superseded rewrites status 'active' back to
# 'superseded' whenever the row it updates is already superseded. When a dedupe
# (2026-10-31, "same url+title") kept the OLDER id and superseded the id the
# scraper still produces, the nightly upsert keeps refreshing the superseded
# row (last_seen, price) and never touches the active twin the site shows.
# saintlouisgroup.com: 29 such URLs; 333 across 48 brokers (2026-11-22 query).
# Fix at the writer: if a row's id is superseded and EXACTLY ONE active row of
# the same broker has the same url, send the row under that active id instead.
# The upsert then updates the live row through the ordinary path, so the
# price-change detector sees any price change (nothing is copied silently).
# Never raises; a failed lookup leaves the rows unchanged and says so.
def remap_superseded_ids(client, rows: list, log: Callable = print) -> Tuple[list, dict]:
    stats = {"superseded_hit": 0, "remapped": 0, "no_active_twin": 0,
             "ambiguous": 0, "lookup_failed": 0}
    if not rows or client is None:
        return rows, stats
    try:
        by_id = {r["id"]: r for r in rows if r.get("id")}
        sup = {}
        ids = list(by_id)
        for i in range(0, len(ids), 100):
            res = (client.table("listings_direct").select("id,url,broker_domain")
                   .in_("id", ids[i:i + 100]).eq("status", "superseded").execute().data) or []
            for x in res:
                sup[x["id"]] = x
        if not sup:
            return rows, stats
        stats["superseded_hit"] = len(sup)
        by_dom: dict = {}
        for x in sup.values():
            if x.get("url") and x.get("broker_domain"):
                by_dom.setdefault(x["broker_domain"], set()).add(x["url"])
        twins: dict = {}                       # (domain, url) -> [active ids]
        for dom, urls in by_dom.items():
            urls = sorted(urls)
            for i in range(0, len(urls), 50):
                res = (client.table("listings_direct").select("id,url,broker_domain")
                       .eq("broker_domain", dom).in_("url", urls[i:i + 50])
                       .eq("status", "active").execute().data) or []
                for x in res:
                    twins.setdefault((x["broker_domain"], x["url"]), []).append(x["id"])
        for sid, x in sup.items():
            t = [i for i in twins.get((x.get("broker_domain"), x.get("url")), []) if i != sid]
            if len(t) == 1:
                by_id[sid]["id"] = t[0]
                stats["remapped"] += 1
            elif not t:
                stats["no_active_twin"] += 1
            else:
                stats["ambiguous"] += 1
        log(f"   ⓘ  superseded-twin check: {stats['superseded_hit']} row(s) are superseded; "
            f"{stats['remapped']} written to their active twin, "
            f"{stats['no_active_twin']} have no active twin (left as is: hidden live), "
            f"{stats['ambiguous']} ambiguous (left as is)")
    except Exception as exc:                    # never break the write; say so
        stats["lookup_failed"] = 1
        log(f"   ⚠️  superseded-twin check failed ({type(exc).__name__}: {exc}); "
            f"rows written under their own ids")
    return rows, stats
