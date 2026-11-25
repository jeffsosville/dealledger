"""
cr_w3_platforms - platform readers for the w3-platforms job (2026-11-23).

One reader per listing PLATFORM, each scoped to the named brokers only (an
explicit domain allow-list; a platform signature in the page source is never
enough on its own, so nothing here can capture a broker nobody checked):

  DealRelations widget   sunbeltofflorida.com, mainstreetmain.com
                         (the older, on-hold DealRelations adapter stays parked;
                          this is the widget reader only)
  BBF office feed        acquisitionexperts.net   (bbms.info, folder bbf-170 only)
  Salesforce/Angular     blbrokers.com            ("View More" button)
  Legacy embed           newleafbrokerage.com     (getlegacyapp.com listing API)
  7C Buying/Filter JSON  7cint.com
  "listingBox" cards     bossgi.com               (own page only, never the MLS feed)

Tupelo (kcapex.com, businessmodificationgroup.com) keeps its handler in V6;
this module supplies its status mapping and the displayed-count note.

V6 hooks (minimal): install() appends the handlers to API_HANDLERS and
registers V6 helpers; feed_displayed() hands the broker's own count to the
displayed-count tracker; drop_index_url_rows() is the last-line guard that no
row's url equals the listings index (blbrokers.com wrote 20 such rows).

Pure parsing functions take HTML/JSON and return plain dicts so they can be unit
tested without the network.
"""
from __future__ import annotations

import html as _html
import json
import math
import re
import time
from urllib.parse import quote, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

# --------------------------------------------------------------------------
# shared state / small helpers
# --------------------------------------------------------------------------

DISPLAYED: dict[str, dict] = {}     # bare host -> {"count", "text", "note"}
SCOPE_LOG: dict[str, list] = {}     # bare host -> [(what, why), ...] left out on purpose

_H: dict = {}                       # V6 helpers, set by install()


def _bare(host: str) -> str:
    host = (host or "").lower().split(":")[0]
    return host[4:] if host.startswith("www.") else host


def note_displayed(domain: str, count, text: str = "", note: str = "") -> None:
    """Record the broker's own count for `domain` (read by feed_displayed)."""
    if count is None:
        return
    DISPLAYED[_bare(domain)] = {"count": int(count), "text": text or None,
                                "note": note or None}


def feed_displayed(tracker, url: str) -> None:
    """V6 hook: give the tracker the stated count an API handler found.

    Sets .stated directly (the tracker's result() returns it unchanged when no
    page-level candidates exist, which is the case for every handler here).
    Never raises."""
    try:
        d = DISPLAYED.pop(_bare(urlparse(url).netloc), None)
        if d and tracker is not None and tracker.stated is None:
            tracker.stated = {"count": d["count"], "source": "stated",
                              "text": d["text"], "note": d["note"]}
    except Exception:
        pass


def _norm_index(u: str) -> str:
    return (u or "").strip().rstrip("/")


def drop_index_url_rows(listings, index_url: str):
    """Principle 8-safe guard: drop only rows whose url IS the listings index
    (exact match after trailing-slash trim). A fragment ('#...') or any query
    keeps a url distinct, so synthetic per-card urls and ?listingId= urls pass.
    Returns (kept, dropped_count)."""
    idx = _norm_index(index_url)
    if not idx:
        return listings, 0
    kept = [l for l in listings if _norm_index(l.get("url")) != idx]
    return kept, len(listings) - len(kept)


def money(text) -> float | None:
    """'$1,595,000' / '1,595,000' / '$2.5m' / '$750K' -> float; else None."""
    if text is None:
        return None
    if isinstance(text, (int, float)):
        return float(text) if text > 0 else None
    t = str(text).replace("\xa0", " ").strip()
    m = re.search(r"\$?\s*([\d][\d,]*(?:\.\d+)?)\s*([kKmM]\b|[kKmM](?![a-z]))?", t)
    if not m:
        return None
    try:
        v = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    suf = (m.group(2) or "").lower()
    v *= 1_000 if suf == "k" else 1_000_000 if suf == "m" else 1
    return v if v > 0 else None


STATE_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI",
    "wyoming": "WY", "district of columbia": "DC",
}


def state_code(name) -> str | None:
    n = re.sub(r"\s+", " ", str(name or "")).strip().lower()
    if len(n) == 2 and n.upper() in STATE_ABBR.values():
        return n.upper()
    return STATE_ABBR.get(n)


def _clean_title(t) -> str:
    t = _html.unescape(re.sub(r"\s+", " ", str(t or ""))).strip()
    t = re.sub(r"^[|\-–—\s]+", "", t)          # " | Title" (mainstreetmain)
    t = re.sub(r"\s*\|\s*BottomLine Business Brokers\s*$", "", t, flags=re.I)
    return t.strip()


def _status(text, domain):
    """('active'|'sold'|'DROP', label) through V6's listing_status table.
    Unknown text -> ('active', None): a card on the broker's own for-sale list
    stays active unless the broker's words say otherwise."""
    look = _H.get("status")
    if text and look:
        hit = look(text, domain)
        if hit:
            return hit
    return ("active", None)


def _mk(url, broker_name, **kw):
    return _H["make"](url, broker_name, **kw)


def _proxy(domain: str) -> bool:
    fn = _H.get("needs_proxy")
    return bool(fn(domain)) if fn else False


def _get(fetcher, url, domain, use_proxy=None):
    """HTML text of url via V6's fetcher (proxy escalation included) or None."""
    try:
        html, _m = fetcher.fetch(
            url, use_proxy=_proxy(domain) if use_proxy is None else use_proxy)
        return html
    except Exception:
        return None


def _get_plain(fetcher, url):
    """Raw response text through the fetcher's session (no content rewriting)."""
    try:
        r = fetcher.session.get(url, timeout=25)
        return r.text if r.status_code == 200 else None
    except Exception:
        return None


# --------------------------------------------------------------------------
# DealRelations widget (sunbeltofflorida.com, mainstreetmain.com)
# --------------------------------------------------------------------------

DR_DOMAINS = frozenset({"sunbeltofflorida.com", "mainstreetmain.com"})
# Jeff, 22 Nov: real estate only, not businesses for sale.
DR_SCOPE_EXCLUDE = {
    "mainstreetmain.com": {
        "2nd-generation-restaurant-real-estate-for-sale":
            "real estate only ($2.499M restaurant-site real estate)",
        "beach-restaurant-retail-office-redevelopment":
            "real estate redevelopment site",
    },
}
_DR_FEED_RE = re.compile(
    r"https?://[a-z0-9.-]+\.dealrelations\.com/listing_feeds/\d+", re.I)
_DR_TOTAL_RE = re.compile(r"Showing\s+(?:[\d,]+\s*[-–]\s*[\d,]+|[\d,]+)\s+of\s+([\d,]+)\s+listings?", re.I)


def dr_cards(html: str):
    """Cards of one DealRelations widget page -> list of dicts (page order)."""
    soup = BeautifulSoup(html or "", "html.parser")
    out = []
    for art in soup.select("article.dr-card"):
        a = art.select_one(".dr-card__title a[href]") or art.select_one("a.dr-button[href]")
        btn = art.select_one("a.dr-button[href]")
        href = (btn or a).get("href") if (btn or a) else None
        title = _clean_title(art.select_one(".dr-card__title").get_text(" ", strip=True)
                             if art.select_one(".dr-card__title") else "")
        hi = art.select_one(".dr-card__listing-highlights")
        price = art.select_one(".dr-card__price")
        status = art.select_one(".dr-card__status")
        metas = [m.get_text(" ", strip=True) for m in art.select(".dr-card__meta")]
        out.append({
            "href": href, "title": title,
            "teaser": hi.get_text(" ", strip=True) if hi else "",
            "price_text": price.get_text(" ", strip=True) if price else "",
            "status_text": status.get_text(" ", strip=True) if status else "",
            "revenue": next((money(m.split(":", 1)[1]) for m in metas
                             if m.lower().startswith("revenue")), None),
            "profit": next((money(m.split(":", 1)[1]) for m in metas
                            if m.lower().startswith(("profit", "cash flow", "sde", "ebitda"))), None),
        })
    return out


def dr_total(html: str) -> int | None:
    m = _DR_TOTAL_RE.search(BeautifulSoup(html or "", "html.parser").get_text(" ", strip=True))
    return int(m.group(1).replace(",", "")) if m else None


def dr_identity_url(href: str, page_url: str, feed_host: str | None):
    """(identity_url, slug) for one card link.

    Own-site widget (sunbeltofflorida.com): /deal-listing/<slug>/ with the
    ?back=...&json_url=... query stripped - the query changes with the page a
    card sits on and made duplicate rows.
    Hosted feed (wesellmainstreet.dealrelations.com): the broker's own
    DealRelations account, /listings/<slug>."""
    if not href:
        return None, None
    full = urljoin(page_url, _html.unescape(href))
    p = urlparse(full)
    m = re.search(r"/deal-listing/([^/?#]+)", p.path)
    if m:
        return urlunparse((p.scheme, p.netloc, f"/deal-listing/{m.group(1)}/", "", "", "")), m.group(1)
    m = re.search(r"/listings/([^/?#]+)", p.path)
    if m:
        host = feed_host or p.netloc
        slug = re.sub(r"\.json$", "", m.group(1))
        return f"https://{host}/listings/{slug}", slug
    return None, None


def _dr_collect(fetcher, domain, first_html, first_url, page_url_fn, feed_host):
    """Walk every page of one widget. Returns (cards[(card, id_url, slug)], total, pages)."""
    total = dr_total(first_html)
    seen, cards, pages = set(), [], 0
    html, url = first_html, first_url
    page = 1
    per_page = None
    while html and page <= 25:
        pages += 1
        page_cards = dr_cards(html)
        new = 0
        for c in page_cards:
            idu, slug = dr_identity_url(c["href"], url, feed_host)
            if not idu or slug in seen:
                continue
            seen.add(slug)
            cards.append((c, idu, slug))
            new += 1
        per_page = per_page or len(page_cards)
        if not new:
            break
        if total and (len(seen) >= total or (per_page and page * per_page >= total)):
            break
        if not total and page >= 1 and (per_page and len(page_cards) < per_page):
            break
        page += 1
        url = page_url_fn(page)
        html = _get(fetcher, url, domain, use_proxy=False if feed_host else None)
        time.sleep(0.4)
    return cards, total, pages


def handler_dealrelations(url, html, broker_name, fetcher):
    domain = _bare(urlparse(url).netloc)
    if domain not in DR_DOMAINS:
        return None
    low = html or ""
    feed = _DR_FEED_RE.search(_html.unescape(low))
    feed_host = None
    if "dr-card" in low:                       # widget rendered on the broker's page
        base = url.split("#")[0].split("?")[0]
        first_html, first_url = low, url
        page_fn = lambda n: f"{base}?dl_page={n}"
    elif feed:                                 # iframe to the broker's DealRelations feed
        feed_url = feed.group(0)
        feed_host = urlparse(feed_url).netloc
        first_url = f"{feed_url}?page=1&per_page=12"
        first_html = _get(fetcher, first_url, domain, use_proxy=False)
        page_fn = lambda n: f"{feed_url}?page={n}&per_page=12"
    else:
        return None
    if not first_html or "dr-card" not in first_html:
        return None
    cards, total, pages = _dr_collect(fetcher, domain, first_html, first_url, page_fn, feed_host)
    excl = DR_SCOPE_EXCLUDE.get(domain, {})
    out = []
    for c, idu, slug in cards:
        if slug in excl:
            SCOPE_LOG.setdefault(domain, []).append((c["title"], excl[slug]))
            continue
        st, label = _status(c["status_text"], domain)
        if st == "DROP" or not c["title"]:
            SCOPE_LOG.setdefault(domain, []).append((c["title"] or slug, f"status {c['status_text']!r}"))
            continue
        row = _mk(url, broker_name, title=c["title"], detail_url=idu,
                  asking_price=money(c["price_text"]), cash_flow=c["profit"],
                  revenue=c["revenue"], description=c["teaser"] or c["title"])
        row["source_listing_id"] = slug
        if re.search(r"real estate for sale|\bspace\b", c["title"], re.I):
            SCOPE_LOG.setdefault(domain, []).append(
                (c["title"], "FLAG (kept): possible real-estate-only / space listing, not on Jeff's exclusion list"))
        row["status"] = st
        if label:
            row["broker_status_text"] = label
        out.append(row)
    note_displayed(domain, total, f"Showing N of {total} listings" if total else "",
                   f"{pages} page(s) read; {len(cards)} cards, {len(out)} emitted"
                   + (f"; {len(cards) - len(out)} left out of scope" if len(cards) != len(out) else ""))
    print(f"   DealRelations widget: {len(cards)} cards over {pages} page(s), "
          f"broker total {total}, {len(out)} emitted")
    return out or None


# --------------------------------------------------------------------------
# BBF office feed (acquisitionexperts.net)
# --------------------------------------------------------------------------

BBF_DOMAIN = "acquisitionexperts.net"
BBF_FOLDER = "bbf-170"            # this office only; never the full BBF/MLS feed
BBF_OWN_URL = f"https://bbms.info/cgi-bin/a-bus2.asp?folder={BBF_FOLDER}&src=bus-own"
BBF_DETAIL = "https://bbms.info/cgi-bin/a-bus-d.asp?folder=" + BBF_FOLDER + "&LIST_NUMBER={}"


def bbf_rows(html: str):
    """Parse the grouped table into dicts. Only BBF-170-* numbers are kept."""
    soup = BeautifulSoup(html or "", "html.parser")
    rows, category = [], None
    for tbl in soup.find_all("table"):
        cls = tbl.get("class") or []
        if "special" not in cls:
            h = tbl.get_text(" ", strip=True)
            if h and len(h) < 60 and not re.search(r"\d{3}", h):
                category = h
            continue
        for tr in tbl.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 7:
                continue
            m = re.search(r"\bBBF-170-\d+\b", tds[0].get_text(" ", strip=True))
            if not m:
                continue
            mid = tds[2]
            flags = " ".join(mid.get_text(" ", strip=True).split())
            spans = [s.get_text(" ", strip=True) for s in mid.find_all("span", class_="srchpg2txt")]
            detail = next((s for s in reversed(spans) if s and "*" not in s), "")
            rows.append({
                "number": m.group(0), "category": category or "",
                "location": tds[1].get_text(" ", strip=True), "detail": detail,
                "flags": flags,
                "price": money(tds[3].get_text(strip=True)),
                "down": money(tds[4].get_text(strip=True)),
                "earn": money(tds[5].get_text(strip=True)),
                "sales": money(tds[6].get_text(strip=True)),
            })
    return rows


def bbf_dedupe(rows):
    """Same business filed under two categories (BBF-170-04312 = 04314: 'Office
    Inter. Design' and 'Bldg Renovations', identical price/down/earnings/sales/
    county): collapse on the numbers + county, keeping the LOWEST listing number
    so the survivor does not depend on page order. Returns (kept, dups)."""
    groups = {}
    for r in rows:
        key = (r["location"].lower(), r["price"], r["down"], r["earn"], r["sales"])
        groups.setdefault(key, []).append(r)
    kept, dups = [], []
    for g in groups.values():
        g = sorted(g, key=lambda r: r["number"])
        kept.append(g[0])
        dups.extend((x["number"], g[0]["number"]) for x in g[1:])
    order = {r["number"]: i for i, r in enumerate(rows)}
    kept.sort(key=lambda r: order[r["number"]])
    return kept, dups


def _bbf_location(loc):
    # "Orange, Florida USA" -> county Orange, state FL
    parts = [p.strip() for p in (loc or "").split(",")]
    county = parts[0] if parts else None
    st = None
    if len(parts) > 1:
        st = state_code(re.sub(r"\s+(USA|US)$", "", parts[1].strip(), flags=re.I))
    return county, st


def handler_bbf(url, html, broker_name, fetcher):
    domain = _bare(urlparse(url).netloc)
    if domain != BBF_DOMAIN or BBF_FOLDER not in (html or "").lower():
        return None
    own = _get_plain(fetcher, BBF_OWN_URL) or _get(fetcher, BBF_OWN_URL, domain, use_proxy=False)
    rows = bbf_rows(own) if own else []
    if not rows:
        return None
    kept, dups = bbf_dedupe(rows)
    out = []
    for r in kept:
        county, st = _bbf_location(r["location"])
        flags = r["flags"]
        label = "Contract Pending" if re.search(r"contract\s+pending", flags, re.I) else None
        title = r["detail"] or r["category"] or r["number"]
        if county:
            title = f"{title} - {county} County, {st or ''}".rstrip(", ")
        desc = _bbf_description(fetcher, r["number"]) or f"{r['category']}: {r['detail']}"
        row = _mk(url, broker_name, title=title, detail_url=BBF_DETAIL.format(r["number"]),
                  asking_price=r["price"], cash_flow=r["earn"], revenue=r["sales"],
                  city=None, state=st, description=desc)
        row["source_listing_id"] = r["number"]
        row["status"] = "active"
        if label:
            row["broker_status_text"] = label
        out.append(row)
    SCOPE_LOG.setdefault(domain, []).extend(
        (f"{a} duplicate of {b}", "same business under two categories") for a, b in dups)
    note_displayed(domain, len(kept),
                   f"{len(rows)} BBF-170 cards, {len(kept)} businesses",
                   f"bbms.info folder {BBF_FOLDER}; collapsed {len(dups)} duplicate card(s)")
    print(f"   BBF office feed: {len(rows)} cards -> {len(kept)} businesses "
          f"({len(dups)} duplicates collapsed)")
    return out or None


def _bbf_description(fetcher, number):
    try:
        r = fetcher.session.get(BBF_DETAIL.format(number), timeout=25)
        if r.status_code != 200:
            return None
        t = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
        m = re.search(r"Company Overview\s+(.*?)(?:\s{2,}|$)", t, re.S)
        if m:
            return re.sub(r"\s+", " ", m.group(1))[:1000]
    except Exception:
        pass
    finally:
        time.sleep(0.3)
    return None


# --------------------------------------------------------------------------
# bossgi.com - own featured page, "listingBox" cards (same template as tntmergers)
# --------------------------------------------------------------------------

BOSSGI_DOMAIN = "bossgi.com"
# Jeff: flag, don't drop (partly a franchise-concept pitch).
BOSSGI_FLAG = {"4110-00542": "partly a franchise-concept pitch"}


def listingbox_cards(html: str, base_url: str):
    soup = BeautifulSoup(html or "", "html.parser")
    out = []
    for box in soup.select("div.listingBox"):
        a = box.select_one(".listingTitle a[href]")
        if not a:
            continue
        def val(cls):
            el = box.select_one(f".{cls} .descriptionValue")
            return el.get_text(" ", strip=True) if el else ""
        price_el = box.select_one(".priceDescriptionValue")
        badge = box.select_one(".activeButton, .topLeft div")
        out.append({
            "href": urljoin(base_url, a["href"]),
            "title": _clean_title(a.get_text(" ", strip=True)),
            "industry": val("listingIndustry"), "location": val("listingLocation"),
            "internal_id": val("internalID"),
            "price_text": price_el.get_text(" ", strip=True) if price_el else "",
            "badge": badge.get_text(" ", strip=True) if badge else "",
        })
    return out


def handler_bossgi(url, html, broker_name, fetcher):
    domain = _bare(urlparse(url).netloc)
    if domain != BOSSGI_DOMAIN:
        return None
    path = urlparse(url).path.rstrip("/")
    if "buy-a-business" in path:               # the FL MLS feed is never a source
        return None
    cards, seen = [], set()
    for c in listingbox_cards(html, url):
        if c["href"] in seen:
            continue
        seen.add(c["href"])
        cards.append(c)
    if not cards:
        return None
    out = []
    for c in cards:
        st, label = _status(c["badge"], domain)
        if st == "DROP":
            SCOPE_LOG.setdefault(domain, []).append((c["title"], f"badge {c['badge']!r}"))
            continue
        row = _mk(url, broker_name, title=c["title"], detail_url=c["href"],
                  asking_price=money(c["price_text"]) if not re.search(r"n/?a", c["price_text"], re.I) else None,
                  state=None, description=f"{c['industry']} - {c['location']}".strip(" -") or c["title"])
        row["source_listing_id"] = c["internal_id"] or None
        row["status"] = st
        if label:
            row["broker_status_text"] = label
        if c["internal_id"] in BOSSGI_FLAG:
            SCOPE_LOG.setdefault(domain, []).append(
                (c["title"], f"FLAG (kept): {BOSSGI_FLAG[c['internal_id']]}"))
        out.append(row)
    note_displayed(domain, len(cards), "listingBox cards on the featured page",
                   "own page only; /buy-a-business/ MLS feed never read")
    print(f"   bossgi listingBox cards: {len(cards)}")
    return out or None


# --------------------------------------------------------------------------
# blbrokers.com - Salesforce/Angular page, "View More" appends result.page
# --------------------------------------------------------------------------

BL_DOMAIN = "blbrokers.com"


def bl_cards(html: str, base_url: str):
    """Active-list cards only: ng-repeat 'listing in result.page'. The 'You may
    also be interested in' Sold! carousel is result.extraPage and is excluded.
    Each card renders twice (desktop + mobile layout); de-duplicated by href."""
    soup = BeautifulSoup(html or "", "html.parser")
    out, seen = [], set()
    for a in soup.select('a[ng-repeat="listing in result.page"]'):
        href = a.get("href") or ""
        if not href or href in seen:
            continue
        seen.add(href)

        def bind(sub):
            el = a.find(attrs={"ng-bind": re.compile(re.escape(sub))})
            return el.get_text(" ", strip=True) if el else ""
        st_el = a.find(attrs={"ng-if": "listing.status"})
        num = bind("listing.listingNo")
        m = re.search(r"-(\d{3,6})$", href)
        out.append({
            "href": urljoin(base_url, href),
            "title": _clean_title(bind("listing.listingName")),
            "loc": bind("listing.city") or "",
            "price": money(bind("listing.price")) if bind("listing.price") else None,
            "sales": money(bind("listing.totalSales")) if bind("listing.totalSales") else None,
            "income": money(bind("listing.income")) if bind("listing.income") else None,
            "number": num or (m.group(1) if m else ""),
            "status_text": st_el.get_text(" ", strip=True) if st_el else "",
            "teaser": " ".join((a.get_text(" ", strip=True) or "").split())[:600],
        })
    return out


def bl_counter(html: str):
    """(shown, total) from the 'N of M' View More button, else None."""
    t = BeautifulSoup(html or "", "html.parser").get_text(" ", strip=True)
    m = re.search(r"(\d+)\s+of\s+(\d+)\s*[·.\-]*\s*View More", t, re.I)
    return (int(m.group(1)), int(m.group(2))) if m else None


def _bl_expand(fetcher, url, use_proxy):
    """Playwright: click View More until the counter reaches N of N."""
    browser = fetcher._get_browser(use_proxy)
    ua = (_H.get("user_agents") or [None])[0]
    page = browser.new_page(user_agent=ua) if ua else browser.new_page()
    try:
        page.goto(url, timeout=45000, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        for _ in range(12):
            btn = page.locator('[ng-click="next()"]')
            if btn.count() == 0 or not btn.first.is_visible():
                break
            before = page.locator('a[ng-repeat="listing in result.page"]').count()
            # JS click: a cookie banner can sit over the button and time a real click out
            page.evaluate("() => document.querySelector('[ng-click=\"next()\"]').click()")
            try:
                page.wait_for_function(
                    "n => document.querySelectorAll('a[ng-repeat=\"listing in result.page\"]').length > n",
                    arg=before, timeout=12000)
            except Exception:
                break
        page.wait_for_timeout(800)
        return page.content()
    finally:
        page.close()


def handler_blbrokers(url, html, broker_name, fetcher):
    domain = _bare(urlparse(url).netloc)
    if domain != BL_DOMAIN or "bllistings" not in (html or "").lower():
        return None
    full = None
    try:
        full = _bl_expand(fetcher, url, _proxy(domain))
    except Exception as e:
        print(f"   blbrokers expand failed: {type(e).__name__}")
    use = full if full and bl_cards(full, url) else html
    cards = bl_cards(use, url)
    if not cards:
        return None
    ctr = bl_counter(use)
    out = []
    for c in cards:
        st, label = _status(c["status_text"], domain)
        if st == "DROP" or not c["title"]:
            SCOPE_LOG.setdefault(domain, []).append((c["title"] or c["href"], f"status {c['status_text']!r}"))
            continue
        last = [p.strip() for p in c["loc"].split(",")][-1] if c["loc"] else ""
        row = _mk(url, broker_name, title=c["title"], detail_url=c["href"],
                  asking_price=c["price"], cash_flow=c["income"], revenue=c["sales"],
                  state=state_code(last), description=c["teaser"] or c["title"])
        row["source_listing_id"] = c["number"] or None
        row["status"] = st
        if label:
            row["broker_status_text"] = label
        out.append(row)
    if ctr:
        note_displayed(domain, ctr[1], f"{ctr[0]} of {ctr[1]} View More",
                       "after expanding View More" if full else "NOT expanded (first page only)")
    print(f"   blbrokers: {len(cards)} cards, counter {ctr}")
    return out or None


# --------------------------------------------------------------------------
# newleafbrokerage.com - Legacy embed (getlegacyapp.com), paged JSON
# --------------------------------------------------------------------------

NL_DOMAIN = "newleafbrokerage.com"
_NL_PATH_BLOCK = re.compile(r"/cre(/|$)", re.I)       # commercial real estate: never crawled


def legacy_rows(items, base_url, domain):
    """JSON items -> list of (item, status, label). Under Contract stays active."""
    out = []
    for it in items or []:
        st, label = _status(it.get("Status"), domain)
        out.append((it, st, label))
    return out


def _nl_fetch_pages(fetcher, url, use_proxy):
    """Render once with Playwright to learn the public embed call (URL + key header
    are read from the browser's own request, never stored), then page with requests."""
    import requests
    browser = fetcher._get_browser(use_proxy)
    ua = (_H.get("user_agents") or [None])[0]
    page = browser.new_page(user_agent=ua) if ua else browser.new_page()
    caps = []
    page.on("response", lambda r: caps.append(r) if "/embed/" in r.url and "/listing/" in r.url else None)
    try:
        page.goto(url, timeout=45000, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass
        page.wait_for_timeout(1500)
        if not caps:
            return None, None
        r0 = caps[0]
        d0 = json.loads(r0.text())
        hdr = {k: v for k, v in r0.request.headers.items()
               if k.lower() in ("ocp-apim-subscription-key", "accept")}
        base = r0.url.split("?")[0]
    finally:
        page.close()
    items = list(d0.get("ObjListings") or [])
    total = d0.get("TotalCount")
    pages = d0.get("PageCount") or 1
    size = d0.get("PageSize") or 20
    pn = 2
    while pn <= min(int(pages), 20) or (total and len(items) < total and pn <= 20):
        rr = requests.get(base, params={"pageNumber": pn, "pageSize": size}, headers=hdr, timeout=30)
        if rr.status_code != 200 or not rr.text.strip():
            break
        got = (rr.json() or {}).get("ObjListings") or []
        if not got:
            break
        items.extend(got)
        pn += 1
    return items, total


def handler_newleaf(url, html, broker_name, fetcher):
    domain = _bare(urlparse(url).netloc)
    if domain != NL_DOMAIN or "<legacy-listings" not in (html or "").lower():
        return None
    if _NL_PATH_BLOCK.search(urlparse(url).path):
        return None
    items, total = _nl_fetch_pages(fetcher, url, _proxy(domain))
    if not items:
        return None
    base = url.split("#")[0]
    seen, out = set(), []
    for it, st, label in legacy_rows(items, base, domain):
        guid = it.get("Guid")
        if not guid or guid in seen:
            continue
        seen.add(guid)
        if st == "DROP":
            SCOPE_LOG.setdefault(domain, []).append((it.get("Title"), f"status {it.get('Status')!r}"))
            continue
        row = _mk(url, broker_name, title=_clean_title(it.get("Title")),
                  detail_url=f"{base}#legacy-listing={guid}",
                  asking_price=it.get("Price"), cash_flow=it.get("Cashflow"),
                  state=state_code(it.get("State")), description=(it.get("Description") or it.get("Title") or "")[:1000])
        row["source_listing_id"] = guid
        # a '#legacy-listing=<guid>' fragment is identity, not a per-listing page
        # (same rule as the synthetic '#dl-' urls)
        row["url_is_listing_specific"] = False
        row["status"] = st
        if label:
            row["broker_status_text"] = label
        # "Price on request" is the widget's wording for a withheld price
        # (Jeff, 22 Nov); the JSON carries Price null for those cards.
        # (the phrase is Jeff's reading of the rendered card; my headless render could
        # not see the widget text, so the null-price = "Price on request" link is unproven)
        if row["asking_price"] is None:
            row["price_not_disclosed"] = True
            row["broker_price_text"] = "Price on request"
        out.append(row)
    for it in items:
        if re.search(r"fintech|proprietary tech|vendor marketplace", it.get("Title") or "", re.I):
            SCOPE_LOG.setdefault(domain, []).append((it.get("Title"), "FLAG (kept): borderline digital/startup per Jeff"))
    note_displayed(domain, total if total else len(seen), f"TotalCount {total}",
                   f"getlegacyapp embed API, {len(items)} item(s)")
    print(f"   NewLeaf Legacy embed: {len(items)} items, total {total}, {len(out)} emitted")
    return out or None


# --------------------------------------------------------------------------
# 7cint.com - /Buying/Filter JSON
# --------------------------------------------------------------------------

C7_DOMAIN = "7cint.com"
C7_LISTING_TYPES = {1: "Established Business for Sale", 2: "Asset Sale",
                    3: "Business Real Estate for Sale", 4: "Business Real Estate for Lease",
                    5: "Miscellaneous Listing"}
_BUILDING_ONLY_RE = re.compile(
    r"^\s*(?:stand[\s-]?alone|vacant|commercial)?\s*(?:building|land|lot|property|parcel)\s*$", re.I)


def c7_scope(item):
    """None = in scope, else the reason it is left out (logged, never silent)."""
    head = (item.get("ListingHeadline") or "").strip()
    if not head:
        return "no headline (the site's own page skips these)"
    if item.get("ListingType") == 4:
        return "Business Real Estate for Lease (not a business for sale)"
    if _BUILDING_ONLY_RE.match(head):
        return "building/land only"
    return None


def c7_row(item, index_url, broker_name, domain):
    head = _clean_title(item.get("ListingHeadline"))
    loc = (item.get("LocationConfidentialityExposure") or "").strip()
    city = state = None
    if loc:
        # "Austin, Travis-Texas" -> city Austin, state TX
        parts = [p.strip() for p in loc.split(",")]
        city = parts[0] or None
        if len(parts) > 1 and "-" in parts[-1]:
            state = state_code(parts[-1].rsplit("-", 1)[1])
    desc = BeautifulSoup((item.get("Description") or "").replace("<br />", " ").replace("<br>", " "),
                         "html.parser").get_text(" ", strip=True)
    p = urlparse(index_url)
    detail = f"{p.scheme}://{p.netloc}/buying/details?name={quote(item.get('ListingHeadline').strip())}"
    row = _mk(index_url, broker_name, title=head, detail_url=detail,
              asking_price=item.get("AskingPrice"), cash_flow=money(item.get("CashFlow")),
              revenue=money(item.get("GrossRevenue")), city=city, state=state,
              description=(desc or head)[:1000])
    row["source_listing_id"] = str(item.get("Id")) if item.get("Id") is not None else None
    row["status"] = "active"
    return row


def handler_7cint(url, html, broker_name, fetcher):
    domain = _bare(urlparse(url).netloc)
    if domain != C7_DOMAIN or "/buying/filter" not in (html or "").lower():
        return None
    p = urlparse(url)
    ep = f"{p.scheme}://{p.netloc}/Buying/Filter"
    items, page, total = [], 1, None
    while page <= 30:
        try:
            r = fetcher.session.get(ep, params={"industry": "", "type": "", "searchQuery": "",
                                                "page": page, "pageSize": 50, "clearAll": "true"},
                                    timeout=30)
            if r.status_code != 200:
                break
            d = r.json()
        except Exception:
            break
        if not d.get("success"):
            break
        data = d.get("data") or []
        total = d.get("totalRecords", total)
        if not data:
            break
        items.extend(data)
        if total and len(items) >= total:
            break
        page += 1
    if not items:
        return None
    out, names = [], {}
    for it in items:
        why = c7_scope(it)
        if why:
            SCOPE_LOG.setdefault(domain, []).append((it.get("ListingHeadline") or f"Id {it.get('Id')}", why))
            continue
        row = c7_row(it, url, broker_name, domain)
        k = row["url"]
        if k in names:                       # two listings, one headline: keep both by Id
            row = c7_row(it, url, broker_name, domain)
            row["url"] = f"{k}&id={it.get('Id')}"
            row["id"] = _H["stable_id"](row["title"], row["url"], url, urlparse(url).netloc)
        names[k] = 1
        out.append(row)
    flagged = [it for it in items if it.get("ListingType") == 3]
    for it in flagged:
        SCOPE_LOG.setdefault(domain, []).append(
            (it.get("ListingHeadline"), "FLAG (kept): type 'Business Real Estate for Sale' with the business (RealEstate included)"))
    note_displayed(domain, len(out), f"{total} records from /Buying/Filter",
                   f"in-scope count after exclusions; {len(items) - len(out)} left out")
    print(f"   7cint Buying/Filter: {len(items)} records, {len(out)} in scope")
    return out or None


# --------------------------------------------------------------------------
# registration (V6 hook)
# --------------------------------------------------------------------------

HANDLERS = [
    ("dealrelations.com/listing_feeds", handler_dealrelations),
    ("bbms.info", handler_bbf),
    ('class="listingbox"', handler_bossgi),
    ("bllistings", handler_blbrokers),
    ("<legacy-listings", handler_newleaf),
    ("/buying/filter", handler_7cint),
]


def install(api_handlers: list, helpers: dict) -> None:
    """Append this module's handlers to V6's API_HANDLERS (idempotent) and keep
    the V6 helpers the readers need: make (_make_api_listing), status
    (_ls_lookup), needs_proxy, user_agents, stable_id."""
    _H.update(helpers)
    have = {id(h) for _s, h in api_handlers}
    for sig, fn in HANDLERS:
        if id(fn) not in have:
            api_handlers.append((sig, fn))
