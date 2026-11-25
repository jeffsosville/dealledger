"""rv-dealrelations (2026-11-24): reader for DEALRELATIONS-HOSTED broker sites
(<broker>.dealrelations.com/listings and the /listing_feeds/<id> embed of the
same list).

Why it exists. V6's generic card path read these pages with the largest dollar
figure as the price, so asking_price = Revenue on most Brio rows, and it never
got past page 1 (page 2+ of the hosted /listings sits behind a Cloudflare
managed challenge: HTTP 403, cf-mitigated: challenge). Then the crawl closed
'ok' because the counted figure (15 cards) matched the 15 it had seen, and the
absence rule retired every row on pages 2-3 (preschoolbusinesssolutions: 34).

What this reader does
  * price   = "Asking Price"; revenue = "Revenue" (Undisclosed -> None);
    cash_flow = "Adjusted EBITDA" / "Cash Flow" / "SDE" ("Undisclosed" -> None).
    The row has no cash-flow-basis field, so an EBITDA basis is NOT recorded
    anywhere (see notes).
  * identity (source_listing_id) = "Reference: Bxxxxx".
  * title from the card heading (no "<Broker Name> | " prefix; a leading
    "<broker> | " or "| " is stripped if one is ever present).
  * pagination: follow rel=next until a page adds no new Reference.
  * never works around a block: pages >= 2 are fetched with plain `requests`,
    no proxy, no browser impersonation. A challenge/403 ends the walk and the
    crawl is declared INCOMPLETE through the broker-count channel
    (cr_w3_platforms.note_displayed): the stated count is the pager's upper
    bound (last page number x cards per page), so crawl_run closes 'partial'
    and the absence rule (which only reads 'ok' runs) cannot fire.
  * FEED_IDS: the broker's own embed feed (/listing_feeds/<id>) is an ordinary
    200 page with the same cards. Where its id is already public in our stored
    URLs (item_id=...) the walk uses it, and row urls keep the feed's
    "?item_id=<id>&from=listing_feeds#top_registration_form" form so ids stay
    those of the stored rows (id = sha256(url); principle 4).
"""
from __future__ import annotations

import re
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

import cr_w3_platforms as _w3

# Domains this reader is allowed to take. Brio is the known-good test; the rest
# are added only after the scope check shows the same template (see notes).
RV_DR_DOMAINS = frozenset({
    "briobrokers.dealrelations.com",
    "preschoolbusinesssolutions.dealrelations.com",
})
# Public feed ids (item_id in the stored urls of the same domain).
FEED_IDS = {"briobrokers.dealrelations.com": "5092"}

MAX_PAGES = 25
_REF_RE = re.compile(r"Reference:\s*([A-Za-z0-9._-]+)", re.I)
_CF_MARKERS = ("just a moment", "cf-mitigated", "challenges.cloudflare.com", "attention required")
_CASH_LABELS = ("adjusted ebitda", "ebitda", "cash flow", "sde", "seller", "profit", "net income")


def _bare(host):
    return _w3._bare(host)


def is_challenge(status, text) -> bool:
    low = (text or "")[:6000].lower()
    return status in (403, 429, 503) or any(m in low for m in _CF_MARKERS)


def clean_title(title, broker_name="") -> str:
    t = re.sub(r"\s+", " ", str(title or "")).strip()
    if broker_name:
        t = re.sub(r"^" + re.escape(broker_name.strip()) + r"\s*\|\s*", "", t, flags=re.I)
    return re.sub(r"^\|\s*", "", t).strip()


def parse_page(html: str):
    """-> (cards, pager) for one hosted/feed page.

    card: title, href, status_text, labels{lowercase label: value text}, reference,
          teaser, location.  pager: {"pages": highest page number shown,
          "has_next": bool, "next_href": str|None}."""
    soup = BeautifulSoup(html or "", "html.parser")
    cards = []
    for li in soup.select("ul.listings > li"):
        el = li.select_one(".listings-element") or li
        a = el.select_one(".listings-title a") or el.select_one("a.listing-link")
        if not a or not a.get("href"):
            continue
        labels = {}
        for t, v in zip(el.select(".listing-financials-text"), el.select(".listing-financials-value")):
            labels[t.get_text(" ", strip=True).rstrip(":").strip().lower()] = v.get_text(" ", strip=True)
        bl = el.select_one(".bottom-left-cell")
        m = _REF_RE.search(bl.get_text(" ", strip=True)) if bl else None
        txt = el.select_one(".listings-text")
        teaser = re.sub(r"\s*\.\.\.\s*More details.*$", "", txt.get_text(" ", strip=True), flags=re.I) if txt else ""
        loc = next((d.get_text(" ", strip=True) for d in el.select(".inside-top-row div.top-row")
                    if d.get_text(strip=True)), "")
        st = el.select_one(".right-top-row")
        cards.append({
            "title": a.get_text(" ", strip=True), "href": a["href"],
            "status_text": st.get_text(" ", strip=True) if st else "",
            "labels": labels, "reference": m.group(1) if m else None,
            "teaser": teaser, "location": loc,
        })
    pages, nxt = 1, None
    for a in soup.select(".pagination a"):
        t = a.get_text(strip=True)
        if t.isdigit():
            pages = max(pages, int(t))
        if "next" in (a.get("rel") or []) or t.lower().startswith("next"):
            nxt = nxt or a.get("href")
    return cards, {"pages": pages, "has_next": bool(nxt), "next_href": nxt}


def _plain_get(url):
    """(status, text) with ordinary headers. No proxy, no impersonation."""
    ua = (_w3._H.get("user_agents") or ["Mozilla/5.0"])[0]
    try:
        r = requests.get(url, headers={"User-Agent": ua, "Accept": "text/html"}, timeout=30)
        return r.status_code, r.text
    except Exception as e:                      # network error: treat as unreachable
        return 0, str(e)


def walk(first_html, first_url, base_for_pages, get=None, sleep=0.5):
    """Walk the pager from page 1.

    Returns (cards, info). info: pages_seen, pager_pages, per_page,
    complete (bool), stop (reason)."""
    get = get or _plain_get
    cards, seen = [], set()
    html, page = first_html, 1
    info = {"pages_seen": 0, "pager_pages": 1, "per_page": None, "complete": True, "stop": "last page"}
    while html and page <= MAX_PAGES:
        pc, pager = parse_page(html)
        info["pages_seen"] += 1
        info["pager_pages"] = max(info["pager_pages"], pager["pages"])
        info["per_page"] = info["per_page"] or len(pc)
        new = 0
        for c in pc:
            key = c["reference"] or c["href"].split("?")[0].split("#")[0]
            if key in seen:
                continue
            seen.add(key)
            cards.append(c)
            new += 1
        if not pager["has_next"]:
            break
        if not new:
            info["stop"] = f"page {page} added no new Reference"
            break
        page += 1
        if sleep:
            time.sleep(sleep)
        status, text = get(f"{base_for_pages}?page={page}")
        if status != 200 or is_challenge(status, text):
            info["complete"] = False
            info["stop"] = (f"page {page} blocked (HTTP {status}, Cloudflare challenge)"
                            if is_challenge(status, text) else f"page {page} unreachable (HTTP {status})")
            break
        html = text
    else:
        if html:
            info["complete"] = False
            info["stop"] = f"MAX_PAGES {MAX_PAGES}"
    return cards, info


def _row_url(href, page_url, domain):
    full = urljoin(page_url, href.replace("&amp;", "&"))
    feed = FEED_IDS.get(domain)
    if feed:                                     # keep the stored feed identity
        slug = re.search(r"/listings/([^/?#]+)", urlparse(full).path)
        if slug:
            return f"https://{domain}/listings/{slug.group(1)}?item_id={feed}&from=listing_feeds#top_registration_form"
    return full


def cash_flow_of(labels):
    for lab, val in labels.items():
        if any(k in lab for k in _CASH_LABELS):
            return _w3.money(val), lab
    return None, None


def rows_from_cards(cards, url, broker_name, domain):
    out = []
    for c in cards:
        st, label = _w3._status(c["status_text"], domain)
        if st == "DROP" or not c["title"]:
            _w3.SCOPE_LOG.setdefault(domain, []).append((c["title"] or c["reference"], f"status {c['status_text']!r}"))
            continue
        labels = c["labels"]
        price = _w3.money(next((v for k, v in labels.items() if k.startswith("asking")), None))
        rev = _w3.money(next((v for k, v in labels.items() if k.startswith("revenue")), None))
        cf, _lab = cash_flow_of(labels)
        loc = c["location"]
        row = _w3._mk(url, broker_name, title=clean_title(c["title"], broker_name),
                      detail_url=_row_url(c["href"], url, domain),
                      asking_price=price, cash_flow=cf, revenue=rev,
                      state=_w3.state_code(loc.split(",")[-1].strip()) if loc else None,
                      city=loc.split(",")[0].strip() if loc and "," in loc else None,
                      description=c["teaser"] or c["title"])
        row["source_listing_id"] = c["reference"]
        row["status"] = st
        if label:
            row["broker_status_text"] = label
        out.append(row)
    return out


def handler_rv_dealrelations(url, html, broker_name, fetcher):
    domain = _bare(urlparse(url).netloc)
    if domain not in RV_DR_DOMAINS:
        return None
    if "listings-financials1" not in (html or ""):
        return None
    base = f"https://{urlparse(url).netloc}/listings"
    cards1, _p = parse_page(html)
    if not cards1:
        return None
    feed = FEED_IDS.get(domain)
    first_html, base_pages, src = html, base, "hosted /listings"
    if feed:                                      # plain-200 embed feed of the same list
        feed_url = f"https://{urlparse(url).netloc}/listing_feeds/{feed}"
        status, text = _plain_get(feed_url)
        if status == 200 and not is_challenge(status, text) and parse_page(text)[0]:
            first_html, base_pages, src = text, feed_url, f"feed {feed}"
    cards, info = walk(first_html, url, base_pages)
    rows = rows_from_cards(cards, url, broker_name, domain)
    pp = info["pager_pages"]
    if info["complete"]:
        displayed, text = len(cards), f"pager: {pp} page(s), all read ({src})"
    else:
        displayed = max(pp * (info["per_page"] or len(cards)), len(cards) + 1)
        text = f"pager shows {pp} page(s); {info['stop']} ({src}); count is an upper bound"
    _w3.note_displayed(domain, displayed, text, f"{info['pages_seen']} page(s) read; {len(cards)} cards")
    for c in cards:
        if re.search(r"NNN Lease|investment property", c["title"], re.I):
            _w3.SCOPE_LOG.setdefault(domain, []).append(
                (c["title"], "FLAG (kept, held by needs_review): real-estate-only per Jeff 23 Nov"))
    print(f"   DealRelations hosted: {len(cards)} cards over {info['pages_seen']}/{pp} page(s) via {src}; "
          f"{'COMPLETE' if info['complete'] else 'INCOMPLETE: ' + info['stop']}")
    return rows or None


HANDLERS = [("listings-financials1", handler_rv_dealrelations)]


def install(api_handlers: list) -> None:
    have = {id(h) for _s, h in api_handlers}
    for sig, fn in HANDLERS:
        if id(fn) not in have:
            api_handlers.append((sig, fn))
