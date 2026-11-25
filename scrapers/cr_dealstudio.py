"""
cr_dealstudio - card reader for Deal Studio (Business Brokerage Press) broker sites.

Detected by PAGE MARKERS, never a domain list: the page source carries
"deal-studio.com" assets, "Site Design by Deal Studio" or "Business Brokerage
Press", AND the page holds Deal Studio listing cards (div.listingBox whose
title links to /listing/<slug>/). Both are required, so a broker that merely
credits the agency in a footer but has no card grid does not engage.

Per card:
  url          the /listing/<slug>/ detail link
  title        card title
  price        ONLY the .priceDescriptionValue whose label (priceDescriptionName)
               is absent or reads Price / Asking Price. SDE, revenue, EBITDA
               and the like are never the price. (mainascent wraps its Price in
               a div.sde - the label, not the wrapper, decides.) "N/A" -> None.
  source_listing_id  the card's Internal ID / Reference ID (identity never uses
               price, principle 4; the row id itself comes from the detail URL).
  status       broker badge through V6's listing_status table: SOLD -> sold,
               UNDER CONTRACT / PENDING -> active + label, COMING SOON -> not
               emitted. ACTIVE / NEW -> active.

Financials: SDE / Cash Flow -> cash_flow and Gross Revenue / Revenue -> revenue from
the card's labelled fields; where the card has none, one detail GET per card through
V6's enrich_from_detail (what the generic path did), so no stored financial is blanked.

Pagination: the site's own (Toolset Views wpv_paged links, rel=next,
a.next.page-numbers). A page that adds no new detail URL ends the walk.

Pure parsing in read_cards(); V6 hook = install().
"""
from __future__ import annotations

import random
import re
import time
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

import cr_w3_platforms as _w3

SIGNATURES = ("deal-studio.com", "site design by deal studio", "business brokerage press")
MAX_PAGES = 25
_H: dict = {}          # V6 helpers, set by install(): enrich, delay, max_details
SCOPE_LOG: dict[str, list] = {}


def is_deal_studio(html: str) -> bool:
    low = (html or "").lower()
    return any(s in low for s in SIGNATURES)


def _price_ok(label: str) -> bool:
    l = re.sub(r"[^a-z ]", "", (label or "").lower()).strip()
    return l in ("", "price", "asking price", "listing price", "list price")


_CF_LABELS = {"sde", "cash flow", "seller discretionary earnings", "seller's discretionary earnings",
              "adjusted cash flow"}
_REV_LABELS = {"gross revenue", "revenue", "annual revenue", "gross sales", "annual sales"}


def _fin_fields(box) -> dict:
    """Labelled card figures: SDE/Cash Flow -> cash_flow, Gross Revenue/Revenue ->
    revenue. The label decides (never the wrapper class, never cross-assigned);
    EBITDA and everything else is ignored. "N/A" / blank -> not set."""
    out = {}
    for name in box.select(".listingContent .descriptionName"):
        lab = re.sub(r"[^a-z' ]", "", name.get_text(" ", strip=True).lower()).strip()
        key = "cash_flow" if lab in _CF_LABELS else "revenue" if lab in _REV_LABELS else None
        val = name.find_next_sibling(class_="descriptionValue")
        if key and val and key not in out:
            txt = val.get_text(" ", strip=True)
            if txt and not re.search(r"n/?a", txt, re.I):
                v = _w3.money(txt)
                if v:
                    out[key] = v
    return out


def read_cards(html: str, base_url: str):
    """-> list of card dicts (deduplicated by detail href), [] when none."""
    soup = BeautifulSoup(html or "", "html.parser")
    out, seen = [], set()
    for box in soup.select("div.listingBox"):
        a = box.select_one(".listingTitle a[href]")
        if not a:
            continue
        href = urljoin(base_url, a["href"])
        if "/listing/" not in urlparse(href).path or href in seen:
            continue
        seen.add(href)

        def val(cls):
            el = box.select_one(f".{cls} .descriptionValue")
            return el.get_text(" ", strip=True) if el else ""
        price_text, price_label = "", ""
        pv = box.select_one(".priceDescriptionValue")
        if pv:
            pn = pv.find_previous_sibling(class_="priceDescriptionName")
            price_label = pn.get_text(" ", strip=True) if pn else ""
            if _price_ok(price_label):
                price_text = pv.get_text(" ", strip=True)
        badges = [d.get_text(" ", strip=True) for d in box.select(".topLeft div")]
        status_badge = next((b for b in badges if b and b.upper() != "NEW"), "")
        out.append({
            "href": href,
            "title": _w3._clean_title(a.get_text(" ", strip=True)),
            "industry": val("listingIndustry"), "location": val("listingLocation"),
            "internal_id": val("internalID"),
            "price_text": price_text, "price_label": price_label,
            "badge": status_badge,
            "fin": _fin_fields(box),
        })
    return out


def next_page_urls(html: str, base_url: str) -> list[str]:
    """The site's own pager links (same path), in page order, no duplicates."""
    soup = BeautifulSoup(html or "", "html.parser")
    found, base_path = [], urlparse(base_url).path.rstrip("/")
    cands = [a["href"] for a in soup.select("a[href]")
             if "wpv_paged=" in a["href"] or "page-numbers" in " ".join(a.get("class") or [])
             or "next" in (a.get("rel") or [])]
    for h in cands:
        u = urljoin(base_url, h.replace("&amp;", "&"))
        p = urlparse(u)
        if p.path.rstrip("/").split("/page/")[0] != base_path:
            continue
        if u not in found and u != base_url:
            found.append(u)
    def key(u):
        m = re.search(r"wpv_paged=(\d+)|/page/(\d+)", u)
        return int(next(g for g in m.groups() if g)) if m else 0
    return sorted(found, key=key)


def _enrich_missing_financials(rows, fetcher, domain):
    """Cards often carry no SDE / revenue (tbxllc.com, most sold stubs). The generic
    V6 path read them from each detail page; this handler returns before that phase,
    so do the same here: one plain GET per card still lacking cash_flow or revenue,
    through V6's own ListingExtractor.enrich_from_detail, capped at MAX_DETAIL_PAGES
    (the same per-broker cap and per-fetch delay the generic path uses). The card's
    own asking_price is authoritative and restored afterwards (labelled Price only).
    NOTE: this is one detail GET per such card on every crawl; the generic path
    skipped listings it already held (stored-details reuse), this one cannot."""
    enrich, fetch = _H.get("enrich"), getattr(fetcher, "_fetch_http", None)
    if not (enrich and fetch):
        return
    todo = [r for r in rows if not (r.get("cash_flow") and r.get("revenue"))]
    todo = todo[: _H.get("max_details", 60)]
    done = 0
    for r in todo:
        try:
            time.sleep(random.uniform(*_H.get("delay", (0.5, 1.0))))
            html = fetch(r["url"], use_proxy=False)
            if not html:
                continue
            price, status, label = r.get("asking_price"), r.get("status"), r.get("broker_status_text")
            enrich(r, html)
            r["asking_price"], r["status"] = price, status
            if label:
                r["broker_status_text"] = label
            r["url"] = r["url"]
            done += 1
        except Exception:
            continue
    if todo:
        print(f"   Deal Studio detail pages read for missing SDE/revenue: {done}/{len(todo)}")


def handler_dealstudio(url, html, broker_name, fetcher):
    if not is_deal_studio(html):
        return None
    domain = _w3._bare(urlparse(url).netloc)
    if domain == _w3.BOSSGI_DOMAIN:          # has its own reader (MLS feed excluded)
        return None
    cards = read_cards(html, url)
    if not cards:
        return None
    seen = {c["href"] for c in cards}
    queue, visited, pages = next_page_urls(html, url), {url}, 1
    while queue and pages < MAX_PAGES:
        nxt = queue.pop(0)
        if nxt in visited:
            continue
        visited.add(nxt)
        page_html = _w3._get(fetcher, nxt, domain)
        if not page_html:
            break
        pages += 1
        new = [c for c in read_cards(page_html, nxt) if c["href"] not in seen]
        if not new:
            continue                          # a repeated page is not more data
        for c in new:
            seen.add(c["href"])
        cards.extend(new)
        for u in next_page_urls(page_html, nxt):
            if u not in visited and u not in queue:
                queue.append(u)
    out = []
    for c in cards:
        st, label = _w3._status(c["badge"], domain)
        if st == "DROP":
            SCOPE_LOG.setdefault(domain, []).append((c["title"], f"badge {c['badge']!r}"))
            continue
        price = _w3.money(c["price_text"]) if c["price_text"] and not re.search(r"n/?a", c["price_text"], re.I) else None
        row = _w3._mk(url, broker_name, title=c["title"], detail_url=c["href"], asking_price=price,
                      state=_w3.state_code(c["location"]) if c["location"] else None,
                      description=f"{c['industry']} - {c['location']}".strip(" -") or c["title"])
        row["source_listing_id"] = c["internal_id"] or None
        for f, v in c["fin"].items():
            row[f] = v
        row["status"] = st
        if label:
            row["broker_status_text"] = label
        out.append(row)
    _enrich_missing_financials(out, fetcher, domain)
    _w3.note_displayed(domain, len(cards), "Deal Studio listing cards (all pages)",
                       f"{pages} page(s) read")
    untitled = len(BeautifulSoup(html or "", "html.parser").select("div.listingBox")) - len(
        BeautifulSoup(html or "", "html.parser").select("div.listingBox .listingTitle a[href]"))
    if untitled:
        SCOPE_LOG.setdefault(domain, []).append(("untitled cards (sold stubs)", str(untitled)))
    print(f"   Deal Studio cards: {len(cards)} on {pages} page(s), {len(out)} emitted"
          + (f"; {untitled} untitled card(s) on page 1 not emitted" if untitled else ""))
    return out or None


def install(api_handlers: list, helpers: dict | None = None) -> None:
    """Append one entry per marker signature (idempotent); keep V6 helpers."""
    _H.update(helpers or {})
    have = {id(h) for _s, h in api_handlers}
    if id(handler_dealstudio) in have:
        return
    for sig in SIGNATURES:
        api_handlers.append((sig, handler_dealstudio))
