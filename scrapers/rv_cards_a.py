"""
rv_cards_a - card fields read by LABEL, never "first $ amount" (job-review
2026-11-24, task rv-cards-a).

Four brokers whose cards are read by the generic V6 detector, which mixes the
card's money fields (or its title) up:

  b3brokers.com        WP-Views "search results" template: Listing ID / Total
  coastalbizbrokers.com  Sales (or Cash Flow) / Price. Price comes from the
                       "Price:" label, Total Sales is REVENUE, Cash Flow is cash
                       flow. Identity = Listing ID. One parser for both.
                       b3: CRE cards (land / building, Total Sales N/A) are out of
                       scope and gated by a template-scoped test.
                       coastal: a license-only card is dropped.
  www.biz-match.com    one line "Price: $X Sales Revenue: $Y|Undisclosed|Not
                       applicable Net Profit: $Z|Undisclosed", three labelled
                       regexes. Undisclosed / Not applicable -> None. The
                       description is never searched for dollars.
  cbb-tristate.com     GeoDirectory cards: the heading is the title (the badge
                       over the image is the CATEGORY), identity = "Listing ID",
                       price = "Asking Price (USD)", "Cash Flow", Site Type kept.

Wired as one hook in dealledger_scraper_v6.extract_via_api(), ahead of the
generic detector. The readers are pure functions of (url, html, fetcher, ctx);
ctx is the same row-builder namespace cr_card_fields uses, so ids are V6's own
(url identity, principle 4). Nothing here touches the database.
"""
from __future__ import annotations

import re
from types import SimpleNamespace
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

try:
    from cr_card_fields import _price_fields, _fin, _clean, _bare, DROP
except Exception:                        # pragma: no cover
    from scrapers.cr_card_fields import _price_fields, _fin, _clean, _bare, DROP  # type: ignore
try:
    from listing_status import lookup_status
except Exception:                        # pragma: no cover
    lookup_status = None

# (host, path prefix) each reader is scoped to. b3's home page and blog are not
# the card page; coastal's /all-listings is the NEBBA Plus MLS and must never match.
SCOPE = {
    "b3brokers.com": "/businesses-for-sale",
    "coastalbizbrokers.com": "/featured-businesses-for-sale",
    "biz-match.com": "/listings",
    "cbb-tristate.com": "/places",
}


def _soup(html):
    return BeautifulSoup(html or "", "html.parser")


def in_scope(url):
    p = urlparse(url)
    prefix = SCOPE.get(_bare(p.netloc))
    return bool(prefix) and p.path.rstrip("/").lower().startswith(prefix)


# --------------------------------------------------------------------------
# status: badge over the image + title marker, most final wins
# --------------------------------------------------------------------------
_RANK = {"active": 0, "inactive": 1, DROP: 2, "sold": 3}


def _apply_status(row, texts, title, domain, ctx):
    hits = []
    for t in texts:
        if t and lookup_status is not None:
            h = lookup_status(t, domain)
            if h:
                hits.append(h)
    h = ctx.title_status(title, domain)
    if h:
        hits.append(h)
    hits = [h for h in hits if h[0] in _RANK]
    if not hits:
        return None
    status, label = max(hits, key=lambda h: (_RANK[h[0]], bool(h[1])))   # the labelled hit wins a tie
    if status == DROP:
        return DROP
    row["status"] = status
    if label:
        row["broker_status_text"] = label
    return None


# --------------------------------------------------------------------------
# b3brokers.com + coastalbizbrokers.com: WP-Views listing-box template
# --------------------------------------------------------------------------
# Template-scoped real-estate gate. A b3 card is land/building when its Total
# Sales is N/A (a business has sales) AND its title says so. Gating on the
# N/A alone would drop "Established South American Restaurant with Real
# Estate" (N/A sales, a restaurant), and a title word alone would drop
# "...with Real Estate" restaurants; both together is the CRE card.
_CRE_TITLE = re.compile(
    r"\bCRE\b|\bcommercial\s+(?:site|land|lot|building|property)\b|\bzoned\b|"
    r"\b(?:industrial|commercial)\s+(?:yard|parcel|lot)\b|\bvacant\s+land\b|\bacres?\b",
    re.I)
_OPERATING = re.compile(
    r"\b(?:restaurant|bar|grill|cafe|café|store|shop|hotel|motel|franchise|"
    r"business(?:es)?\s+for\s+sale|salon|spa|laundromat|car\s*wash|gas\s+station)\b", re.I)
_LICENSE_ONLY = re.compile(r"^\s*(?:\w+\s+){0,2}liquor\s+license\s*$", re.I)


def is_cre_card(title, sales_text, industry):
    """True for a b3 land/building card. Needs ALL of: no sales figure, a CRE
    title, and an industry of Retail / blank (never Restaurant and Food etc.)."""
    if _fin(sales_text):
        return False
    if not _CRE_TITLE.search(title or ""):
        return False
    ind = (industry or "").strip().lower()
    return ind in ("", "retail", "real estate", "commercial real estate")


def is_license_only(title, industry, description):
    """A coastal card that sells only a license: the whole title is the license."""
    return bool(_LICENSE_ONLY.match(_clean(title)))


def _box_fields(box):
    f = {}
    for lab in box.select("span.description-name"):
        val = lab.find_next_sibling("span")
        f[_clean(lab.get_text()).rstrip(":").lower()] = _clean(val.get_text(" ")) if val else ""
    return f


def read_listing_boxes(url, html, name, fetcher, ctx):
    soup = _soup(html)
    domain = _bare(urlparse(url).netloc)
    rows, drops, seen = [], [], set()
    for box in soup.select("div.listing-box"):
        a = box.select_one(".listing-title a[href]")
        if not a:
            continue
        title = _clean(a.get_text(" "))
        detail = ctx.norm_url(urljoin(url, a["href"]))
        if not title or detail in seen:
            continue
        seen.add(detail)
        f = _box_fields(box)
        price = box.select_one(".price-description-value")
        excerpt = box.select_one(".listing-excerpt")
        desc = _clean(excerpt.get_text(" ")) if excerpt else title
        if domain == "b3brokers.com" and is_cre_card(title, f.get("total sales"), f.get("industry")):
            drops.append((title[:80], "real estate (CRE) card"))
            continue
        if domain == "coastalbizbrokers.com" and is_license_only(title, f.get("industry"), desc):
            drops.append((title[:80], "license only"))
            continue
        loc = f.get("location") or ""
        row = ctx.make_row(url, name, title=title, detail_url=detail,
                           revenue=_fin(f.get("total sales")),
                           cash_flow=_fin(f.get("cash flow")),
                           state=ctx.state_from_location(loc) or ctx.state_from_location(title),
                           description=desc)
        _price_fields(row, price.get_text(" ") if price else "")
        if f.get("listing id"):
            row["source_listing_id"] = f["listing id"]
        badges = [_clean(x.get_text(" ")) for x in box.select(".image-top-left div")
                  if x.get_text(strip=True)]
        if _apply_status(row, badges, title, domain, ctx) == DROP:
            drops.append((title[:80], "status"))
            continue
        rows.append(row)
    return rows, drops


# --------------------------------------------------------------------------
# www.biz-match.com
# --------------------------------------------------------------------------
_BM_PRICE = re.compile(r"Price:\s*(\$[\d,]+(?:\.\d+)?|Undisclosed|Not applicable)", re.I)
_BM_REV = re.compile(r"Sales Revenue:\s*(\$[\d,]+(?:\.\d+)?|Undisclosed|Not applicable)", re.I)
_BM_NET = re.compile(r"Net Profit:\s*(\$[\d,]+(?:\.\d+)?|Undisclosed|Not applicable)", re.I)


def parse_bizmatch_line(line):
    """(price, revenue, net_profit) from the card's one financial line. A figure
    that reads Undisclosed / Not applicable is None; nothing outside the three
    labelled fields is ever looked at."""
    def one(rx):
        m = rx.search(line or "")
        return _fin(m.group(1)) if m and m.group(1).startswith("$") else None
    return one(_BM_PRICE), one(_BM_REV), one(_BM_NET)


def _bizmatch_pages(url, html, fetcher):
    pages, seen = [(url, html)], {url.rstrip("/")}
    queue = [html]
    while queue:
        soup = _soup(queue.pop(0))
        for a in soup.select("a[href]"):
            m = re.search(r"/listings-(\d+)/?$", a["href"])
            if not m:
                continue
            nxt = urljoin(url, a["href"])
            if nxt.rstrip("/") in seen:
                continue
            seen.add(nxt.rstrip("/"))
            h = fetcher._fetch_http(nxt)
            if h:
                pages.append((nxt, h))
                queue.append(h)
    return pages


def read_bizmatch(url, html, name, fetcher, ctx):
    rows, drops, seen = [], [], set()
    for page_url, page in _bizmatch_pages(url, html, fetcher):
        for dl in _soup(page).select("dl.listings"):
            a = dl.select_one("dt a[href]")
            if not a:
                continue
            title = _clean(a.get_text(" "))
            detail = ctx.norm_url(urljoin(page_url, a["href"]))
            if detail in seen:
                continue
            seen.add(detail)
            dds = dl.find_all("dd")
            # the location is the first dd with no class ("Price Reduced" and the
            # money lines carry one)
            loc = next((_clean(d.get_text(" ")) for d in dds if not d.get("class")), "")
            line = _clean(" ".join(d.get_text(" ") for d in dl.select("dd.inline")))
            price, revenue, net = parse_bizmatch_line(line)
            dd = dl.select_one("dd.description")
            desc = _clean(dd.get_text(" ")) if dd else title
            row = ctx.make_row(url, name, title=title, detail_url=detail,
                               revenue=revenue, cash_flow=net,
                               state=ctx.state_from_location(loc), description=desc)
            if price:
                row["asking_price"] = price
            rows.append(row)
    return rows, drops


# --------------------------------------------------------------------------
# cbb-tristate.com (GeoDirectory)
# --------------------------------------------------------------------------
def _gd_fields(card):
    f = {}
    for m in card.select("div.geodir_post_meta"):
        t = m.select_one(".geodir_post_meta_title")
        if not t:
            continue
        key = _clean(t.get_text()).rstrip(":").lower()
        t_txt = t.get_text()
        # value = the meta block's text after the label
        full = _clean(m.get_text(" "))
        lab = _clean(t_txt)
        f[key] = _clean(full.split(lab, 1)[1]) if lab in full else ""
    return f


def _cbb_pages(url, html, fetcher):
    pages, seen = [(url, html)], {url.rstrip("/")}
    cur = html
    while True:
        nxt = None
        for a in _soup(cur).select("a[href]"):
            if re.search(r"older\s+posts", a.get_text(" "), re.I) or "next" in (a.get("class") or []):
                nxt = urljoin(url, a["href"])
                break
        if not nxt or nxt.rstrip("/") in seen:
            return pages
        seen.add(nxt.rstrip("/"))
        cur = fetcher._fetch_http(nxt)
        if not cur:
            return pages
        pages.append((nxt, cur))
        if len(pages) > 20:               # a pager that never ends is a bug, not inventory
            return pages


_TRAIL_STATE = re.compile(r"(?<![A-Za-z])([A-Z]{2})\)?\s*$")


def _cbb_state(title, ctx):
    """State from a title like "Citgo Gas Station(Luzerne County)PA" or
    "... County NJ": the trailing two capitals, accepted only if they are a state."""
    m = _TRAIL_STATE.search(title or "")
    return (ctx.state_from_location(m.group(1)) if m else None) or ctx.state_from_location(title)


def read_cbb(url, html, name, fetcher, ctx):
    rows, drops, seen = [], [], set()
    domain = _bare(urlparse(url).netloc)
    for page_url, page in _cbb_pages(url, html, fetcher):
        for card in _soup(page).select("div.card-body"):
            h = card.select_one("h2.geodir-entry-title a[href]")
            if not h:
                continue
            title = _clean(h.get_text(" "))
            detail = ctx.norm_url(urljoin(page_url, h["href"]))
            if not title or detail in seen:
                continue
            seen.add(detail)
            f = _gd_fields(card)
            wrap = card.find_parent(lambda t: t.name and t.select_one('[data-badge="default_category"]'))
            cat = wrap.select_one('[data-badge="default_category"]') if wrap else None
            category = _clean(cat.get_text(" ")) if cat else ""
            body = card.select_one(".geodir-field-post_content")
            text = _clean(body.get_text(" ")) if body else ""
            site = f.get("site type", "")
            desc = " ".join(x for x in [text,
                                        f"Category: {category}." if category else "",
                                        f"Site Type: {site}." if site else ""] if x)
            row = ctx.make_row(url, name, title=title, detail_url=detail,
                               cash_flow=_fin(f.get("cash flow")),
                               state=_cbb_state(title, ctx), description=desc)
            _price_fields(row, f.get("asking price (usd)"))
            if f.get("listing id"):
                row["source_listing_id"] = f["listing id"]
            if _apply_status(row, [f.get("property status")], title, domain, ctx) == DROP:
                drops.append((title[:80], f.get("property status")))
                continue
            rows.append(row)
    return rows, drops


_READERS = {
    "b3brokers.com": read_listing_boxes,
    "coastalbizbrokers.com": read_listing_boxes,
    "biz-match.com": read_bizmatch,
    "cbb-tristate.com": read_cbb,
}


def read_index(url, html, name, fetcher, ctx):
    """(rows, drops) for an in-scope page of a registered domain, else None.
    A reader that raises returns ([], []) after saying so; V6 then falls
    through to the generic path (never silent)."""
    try:                                   # agencyforsale.com (rv-agencyforsale) has its own module
        import rv_agencyforsale as _afs
    except ImportError:                    # pragma: no cover
        _afs = None
    if _afs is not None and _afs.in_scope(url):
        if isinstance(ctx, dict):
            ctx = SimpleNamespace(**ctx)
        try:
            return _afs.read_index(url, html, name, fetcher, ctx)
        except Exception as exc:
            print(f"   rv_agencyforsale FAILED {type(exc).__name__}: {str(exc)[:120]}")
            return [], []
    if not in_scope(url):
        return None
    reader = _READERS[_bare(urlparse(url).netloc)]
    if isinstance(ctx, dict):
        ctx = SimpleNamespace(**ctx)
    try:
        return reader(url, html, name, fetcher, ctx)
    except Exception as exc:
        print(f"   rv_cards_a: {reader.__name__} FAILED {type(exc).__name__}: {str(exc)[:120]}")
        return [], []
