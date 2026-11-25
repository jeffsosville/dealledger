"""
rv_agencyforsale - agencyforsale.com (job-review 2026-11-24, task rv-agencyforsale).

Insurance agencies, ~250 listings, 25 per page. The cards show NO status; the
site's own filter does (search[status] = unsold | pending | sold). The generic
V6 path crawled the unfiltered list, so ~180 SOLD agencies were stored active,
and it took the card's FIRST dollar amount as the price. That first amount is the
Premium (written premium, an insurance book size), not the asking price, and a
revenue figure was lifted from prose.

This reader crawls the three filters itself (sequential, <= 10 pages each, a
pause between requests):

  unsold   -> active
  pending  -> active, broker_status_text = the badge verbatim ("Sale Pending")
  sold     -> status sold, broker_status_text = the badge verbatim ("Sold!")

A listing that shows up under more than one filter takes the most final status
(sold > pending > unsold).

Fields, each read by label:
  asking_price = "Listing price:" on the card ("Sold!" -> None; nothing invented)
  revenue      = None. Premium is written premium, not revenue.
  description  = card text + "Written premium: $X." + the detail page's Annual
                 Revenue / Expenses / policies / retention, + the consultant named
                 on the card (or "no consultant named")
  title        = the detail page <h1> (the card's <h3> is "Allstate Agency" on
                 most cards); identity = the listing's own URL, which is V6's
                 url-hash id, so existing rows keep matching (principle 4).
"""
from __future__ import annotations

import re
import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

try:
    from cr_card_fields import _price_fields, _fin, _clean, _bare
except Exception:                        # pragma: no cover
    from scrapers.cr_card_fields import _price_fields, _fin, _clean, _bare  # type: ignore

HOST = "agencyforsale.com"
BASE = "https://agencyforsale.com/listings"
STATUSES = ("unsold", "pending", "sold")        # least -> most final
MAX_PAGES = 10
PAUSE = 0.6                                       # seconds between requests
_UUID = re.compile(r"/listings/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)
_TOTAL = re.compile(r"Displaying\s+(?:all\s+(\d+)\s+listings|listings\s+\d+\s*[–-]\s*(\d+)\s+of\s+(\d+))", re.I)
_RANK = {"unsold": 0, "pending": 1, "sold": 2}


def in_scope(url):
    p = urlparse(url)
    return _bare(p.netloc) == HOST and p.path.rstrip("/").lower() == "/listings"


def page_url(status, page):
    return f"{BASE}?page={page}&search%5Bstatus%5D={status}"


def parse_total(html):
    """(last_index_on_page, total) from "Displaying listings 1-25 of 66" /
    "Displaying all 3 listings"; (None, None) if absent."""
    t = _clean(BeautifulSoup(html or "", "html.parser").get_text(" "))
    m = _TOTAL.search(t)
    if not m:
        return None, None
    if m.group(1):
        return int(m.group(1)), int(m.group(1))
    return int(m.group(2)), int(m.group(3))


def parse_cards(html):
    """[{href, h3, price_text, premium, location, badge, consultant, text}] in page order."""
    out = []
    soup = BeautifulSoup(html or "", "html.parser")
    for c in soup.select("div.mb-8.overflow-hidden.rounded-lg.bg-white.shadow"):
        a = next((x for x in c.select('a[href^="/listings/"]') if _UUID.search(x["href"])), None)
        if not a:
            continue
        fields = {}
        for sp in c.select("span.mr-4"):
            lab = sp.select_one("span.font-semibold")
            if lab:
                key = _clean(lab.get_text()).rstrip(":").lower()
                fields[key] = _clean(sp.get_text(" ").replace(lab.get_text(), "", 1))
        loc = c.select_one("span.font-semibold.mr-4.inline-block")
        cons = c.select_one('a[href^="/consultants/profiles/"] p.font-semibold')
        badge = c.select_one("span.rounded-full")
        h3 = c.select_one("h3")
        body = c.select_one("div.px-4.py-5.sm\\:p-6")      # not the header block (same px/py classes)
        out.append({
            "href": a["href"],
            "h3": _clean(h3.get_text(" ")) if h3 else "",
            "price_text": fields.get("listing price", ""),
            "premium": fields.get("premium", ""),
            "location": _clean(loc.get_text(" ")) if loc else "",
            "badge": _clean(badge.get_text(" ")) if badge else "",
            "consultant": _clean(cons.get_text(" ")) if cons else None,
            "text": _clean(body.get_text(" ")) if body else "",
        })
    return out


def parse_detail(html):
    """{title, location, fields} from a listing page; fields are the Financial
    Overview label/value pairs (Price, Premium, Annual Revenue, Expenses, ...)."""
    soup = BeautifulSoup(html or "", "html.parser")
    for x in soup(["script", "style", "nav"]):
        x.decompose()
    main = soup.find("main") or soup
    h1 = main.find("h1")
    lines = [l for l in (_clean(x) for x in main.get_text("\n").split("\n")) if l]
    fields = {}
    if "Financial Overview" in lines:
        i = lines.index("Financial Overview") + 1
        while i + 1 < len(lines) and not lines[i].startswith("Contact the Seller"):
            fields[lines[i].lower()] = lines[i + 1]
            i += 2
    loc = ""
    if h1 is not None and h1.get_text(strip=True) in lines:
        j = lines.index(_clean(h1.get_text(" "))) + 1
        for cand in lines[j:j + 3]:
            if "," in cand or " - " in cand:
                loc = cand
                break
    return {"title": _clean(h1.get_text(" ")) if h1 else "", "location": loc, "fields": fields}


def crawl_index(first_url, first_html, fetcher, log=print):
    """{listing_href: (status, card)} across the three filters, plus per-status
    totals the site printed and the distinct counts seen."""
    found, totals, distinct = {}, {}, {}
    for status in STATUSES:
        seen_here = set()
        for page in range(1, MAX_PAGES + 1):
            if (status == "unsold" and page == 1 and first_html
                    and "status%5d=unsold" in first_url.lower().replace("[", "%5b").replace("]", "%5d")):
                html = first_html
            else:
                time.sleep(PAUSE)
                html = fetcher._fetch_http(page_url(status, page))
            cards = parse_cards(html)
            last, total = parse_total(html)
            if total is not None:
                totals[status] = total
            new = 0
            for c in cards:
                if c["href"] in seen_here:
                    continue
                seen_here.add(c["href"])
                new += 1
                prev = found.get(c["href"])
                if prev is None or _RANK[status] > _RANK[prev[0]]:
                    found[c["href"]] = (status, c)
            if not cards or not new or last is None or last >= (total or 0):
                break
        distinct[status] = len(seen_here)
    log(f"   agencyforsale: site totals {totals}; distinct listings seen {distinct}")
    return found, totals, distinct


def build_row(href, status, card, detail, url, name, ctx):
    detail = detail or {"title": "", "location": "", "fields": {}}
    f = detail["fields"]
    title = detail["title"] or (f"{card['h3']} - {card['location']}" if card["location"] else card["h3"])
    detail_url = ctx.norm_url(urljoin(url, href))
    bits = [card["text"]]
    prem = _fin(f.get("premium") or card["premium"])
    if prem:
        bits.append(f"Written premium: ${prem:,.0f}.")
    for lab, key in (("Annual revenue", "annual revenue"), ("Expenses", "expenses"),
                     ("Policies in force", "policies in force"), ("Retention", "retention ratio"),
                     ("Loss ratio", "loss ratio")):
        if f.get(key):
            bits.append(f"{lab}: {f[key]}.")
    bits.append(f"Consultant: {card['consultant']}." if card["consultant"] else "Consultant: none named.")
    loc = detail["location"] or card["location"]
    row = ctx.make_row(url, name, title=title, detail_url=detail_url,
                       revenue=None, cash_flow=None,
                       state=ctx.state_from_location(loc) or ctx.state_from_location(title),
                       description=" ".join(b for b in bits if b))
    # "Listing price: Sold!" is no price. Withheld wording is handled by the shared helper.
    _price_fields(row, card["price_text"] if "sold" not in card["price_text"].lower() else "")
    # The filter the listing was found under is the positive evidence for its status
    # (principle 14): unsold -> active is an observation here, not a default.
    row["status"] = "active"
    if status == "sold":
        row["status"] = "sold"
        row["broker_status_text"] = card["badge"] or "Sold"
    elif status == "pending":
        row["status"] = "active"
        row["broker_status_text"] = card["badge"] or "Pending"
    return row


def read_index(url, html, name, fetcher, ctx):
    if not in_scope(url):
        return None
    found, totals, distinct = crawl_index(url, html, fetcher)
    rows = []
    for href, (status, card) in found.items():
        time.sleep(PAUSE)
        try:
            detail = parse_detail(fetcher._fetch_http(urljoin(url, href)))
        except Exception as exc:                       # a detail page is an enrichment, not the row
            print(f"   agencyforsale: detail {href[-8:]} failed {type(exc).__name__}")
            detail = None
        rows.append(build_row(href, status, card, detail, url, name, ctx))
    return rows, []
