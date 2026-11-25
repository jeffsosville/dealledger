"""rv-detail (2026-11-24): detail-page readers for two brokers whose index
cards carry no figures.

progressivepracticesales.com
    /practices-for-sale/ is an Elementor loop grid of the `listing` custom post
    type: 89 cards (post-<id> in the class list, one page, the region tabs only
    filter it). The REST API (/wp-json/...) answers 403 "Forbidden" to ordinary
    requests, so it is not used and no workaround is attempted. A card holds a
    location and 3 tags only; the detail page (/addison-county-vermont/) has
    "<Practice type> for Sale", "Gross Collections", "Years Established",
    "Clinic Profit" ($ or "Coming Soon"). There is NO asking price on either, so
    asking_price stays empty: the old generic read stored Gross Collections as
    the price (price = revenue on 52 rows).
    Identity = the card's own url (unchanged, so existing ids hold), never the
    title: San Diego / Northern CA / Las Vegas / Manhattan / Pittsburgh /
    Charleston / Richmond each appear twice as separate practices.
    One sequential pass: one GET per practice, 0.6 s apart.

laskacompany.com (WP-REST)
    REST gives title + url only. /listing/<slug>/ carries labelled fields:
    Location, Year Established, Asking Price, Asset Value, Gross Sales,
    Cash Flow ("Contact Agent" / "N/A ..." -> no value). State = what the
    Location says ("Treasure Valley, ID"); Idaho is the default only when the
    page names none (Laska sells in the Boise area).
"""
from __future__ import annotations

import re
import time
from urllib.parse import urlparse

from bs4 import BeautifulSoup

import cr_w3_platforms as _w3

PPS_DOMAIN = "progressivepracticesales.com"
PPS_LOOP_ID = "995"


# --------------------------------------------------------------------------
# progressivepracticesales.com
# --------------------------------------------------------------------------

def pps_cards(html: str):
    """Index page -> [{post_id, href, location, tags}] in page order, one per post id."""
    soup = BeautifulSoup(html or "", "html.parser")
    out, seen = [], set()
    for it in soup.select(f'div.e-loop-item[data-elementor-id="{PPS_LOOP_ID}"]'):
        m = re.search(r"\bpost-(\d+)\b", " ".join(it.get("class") or []))
        a = it.select_one("a[href]")
        t = it.select_one(".elementor-heading-title")
        if not (m and a and t) or m.group(1) in seen:
            continue
        seen.add(m.group(1))
        out.append({
            "post_id": m.group(1), "href": a["href"].strip(),
            "location": re.sub(r"\s+", " ", t.get_text(" ", strip=True)),
            "tags": [x.get_text(" ", strip=True) for x in it.select(".elementor-icon-list-text")
                     if x.get_text(strip=True)],
        })
    return out


def pps_detail(html: str) -> dict:
    """Detail page -> {practice_type, collections, profit, years}. Missing -> None.
    'Clinic Profit: Coming Soon' is None, never 0."""
    soup = BeautifulSoup(html or "", "html.parser")
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    m = re.search(r"Schedule a Consultation\s+(.{3,80}?)\s+for Sale\b", text)
    ptype = m.group(1).strip() if m else None

    def money_after(label):
        mm = re.search(label + r"\s+\$\s*([\d,]+)", text)
        return float(mm.group(1).replace(",", "")) if mm else None

    y = re.search(r"Years Established\s+(\d{1,3})\b", text)
    return {"practice_type": ptype, "collections": money_after("Gross Collections"),
            "profit": money_after("Clinic Profit"), "years": int(y.group(1)) if y else None}


def pps_title(practice_type, location) -> str:
    return f"{practice_type or 'Chiropractic Practice'} – {location}"


def pps_state(location):
    m = re.search(r"\b([A-Z]{2})\s*$", location or "")
    return _w3.state_code(m.group(1)) if m else None


def handler_pps(url, html, broker_name, fetcher, sleep=0.6, fetch=None):
    domain = _w3._bare(urlparse(url).netloc)
    if domain != PPS_DOMAIN:
        return None
    cards = pps_cards(html)
    if not cards:
        return None
    get = fetch or (lambda u: _w3._get(fetcher, u, domain, use_proxy=False))
    rows, failed = [], 0
    for i, c in enumerate(cards):
        d = {}
        try:
            page = get(c["href"])
            d = pps_detail(page) if page else {}
        except Exception:
            d = {}
        if not d or (d.get("collections") is None and d.get("practice_type") is None):
            failed += 1
        row = _w3._mk(url, broker_name, title=pps_title(d.get("practice_type"), c["location"]),
                      detail_url=c["href"], asking_price=None,
                      cash_flow=d.get("profit"), revenue=d.get("collections"),
                      state=pps_state(c["location"]),
                      description="; ".join(c["tags"]) or c["location"])
        row["source_listing_id"] = c["post_id"]
        row["status"] = "active"
        rows.append(row)
        if sleep and i + 1 < len(cards):
            time.sleep(sleep)
    print(f"   practices: {len(cards)} cards, {len(cards) - failed} detail pages read, {failed} unread")
    return rows or None


# --------------------------------------------------------------------------
# laskacompany.com
# --------------------------------------------------------------------------

LASKA_LABELS = ["Location", "Year Established", "Asking Price", "Asset Value",
                "Gross Sales", "Cash Flow", "Business Description"]
_LASKA_SPLIT = re.compile(r"\b(" + "|".join(re.escape(x) for x in LASKA_LABELS) + r")\b")


def laska_detail(html: str, default_state="ID") -> dict:
    """/listing/<slug>/ -> {state, state_source, location, year, price, asset_value,
    revenue, cash_flow}. A value counts only when it starts with $ ('Contact Agent',
    'N/A - asset only sale' are no value)."""
    soup = BeautifulSoup(html or "", "html.parser")
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    start = text.find("Location ")
    seg = {}
    if start >= 0:
        parts = _LASKA_SPLIT.split(text[start:])
        # parts = ['', 'Location', ' Treasure Valley, ID ', 'Year Established', ...]
        for lab, val in zip(parts[1::2], parts[2::2]):
            seg.setdefault(lab, val.strip())

    def money(label):
        v = seg.get(label, "")
        return _w3.money(v) if v.startswith("$") else None

    loc = seg.get("Location") or None
    st = None
    if loc:
        m = re.search(r"(?:,|\s)\s*([A-Z]{2})\s*$", loc)
        st = _w3.state_code(m.group(1)) if m else None
    y = re.match(r"(\d{4})\b", seg.get("Year Established", ""))
    return {"location": loc, "state": st or default_state,
            "state_source": "page" if st else "default",
            "year": int(y.group(1)) if y else None,
            "price": money("Asking Price"), "asset_value": money("Asset Value"),
            "revenue": money("Gross Sales"), "cash_flow": money("Cash Flow")}


DETAIL_READERS = {"www.laskacompany.com": laska_detail, "laskacompany.com": laska_detail}


def handles(domain: str) -> bool:
    return (domain or "").lower() in DETAIL_READERS


def enrich_wp(scraper, listings, verbose=True, sleep=0.5):
    """WPRestScraper hook: fill price / revenue / cash_flow / state from each
    ACTIVE listing's own page. Existing REST values are never overwritten."""
    reader = DETAIL_READERS[scraper.domain.lower()]
    got = failed = 0
    todo = [r for r in listings if r.get("status") == "active" and r.get("listing_url")]
    for rec in todo:
        try:
            r = scraper.session.get(rec["listing_url"], timeout=20)
            fin = reader(r.text) if r.status_code == 200 else None
        except Exception:
            fin = None
        if not fin:
            failed += 1
            continue
        got += 1
        for k in ("price", "revenue", "cash_flow"):
            if not rec.get(k) and fin.get(k):
                rec[k] = fin[k]
        if rec.get("price"):
            rec["price_not_disclosed"] = False
        if not rec.get("state") and fin.get("state"):
            rec["state"] = fin["state"]
        if fin.get("state_source") == "default":
            rec["state_source"] = "default"
        time.sleep(sleep)
    if verbose:
        print(f"[WP:{scraper.domain}] detail pages: {len(todo)} active, {got} read, {failed} unread")
