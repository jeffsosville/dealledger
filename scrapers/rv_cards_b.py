"""
rv_cards_b - card readers for the 2026-11-24 job-review task rv-cards-b
(Jeff's hand review of 2026-11-23). Bug class: revenue stored as price, and
pagination not followed.

  routeadvisors.com            Elementor loop cards; price = "Asking Price" (we
                               stored "Gross Revenue"); 3 pages via the pager
                               links; no detail fetch (it returned "403 -
                               Forbidden" titles).
  bizbrokeragehub.com          DealRelations "dr-card" grid, 12 a page, ?dl_page=N;
                               price = the card's price (we stored Revenue).
  number1businessbroker.com    Business Brokers CRM (verticacrm) cards, loaded
                               by a POST the page makes itself; labelled blocks
                               Asking Price / Gross Revenue / Net Cashflow.

Each reader is scoped to its named domain only (explicit allow-list).

V6 hook (one block in extract_via_api): read() returns a list of listing dicts,
or None for any other domain, or when the reader found nothing (V6 then falls
through to the generic path, saying so). Ids are the ones V6 already mints
(_make_api_listing -> stable_listing_id -> ID_KEY_RULES), so nothing here
changes identity except the two ID_KEY_RULES entries added in V6.

Pure parsing functions take HTML/JSON text and return plain dicts so they can
be unit tested without the network.
"""
from __future__ import annotations

import html as _html
import json
import re
from types import SimpleNamespace
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup

try:
    from listing_status import DROP, lookup_status
except Exception:                       # pragma: no cover
    DROP, lookup_status = "DROP", None
try:
    from cr_w3_platforms import note_displayed, state_code
except Exception:                       # pragma: no cover
    note_displayed = lambda *a, **k: None   # noqa: E731
    state_code = lambda n: None             # noqa: E731

DOMAINS = frozenset({"routeadvisors.com", "bizbrokeragehub.com",
                     "number1businessbroker.com"})

MAX_PAGES = 12                          # hard stop; every domain here has <= 4


def _bare(host):
    h = (host or "").lower().split(":")[0]
    return h[4:] if h.startswith("www.") else h


def _clean(text):
    return re.sub(r"\s+", " ", _html.unescape(text or "").replace("\xa0", " ")).strip()


_AMOUNT = re.compile(r"\$\s*([\d][\d,]*(?:\.\d+)?)")


def money(text):
    """'$2,900,000' -> 2900000.0. 'TBD', '$', '$0', '' -> None. Only a figure
    with a dollar sign counts, so a stray count ('20 vehicles') never becomes
    a price."""
    m = _AMOUNT.search(text or "")
    if not m:
        return None
    try:
        v = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    return v if v > 0 else None


def _state(ctx, *texts):
    for t in texts:
        if not t:
            continue
        try:
            st = ctx.state_from_location(t)
        except Exception:
            st = None
        if st:
            return st
    return None


def _state_from_place(ctx, place):
    """'Rainbow City, AL' / 'Nevada' / 'Bronx County, New York' -> 2-letter US
    state, else None ('ON, Canada' and 'South East' are not US states)."""
    parts = [p.strip() for p in (place or "").split(",") if p.strip()]
    for p in reversed(parts):
        st = state_code(p)
        if st:
            return st
    return None


def _fetch(fetcher, ctx, url):
    try:
        np = getattr(ctx, "needs_proxy", None)
        html, _m = fetcher.fetch(url, use_proxy=bool(np(_bare(urlparse(url).netloc))) if np else False)
        return html
    except Exception as exc:
        print(f"   rv_cards_b: fetch failed {url} ({type(exc).__name__}: {str(exc)[:80]})")
        return None


def _badge_status(texts, domain):
    """(status, label) from the broker's own badge texts. Sold -> sold; On Hold
    -> inactive; Pending / Under Contract / LOI / In Contract -> active + label;
    otherwise active. ('DROP', label) for Coming Soon / For Lease."""
    best = ("active", None)
    for t in texts:
        t = _clean(t)
        if not t:
            continue
        hit = lookup_status(t, domain) if lookup_status else None
        if not hit and re.search(r"\bin\s+contract\b|\bsale\s+pending\b", t, re.I):
            hit = ("active", re.sub(r"\s*!+\s*$", "", t))
        if not hit:
            continue
        status, label = hit
        if status == DROP:
            return DROP, label
        if status == "sold":
            return "sold", label
        if status == "inactive":
            best = ("inactive", label)
        elif label and best[0] == "active" and not best[1]:
            best = ("active", label)
    return best


# --------------------------------------------------------------------------
# routeadvisors.com
# --------------------------------------------------------------------------
RA_LABELS = ("route #", "location", "asking price", "gross revenue", "cash flow",
             "financing options", "equipment", "# of vehicles", "# of employees",
             "manager status", "reason for sale")


def ra_cards(html):
    """One dict per `div.entry` route card. Values are the raw card strings."""
    soup = BeautifulSoup(html or "", "html.parser")
    out = []
    for e in soup.select("div.entry"):
        h = e.select_one("h2")
        title = _clean(h.get_text(" ")) if h else ""
        if not title:
            continue
        ribbons = [_clean(r.get_text(" ")) for r in e.select(".ribbon")]
        lines = [_clean(l) for l in e.get_text("\n").split("\n")]
        lines = [l for l in lines if l]
        fields, i = {}, 0
        while i < len(lines):
            m = re.match(r"^([^:]{2,30}):\s*(.*)$", lines[i])
            lab = m.group(1).strip().lower() if m else None
            if lab in RA_LABELS:
                val = m.group(2)
                if not val and i + 1 < len(lines):
                    nxt = lines[i + 1]
                    nm = re.match(r"^([^:]{2,30}):\s*(.*)$", nxt)
                    if not (nm and nm.group(1).strip().lower() in RA_LABELS) and nxt != "Request Info":
                        val = nxt
                        i += 1
                fields[lab] = val
            i += 1
        a = e.select_one("a[href*='/routes/']")
        out.append({"title": title, "ribbons": ribbons, "fields": fields,
                    "href": a["href"] if a else None})
    return out


def ra_pager_urls(html, base_url):
    """Pager links in page order ('Page2', 'Page3', 'Next »'), absolute, deduped."""
    soup = BeautifulSoup(html or "", "html.parser")
    urls = []
    for a in soup.select("a[href]"):
        t = _clean(a.get_text(" "))
        if re.match(r"^(Page\s*\d+|\d+|Next\b.*)$", t, re.I) and "/available-routes" in a["href"]:
            u = urljoin(base_url, a["href"])
            if u not in urls:
                urls.append(u)
    return urls


def ra_row(card, url, name, ctx):
    f = card["fields"]
    route = (f.get("route #") or "").strip()
    price, rev, cf = money(f.get("asking price")), money(f.get("gross revenue")), money(f.get("cash flow"))
    if route.upper() == "TBD" and price is None and rev is None and cf is None:
        return None, "placeholder card (Route # TBD, no figures)"
    status, label = _badge_status(card["ribbons"], "routeadvisors.com")
    if status == DROP:
        return None, f"{card['title']} ({label})"
    place = f.get("location") or ""
    first = place.split(",")[0].strip()
    city = None if re.match(r"^(confidential|undisclosed)$", first, re.I) else (first if "," in place else None)
    desc = _clean(" | ".join(x for x in (
        card["title"],
        f"Route #{route}" if route else "",
        f"Location: {place}" if place else "",
        f"Financing: {f['financing options']}" if f.get("financing options") else "",
        f"Equipment: {f['equipment']}" if f.get("equipment") else "",
        f"Vehicles: {f['# of vehicles']}" if f.get("# of vehicles") else "",
        f"Employees: {f['# of employees']}" if f.get("# of employees") else "",
        f"Manager: {f['manager status']}" if f.get("manager status") else "",
        f"Reason for sale: {f['reason for sale']}" if f.get("reason for sale") else "") if x))
    detail = urljoin(url, card["href"]) if card.get("href") else None
    row = ctx.make_row(url, name, title=card["title"], detail_url=detail, asking_price=price,
                       cash_flow=cf, revenue=rev, city=city,
                       state=_state_from_place(ctx, place), description=desc)
    if re.fullmatch(r"\d{7}", route):
        row["source_listing_id"] = route
    row["status"] = status
    if label:
        row["broker_status_text"] = label
    return row, None


def read_routeadvisors(url, html, name, fetcher, ctx):
    pages, seen_pages, cards = [html], {urljoin(url, "").rstrip("/")}, []
    cards.extend(ra_cards(html))
    queue = ra_pager_urls(html, url)
    while queue and len(pages) < MAX_PAGES:
        u = queue.pop(0)
        key = u.rstrip("/")
        if key in seen_pages:
            continue
        seen_pages.add(key)
        h = _fetch(fetcher, ctx, u)
        if not h:
            print(f"   rv_cards_b: routeadvisors page {u} not fetched - result may be partial")
            continue
        got = ra_cards(h)
        if not got:
            continue
        pages.append(h)
        cards.extend(got)
        for nu in ra_pager_urls(h, u):
            if nu.rstrip("/") not in seen_pages and nu not in queue:
                queue.append(nu)
    rows, skipped, seen = [], [], set()
    for c in cards:
        row, why = ra_row(c, url, name, ctx)
        if row is None:
            skipped.append(why)
            continue
        if row["url"] in seen:
            continue
        seen.add(row["url"])
        rows.append(row)
    for s in skipped:
        print(f"   rv_cards_b: not emitted: {s}")
    note_displayed("routeadvisors.com", len(rows),
                   f"{len(rows)} route cards over {len(pages)} page(s)",
                   "no stated total on the site; counted from the pager's pages")
    print(f"   rv_cards_b routeadvisors: {len(cards)} cards over {len(pages)} page(s) -> {len(rows)} listings")
    return rows


# --------------------------------------------------------------------------
# bizbrokeragehub.com
# --------------------------------------------------------------------------
BBH_HOST = "https://bizbrokeragehub.com"
_BBH_TOTAL = re.compile(r"Showing\s+[\d,]+\s+of\s+([\d,]+)\s+listings?", re.I)
_BBH_REF = re.compile(r"[-–—]\s*(\d{3,6})\s*$")


def bbh_total(html):
    m = _BBH_TOTAL.search(BeautifulSoup(html or "", "html.parser").get_text(" "))
    return int(m.group(1).replace(",", "")) if m else None


def bbh_page_urls(html, base_url):
    soup = BeautifulSoup(html or "", "html.parser")
    out = []
    for a in soup.select("a[href*='dl_page=']"):
        u = urljoin(base_url, a["href"]).split("#")[0]
        if u not in out:
            out.append(u)
    return out


def bbh_cards(html):
    soup = BeautifulSoup(html or "", "html.parser")
    out = []
    for art in soup.select("article.dr-card"):
        t = art.select_one(".dr-card__title")
        a = (t.find("a", href=True) if t else None) or art.select_one("a[href*='/deal-listing/']")
        if not t or not a:
            continue
        metas = [_clean(m.get_text(" ")) for m in art.select(".dr-card__meta")]
        def meta(label):
            for m in metas:
                mm = re.match(rf"^{label}\s*:\s*(.*)$", m, re.I)
                if mm:
                    return mm.group(1)
            return None
        hl = art.select_one(".dr-card__listing-highlights")
        p, s = art.select_one(".dr-card__price"), art.select_one(".dr-card__status")
        out.append({"title": _clean(t.get_text(" ")), "href": a["href"],
                    "price": _clean(p.get_text(" ")) if p else "",
                    "status": _clean(s.get_text(" ")) if s else "",
                    "revenue": meta("Revenue"), "profits": meta("Profits"),
                    "highlights": _clean(hl.get_text(" ")) if hl else ""})
    return out


BBH_JSON = "https://bizbrokeragehub.dealrelations.com/listings/{}.json?item_id=5434"


def bbh_json_state(js_text, ctx):
    """State from the listing's own JSON feed: structured_data.name reads
    'Kitchen & Bath Business for Sale | Suffolk County, NY'."""
    try:
        d = json.loads(js_text)
        sd = d.get("structured_data")
        sd = json.loads(sd) if isinstance(sd, str) else (sd or {})
        name = sd.get("name") or ""
    except Exception:
        return None
    tail = name.split("|")[-1] if "|" in name else name
    return _state_from_place(ctx, tail) or _state(ctx, tail)


def _bbh_state_from_feed(fetcher, ctx, slug):
    try:
        r = fetcher.session.get(BBH_JSON.format(slug), timeout=20)
        return bbh_json_state(r.text, ctx) if r.status_code == 200 else None
    except Exception:
        return None


def bbh_row(card, url, name, ctx):
    m = re.search(r"/deal-listing/([^/?#]+)", card["href"])
    if not m:
        return None, f"{card['title']} (no deal-listing link)"
    slug = m.group(1)
    status, label = _badge_status([card["status"]], "bizbrokeragehub.com")
    if status == DROP:
        return None, f"{card['title']} ({label})"
    hl = re.sub(r"^Watch video\s*", "", card["highlights"])
    # Clean public page URL (no ?back=..dl_page=N tracking). The row id is
    # pinned to the slug by ID_KEY_RULES in V6, so a listing that moves between
    # pages keeps its id.
    detail = f"{BBH_HOST}/deal-listing/{slug}/"
    row = ctx.make_row(url, name, title=card["title"], detail_url=detail,
                       asking_price=money(card["price"]), cash_flow=money(card["profits"]),
                       revenue=money(card["revenue"]),
                       state=_state(ctx, card["title"], hl[:300]), description=hl[:1000] or card["title"])
    if not row.get("state") and getattr(ctx, "fetcher", None) is not None:
        row["state"] = _bbh_state_from_feed(ctx.fetcher, ctx, slug)
    rm = _BBH_REF.search(card["title"])
    if rm:
        row["source_listing_id"] = rm.group(1)
    row["status"] = status
    if label:
        row["broker_status_text"] = label
    return row, None


def read_bizbrokeragehub(url, html, name, fetcher, ctx):
    base = url if "dl_page=" not in url else url.split("?")[0]
    cards = bbh_cards(html)
    total = bbh_total(html)
    seen_pages = {base.rstrip("/")}
    queue = [u for u in bbh_page_urls(html, base) if u.rstrip("/") != base.rstrip("/")]
    fetched = 1
    while queue and fetched < MAX_PAGES:
        u = queue.pop(0)
        if u in seen_pages:
            continue
        seen_pages.add(u)
        h = _fetch(fetcher, ctx, u)
        got = bbh_cards(h) if h else []
        if not got:
            print(f"   rv_cards_b: bizbrokeragehub page {u} gave no cards - result may be partial")
            continue
        fetched += 1
        cards.extend(got)
        total = total or bbh_total(h)
        for nu in bbh_page_urls(h, u):
            if nu not in seen_pages and nu not in queue:
                queue.append(nu)
    rows, seen = [], set()
    for c in cards:
        row, why = bbh_row(c, url, name, ctx)
        if row is None:
            print(f"   rv_cards_b: not emitted: {why}")
            continue
        if row["url"] in seen:
            continue
        seen.add(row["url"])
        rows.append(row)
    if total:
        note_displayed("bizbrokeragehub.com", total, f"Showing 12 of {total} listings",
                       f"pages read via ?dl_page=N ({fetched})")
        if len(rows) != total:
            print(f"   rv_cards_b: bizbrokeragehub site says {total}, read {len(rows)}")
    print(f"   rv_cards_b bizbrokeragehub: {len(cards)} cards over {fetched} page(s) -> {len(rows)} listings (site: {total})")
    return rows


# --------------------------------------------------------------------------
# number1businessbroker.com  (Business Brokers CRM)
# --------------------------------------------------------------------------
N1_HOST = "https://number1businessbroker.com"
N1_API = "https://api3.verticacrm.com/api/bbcrmapi/submitsearchtoolnew"
N1_PER_PAGE = 10                        # what the site's own lazy-load asks for
_N1_COUNT = re.compile(r"([\d,]+)\s+Listings?\s+Selected", re.I)
_N1_LEADING = re.compile(r"[?&]listing=(\d+)")
N1_LABELS = {"asking price": "price", "gross revenue": "revenue", "net cashflow": "cash_flow",
             "net cash flow": "cash_flow", "ebitda": "ebitda", "listing id": "listing_id"}


def n1_count(html):
    m = _N1_COUNT.search(BeautifulSoup(html or "", "html.parser").get_text(" "))
    return int(m.group(1).replace(",", "")) if m else None


def n1_cards(html, base_url=N1_HOST + "/search/"):
    """Cards from the CRM result fragment (or the rendered page). Labelled
    blocks only: each <div class=mob>Label<p>value</p></div> maps by its label;
    nothing falls back to 'the first $ amount'. The commented-out legacy price
    block inside each card is a comment node and never read."""
    soup = BeautifulSoup(html or "", "html.parser")
    out = []
    for c in soup.select("div.main"):
        a = c.find("a", href=re.compile(r"listing=\d+"))
        nm = c.select_one(".name")
        if not a or not nm:
            continue
        badge_el = nm.find("p")
        badge = _clean(badge_el.get_text(" ")) if badge_el else ""
        title = _clean(nm.get_text(" "))
        if badge and title.startswith(badge):
            title = title[len(badge):].strip()
        elif badge:
            title = _clean(title.replace(badge, " "))
        blocks = {}
        for d in c.find_all("div", class_="mob"):
            p = d.find("p")
            if p is None:
                continue
            val = _clean(p.get_text(" "))
            lab = _clean(d.get_text(" "))
            lab = _clean(lab[:len(lab) - len(val)]) if val and lab.endswith(val) else lab
            key = N1_LABELS.get(lab.lower())
            if key:
                blocks[key] = val
        loc = ""
        for d in c.find_all("div", class_="mob", recursive=False):
            if d.find("p") is None and "name" not in (d.get("class") or []):
                t = _clean(d.get_text(" "))
                if t and len(t) < 90:
                    loc = t
                    break
        desc_el = [d for d in c.find_all("div", class_="mob", recursive=False)
                   if d.find("p") is None and "name" not in (d.get("class") or [])]
        desc = _clean(desc_el[1].get_text(" ")) if len(desc_el) > 1 else ""
        out.append({"title": title, "badge": badge, "href": a["href"], "blocks": blocks,
                    "location": loc, "description": desc})
    return out


def n1_row(card, url, name, ctx):
    href = card["href"].strip()
    m = _N1_LEADING.search(href)
    if not m:
        return None, f"{card['title']} (no listing number in link)"
    status, label = _badge_status([card["badge"]], "number1businessbroker.com")
    if status == DROP:
        return None, f"{card['title']} ({label})"
    b = card["blocks"]
    row = ctx.make_row(url, name, title=card["title"], detail_url=urljoin(N1_HOST, href),
                       asking_price=money(b.get("price")), cash_flow=money(b.get("cash_flow")),
                       revenue=money(b.get("revenue")),
                       state=_state_from_place(ctx, card["location"]),
                       description=(card["description"] or card["title"])[:1000])
    row["source_listing_id"] = m.group(1)
    row["status"] = status
    if label:
        row["broker_status_text"] = label
    return row, None


def _n1_post(fetcher, page):
    r = fetcher.session.post(
        N1_API,
        data={"search": f"pagenumber={page}&resultsperpage={N1_PER_PAGE}&sort=createDate%3Adesc",
              "sort": "createDate:desc", "lazyload": "1"},
        headers={"Origin": N1_HOST, "Referer": N1_HOST + "/search/"}, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    return (json.loads(r.text).get("body") or {}).get("html") or ""


def read_number1(url, html, name, fetcher, ctx):
    cards, total, seen = [], None, set()
    for page in range(1, MAX_PAGES + 1):
        try:
            frag = _n1_post(fetcher, page)
        except Exception as exc:
            print(f"   rv_cards_b: number1 page {page} failed ({type(exc).__name__}: {str(exc)[:80]})")
            if page == 1:
                return None             # V6 falls through to the generic path
            break
        total = total or n1_count(frag)
        got = [c for c in n1_cards(frag) if c["href"] not in seen]
        if not got:
            break
        seen.update(c["href"] for c in got)
        cards.extend(got)
        if total and len(cards) >= total:
            break
    rows, ids = [], set()
    for c in cards:
        row, why = n1_row(c, url, name, ctx)
        if row is None:
            print(f"   rv_cards_b: not emitted: {why}")
            continue
        if row["source_listing_id"] in ids:
            continue
        ids.add(row["source_listing_id"])
        rows.append(row)
    if total:
        note_displayed("number1businessbroker.com", total, f"{total} Listings Selected",
                       "CRM result count; 10 cards a page")
        if len(rows) != total:
            print(f"   rv_cards_b: number1 site says {total}, read {len(rows)}")
    print(f"   rv_cards_b number1: {len(cards)} cards -> {len(rows)} listings (site: {total})")
    return rows or None


# --------------------------------------------------------------------------
_READERS = {
    "routeadvisors.com": read_routeadvisors,
    "bizbrokeragehub.com": read_bizbrokeragehub,
    "number1businessbroker.com": read_number1,
}


def read(url, html, name, fetcher, ctx):
    """Listing dicts for one of the three domains, else None. A reader that
    raises or finds nothing returns None after saying so (never silent), and V6
    continues down its generic path."""
    reader = _READERS.get(_bare(urlparse(url).netloc))
    if reader is None:
        return None
    if isinstance(ctx, dict):
        ctx = SimpleNamespace(**ctx)
    ctx.fetcher = fetcher
    try:
        rows = reader(url, html, name, fetcher, ctx)
    except Exception as exc:
        print(f"   rv_cards_b: {reader.__name__} FAILED {type(exc).__name__}: {str(exc)[:120]}")
        return None
    if not rows:
        print(f"   rv_cards_b: {reader.__name__} found nothing - falling back to the generic path")
        return None
    return rows
