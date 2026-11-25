"""
rv_nodetail - per-domain readers for brokers whose listings have no normal
detail page (job-review, task rv-nodetail, 2026-11-24).

  nerestaurantbrokers.com   Wix, 3 category pages, cards link to a listing
                            sheet document (/_files/ugd/...), never a page.
  newbizbrokers.com         Wix, one catalog page, 22 cards, no links at all.
  marigoldresources.com     /for-sale/ is a list of (mostly image) links to
                            public.*.basecamp.com/p/... pages the broker wrote,
                            plus short domains (marigold.mobi/skipark ...) that
                            redirect there.

Each reader is scoped to its own domain by an explicit allow-list (a platform
signature is never enough on its own). Pure parsing functions take HTML and
return plain dicts so they are unit-testable offline; the handlers only add
the fetching and the V6 row construction. Hooked into V6 through
install(API_HANDLERS, helpers) - one line in dealledger_scraper_v6.py.

Identity (principle 4: never a price, never a volatile URL):
  nerestaurantbrokers.com  sha256("<host>|<listing code>") - or, for the four
                           cards that print no code, "<host>|<normalised title>".
                           The row URL is the listing sheet (it changes when the
                           broker uploads a new sheet, so it is NOT the identity).
  newbizbrokers.com        page URL + "#<slug of title>" (sha256 of that URL,
                           V6's usual rule for a detail URL).
  marigoldresources.com    the final Basecamp URL (after redirects).
"""
from __future__ import annotations

import hashlib
import html as _html
import re
from urllib.parse import urlparse, urldefrag, urljoin

from bs4 import BeautifulSoup

NERB = "nerestaurantbrokers.com"
NEWBIZ = "newbizbrokers.com"
MARIGOLD = "marigoldresources.com"
DOMAINS = frozenset({NERB, NEWBIZ, MARIGOLD})

NERB_PAGES = ("/full-service-restaurants", "/fast-food-restaurants", "/liquor-businesses")

SCOPE_LOG: dict[str, list] = {}      # bare host -> [(what, why)] left out / flagged on purpose
_H: dict = {}                        # V6 helpers set by install()


def _bare(host: str) -> str:
    host = (host or "").lower().split(":")[0]
    return host[4:] if host.startswith("www.") else host


def _log(domain, what, why):
    SCOPE_LOG.setdefault(domain, []).append((what, why))


def _ws(t) -> str:
    return re.sub(r"\s+", " ", _html.unescape(str(t or "")).replace("​", " ").replace("\xa0", " ")).strip()


def money(text):
    """'$1,250,000' / '$395K' / '$3.80MM' / '1.63MM' -> float (first figure), else None.
    Also reads the broker's '$675.000.00' (dots as thousands separators)."""
    t = _ws(text)
    m = re.search(r"\$?\s*(\d{1,3})\.(\d{3})\.(\d{2})\b", t)
    if m:
        return float(m.group(1) + m.group(2))
    m = re.search(r"\$?\s*\$?\s*(\d[\d,]*(?:\.\d+)?)\s*(MM|mm|M|m|K|k)?(?![A-Za-z])", t)
    if not m:
        return None
    try:
        v = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    suf = (m.group(2) or "").lower()
    v *= 1_000_000 if suf in ("mm", "m") else 1_000 if suf == "k" else 1
    return v if v > 0 else None


def _norm_title(t) -> str:
    return re.sub(r"[^a-z0-9]+", "", _ws(t).lower())


def _slug(t, max_len=80) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", _ws(t).lower()).strip("-")
    return s[:max_len].strip("-")


def _sha(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _soup(html):
    s = BeautifulSoup(html or "", "html.parser")
    for t in s(["script", "style"]):
        t.decompose()
    return s


def _card_containers(soup, label_re):
    """For each text node matching label_re, the LARGEST ancestor that still
    holds exactly one such node: one card each, however the Wix repeater nests."""
    out, seen = [], set()
    for n in soup.find_all(string=label_re):
        el, best = n.parent, n.parent
        while el is not None and el.parent is not None and len(el.find_all(string=label_re)) == 1:
            best, el = el, el.parent
        if id(best) not in seen:
            seen.add(id(best))
            out.append(best)
    return out


# --------------------------------------------------------------------------
# nerestaurantbrokers.com
# --------------------------------------------------------------------------

_NERB_LABEL = (r"SELLING PRICE|LISTING ID|SALES VOLUME|DOWN PAYMENT|DN\. PAYMENT|LEASE|YRS?\. LEFT|"
               r"SQUARE FOOTAGE|HOURS OF OPERATION|SEATING|COMMENTS|REASON FOR SELLING|"
               r"VERIFIABLE ANNUAL NET INCOME|ANNUAL PROFIT|ESTIMATED PROFIT|TYPE OF BUSINESS|LOCATION|"
               r"CLICK FOR LISTING SHEET")
_NERB_PRICE_RE = re.compile(r"SELLING\s+PRICE", re.I)
_NERB_CODE_RE = re.compile(r"LISTING\s*ID\s*:?\s*([A-Z]{2}\d{3,5})\b")
_NERB_STATUS_RE = re.compile(r"(?:under agreement|under contract|sale pending|pending|under loi|sold|"
                             r"accepted offer|has accepted offer)\W*", re.I)
_NERB_NOISE = re.compile(r"^(?:(?:NDA|Stock Image|Demographics|Anchor\s*\d+|\d{1,2})\s+)+", re.I)


def _nerb_field(flat: str, label: str):
    m = re.search(label + r"\s*:?\s*(.*?)(?=\s(?:%s)\s*:|\s(?:CLICK FOR LISTING SHEET)|$)" % _NERB_LABEL,
                  flat, re.I)
    return _ws(m.group(1)) if m else None


def _annual(value_text):
    """A stated ANNUAL figure ('$1.5M/ yr.', '$150,000 per year', '100,000 yr.'); weekly
    or monthly figures are not annualised (None)."""
    t = _ws(value_text)
    if not t or re.search(r"/\s*wk|per\s+wk|a\s+week|weekly|per\s+month|/\s*mo\b", t, re.I):
        return None
    if re.search(r"/\s*yr|\byr\b|per\s+year|annual|\bannually", t, re.I) or re.fullmatch(r"\$?[\d,.]+\s*[KkMm]?", t):
        return money(t)
    return None


def parse_nerb_page(html: str, page_url: str) -> list[dict]:
    """The cards on one category page, in page order. No dedupe here."""
    soup = _soup(html)
    cards = []
    for el in _card_containers(soup, _NERB_PRICE_RE):
        flat = _ws(el.get_text(" "))
        if not _NERB_PRICE_RE.search(flat):
            continue
        head = flat[:_NERB_PRICE_RE.search(flat).start()]
        head = _NERB_NOISE.sub("", head).strip()
        head = re.split(r"\s(?:LISTING ID|CLICK FOR LISTING SHEET)\b", head, maxsplit=1, flags=re.I)[0]
        m = _NERB_CODE_RE.search(flat)
        code = m.group(1) if m else None
        tob = re.search(r"TYPE OF BUSINESS\s*:\s*(.*?)(?=\s(?:LOCATION|SELLING PRICE)\s*:|$)", flat, re.I)
        loc = re.search(r"\bLOCATION\s*:\s*(.*?)(?=\s(?:%s)\s*:|$)" % _NERB_LABEL, flat, re.I)
        if tob and tob.group(1).strip():
            title = _ws(tob.group(1)).rstrip(".")
            if loc and loc.group(1).strip():
                where = re.split(r"\s*\(", _ws(loc.group(1)), maxsplit=1)[0].rstrip(".")
                title = f"{title} ({where})"
        else:
            title = _ws(re.sub(r"\bTYPE OF BUSINESS\s*:", "", head, flags=re.I))
        title = _ws(title.replace("~", " ").replace("’", "'"))
        title = re.sub(r"\s+-\s+(?=[A-Z])", " - ", title)
        title = re.sub(r"\bAR EA\b", "AREA", title)     # Wix splits "AREA" across two spans on GV2579
        price_txt = _nerb_field(flat, r"SELLING\s+PRICE") or ""
        # status: a card line that IS the label ("UNDER AGREEMENT"); a sentence
        # that merely says "recently sold" is history, not a status.
        status_text = None
        for s in el.stripped_strings:
            if _NERB_STATUS_RE.fullmatch(_ws(s)):
                status_text = _ws(s)
                break
        if status_text is None:
            # Wix draws the ribbon as a separate sibling component right AFTER its card
            # (JR9977 "UNDER AGREEMENT"); the next card or the next "Anchor N" marker
            # ends that card's territory.
            for sib in el.find_next_siblings():
                st = _ws(sib.get_text(" "))
                if _NERB_PRICE_RE.search(st) or re.fullmatch(r"Anchor\s*\d+", st, re.I):
                    break
                if _NERB_STATUS_RE.fullmatch(st):
                    status_text = st
                    break
        sheet = None
        for a in el.find_all("a", href=True):
            href, txt = a["href"], _ws(a.get_text(" "))
            if re.search(r"nda", txt, re.I) and not re.search(r"listing\s*sheet", txt, re.I):
                continue
            if re.search(r"buyer(?:%20|\s|-)*nda", href, re.I):
                continue                                   # never the Buyer NDA
            if "/_files/ugd/" in href and re.search(r"listing\s*sheet", txt, re.I):
                sheet = urljoin(page_url, href)
                break
        net = (_nerb_field(flat, r"VERIFIABLE\s+ANNUAL\s+NET\s+INCOME")
               or _nerb_field(flat, r"ANNUAL\s+PROFIT") or _nerb_field(flat, r"ESTIMATED\s+PROFIT"))
        cards.append({
            "code": code, "title": title, "price": money(price_txt), "price_text": price_txt,
            "revenue": _annual(_nerb_field(flat, r"SALES\s+VOLUME") or ""),
            "cash_flow": _annual(net or ""),
            "sheet": sheet, "status_text": status_text, "page_url": page_url,
            "text": flat[:1000],
        })
    return cards


def merge_nerb(pages: list[tuple[str, str]]) -> list[dict]:
    """pages = [(page_url, html)] in the fixed category order. One row per
    listing: by listing code, then by identical normalised title (the North
    Conway pub is DS4456 on one page and DS2212 on another). The first page
    that shows a listing wins; later pages only fill a missing sheet URL."""
    by_code: dict[str, dict] = {}
    by_title: dict[str, dict] = {}
    ordered: list[dict] = []
    for page_url, html in pages:
        for c in parse_nerb_page(html, page_url):
            nt = _norm_title(c["title"])
            hit = by_code.get(c["code"]) if c["code"] else None
            if hit is None and len(nt) >= 25:
                hit = by_title.get(nt)
            if hit is not None:
                if c["code"] and c["code"] != hit["code"]:
                    hit.setdefault("aliases", []).append(c["code"])
                if not hit["sheet"] and c["sheet"]:
                    hit["sheet"] = c["sheet"]
                if c["status_text"] and not hit["status_text"]:
                    hit["status_text"] = c["status_text"]
                for k in ("price", "revenue", "cash_flow"):
                    if hit[k] is None and c[k] is not None:
                        hit[k] = c[k]
                continue
            c = dict(c, aliases=[])
            ordered.append(c)
            if c["code"]:
                by_code[c["code"]] = c
            by_title[nt] = c
    return ordered


def _nerb_state(title):
    m = re.search(r",\s*(NH|MA|CT|RI|ME|VT|NY)\b", title or "")
    return m.group(1) if m else None


def nerb_rows(cards, base_url, broker_name, *, make, status_lookup=None):
    host = urlparse(base_url).netloc
    root = f"{urlparse(base_url).scheme or 'https'}://{host}"
    rows = []
    for c in cards:
        ident = c["code"] or _norm_title(c["title"])
        url = c["sheet"] or (f"{root}{urlparse(c['page_url']).path}#{c['code'] or _slug(c['title'])}")
        desc = c["text"]
        row = make(base_url, broker_name, title=c["title"], detail_url=url,
                   asking_price=c["price"], cash_flow=c["cash_flow"], revenue=c["revenue"],
                   state=_nerb_state(c["title"]), description=desc)
        row["id"] = _sha(f"{_bare(host)}|{ident}")      # the code, never the sheet URL
        row["url_is_listing_specific"] = bool(c["sheet"])
        row["source_listing_id"] = c["code"]
        if c.get("aliases"):
            row["broker_note"] = "also listed as " + ", ".join(c["aliases"])
        st, label = "active", None
        if c["status_text"]:
            hit = status_lookup(c["status_text"], _bare(host)) if status_lookup else None
            if hit and hit[0] == "sold":
                st, label = "sold", hit[1] or c["status_text"]
            else:
                st, label = "active", c["status_text"]       # under agreement stays active, verbatim
        row["status"] = st
        if label:
            row["broker_status_text"] = label
        rows.append(row)
    return rows


def handler_nerb(url, html, broker_name, fetcher):
    if _bare(urlparse(url).netloc) != NERB:
        return None
    pu = urlparse(url)
    root = f"{pu.scheme}://{pu.netloc}"
    pages = []
    for path in NERB_PAGES:
        purl = root + path
        if pu.path.rstrip("/") == path:
            h = html
        else:
            h = _get(fetcher, purl)
        if not h or not _NERB_PRICE_RE.search(h):
            print(f"   ⚠️  nerestaurantbrokers: category page not read: {path}")
            _log(NERB, path, "category page fetch failed or no cards - rows from it missing this run")
            continue
        pages.append((purl, h))
    cards = merge_nerb(pages)
    if not cards:
        return None
    raw = sum(len(parse_nerb_page(h, u)) for u, h in pages)
    note_displayed(NERB, len(cards), f"{raw} cards on {len(pages)} category pages",
                   f"{raw} cards = {len(cards)} distinct listings (a listing can be shown on two pages)")
    return nerb_rows(cards, url, broker_name, make=_H["make"], status_lookup=_H.get("status"))


# --------------------------------------------------------------------------
# newbizbrokers.com
# --------------------------------------------------------------------------

_NB_PRICE_RE = re.compile(r"Asking\s+Price", re.I)
_SC_PLACES = ("North Myrtle Beach", "Myrtle Beach", "Murrells Inlet", "Georgetown", "Summerville",
              "Surfside Beach", "Pawleys Island", "Little River", "Conway", "Charleston", "Loris")
_STATE_NAMES = {"south carolina": "SC", "north carolina": "NC", "virginia": "VA", "georgia": "GA"}


def nb_location(text):
    """(city, state) read from the card's own words; (None, None) when it says nothing."""
    t = _ws(text)
    m = re.search(r"(?i:located\s+in)\s+([A-Za-z][A-Za-z .]+?)\s*,\s*([A-Z]{2})\b", t)
    if m:
        return m.group(1).strip().title(), m.group(2)
    m = re.search(r"\b((?:[A-Z][a-z]+ ){0,2}[A-Z][a-z]+),\s*(South Carolina|North Carolina|Virginia|Georgia)\b", t)
    if m:
        return m.group(1), _STATE_NAMES[m.group(2).lower()]
    for p in _SC_PLACES:
        if re.search(r"\b%s\b" % re.escape(p), t):
            return p, "SC"
    return None, None


def parse_newbiz(html: str) -> list[dict]:
    soup = _soup(html)
    out = []
    for el in _card_containers(soup, _NB_PRICE_RE):
        lines = [_ws(s) for s in el.stripped_strings if _ws(s)]
        i = next((k for k, l in enumerate(lines) if _NB_PRICE_RE.match(l)), None)
        if i is None or i == 0:
            continue
        title = " ".join(lines[:i])
        rest = lines[i:]
        price = money(re.sub(r"(?i)^asking\s+price\s*:?", "", rest[0]))
        cf_line = next((l for l in rest[1:3] if re.match(r"(?i)cash\s*flow", l)), None)
        cash = None
        if cf_line:
            v = re.sub(r"(?i)^cash\s*flow\s*:?", "", cf_line).strip()
            cash = None if re.match(r"(?i)n/?a\b", v) else money(v)
        body = [l for l in rest[1:] if l is not cf_line and not l.startswith("Call Ken")]
        desc = _ws(" ".join(body))
        city, state = nb_location(title + " " + desc)
        out.append({"title": title, "price": price, "cash_flow": cash, "description": desc,
                    "city": city, "state": state})
    return out


def newbiz_rows(cards, base_url, broker_name, *, make):
    page = urldefrag(base_url)[0]
    seen: dict[str, int] = {}
    rows = []
    for c in cards:
        slug = _slug(c["title"])
        n = seen.get(slug, 0) + 1
        seen[slug] = n
        if n > 1:
            slug = f"{slug}-{n}"                       # two identical titles: order breaks the tie
            _log(NEWBIZ, c["title"], "duplicate title on the page - suffixed")
        row = make(base_url, broker_name, title=c["title"], detail_url=f"{page}#{slug}",
                   asking_price=c["price"], cash_flow=c["cash_flow"], city=c["city"],
                   state=c["state"], description=c["description"][:1000] or c["title"])
        row["url_is_listing_specific"] = False          # a fragment is not a per-listing page
        rows.append(row)
    return rows


def handler_newbiz(url, html, broker_name, fetcher):
    if _bare(urlparse(url).netloc) != NEWBIZ:
        return None
    cards = parse_newbiz(html)
    if not cards:
        return None
    note_displayed(NEWBIZ, len(cards), "catalog cards", "Wix cards, no detail pages")
    return newbiz_rows(cards, url, broker_name, make=_H["make"])


# --------------------------------------------------------------------------
# marigoldresources.com
# --------------------------------------------------------------------------

_MG_LINK_RE = re.compile(
    r"^https?://(?:public\.[a-z0-9]+\.basecamp\.com/p/[A-Za-z0-9]+"
    r"|(?:www\.)?marigold\.mobi/[A-Za-z0-9_-]+"
    r"|(?:www\.)?marigold\.com/therapy)/?$", re.I)
_BASECAMP_FINAL_RE = re.compile(r"^https?://public\.[a-z0-9]+\.basecamp\.com/p/[A-Za-z0-9]+$", re.I)
_MG_STATUS_RE = re.compile(r"[-–—]\s*(In Closing|Under (?:dual signed )?(?:Letter of Intent|LOI|Contract)[^.]*)\s*$", re.I)


def parse_marigold_index(html: str, page_url: str):
    """([(href, status_label_or_None)], [unlinked image notes]) from /for-sale/.
    Image links count (a text-only scan misses most of them); links to anything
    but a Basecamp page or one of the three short domains are ignored."""
    soup = _soup(html)
    scope = soup.find(class_=re.compile(r"entry-content")) or soup
    links, seen = [], set()
    for a in scope.find_all("a", href=True):
        href = a["href"].strip()
        if not _MG_LINK_RE.match(href) or href in seen:
            continue
        seen.add(href)
        m = _MG_STATUS_RE.search(_ws(a.get_text(" ")))
        links.append((href, _ws(m.group(1)) if m else None))
    unlinked = []
    for img in scope.find_all("img"):
        if not img.find_parent("a", href=True):
            src = img.get("src", "")
            unlinked.append(src.rsplit("/", 1)[-1])
    return links, unlinked


def _latest(lines, label_re):
    """The figure on the most recent 'YYYY <label>: $X' line; a line with no year
    (a trailing-average block) counts as the newest."""
    best = None
    for l in lines:
        m = re.match(r"^\*?\s*(?:(\d{4})\s+)?(?:Annual\s+)?%s\s*[:=]\s*(.*)$" % label_re, l, re.I)
        if not m:
            continue
        v = money(m.group(2))
        if v is None:
            continue
        yr = int(m.group(1)) if m.group(1) else 9999
        if best is None or yr > best[0]:
            best = (yr, v)
    return best[1] if best else None


def parse_basecamp(html: str) -> dict | None:
    soup = _soup(html)
    h1 = soup.find("h1")
    title = _ws(h1.get_text(" ")) if h1 else _ws(soup.title.string if soup.title and soup.title.string else "")
    if not title:
        return None
    title = re.sub(r"^Acquisition Opportunity\s*:\s*", "", title, flags=re.I)
    title = re.sub(r"\s*(?:Acquisition\s+)?Opportunity\s*$", "", title, flags=re.I).strip() or title
    lines = [_ws(s) for s in soup.stripped_strings if _ws(s)]
    price = None
    for l in lines:                                     # a stated NEW price beats the stale line
        m = re.match(r"^New Price\s*:\s*(.*)$", l, re.I)
        if m and money(m.group(1)):
            price = money(m.group(1))
            break
    if price is None:
        for l in lines:
            m = re.match(r"^\*?\s*(?:NEW\s+)?Minimum\s+Business\s+Asking\s+Price\s*:\s*(.*)$", l, re.I)
            if m and money(m.group(1)):
                price = money(m.group(1))
                break
    revenue = _latest(lines, r"Gross\s+Sales")
    sde = _latest(lines, r"SDE")
    if sde is None:                                     # "$218K annual SDE"
        for l in lines:
            m = re.match(r"^\$\s*([\d,.]+\s*(?:MM|K)?)\s+annual\s+SDE\b", l, re.I)
            if m:
                sde = money(m.group(1))
                break
    loc = next((re.sub(r"(?i)^location\s*:\s*", "", l) for l in lines if re.match(r"(?i)^location\s*:", l)), None)
    city = state = None
    if loc:
        for name, ab in (("iowa", "IA"), ("illinois", "IL")):
            if name in loc.lower():
                state = ab
        m = re.match(r"^([A-Z][A-Za-z .]+?),\s*(?:Iowa|Illinois)\b", loc)
        city = m.group(1) if m else None
    desc = ""
    for k, l in enumerate(lines):
        if l.lower().startswith("business summary") and k + 2 < len(lines):
            desc = " ".join(x for x in lines[k + 1:k + 4] if x != "#")[:1000]
            break
    return {"title": title, "price": price, "revenue": revenue, "cash_flow": sde,
            "city": city, "state": state, "description": desc or title,
            "new_price_stated": any(re.match(r"^New Price\s*:", l, re.I) for l in lines)}


def marigold_row(parsed, final_url, base_url, broker_name, label, *, make):
    row = make(base_url, broker_name, title=parsed["title"], detail_url=final_url,
               asking_price=parsed["price"], cash_flow=parsed["cash_flow"], revenue=parsed["revenue"],
               city=parsed["city"], state=parsed["state"], description=parsed["description"])
    row["broker_domain"] = urlparse(base_url).netloc      # credited to the broker, not the document host
    row["status"] = "active"
    if label:
        row["broker_status_text"] = label                  # "In Closing" / "Under dual signed LOI": active, verbatim
    return row


def _final(fetcher, url):
    """(final_url, html) following redirects, or (None, None)."""
    try:
        r = fetcher.session.get(url, timeout=25, allow_redirects=True)
        if r.status_code == 200:
            return str(r.url), r.text
        return str(r.url), None
    except Exception:
        return None, None


def handler_marigold(url, html, broker_name, fetcher):
    if _bare(urlparse(url).netloc) != MARIGOLD:
        return None
    links, unlinked = parse_marigold_index(html, url)
    rows, dead = [], []
    for href, label in links:
        final, page = _final(fetcher, href)
        if not page or not final or not _BASECAMP_FINAL_RE.match(urldefrag(final)[0]):
            dead.append(href)
            _log(MARIGOLD, href, f"short link does not reach a Basecamp page (ended at {final}); no row")
            continue
        parsed = parse_basecamp(page)
        if not parsed:
            dead.append(href)
            _log(MARIGOLD, href, "Basecamp page has no title; no row")
            continue
        rows.append(marigold_row(parsed, urldefrag(final)[0], url, broker_name, label, make=_H["make"]))
    for name in unlinked:
        _log(MARIGOLD, name, "unlinked image on /for-sale/: no detail page, no row")
    if dead:
        print(f"   ⚠️  marigold: {len(dead)} linked item(s) did not resolve: {', '.join(dead)}")
    if not rows:
        return None
    note_displayed(MARIGOLD, len(links), "linked items on /for-sale/",
                   f"{len(links)} linked items, {len(rows)} resolve to a page, {len(unlinked)} unlinked images")
    return rows


# --------------------------------------------------------------------------
# V6 glue
# --------------------------------------------------------------------------

DISPLAYED: dict[str, dict] = {}


def note_displayed(domain, count, text="", note=""):
    """Hand the broker's own count to cr_w3_platforms' tracker when it is loaded."""
    try:
        import cr_w3_platforms as w3
        w3.note_displayed(domain, count, text, note)
    except Exception:
        DISPLAYED[_bare(domain)] = {"count": count, "text": text, "note": note}


def _get(fetcher, url):
    try:
        r = fetcher.session.get(url, timeout=25)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    try:
        html, _m = fetcher.fetch(url, use_proxy=False)
        return html
    except Exception:
        return None


HANDLERS = [
    ("national restaurant exchange", handler_nerb),
    ("asking price", handler_newbiz),
    ("basecamp.com", handler_marigold),
]


def install(api_handlers: list, helpers: dict) -> None:
    """Append the handlers to V6's API_HANDLERS (idempotent) and keep helpers:
    make (_make_api_listing), status (listing_status.lookup_status)."""
    _H.update(helpers)
    have = {id(h) for _s, h in api_handlers}
    for sig, fn in HANDLERS:
        if id(fn) not in have:
            api_handlers.append((sig, fn))
