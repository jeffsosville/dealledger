"""
cr_card_fields - per-broker card readers for the 2026-11-23 count-readings job
(task w4-card-fields).

Eight brokers whose cards were read wrong, each for a different reason:

  americanbusinessbrokers.com  table; price = the "Listing Price" column, never a
                               sales figure out of the description
  alliantbrokers.com           price = the "Price:" label (we stored "Gross Sale")
                               and 2 of 19 cards were lost to the generic detector
  enlign.com                   price = the card's price line ("Market to Determine"
                               is a withheld price, not a missing one); the page
                               header total counts Coming Soon cards, which are
                               not listings
  lionbusinessadvisors.com     frameset front for lionbusinessbrokers.com; the two
                               listing pages are discovered from the nav each run
  arkansasbusinessbrokers.com  Squarespace JSON index with no titles/prices: read
                               each item's own JSON body
  ubibiz.com                   Wix: the price is the last paragraph of the detail page
  krbb.com                     titles carried "Price $X.XX" appended
  greenandco.biz               a non-200 detail page (WAF) overwrote the card's
                               title with "403 - Forbidden"

Wiring (all in dealledger_scraper_v6.py, three small hooks):
  * extract_via_api()  -> read_index()      whole-index readers (first five + ubibiz)
  * the detail loop    -> is_error_page()   shared guard, every broker
  * end of the card path -> finalize_cards() krbb title suffix

The readers are pure functions of (html, fetcher, ctx); ctx carries V6's row
builder so ids are the ones V6 already minted (detail-URL identity, principle 4).
Nothing here talks to the database.
"""
from __future__ import annotations

import json
import re
from types import SimpleNamespace
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

try:                                    # same phrase set as every other writer
    from listing_status import DROP, lookup_status
except Exception:                       # pragma: no cover - module always present
    DROP, lookup_status = "DROP", None
try:
    from price_disclosure import withheld_price_phrase
except Exception:                       # pragma: no cover
    withheld_price_phrase = lambda text: None  # noqa: E731

# Domains with a reader here (bare host, no www).
READER_DOMAINS = frozenset({
    "americanbusinessbrokers.com", "alliantbrokers.com", "enlign.com",
    "lionbusinessadvisors.com", "arkansasbusinessbrokers.com", "ubibiz.com",
})

_WELL_FORMED = re.compile(r"^(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?$")
_SUFFIX = {"k": 1_000, "m": 1_000_000, "mm": 1_000_000, "million": 1_000_000}
_MONEY = re.compile(
    r"\$?\s*(\d[\d,]*(?:\.\d+)?)(?:\s?(million|mm|m|k)(?![A-Za-z0-9]))?", re.I)


def _bare(host):
    h = (host or "").lower()
    return h[4:] if h.startswith("www.") else h


def _clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def money(text, lo=1, hi=1_000_000_000) -> Optional[float]:
    """First dollar figure in `text` ("$1,575,000", "1,295,000", "$4.72M"), or
    None. Malformed digit grouping ("$5,500,00") is rejected, never guessed."""
    m = _MONEY.search(text or "")
    if not m:
        return None
    num = m.group(1)
    if not _WELL_FORMED.match(num):
        return None
    v = float(num.replace(",", ""))
    if m.group(2):
        v *= _SUFFIX[m.group(2).lower()]
    return v if lo <= v <= hi else None


def _fin(text) -> Optional[float]:
    """A financial figure printed on a card. Under $1,000 is a typo or a
    placeholder ("Gross Sale: $7", "Gross Sales 1"), not a business's revenue."""
    return money(text, lo=1_000)


def _price_fields(row, price_text):
    """Set asking_price from a labelled price value, or record the broker's
    explicit withholding. A bare "-", "N/A" or blank is simply missing."""
    price = money(price_text, lo=1_000)
    if price:
        row["asking_price"] = price
        return
    phrase = withheld_price_phrase(_clean(price_text))
    if phrase:
        row["asking_price"] = None
        row["price_not_disclosed"] = True
        row["broker_price_text"] = phrase


def _status(row, label_text, domain):
    """Apply a broker status label through the shared phrase set. Returns DROP
    for Coming Soon / For Lease (caller must not emit)."""
    if not label_text or lookup_status is None:
        return None
    hit = lookup_status(label_text, domain)
    if not hit:
        return None
    status, label = hit
    if status == DROP:
        return DROP
    row["status"] = status
    if label:
        row["broker_status_text"] = label
    return None


def _soup(html):
    return BeautifulSoup(html or "", "html.parser")


# --------------------------------------------------------------------------
# americanbusinessbrokers.com
# --------------------------------------------------------------------------
_ABB_CODE = re.compile(r"^\s*(ABB\w*-[A-Z]?\d+)\s*[-–—:]\s*", re.I)
_BFS_SUFFIX = re.compile(r"\s*[-–—]\s*business\s+for\s+sale\s*$", re.I)


def read_abb(url, html, name, fetcher, ctx):
    soup = _soup(html)
    rows, drops, seen = [], [], set()
    for tr in soup.select("table#oDivTable tr"):
        tid = tr.get("id") or ""
        if not tid.startswith("L"):
            continue
        th, a = tr.find("th"), tr.select_one("th a[href]")
        tds = tr.find_all("td")
        if not th or not a or len(tds) < 3:
            continue
        detail = ctx.norm_url(urljoin(url, a["href"]))
        if detail in seen:
            continue
        seen.add(detail)
        raw = _clean(a.get_text(" "))
        code = None
        m = _ABB_CODE.match(raw)
        if m:
            code, raw = m.group(1).upper(), raw[m.end():]
        title = _BFS_SUFFIX.sub("", raw).strip()
        loc = _clean(th.get_text("|", strip=True).split("|", 1)[1]) if "|" in th.get_text("|", strip=True) else ""
        drow = soup.find("tr", id="D" + tid[1:])
        desc = _clean(drow.get_text(" ")) if drow else title
        row = ctx.make_row(url, name, title=title, detail_url=detail,
                           revenue=_fin(tds[2].get_text()) if len(tds) > 2 else None,
                           cash_flow=_fin(tds[3].get_text()) if len(tds) > 3 else None,
                           state=ctx.state_from_location(loc), description=desc)
        # Listing Price column only (td 0). Down Payment / Gross Sales / Owner
        # Benefit / Total Assets are other columns and the description's sales
        # figure ("The 2025 sales were over $2,900,000") is prose.
        _price_fields(row, tds[0].get_text())
        if code:
            row["source_listing_id"] = code
        rows.append(row)
    return rows, drops


# --------------------------------------------------------------------------
# alliantbrokers.com
# --------------------------------------------------------------------------
def read_alliant(url, html, name, fetcher, ctx):
    soup = _soup(html)
    rows, drops, seen = [], [], set()
    for a in soup.select("a.single-grid[href]"):
        detail = ctx.norm_url(urljoin(url, a["href"]))
        h = a.select_one(".post-title")
        if not h or detail in seen:
            continue
        seen.add(detail)
        title = _clean(h.get_text(" "))
        fields = {}
        for p in a.select(".price_main_wrap p"):
            lab = p.find("span")
            if lab:
                key = _clean(lab.get_text()).rstrip(":").lower()
                fields[key] = _clean(p.get_text(" ").replace(lab.get_text(" "), "", 1)
                                     if lab.get_text(" ") in p.get_text(" ")
                                     else p.get_text(" ").replace(lab.get_text(), "", 1))
        paras = [p for p in a.select(".single-grid-content p")]
        loc = _clean(paras[0].get_text()) if paras else ""
        desc = _clean(paras[1].get_text(" ")) if len(paras) > 1 else title
        row = ctx.make_row(url, name, title=title, detail_url=detail,
                           revenue=_fin(fields.get("gross sale")),
                           cash_flow=_fin(fields.get("cash flow")),
                           state=ctx.state_from_location(loc) or ctx.state_from_location(title),
                           description=desc)
        _price_fields(row, fields.get("price"))
        hit = ctx.title_status(title, "alliantbrokers.com")
        if hit:
            if hit[0] == DROP:
                drops.append((title[:80], hit[1]))
                continue
            row["status"] = hit[0]
            if hit[1]:
                row["broker_status_text"] = hit[1]
        rows.append(row)
    return rows, drops


# --------------------------------------------------------------------------
# enlign.com
# --------------------------------------------------------------------------
def read_enlign(url, html, name, fetcher, ctx):
    soup = _soup(html)
    rows, drops, seen = [], [], set()
    for art in soup.select("article.listing-card-item"):
        def slot(s):
            el = art.select_one(f'[data-slot="{s}"]')
            return _clean(el.get_text(" ")) if el else ""
        lid = slot("internal-id-value")
        title = slot("title")
        link = art.select_one('[data-slot="title-link"]') or art.select_one("a[href]")
        if not (lid and title and link):
            continue                    # the empty <template> card at the end of the grid
        detail = ctx.norm_url(urljoin(url, link["href"]))
        if detail in seen or detail.rstrip("/") == url.rstrip("/"):
            continue
        seen.add(detail)
        fin = {}
        for li in art.select("li.listing-card-financial"):
            lab = li.select_one(".listing-card-financial-label")
            val = li.select_one(".listing-card-financial-value")
            if lab and val:
                fin[_clean(lab.get_text()).lower()] = _clean(val.get_text())
        bullets = [_clean(b.get_text()) for b in art.select(".listing-card-bullet")]
        loc = slot("location")
        row = ctx.make_row(url, name, title=title, detail_url=detail,
                           revenue=_fin(fin.get("annual sales")),
                           cash_flow=_fin(fin.get("seller discretionary earnings")),
                           state=ctx.state_from_location(loc),
                           description=" ".join(x for x in [slot("industry") + "."] + bullets if x))
        # The card's price line only. "Market to Determine" is the broker
        # withholding the price (published as "Price not disclosed").
        _price_fields(row, slot("price"))
        row["source_listing_id"] = lid
        if _status(row, slot("status"), "enlign.com") == DROP:
            drops.append((title[:80], slot("status")))
            continue
        rows.append(row)
    return rows, drops


# --------------------------------------------------------------------------
# lionbusinessadvisors.com (frameset -> lionbusinessbrokers.com)
# --------------------------------------------------------------------------
_LION_PAGE_TEXT = re.compile(
    r"\b(?:sde|ebitda)\b.*\$?\s*\d+(?:\.\d+)?\s*m\b", re.I)
_LION_SITEMAP_SLUG = re.compile(r"(adjusted[-_]?(?:sde|ebitda)|ebitda[-_]?1m|business-for-sale-[a-z]+/?$)", re.I)
_LION_PREFIX = re.compile(
    r"^\s*(UNDER\s+LOI|COMING\s+SOON|UNDER\s+CONTRACT|SALE\s+PENDING|PENDING|SOLD)\s*[:\-]\s*", re.I)
_LION_POLICY = re.compile(r"We\s+intentionally\s+do\s+not\s+set\s+a\s+public\s+asking\s+price", re.I)


def _frame_target(html, url):
    m = re.search(r"<frame[^>]+src=['\"]([^'\"]+)['\"]", html or "", re.I)
    if m and "blank" not in m.group(1).lower():
        return urljoin(url, m.group(1))
    return None


def _lion_pages(home_url, home_html, fetcher):
    """The listing pages, discovered from the live nav (anchor text names the
    price band: "Adjusted SDE: Up To $1M", "Adjusted EBITDA $1M+"), falling back
    to the sitemap. Nothing hard-coded."""
    found = {}
    soup = _soup(home_html)
    for a in soup.find_all("a", href=True):
        txt = _clean(a.get_text(" "))
        if txt and _LION_PAGE_TEXT.search(txt):
            found.setdefault(urljoin(home_url, a["href"]).split("#")[0], txt)
    if found:
        return list(found)
    root = f"{urlparse(home_url).scheme}://{urlparse(home_url).netloc}"
    pages = []
    try:
        idx = fetcher._fetch_http(root + "/sitemap.xml")
        subs = re.findall(r"<loc>(?:<!\[CDATA\[)?([^\]<]+)", idx)
        for sm in [s for s in subs if "page" in s] or [root + "/sitemap.xml"]:
            body = idx if sm.endswith("/sitemap.xml") else fetcher._fetch_http(sm)
            for u in re.findall(r"<loc>(?:<!\[CDATA\[)?([^\]<]+)", body):
                if _LION_SITEMAP_SLUG.search(u):
                    pages.append(u)
    except Exception:
        pass
    return list(dict.fromkeys(pages))


def read_lion(url, html, name, fetcher, ctx):
    rows, drops, keys = [], [], set()
    home_url, home_html = url, html
    target = _frame_target(html, url)
    if target:
        home_url, home_html = target, fetcher._fetch_http(target)
    pages = _lion_pages(home_url, home_html, fetcher)
    print(f"   cr_card_fields lion: {len(pages)} listing page(s) discovered: "
          + ", ".join(urlparse(p).path for p in pages))
    for page_url in pages:
        try:
            page_html = home_html if page_url.rstrip("/") == home_url.rstrip("/") else fetcher._fetch_http(page_url)
        except Exception as exc:                      # one dead page must not hide the other
            print(f"   cr_card_fields lion: {page_url} failed: {str(exc)[:80]}")
            continue
        soup = _soup(page_html)
        seen = set()
        policy = _LION_POLICY.search(_clean(soup.get_text(" ")))
        for el in soup.find_all(string=re.compile(r"Listed at|Last FY Revenue|Listing\s*#")):
            sec = el.find_parent("section", class_="elementor-inner-section")
            if sec is None or id(sec) in seen:
                continue
            seen.add(id(sec))
            right = None
            for p in sec.find_all("p"):
                if re.search(r"Listed at|Last FY Revenue|Listing\s*#", p.get_text()):
                    right = p
            left = next((p for p in sec.find_all("p") if p is not right and p.get_text(strip=True)), None)
            if left is None and right is not None:
                # card whose title/teaser are not in a <p> (Listing #31191):
                # the element holding the detail link, if it is not the right column
                a0 = sec.find("a", href=re.compile(r"listing-\d+"))
                cand = a0.find_parent(["div", "h2", "h3", "h4"]) if a0 else None
                if cand is not None and right not in cand.find_all("p"):
                    left = cand
            if right is None or left is None:
                continue
            link = left.find("a", href=True)
            strong = left.find("strong")
            lead = _clean((strong or left).get_text(" "))
            prefix = _LION_PREFIX.match(lead)
            label = _clean(prefix.group(1)).upper() if prefix else None
            top = sec.find_parent("section", class_="elementor-top-section")
            if label is None and top is not None:
                for img in top.find_all("img"):
                    m = re.search(r"Listing\s+(Under\s+LOI|Coming\s+Soon)", img.get("alt") or "", re.I)
                    if m:
                        label = m.group(1).upper()
                        break
            if link is not None:
                title = _clean(link.get_text(" "))
            else:
                title = _LION_PREFIX.sub("", lead).strip()
            teaser = _clean(left.get_text(" "))
            if prefix:
                teaser = _LION_PREFIX.sub("", teaser, count=1)
            if re.search(r"not an operating business|^IP Acquisition", teaser + " " + title, re.I):
                drops.append((title[:80], "IP/asset sale (broker says not an operating business)"))
                continue
            right_txt = right.get_text("\n", strip=True)
            lines = {m.group(1).lower(): m.group(2) for m in
                     (re.match(r"(listed at|sde|ebitda|last fy revenue)\s*:?\s*(.*)$", ln, re.I)
                      for ln in right_txt.split("\n")) if m}
            num = re.search(r"Listing\s*#\s*(\d+)", right_txt)
            detail = ctx.norm_url(urljoin(page_url, link["href"])) if link is not None else None
            row = ctx.make_row(url, name, title=title, detail_url=detail,
                               revenue=_fin(lines.get("last fy revenue")),
                               cash_flow=_fin(lines.get("sde")),
                               description=teaser or title)
            if "listed at" in lines:
                _price_fields(row, ("$" + lines["listed at"]) if re.match(r"[\d,]+$", lines["listed at"]) else lines["listed at"])
                if not row.get("asking_price") and re.match(r"\s*open\b", lines["listed at"], re.I):
                    row["price_not_disclosed"] = True
                    row["broker_price_text"] = "Listed at: OPEN"
            elif policy:                                  # $1M+ page: no price by policy
                row["price_not_disclosed"] = True
                row["broker_price_text"] = _clean(policy.group(0))
            if num:
                row["source_listing_id"] = num.group(1)
            if label:
                if _status(row, label, "lionbusinessadvisors.com") == DROP:
                    drops.append((title[:80], label))
                    continue
            if row["id"] in keys:
                continue
            keys.add(row["id"])
            rows.append(row)
    return rows, drops


# --------------------------------------------------------------------------
# arkansasbusinessbrokers.com (Squarespace collection)
# --------------------------------------------------------------------------
def _ark_item_text(item_json):
    body = (item_json.get("body") or "")
    lines = [ln for ln in BeautifulSoup(body, "html.parser").get_text("\n", strip=True).split("\n") if ln.strip()]
    return lines


def _ark_label_value(lines, label):
    """Value printed under/after a "Label:" line in the body."""
    for i, ln in enumerate(lines):
        if re.match(rf"{label}\s*:?\s*$", ln, re.I) and i + 1 < len(lines):
            return lines[i + 1]
        m = re.match(rf"{label}\s*:\s*(\S.*)$", ln, re.I)
        if m:
            return m.group(1)
    return None


def read_arkansas(url, html, name, fetcher, ctx):
    rows, drops, seen = [], [], set()
    base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    api = url if "format=json" in url else url.split("?")[0] + "?format=json"
    try:
        data = json.loads(html)
    except ValueError:
        data = json.loads(fetcher._fetch_http(api))
    items = list(data.get("items") or [])
    pag, hops = data.get("pagination") or {}, 0
    while pag.get("nextPageUrl") and hops < 20:          # null today: all 15 on one page
        hops += 1
        nxt = json.loads(fetcher._fetch_http(base + pag["nextPageUrl"] + ("&" if "?" in pag["nextPageUrl"] else "?") + "format=json"))
        items += nxt.get("items") or []
        pag = nxt.get("pagination") or {}
    for it in items:
        full = it.get("fullUrl")
        if not full or full in seen:
            continue
        seen.add(full)
        detail = ctx.norm_url(base + full)
        try:
            d = json.loads(fetcher._fetch_http(detail.split("?")[0] + "?format=json")).get("item") or {}
        except Exception as exc:
            print(f"   cr_card_fields arkansas: {full} detail failed: {str(exc)[:80]}")
            continue
        lines = _ark_item_text(d)
        title = _clean(d.get("title") or (lines[0] if lines else ""))
        cats = [c for c in (it.get("categories") or [])]
        if not title:
            continue
        if any(c.lower() == "real estate" for c in cats):
            drops.append((title[:80], "building / real estate only"))
            continue
        loc = _clean(_ark_label_value(lines, "Location") or "")
        parts = [p.strip() for p in loc.split(",")]
        city = parts[-2] if len(parts) >= 3 and re.match(r"[A-Z]{2}\b", parts[-1]) else None
        excerpt = BeautifulSoup(it.get("excerpt") or "", "html.parser")
        desc = _clean(" ".join(lines[1:]))[:1000]
        row = ctx.make_row(url, name, title=title, detail_url=detail,
                           city=city, state=ctx.state_from_location(loc), description=desc)
        price_val = _ark_label_value(lines, "Price")
        if price_val is not None:
            _price_fields(row, price_val)
        strong = excerpt.find("strong")
        marker = _clean(strong.get_text()).rstrip("!") if strong else ""
        if re.search(r"[—-]\s*SALE PENDING\s*[—-]", " ".join(lines[:4]), re.I):
            marker = marker or "SALE PENDING"
        if marker and _status(row, marker, "arkansasbusinessbrokers.com") == DROP:
            drops.append((title[:80], marker))
            continue
        rows.append(row)
    return rows, drops


# --------------------------------------------------------------------------
# ubibiz.com (Wix)
# --------------------------------------------------------------------------
_UBI_PRICE_PARA = re.compile(
    r"^(?:(?:reduced|asking|price)\s*(?:to|:)?\s*)?\$?\s*(\d{1,3}(?:\s*,\s*\d{3})+|\d{4,8})\b")


def ubibiz_price(paragraphs):
    """The price UBI prints as the LAST paragraph of the detail text box
    ("$195,000 or offer", "95000 or offer", "770, 000 or ofer + inventory",
    "reduced to $195,000 or offer"). Only a short final paragraph that opens
    with the figure counts; the description's other dollar amounts never do."""
    paras = [_clean(p) for p in paragraphs if _clean(p)]
    paras = [p for p in paras if not p.startswith("©")]
    if not paras:
        return None
    last = paras[-1]
    if len(last) > 70:
        return None
    m = _UBI_PRICE_PARA.match(last.lower())
    if not m:
        return None
    v = float(re.sub(r"[\s,]", "", m.group(1)))
    return v if 1_000 <= v <= 100_000_000 else None


def read_ubibiz(url, html, name, fetcher, ctx):
    soup = _soup(html)
    base_path = urlparse(url).path.rstrip("/")
    rows, drops, seen = [], [], set()
    for a in soup.find_all("a", href=True):
        href = urljoin(url, a["href"]).split("#")[0]
        # Wix: the index is /buy-a-business, the detail pages /buy-a-business-1/<slug>
        if not re.match(re.escape(base_path) + r"(?:-\d+)?/[^/]+$", urlparse(href).path) or href in seen:
            continue
        seen.add(href)
        detail = ctx.norm_url(href)
        title = None
        desc = ""
        price = None
        try:
            dsoup = _soup(fetcher._fetch_http(detail))
            for x in dsoup(["script", "style"]):
                x.decompose()
            paras = [p.get_text(" ", strip=True) for p in dsoup.find_all(["p", "h1", "h2", "h3"])]
            paras = [p for p in paras if p]
            # <title> is the listing's own title; the first <h1> is the
            # "Get More Info" contact-form heading.
            tt = _clean(dsoup.title.get_text()) if dsoup.title else ""
            h1s = [_clean(h.get_text(" ")) for h in dsoup.find_all("h1")]
            h1s = [h for h in h1s if h and h.lower() != "get more info"]
            title = tt or (h1s[-1] if h1s else None)
            if title in paras:                     # body = everything after the title heading
                body = paras[len(paras) - 1 - paras[::-1].index(title) + 1:]
            else:
                body = [p for p in paras if p != title]
            price = ubibiz_price(body)
            desc = _clean(" ".join(p for p in body if not p.startswith("©")))[:1000]
        except Exception as exc:
            print(f"   cr_card_fields ubibiz: {detail[-40:]} detail failed: {str(exc)[:80]}")
        title = title or ctx.title_from_slug(detail)
        if not title:
            continue
        row = ctx.make_row(url, name, title=title, detail_url=detail,
                           asking_price=price, description=desc or title)
        rows.append(row)
    return rows, drops


# --------------------------------------------------------------------------
# dispatch (extract_via_api hook)
# --------------------------------------------------------------------------
_READERS = {
    "americanbusinessbrokers.com": read_abb,
    "alliantbrokers.com": read_alliant,
    "enlign.com": read_enlign,
    "lionbusinessadvisors.com": read_lion,
    "arkansasbusinessbrokers.com": read_arkansas,
    "ubibiz.com": read_ubibiz,
}


# Domains whose page header total INCLUDES the Coming Soon cards we drop.
STATED_INCLUDES_DROPS = frozenset({"enlign.com"})
LAST_DROPS = {}


def adjust_stated(tracker, domain):
    """enlign.com's header says "12 listings" and the grid holds 12 cards, 3 of
    them Coming Soon (not listings). Subtract the cards we dropped so the
    broker's count is 9, the number of listings it actually offers, instead of
    marking a complete read INCOMPLETE_LOW. The reduced count is still the
    broker's own stated figure (source stays 'stated', note says why)."""
    d = _bare(domain)
    n = LAST_DROPS.pop(d, 0)
    st = getattr(tracker, "stated", None)
    if not n or d not in STATED_INCLUDES_DROPS or not st or st.get("source") != "stated":
        return
    if st["count"] - n < 1:
        return
    new = dict(st)
    new["count"] = st["count"] - n
    new["note"] = "; ".join(x for x in (st.get("note"), f"stated {st['count']} less {n} Coming Soon") if x)
    tracker.stated = new
    tracker._cands = None            # result() must not re-derive the raw count


def read_index(url, html, name, fetcher, ctx):
    """(rows, drops) for a registered domain, or None for any other domain.
    A registered reader that raises, or finds nothing, returns ([], []) after
    saying so loudly; V6 then falls through to the generic path (never silent)."""
    reader = _READERS.get(_bare(urlparse(url).netloc))
    if reader is None:
        return None
    if isinstance(ctx, dict):
        ctx = SimpleNamespace(**ctx)
    try:
        rows, drops = reader(url, html, name, fetcher, ctx)
        LAST_DROPS[_bare(urlparse(url).netloc)] = len(drops)
        return rows, drops
    except Exception as exc:
        print(f"   cr_card_fields: {reader.__name__} FAILED {type(exc).__name__}: {str(exc)[:120]}")
        return [], []


# --------------------------------------------------------------------------
# shared guard: a detail "page" that is really an error page
# --------------------------------------------------------------------------
_ERROR_HEAD = re.compile(
    r"^\W*(?:(?:error\s*)?(?:4\d\d|5\d\d)\b.*|forbidden|access\s+denied|not\s+found|"
    r"page\s+not\s+found|just\s+a\s+moment\.*|attention\s+required.*|"
    r"service\s+(?:temporarily\s+)?unavailable|bad\s+gateway|too\s+many\s+requests|"
    r"request\s+blocked|you\s+have\s+been\s+blocked)\W*$", re.I)


def is_error_page(html) -> bool:
    """True when a fetched "detail page" is an HTTP error / WAF block page:
    its <title> (or its only <h1>) is an error phrase ("403 - Forbidden",
    "Access Denied", "Just a moment...") AND the visible text is short.
    The fetcher's Playwright path hands back the error body without a status,
    and enrich_from_detail then replaced the card's title with it (greenandco).
    Strict on purpose (principle 8): a real page with a long body never matches."""
    if not html:
        return True
    soup = _soup(html)
    for x in soup(["script", "style", "noscript", "template"]):
        x.decompose()
    text = _clean(soup.get_text(" "))
    if len(text) > 1500:
        return False
    heads = []
    if soup.title and soup.title.string:
        heads.append(_clean(soup.title.string))
    h1s = soup.find_all("h1")
    if len(h1s) == 1:
        heads.append(_clean(h1s[0].get_text(" ")))
    return any(_ERROR_HEAD.match(h) for h in heads if h)


# --------------------------------------------------------------------------
# krbb.com: "Title Price $250,000.00" -> "Title"
# --------------------------------------------------------------------------
_KRBB_PRICE_TAIL = re.compile(r"\s*\bPrice\s*\$\s*[\d,]+(?:\.\d+)?\s*$", re.I)


def strip_price_suffix(title):
    return _KRBB_PRICE_TAIL.sub("", title or "").strip() or title


TITLE_SUFFIX_DOMAINS = frozenset({"krbb.com"})


def finalize_cards(domain, cards):
    """Post-pass over a generic-path broker's final cards. krbb: titles come off
    the Wix product card as "<name> Price $X.XX". Ids are url-keyed, so retitling
    never changes identity. Returns how many titles changed."""
    if _bare(domain) not in TITLE_SUFFIX_DOMAINS:
        return 0
    n = 0
    for c in cards:
        new = strip_price_suffix(c.get("title"))
        if new and new != c.get("title"):
            c["title"] = new
            n += 1
    return n
