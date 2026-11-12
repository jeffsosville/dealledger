"""
displayed_count — the broker's own count of what it has for sale.

Why this exists (2026-11-11)
----------------------------
A crawl closed 'ok' whenever the page loaded and something parsed.
Nothing compared what we saw to what the broker says it has:

    commercialroutesforsale.com   30 seen of 61 on the page    -> 'ok'
    vikingmergers.com            411 seen of 62 ("Showing 62") -> 'ok'
    peakbusinessbrokersllc.com     1 seen of 33 on the page    -> 'ok'

An 'ok' crawl is read downstream as "the broker's whole index was seen"
(v_dom_direct, the absence rule). A short crawl then retires real listings;
an inflated one keeps dead ones live. Both are silent (principles 1, 7, 11).

This module is pure (no network, no DB). It answers two questions:

  displayed count  — what the broker says it lists, from the index HTML.
      'stated'  : the page says it ("Showing 62 items", "1-12 of 85",
                  "For Sale (56)", "33 listings").
      'counted' : no stated count; count per-card labels ("Asking Price:",
                  "More Info") on the index pages we fetched. Weaker: it
                  only sees the pages the crawler reached, so it cannot
                  detect a pagination stop. Advisory only — see
                  COUNTED_MARKS_PARTIAL.
  verdict          — is listings_seen within tolerance of the displayed count.

Sectioned pages (For Sale N / Under Contract N / Sold N): the displayed count
is FOR SALE + UNDER CONTRACT (Jeff's decision 6, 2026-11-11: under-contract
listings are still listed and are emitted active with broker_status_text, so
they belong in the count we compare against — eatz "For Sale (56)" +
"Under Contract (16)" = 72). Sold / Coming Soon / For Lease go in the note
only (Coming Soon is not emitted, decision 5). The under-contract phrase set
is scrapers/listing_status.py when importable.

Guards (2026-11-12, ported from the job-3 count-sweep harness, where 1,574
broker index pages showed each failure):

  * phone-number tail   — "(925) 932-2450 | Listings" is lbaweb.com's phone
                          number beside its nav, not 2,450 listings.
  * year                — "© 2026 Routes" (routesforsale.net), "since 1985":
                          a bare-number hit in 1990–2035 is not a count.
  * track record        — "sold 500 businesses", "helped 45 businesses": a
                          count right after sold/closed/helped/... is history.
  * implausible         — a weak (label / noun / "Found N") count more than
                          10x every independent signal (per-card labels,
                          distinct detail links, and — when the caller knows
                          it — the listings seen) is a zip code, an MLS id or
                          a founding year (aspectbrokers 7610, shumacher 1985).
                          With no signal at all (JS page, nothing seen) the
                          count is kept: no evidence either way.
  * new forms           — "Found 1085 Business Listings" (execbb.com),
                          "N Business Listings", "N Listings Selected".
"""
from __future__ import annotations

import html as _html
import re
from urllib.parse import urlparse, parse_qs

# Completeness tolerance (owner spec 2026-11-11: ~90% / ~110%).
LOW_RATIO = 0.90
HIGH_RATIO = 1.10
# A 'counted' figure is our own heuristic, not the broker's statement. It is
# recorded and reported, and the absence rule should ignore a crawl it flags
# (count_complete = false), but it does not by itself close a crawl 'partial'.
COUNTED_MARKS_PARTIAL = False
# Counts outside this range are not believable as an index size.
MIN_COUNT, MAX_COUNT = 1, 20000

_NUM = r"(\d{1,3}(?:,\d{3})+|\d{1,5})"
_NOUN = (r"(?:items?|results?|(?:business\s+)?listings?(?:\s+selected)?|businesses(?:\s+for\s+sale)?|"
         r"properties|opportunities|routes?|practices|matches)")
# Words after a count that make it a track-record claim, not an inventory.
_NOT_INVENTORY = re.compile(
    r"^\s*(?:\w+\s+){0,2}?(?:sold|closed|completed|transactions?|since|years?|"
    r"successfully|brokered|valued|in\s+sales)\b", re.I)
_BEFORE_NOT_INVENTORY = re.compile(
    r"(?:over|more\s+than|nearly|almost|about|approximately|top|\+|"
    r"\bsince|\best\.?|\bestablished|\bfounded|©|\(c\)|\bcopyright)\s*$", re.I)
# "We have sold 500 businesses", "helped over 45 businesses": a count right
# after a track-record verb (only filler words between) is history, not
# inventory. Filler only: a status-filter nav "Active Sold Under Contract
# Wanted 108 listings" (unbroker.com) must still count.
_TRACK_RECORD_BEFORE = re.compile(
    r"\b(?:sold|closed|helped|brokered|sell|served|represented)"
    r"(?:\s+(?:over|more|than|nearly|almost|about|approximately|a|total|of|the|"
    r"our|we|have|successfully|since|to|and))*\s*$", re.I)
# The end of a phone number just before the count: "(925) 932-" / "925-932-" /
# "925.932." + "2450 Listings" (lbaweb.com nav).
_PHONE_TAIL = re.compile(r"(?:\(\d{3}\)\s*|\b\d{3}[\s.-])\d{3}[\s.-]$")
# A bare number in this range beside a noun is a year ("© 2026 Routes").
YEAR_MIN, YEAR_MAX = 1990, 2035
# A weak count more than this many times every independent signal is dropped.
IMPLAUSIBLE_RATIO = 10
# Levels whose count is a bare number next to a word: guarded by plausibility.
# section / range_of / showing are pagination statements and are not.
_WEAK_LEVELS = frozenset({"found", "label", "noun"})

# Section labels for "Label (N)" counts, matched as a whole label (a tag or
# line boundary before it) so "Business For Sale (774)" — a sale-type facet on
# eatz-associates.com — is not read as the "For Sale" status section.
_FOR_SALE_LABELS = (r"for\s+sale|available(?:\s+(?:listings|businesses))?|"
                    r"active(?:\s+listings)?|current\s+listings|"
                    r"(?:all\s+)?listings|businesses(?:\s+for\s+sale)?|properties")
try:
    from listing_status import UNDER_CONTRACT_PHRASES as _LS_UC
except Exception:                    # module not present (job 3 not applied)
    _LS_UC = None
# Longest first so "contract pending" is tried before "pending".
_UNDER_CONTRACT_SECTION = (
    "|".join(re.escape(p).replace(r"\ ", r"[\s\-]+")
             for p in sorted(_LS_UC, key=len, reverse=True))
    if _LS_UC else
    r"under\s+contract|contract\s+pending|sale\s+pending|pending|under\s+loi|"
    r"under\s+offer|(?:has\s+)?accepted\s+offer|off[\s\-]+market")
_OTHER_SECTIONS = {
    "under_contract": _UNDER_CONTRACT_SECTION,
    "sold": r"(?:recently\s+)?sold|closed(?:\s+deals)?",
    "coming_soon": r"coming\s+soon",
    "for_lease": r"for\s+lease",
    "new_listing": r"new\s+listings?",
}

_PRICE_LABEL = re.compile(
    r"\b(?:asking\s+|list(?:ing)?\s+|sale\s+|purchase\s+)?price\s*(?::|(?=\$\s?\d))", re.I)
_CTA_LABEL = re.compile(
    r"\b(?:more\s+info|view\s+listing|view\s+details|view\s+business|"
    r"listing\s+details|see\s+details)\b", re.I)
_MIN_CARDS = 3


def _texts(page_html: str) -> tuple[str, str]:
    """(flat, bounded): visible text with tags as spaces, and with tags as
    ' | ' so a label can be anchored to its own element."""
    raw = page_html or ""
    # FacetWP renders its facets (with the "For Sale (56)" status counts)
    # into a JSON string inside a <script>; pull those fragments out before
    # scripts are dropped.
    unesc = raw.replace('\\"', '"').replace("\\/", "/")
    facets = re.findall(r'(?s)<div class="facetwp-(?:checkbox|radio)\b.*?</div>', unesc)
    t = re.sub(r"(?is)<(script|style|noscript|template)\b[^>]*>.*?</\1>", " ", raw)
    t = re.sub(r"(?s)<!--.*?-->", " ", t)
    if facets:
        t += " " + " ".join(f"<p>{f}</p>" for f in facets)
    bounded = _html.unescape(re.sub(r"<[^>]+>", " | ", t))
    bounded = re.sub(r"\s+", " ", bounded)
    flat = re.sub(r"\s*\|\s*", " ", bounded)
    flat = re.sub(r"\s+", " ", flat)
    return flat, bounded


def _int(s: str) -> int | None:
    try:
        n = int(s.replace(",", ""))
    except (TypeError, ValueError):
        return None
    return n if MIN_COUNT <= n <= MAX_COUNT else None


def _phone_tail(flat: str, num_start: int) -> bool:
    """True when the digits at num_start finish a phone number."""
    return bool(_PHONE_TAIL.search(flat[max(0, num_start - 16):num_start]))


def _is_year(n: int | None) -> bool:
    return n is not None and YEAR_MIN <= n <= YEAR_MAX


def link_signal(page_html: str) -> int:
    """Distinct detail-like links on a page: the largest group of hrefs that
    share a parent path (or a path + id-like query keys, ?listingid=...).
    Groups under '/' and groups smaller than 3 do not count. Host is ignored,
    so relative and absolute links to one listing path group together."""
    groups: dict[str, set] = {}
    for h in re.findall(r"""(?is)<a\b[^>]*?\bhref\s*=\s*["']([^"'#][^"']*)["']""", page_html or ""):
        h = _html.unescape(h.strip())
        if h.lower().startswith(("javascript", "mailto:", "tel:")):
            continue
        try:
            u = urlparse(h)
        except ValueError:
            continue
        path = u.path.rstrip("/")
        q = parse_qs(u.query)
        idq = sorted(k for k in q if re.search(r"id|listing|bus|ref|num", k, re.I))
        if idq:
            key = (path or "/") + "?" + ",".join(idq)
            groups.setdefault(key, set()).add(path + "?" + "&".join(f"{k}={q[k][0]}" for k in idq))
            continue
        segs = [x for x in path.split("/") if x]
        if not segs:
            continue
        parent = "/" + "/".join(segs[:-1])
        if parent == "/":
            continue
        groups.setdefault(parent, set()).add(path)
    best = max((len(v) for v in groups.values()), default=0)
    return best if best >= 3 else 0


def page_signal(page_html: str) -> int:
    """The larger of the page's per-card label count and its link signal."""
    c = count_cards(page_html)
    return max(c["count"] if c else 0, link_signal(page_html))


def _plausible(n: int, signal: int | None) -> bool:
    """A weak count is implausible only against positive evidence."""
    return not signal or n <= IMPLAUSIBLE_RATIO * signal


def _sections(bounded: str) -> tuple[list[tuple[int, str]], dict[str, int]]:
    """'For Sale (56)'-style counts: (for-sale hits, other sections)."""
    lab = r"(?:^|\|)\s*(%s)\s*(?:\|\s*)*[\(\[]\s*" + _NUM + r"\s*[\)\]]"
    fs = []
    for m in re.finditer(lab % _FOR_SALE_LABELS, bounded, re.I):
        n = _int(m.group(2))
        if n is not None:
            fs.append((n, re.sub(r"\s+", " ", m.group(1)).strip() + f" ({m.group(2)})", ""))
    other: dict[str, int] = {}
    for key, pat in _OTHER_SECTIONS.items():
        for m in re.finditer(lab % pat, bounded, re.I):
            n = _int(m.group(2))
            if n is not None:
                other[key] = max(other.get(key, 0), n)
    return fs, other


def _candidates(page_html: str) -> tuple[list[tuple[str, list]], str, dict]:
    """Every stated-count hit on a page, by level in priority order:
    ([(level, [(n, text, context), ...]), ...], sections note, sections). The phone,
    year and track-record guards are applied here; plausibility is not
    (it needs signals from outside the page — see _choose)."""
    flat, bounded = _texts(page_html)
    fs, other = _sections(bounded)
    other_note = ", ".join(f"{k}={v}" for k, v in sorted(other.items()))
    levels: list[tuple[str, list]] = []

    def ctx(m):
        return flat[max(0, m.start() - 50):m.end() + 30]

    # 1. sectioned: for sale + under contract (decision 6)
    if fs and other:
        levels.append(("section", fs))

    # 2. "x-y of N"
    hits = []
    for m in re.finditer(r"(?:showing\s+|displaying\s+|viewing\s+)?\d{1,5}\s*(?:-|–|—|to)\s*\d{1,5}\s+of\s+"
                         + _NUM + r"(?:\s+" + _NOUN + r")?", flat, re.I):
        n = _int(m.group(1))
        if n is not None:
            hits.append((n, m.group(0), ctx(m)))
    if hits:
        levels.append(("range_of", hits))

    # 3. "Showing N [items]" (not "Showing 1-12", handled above)
    hits = []
    for m in re.finditer(r"\bshowing\s+(?:all\s+)?" + _NUM + r"(?!\s*(?:-|–|—|to)\s*\d)"
                         r"(?:\s+" + _NOUN + r")?", flat, re.I):
        n = _int(m.group(1))
        if n is not None:
            hits.append((n, m.group(0), ctx(m)))
    if hits:
        levels.append(("showing", hits))

    # 3b. "Found 1085 Business Listings" (execbb.com), "Found 12 results"
    hits = []
    for m in re.finditer(r"\bfound\s+" + _NUM + r"\s+(?:business(?:es)?\s+)?"
                         r"(?:listings?|businesses|results?|opportunities|matches)\b", flat, re.I):
        n = _int(m.group(1))
        if n is not None and not _phone_tail(flat, m.start(1)):
            hits.append((n, m.group(0), ctx(m)))
    if hits:
        levels.append(("found", hits))

    # 4. a lone "Listings (N)" / "For Sale (N)" label
    if fs and not other:
        levels.append(("label", fs))

    # 5. "N results" / "N listings found" / "N Business Listings" /
    #    "N Businesses" / "N opportunities" / "N Listings Selected"
    hits = []
    for m in re.finditer(r"(?<![\d$.,])" + _NUM + r"\s+(?:" + _NOUN + r")\b", flat, re.I):
        before = flat[max(0, m.start() - 30):m.start()]
        after = flat[m.end():m.end() + 30]
        if _BEFORE_NOT_INVENTORY.search(before) or _NOT_INVENTORY.match(after):
            continue
        if re.search(r"\$\s*$|#\s*$", before) or _TRACK_RECORD_BEFORE.search(before):
            continue
        n = _int(m.group(1))
        if n is None or _is_year(n) or _phone_tail(flat, m.start(1)):
            continue
        hits.append((n, m.group(0), ctx(m)))
    if hits:
        levels.append(("noun", hits))
    return levels, other_note, other


def _choose(levels, other_note, other, signal=None) -> tuple[dict | None, str | None]:
    """(result, dropped): the first level with a hit, its largest hit.

    At a weak level, if that largest hit is implausible the answer is None
    and `dropped` says why. Neither a smaller hit at the same level
    (matthews.com: "2,467 Results" -> "5 Properties") nor a lower level
    (reputedbrokerage: "For Sale (379)" -> "103 properties") is promoted in
    its place: both are weaker evidence than the count just rejected."""
    for level, hits in levels:
        if not hits:
            continue
        n, text, ctx = max(hits, key=lambda h: h[0])
        if level in _WEAK_LEVELS and not _plausible(n, signal):
            return None, (f"stated '{text.strip()[:80]}' ({level}) dropped: "
                          f">{IMPLAUSIBLE_RATIO}x signal {signal}")
        alts = sorted({h[0] for h in hits if h[0] != n})
        note = "; ".join(x for x in (
            f"level={level}",
            ctx and f"context: ...{ctx.strip()}...",
            other_note and f"sections: {other_note}",
            alts and "other counts: " + ",".join(map(str, alts))) if x)
        r = {"count": n, "source": "stated", "text": text[:200], "note": note}
        if level == "section":
            uc = other.get("under_contract")
            # Only beside a status section ("For Sale", "Available", "Active"):
            # an "All Listings (N)" total may already include under contract.
            if uc and re.match(r"(?:for\s+sale|available|active)\b", r["text"], re.I):
                r["count"] += uc
                r["text"] = f"{r['text']} + under contract ({uc})"[:200]
                r["note"] += "; count = for sale + under contract (decision 6)"
        return r, None
    return None, None


def parse_stated_count(page_html: str, seen: int | None = None) -> dict | None:
    """The count the page states, or None.

    Returns {"count", "source": "stated", "text", "note"}. Priority:
      1. a For Sale section count beside other status sections
         ("For Sale (56)" + "Under Contract (16)")
      2. "1-12 of 85" / "Showing 1 to 12 of 85 results"
      3. "Showing 62 items" / "Showing 62"
      3b. "Found 1085 Business Listings"
      4. "Properties (N)" / "Listings (N)" / "For Sale (N)" alone
      5. "N results" / "N listings" / "N businesses for sale" /
         "N Business Listings" / "N opportunities" / "N Listings Selected"
    Several hits at one level -> the largest (a "Featured: Showing 7 items"
    widget beside the real "Showing 62 items"); the rest go in the note.
    Levels 3b-5 drop a count more than IMPLAUSIBLE_RATIO x the page's own
    card/link signal and `seen` (the caller's listings seen, when known).
    """
    if not page_html:
        return None
    levels, other_note, other = _candidates(page_html)
    if not levels:
        return None
    return _choose(levels, other_note, other, max(page_signal(page_html), seen or 0))[0]


def count_cards(page_html: str) -> dict | None:
    """Per-card label count on one index page, or None if fewer than 3.

    Counts the most frequent per-card label family: price labels
    ("Asking Price:", "Price:") or calls to action ("More Info",
    "View Listing"). Independent of the pattern the crawler chose, so a
    crawler that matched 1 card on a page with 33 "Asking Price:" labels
    shows up.
    """
    if not page_html:
        return None
    flat, _ = _texts(page_html)
    fams = {"price_label": len(_PRICE_LABEL.findall(flat)),
            "cta_label": len(_CTA_LABEL.findall(flat)),
            "table_rows": _priced_table_rows(page_html)}
    fam, n = max(fams.items(), key=lambda kv: kv[1])
    if n < _MIN_CARDS:
        return None
    return {"count": n, "family": fam}


def _priced_table_rows(page_html: str) -> int:
    """Rows carrying a $ amount in the largest such table (route brokers
    list one listing per <tr>: commercialroutesforsale.com RouteList.aspx)."""
    best = 0
    body = re.sub(r"(?is)<(script|style)\b[^>]*>.*?</\1>", " ", page_html)
    for tbl in re.findall(r"(?is)<table\b.*?</table>", body):
        rows = re.findall(r"(?is)<tr\b.*?(?=<tr\b|</table>)", tbl)
        best = max(best, sum(1 for r in rows if re.search(r"\$\s?\d", r)))
    return best


class DisplayedCountTracker:
    """Feed it every index page the crawler fetched; ask for the result.

    The first page with a stated count wins. Otherwise per-page card counts
    are summed ('counted'). The same HTML fed twice counts once.

    Plausibility (weak levels only) is judged at result() time against the
    signals summed over every page fed (cards, detail links) and the
    caller's listings seen — so page 1 of a 20-page index stating "245
    listings" beside 12 cards is not dropped once the crawl has walked on.
    """

    def __init__(self):
        self.stated: dict | None = None
        self.counted = 0
        self.counted_pages = 0
        self.families: set[str] = set()
        self._seen: set[int] = set()
        self._cands = None           # (levels, other_note, other) of the stating page
        self.link_total = 0          # distinct-page sum of link_signal()
        self.card_total = 0          # distinct-page sum of count_cards()
        self.dropped: str | None = None  # why a weak stated count was rejected

    def add_page(self, page_html: str, count: bool = True) -> None:
        """count=False: look for a stated count only (a page that will be
        fed again, e.g. V6's first fetch before a Playwright re-render)."""
        try:
            if not page_html:
                return
            if self._cands is None:
                levels, other_note, other = _candidates(page_html)
                if levels:
                    self._cands = (levels, other_note, other)
                    # provisional (page-level signal); result() re-judges
                    self.stated = _choose(levels, other_note, other, page_signal(page_html))[0]
            if not count:
                return
            h = hash(page_html)
            if h in self._seen:
                return
            self._seen.add(h)
            self.link_total += link_signal(page_html)
            c = count_cards(page_html)
            if c:
                self.card_total += c["count"]
                self.counted += c["count"]
                self.counted_pages += 1
                self.families.add(c["family"])
        except Exception:            # never break a crawl over a count
            pass

    def result(self, seen: int | None = None) -> dict | None:
        """seen: listings the crawl emitted, as one more plausibility signal."""
        if self._cands is not None:
            try:
                signal = max(self.card_total, self.link_total, seen or 0)
                self.stated, self.dropped = _choose(*self._cands, signal)
            except Exception:        # never break a crawl over a count
                pass
        if self.stated:
            return dict(self.stated)
        if self.counted_pages:
            return {"count": self.counted, "source": "counted", "text": None,
                    "note": "; ".join(x for x in (
                        self.dropped,
                        f"{'+'.join(sorted(self.families))} over "
                        f"{self.counted_pages} page(s)") if x)}
        return None


def count_verdict(seen: int | None, displayed: int | None,
                  low: float = LOW_RATIO, high: float = HIGH_RATIO) -> bool | None:
    """True = complete, False = incomplete, None = no displayed count."""
    if displayed is None or seen is None or displayed < MIN_COUNT:
        return None
    return low * displayed <= seen <= high * displayed


def verdict_label(seen, displayed) -> str:
    v = count_verdict(seen, displayed)
    if v is None:
        return "UNKNOWN (no displayed count)"
    if v:
        return "COMPLETE"
    return "INCOMPLETE_LOW" if seen < displayed else "INCOMPLETE_HIGH"
