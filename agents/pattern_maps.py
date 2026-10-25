#!/usr/bin/env python3
"""
pattern_maps.py — listing-page patterns for brokers that fail every crawl with NO_PATTERN.

V6 uses a cached container selector from data/pattern_cache.json before it tries to
detect one. For sites where detection never works, this tool fetches the listing page
once, asks Claude for the CSS selector of one listing card, and keeps it only if V6's
own gates accept the result (_score_group, the nav-selector reject, from_card, and
validate_listing_set). Python then runs the selector every night; no AI on daily runs.

    python3 agents/pattern_maps.py run --domain naz-business.com          # one site, dry run
    python3 agents/pattern_maps.py run --domain naz-business.com --write  # store if OK
    python3 agents/pattern_maps.py run --limit 20                         # first 20 targets, dry run
    python3 agents/pattern_maps.py run --all --write                      # every target (needs Jeff's go-ahead)
    python3 agents/pattern_maps.py run --targets data/pattern_maps/targets-2026-10-25.csv --limit 100

Never writes to Supabase. --write only updates data/pattern_cache.json; listings change
on the next nightly crawl, through the normal gates. Every run writes
data/pattern_maps/results-<date>.csv with the verdict and sample titles per site.

Verdicts: OK (stored with --write) · REVIEW (passed but weak; not stored) ·
NO_PATTERN · HTTP_403 · CAPTCHA (flag for a human, never retried) · FETCH_ERROR · SKIPPED
"""
import argparse, csv, json, os, re, sys, time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scrapers"))
from dealledger_scraper_v6 import (  # noqa: E402
    PageFetcher, PatternCache, PatternDetector, ListingExtractor,
    _is_nav_selector, _needs_proxy, validate_listing_set,
)

MODEL = "claude-sonnet-4-6"
TARGETS = ROOT / "data" / "pattern_maps" / "targets-2026-10-18.csv"
OUT_DIR = ROOT / "data" / "pattern_maps"
MAX_HTML_CHARS = 90_000
CAPTCHA_RE = re.compile(r"(captcha|cf-challenge|challenge-platform|are you a robot|sgcaptcha|access denied)", re.I)

PROMPT = """You are looking at the listings index page of a US business broker: {url}

Find the repeating element that holds ONE business-for-sale listing (title, and usually a
price or a link to its detail page). Return CSS selectors that select every listing card
on the page and nothing else (not nav menus, footers, filters, team members, blog posts).

Rules:
- Prefer a tag plus one or two stable classes, e.g. "div.listing-card" or "article.type-listing".
- Avoid ids, nth-child, and auto-generated hash classes.
- If the page has no business listings at all (a contact page, a real-estate/IDX search,
  a single listing, an empty "no listings" page), return an empty list.

Reply with JSON only:
{{"selectors": ["best", "second choice", "third choice"], "listings_on_page": <int estimate>, "notes": "<one line>"}}

HTML (scripts and styles removed, truncated):
{html}"""


def trim_html(html):
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "noscript", "svg", "template", "iframe", "link", "meta"]):
        t.decompose()
    keep = {"class", "id", "href", "data-href", "data-url", "onclick"}
    for el in soup.find_all(True):
        el.attrs = {k: v for k, v in el.attrs.items() if k in keep}
    body = soup.body or soup
    text = re.sub(r"\s+", " ", str(body))
    return text[:MAX_HTML_CHARS]


def ask_claude(client, url, html):
    msg = client.messages.create(
        model=MODEL, max_tokens=400,
        messages=[{"role": "user", "content": PROMPT.format(url=url, html=trim_html(html))}])
    text = "".join(c.text for c in msg.content if c.type == "text").strip()
    text = re.sub(r"^```(?:json)?|```$", "", text).strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return [], "unparseable reply"
    sels = [s for s in data.get("selectors") or [] if isinstance(s, str) and s.strip()]
    return sels[:3], data.get("notes", "")


def try_selector(html, url, selector, broker_name):
    """Run V6's own gates on a selector. Returns (verdict, pattern, cards)."""
    if _is_nav_selector(selector):
        return "NO_PATTERN", None, []
    soup = BeautifulSoup(html, "html.parser")
    try:
        els = soup.select(selector)
    except Exception:
        return "NO_PATTERN", None, []
    if len(els) < PatternDetector.MIN_LISTING_ELEMENTS:
        return "NO_PATTERN", None, []
    scored = PatternDetector._score_group(els, url)
    if not scored:
        return "NO_PATTERN", None, []
    cards, seen = [], set()
    for el in els:
        c = ListingExtractor.from_card(el, url, broker_name)
        if c and c["id"] not in seen:
            seen.add(c["id"])
            cards.append(c)
    verdict, reason = validate_listing_set(cards, url)
    n_listing, n_priced = scored
    pattern = {
        "container_selector": selector,
        "count": len(els),
        "score": n_listing + n_priced,
        "sample_text": els[0].get_text(separator=" ", strip=True)[:200],
        "source": "pattern_maps",
        "model": MODEL,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if verdict == "ok" and len(cards) >= PatternDetector.MIN_LISTING_ELEMENTS:
        return "OK", pattern, cards
    if verdict == "review":
        return "REVIEW", pattern, cards
    return "NO_PATTERN", None, cards


def cache_keys(url, domain):
    net = urlparse(url).netloc.lower()
    bare = net.removeprefix("www.")
    return sorted({net, bare, "www." + bare, domain.lower()})


def process(target, fetcher, client, cache, write):
    domain, url = target["domain"], target["crawl_url"]
    row = {"domain": domain, "url": url, "verdict": "", "selector": "", "cards": 0,
           "priced": 0, "with_state": 0, "linked": 0, "sample_titles": "", "notes": ""}
    if target.get("skip_reason"):
        row.update(verdict="SKIPPED", notes=target["skip_reason"])
        return row
    try:
        html, method = fetcher.fetch(url, use_proxy=_needs_proxy(urlparse(url).netloc))
    except Exception as e:
        code = getattr(getattr(e, "response", None), "status_code", None)
        row.update(verdict="HTTP_403" if code == 403 or "403" in str(e) else "FETCH_ERROR",
                   notes=str(e)[:120])
        return row
    if CAPTCHA_RE.search(html[:20000]) and len(html) < 40000:
        row.update(verdict="CAPTCHA", notes="challenge page — check by hand")
        return row

    # 1. V6's own detector first (the site may have changed since the last crawl).
    candidates, notes = [], ""
    det = PatternDetector.detect(html, url)
    if det:
        candidates.append(det["container_selector"])
        notes = "detector"
    # 2. Claude's suggestions.
    if client is not None:
        sels, cnotes = ask_claude(client, url, html)
        candidates += [s for s in sels if s not in candidates]
        notes = "; ".join(x for x in (notes, cnotes) if x)
    row["notes"] = f"{method}; {notes}"[:200]

    best = None
    for sel in candidates:
        verdict, pattern, cards = try_selector(html, url, sel, domain)
        if verdict == "OK":
            best = (verdict, pattern, cards)
            break
        if verdict == "REVIEW" and best is None:
            best = (verdict, pattern, cards)
    if best is None:
        row["verdict"] = "NO_PATTERN"
        return row

    verdict, pattern, cards = best
    row.update(verdict=verdict, selector=pattern["container_selector"], cards=len(cards),
               priced=sum(1 for c in cards if c.get("asking_price")),
               with_state=sum(1 for c in cards if c.get("state")),
               linked=sum(1 for c in cards if c.get("url_is_listing_specific")),
               sample_titles=" | ".join(c["title"][:60] for c in cards[:4]))
    if verdict == "OK" and write:
        pattern["total_listings"] = len(cards)
        for key in cache_keys(url, domain):
            # PatternCache.store() protects an existing entry with a higher past
            # yield. Here that protection is wrong when the cached entry no longer
            # works: an old-schema string pattern V6 can't use, or a selector that
            # fails on today's page (e.g. the "tr" shared by charleston, brgbrokers
            # and homecarebuysell). V6 uses a cached selector without re-detecting,
            # so a broken one blocks the site for good. Replace those outright.
            old = cache.get(key)
            if key in cache.patterns and (
                    old is None or try_selector(html, url, old.get("container_selector", ""), domain)[0] != "OK"):
                cache.patterns[key] = dict(pattern)
                cache.save()
            else:
                cache.store(key, dict(pattern))
    return row


def load_targets(domain=None, path=None):
    rows = list(csv.DictReader(open(path or TARGETS)))
    if domain:
        d = domain.lower().removeprefix("www.")
        rows = [r for r in rows if r["domain"].lower().removeprefix("www.") == d]
        if not rows:
            sys.exit(f"{domain} is not in {Path(path or TARGETS).name}")
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--domain")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--write", action="store_true", help="store OK patterns in data/pattern_cache.json")
    ap.add_argument("--no-ai", action="store_true", help="detector only, no Claude calls")
    ap.add_argument("--targets", help="targets CSV (default: %s)" % TARGETS.name)
    a = ap.parse_args()

    targets = load_targets(a.domain, a.targets)
    if not (a.domain or a.all or a.limit):
        sys.exit("Pick one: --domain D, --limit N, or --all")
    if a.limit:
        targets = [t for t in targets if not t.get("skip_reason")][:a.limit]

    client = None
    if not a.no_ai:
        import anthropic
        client = anthropic.Anthropic()

    fetcher, cache = PageFetcher(), PatternCache(str(ROOT / "data" / "pattern_cache.json"))
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"results-{datetime.now().strftime('%Y-%m-%d')}.csv"
    fields = ["domain", "url", "verdict", "selector", "cards", "priced", "with_state",
              "linked", "sample_titles", "notes"]
    new = not out.exists()
    tally = {}
    try:
        with open(out, "a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            if new:
                w.writeheader()
            for i, t in enumerate(targets, 1):
                row = process(t, fetcher, client, cache, a.write)
                w.writerow(row)
                fh.flush()
                tally[row["verdict"]] = tally.get(row["verdict"], 0) + 1
                print(f"[{i}/{len(targets)}] {row['verdict']:<11} {t['domain']:<40} "
                      f"{row['selector'][:40]:<40} cards={row['cards']} priced={row['priced']}  "
                      f"{row['sample_titles'][:80]}")
                if row["verdict"] == "CAPTCHA":
                    print("   ↳ flagged for manual check; not retried")
                time.sleep(1)
    finally:
        fetcher.close()
    print("\n" + "  ".join(f"{k}={v}" for k, v in sorted(tally.items())))
    print(f"results → {out.relative_to(ROOT)}"
          + ("" if a.write else "   (dry run — add --write to store OK patterns)"))


if __name__ == "__main__":
    main()
