# Location chrome-fallback — broker-level audit

Extends `docs/location-extraction-bug.md`. Method: PostgREST REST API only (no raw
SQL access this session), joins done client-side in Python. `listings_direct` had
38,180 active rows at pull time (vs. 36,176 cited earlier tonight — the table is
still being actively written to; treat all counts here as a snapshot, not a fixed
figure).

## Headline finding: broker-level state concentration is the wrong detector

I built the metric the directive asked for — per-broker modal-state share among
active rows, cross-referenced against `broker_master` — and it's the wrong tool for
this specific bug. Two results show why:

**It misses the confirmed case.** `inbargroup.com` itself has 115 active rows with
real state diversity — NY 60 (52%), PA 13, ME 11, CT 5, MA 4, VT 4, DE 4, NJ 3,
null 3, MS 2. That's nowhere near a >90% concentration. The bug isn't "this broker's
site brands around one city so everything gets that city" in Inbar Group's case —
it's narrower: Inbar Group is a real multi-state firm (`broker_master` shows named
agents in NY, PA, ME, MA, each with their own `regionccode` — the Maine agent,
Charlie MacPherson, is very likely the actual assigned agent on the reported
listing), and only the confidential/vague-location listings fall through to
chrome. A firm-level concentration metric can't see a bug that only hits a handful
of listings within an otherwise-diverse broker.

**It's mostly false positives where it does fire.** 119 domains / 9,221 active rows
clear the >90% modal-share bar. Spot-checking the top ones by name, nearly all read
as genuinely single-market operators, not contamination:
`azbusinessbrokers.com` (AZ, 100%), `vtcommercial.com` (VT, 100%),
`restaurantrealty.com` (CA, 100% — a CA-focused restaurant brokerage),
`baltimorebusinessbrokers.com` (MD, 90%), `seawindsinvestmentrealty.com` (FL, 100%).
These names describe the business; the concentration is real inventory, not a bug.

One entry on this list is a useful, independent corroboration though:
**`pavilionservices.com` — 1,201 active rows, 100% `NC`.** This domain is the same
broker family already confirmed as wholesale junk in tonight's other investigation
(`docs/junk-rules-proposed.sql` / the incident doc's Pavilion Business Services
placeholder-category rows). 100% single-state concentration on a broker whose
listings are already known to be fabricated category placeholders is consistent
with chrome-fallback (or some other non-signal default) filling in a state for
content that was never a real listing at all — but this is corroborating context,
not new proof; the junk-ness there was established independently, by content, not
by this location signal.

Full ranked list (293 domains with >=5 active rows, 119 clearing >90% concentration)
saved for reference in `/tmp/broker_state_stats.json` (not committed — regenerate
from `listings_direct` + `broker_master` if needed; it's a snapshot, not a fixed
count given the table's write volatility noted above).

## What actually finds the bug: confidentiality-about-*location* language, cross-referenced against a populated state

This is the correct detector, and it's already narrow:

| Query | Active rows |
|---|---|
| `description` contains `"not located where indicated"` | 2 (one listing, crawled twice — the reported case) |
| `description` contains `"location withheld"` | 0 |
| `description` contains `"confidential location"` | 0 |
| `description` contains generic `"confidential"` (any context) | 1,127 |
| ...of those, with a non-null `state` | 1,020 (90.5%) |

**The generic "confidential" count is not a usable proxy — I checked it and it
overcounts by roughly 500x.** I pulled the top non-inbargroup domains by count of
"confidential"-containing rows and read six real examples across six different
brokers (`ibexbeyond.com`, `murraybizbuy.com`, `dynamitebrokers.com`,
`fusionadvantage.com`, `cibb.com`, `century21semiao.com`). Every one had a real,
specific, correctly-extracted location already in the visible listing text
("Welding Supply House, Oklahoma", "Cape May County", "Bethesda", "84 Beech Street,
Belleville, NJ"). "Confidential" in these rows refers to financials/NDA language,
not location. None showed the failure signature.

**So: as of this pull, the exact bug reported — a confidential-*about-location*
listing getting a chrome-derived state — has one confirmed instance (one listing,
double-counted by a duplicate crawl), not a mass-scale problem today.** That does
not make it a non-issue: the mechanism is real and structural (see below), it's just
gated behind a narrower intersection of conditions than "broker's site mentions a
city." It will keep recurring in isolated cases going forward, and it's worth fixing
regardless of today's small blast radius, because the two triggering conditions
(no clean location string in the listing body + a state-shaped string somewhere in
page chrome) are individually common even if their intersection is currently rare.

## Why the mechanism is narrower than "any branded site contaminates everything"

`LocationExtractor.extract()` runs six strategies **in fixed priority order**, each
one searching the *entire* `full_text` independently:

1. `"City, ST"` (comma + 2-letter abbrev)
2. `"City, Full State Name"`
3. `"Located in X"` / `"Location: X"`
4. any full state name, anywhere in the text
5. `"(ST)"`
6. bare 2-letter abbreviation

Strategies 1–2 require comma-formatted text that real listing bodies commonly have
("Bethesda, MD", "Cape May County, NJ") and that page chrome (nav/title/footer)
essentially never has in that exact shape. So when a listing body *does* contain a
real, well-formatted location, strategies 1–2 win regardless of position in the
document, and chrome is never consulted. The chrome-contamination path only opens
when the listing body has **no** comma-formatted location anywhere on the full
page — which is exactly what a deliberately vague confidential listing produces —
and the site's title/footer/branding text separately happens to contain a bare full
state name or 2-letter code (strategies 4–6). That's a real but comparatively
narrow intersection, which matches what the data shows: common overall, rare in
this exact combination.

## Proposed fix (not applied)

Two changes to `scrapers/dealledger_scraper_v6.py`, `ListingExtractor.enrich_from_detail()`:

```diff
     @classmethod
     def enrich_from_detail(cls, listing: dict, html: str) -> dict:
         """
         Re-extract all fields from the full detail page.
         Only overwrites if the detail page has richer data.
         State is the most important field to gain here.
         """
         soup = BeautifulSoup(html, "html.parser")
         full_text = soup.get_text(separator=" ", strip=True)
 
         # Title
         detail_title = cls.best_title_from_detail(soup)
         if detail_title and len(detail_title) > len(listing.get("title") or ""):
             listing["title"] = detail_title
 
         # Description
         desc = cls.best_description_from_detail(soup)
         if desc:
             listing["description"] = desc
 
         # Financials — full page text has much more context
         if not listing.get("asking_price"):
             listing["asking_price"] = positive_or_none(cls.extract_price(full_text))
         if not listing.get("cash_flow"):
             listing["cash_flow"] = positive_or_none(cls.extract_cash_flow(full_text))
         if not listing.get("revenue"):
             listing["revenue"] = positive_or_none(cls.extract_revenue(full_text))
 
         # Location — the key reason we fetch detail pages
-        if not listing.get("state"):
-            loc = LocationExtractor.extract(full_text)
-            if loc:
-                listing["city"]  = listing.get("city") or loc.get("city")
-                listing["state"] = loc["state"]
+        if not listing.get("state"):
+            body_text = desc or full_text
+            if _is_location_confidential(body_text):
+                # Listing explicitly says its location can't be trusted from the
+                # page — do not let LocationExtractor guess from page chrome.
+                listing["city"] = listing.get("city")
+                listing["state"] = None
+                listing["location_confidential"] = True
+            else:
+                # Scope to the listing body first; only fall back to full_text
+                # (which includes title/nav/footer chrome) if the body itself
+                # has nothing — and even then, skip strategies 4-6 (bare state
+                # name / bare abbreviation), which are the only strategies weak
+                # enough for chrome text to win.
+                loc = LocationExtractor.extract(body_text, allow_weak=True) if body_text else None
+                if not loc and body_text != full_text:
+                    loc = LocationExtractor.extract(full_text, allow_weak=False)
+                if loc:
+                    listing["city"]  = listing.get("city") or loc.get("city")
+                    listing["state"] = loc["state"]
```

And in `LocationExtractor`:

```diff
+    CONFIDENTIAL_LOCATION = re.compile(
+        r'not located where indicated|location\s+withheld|confidential\s+location'
+        r'|location\s+(?:is\s+)?approximate|approximate\s+location',
+        re.IGNORECASE,
+    )
+
     @classmethod
-    def extract(cls, text: str) -> dict | None:
+    def extract(cls, text: str, allow_weak: bool = True) -> dict | None:
         if not text:
             return None
         ...
         # 4. Full state name anywhere (longest match first)
+        if not allow_weak:
+            return None
         text_lower = text.lower()
```

(`_is_location_confidential` would just call `LocationExtractor.CONFIDENTIAL_LOCATION.search(text)`.)

This keeps full-page recall for financials (where it's working correctly and
there's no equivalent chrome-contamination risk) and for the strong location
patterns (1–3), while closing the specific hole: weak pattern matches (4–6) —
the only ones broad enough for a title tag or footer to satisfy — now require the
listing's own body text first, and confidentiality-about-location language now
short-circuits to `NULL` instead of guessing. Not applied to the working tree; this
is a proposed diff for review.

## Scope note

Did not touch the revenue ×1,000,000 bug (separate investigation, per
`docs/location-extraction-bug.md` — handed to `docs/revenue-inflation-bug.md`).
Did not touch `docs/junk-rules-proposed.sql` or the Pavilion broker-kill proposal —
only referenced it as corroborating context for the concentration list.
