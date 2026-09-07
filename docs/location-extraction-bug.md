# State/location extraction bug — inbargroup.com sample, confirmed

Verified 2026-09-03 against `kqckuedsyyosmccushyd` and the live `inbargroup.com` page.

## The reported case

`listings_direct` id `4347fc2cc06a08b6` / `716fdc604dd8190d` (duplicate crawl of the
same URL), "B2B Distributor of Commercial Products, Lighting, Safety Equipment and
Supplies":

- `city`: `"New York"`, `state`: `"NY"`
- Description begins: `**** Confidential listing not located where indicated ****`
- BizQuest's own listing for this business reportedly shows `ME` (Maine — the
  assigned agent's office). We recorded `NY`, which is Inbar Group's own HQ.

**Confirmed: the live page's `<title>` tag is literally**
`"... - New York Business Broker – Inbar Group, Inc"`. That is almost certainly
where `NY` came from.

## Root cause (confirmed by reading `scrapers/dealledger_scraper_v6.py`)

`ListingExtractor.enrich_from_detail()` (line ~1377):

```python
soup = BeautifulSoup(html, "html.parser")
full_text = soup.get_text(separator=" ", strip=True)
...
if not listing.get("state"):
    loc = LocationExtractor.extract(full_text)
```

Two compounding problems:

1. `full_text` is the **entire page's text** — `<title>`, nav, footer, script/style
   tag contents (nothing strips `<script>`/`<style>` before `get_text()`), everything
   — not scoped to the listing body. `LocationExtractor.extract()` runs its cascading
   regex strategies (City,ST → City,FullState → "Located in X" → any full state name
   anywhere → state in parens → bare abbreviation) over that whole blob and returns
   the **first** match. A broker whose site brands around a city (title tag, footer
   address, "NYC Business Brokers" tagline) will win that match on every listing where
   the body itself has no earlier, clearer location — which is exactly what happens
   when the listing is confidential and deliberately vague about location.
2. There is **no check anywhere** for confidentiality/location-withheld language
   before calling `LocationExtractor.extract()`. The function doesn't know a
   disclaimer exists; it just extracts the first plausible location token, chrome or
   not.

The docstring at the top of the file (line 9) explicitly frames this as "where state
... [is] re-extracted from the full page" — the full-page scope is a deliberate design
choice for recall (it does correctly rescue real listings whose card text had no
location but whose detail page does), it just has no exclusion for page chrome or for
confidentiality language.

## Quantifying broker-level impact and the confidentiality-language count

Both handed off for deeper analysis (broker_master cross-reference for the first,
description/summary text scanning for the second) — results to follow in
`docs/location-chrome-fallback-audit.md`.

**What's already confirmed on the confidentiality-language count (Q2):**

- `listings_direct.description` — exact phrases from the report:
  - `"not located where indicated"`: **2** rows (both the same inbargroup.com listing,
    crawled twice — `status='active'` on both)
  - `"location withheld"`: 0
  - `"confidential location"`: 0
- Broader phrasing sweep (not in the original report, run to check whether the exact
  phrases undercount the real pattern): `"confidential"` alone appears in **1,718**
  `listings_direct` descriptions (this includes generic "sign an NDA" boilerplate
  unrelated to location, so it's an upper bound, not a clean count), `"not disclosed"`
  in 51, `"exact location"` in 23, `"general area"` in 3, `"region only"` in 1.
- **`bizquest_listings.summary` is `NULL` on all 44,174 rows — 0%.** The 0-count
  against that table isn't evidence BizQuest listings lack this language; it's
  evidence we never capture any narrative text field for BizQuest listings at all.
  The confidentiality-language check is only currently meaningful against
  `listings_direct`.

**Recommendation once the broker-level audit is in:** any row matching a
confidentiality/location-withheld pattern should have `state`/`city` set to `NULL`
rather than whatever the extractor guessed, and the extractor should stop being
called on chrome-included full-page text for any listing where such language is
present in the body.

## Item 3 — cash flow and revenue for this listing

Confirmed directly from the live page and the stored row:

| Field | Live page | Stored (`listings_direct`) | Match |
|---|---|---|---|
| Asking price | $19,440,000 | $19,440,000.0 | ✓ |
| Cash flow | $3,138,984 | $3,138,984.0 | ✓ |
| **Revenue** | **$8,968,525** | **$8,968,525,000,000.0** | ✗ — off by exactly 1,000,000x |

This is a separate bug from the location issue, and it is not isolated to this
listing — spot-checked two more inbargroup.com rows in the same pull and both show
the identical 1,000,000x pattern: a water-delivery listing's stored revenue is
$1,080,000,000,000 (the description text itself, captured verbatim by the scraper,
says "Gross Revenue: $1,080,000"), and an electrical-supply listing's stored revenue
is $1,670,000,000,000 against its own description's "Gross Revenue: $1,670,000".

**Root cause not yet found.** I fetched the live page and confirmed the correct
figure ("$8,968,525", no `K`/`M`/`B` suffix character, no alternate formatting)
appears verbatim and repeatedly in the raw HTML — in the narrative "Financial
Summary" paragraph and in at least two separate structured `description-value` spans,
plus once inside what looks like a JSON-escaped string (likely a hidden
script-tag payload). `cash_flow` and `asking_price` are extracted by the identical
`_money_near()` mechanism from the same page and are both correct, which argues
against a generic extraction-regex bug and for something specific to how the
`revenue` value is handled after extraction, or specific to how multiple duplicate
occurrences of the revenue label on this page interact with something downstream.
This needs actual code-path tracing against live data, not a guess — handed off,
results to follow in `docs/revenue-inflation-bug.md`.
