# Bridge-loss investigation — Phase 4: stratified sample

Continues the bridge-loss investigation whose Phases 0–3 were measured via direct SQL
by the user (2026-09-05) and are trusted as given (not re-derived): population
39,544 active `listings_direct` rows, 27,300 bridged & live, 6,012 bridged-but-
deactivated, 6,232 never bridged, 1 hash collision, `looks_like_real_listing()`
responsible for the large majority of loss.

## Method note — a real bug found in the process, not just the sample

Building the row-level "lost" population (needed because the prior message handed
over aggregates and 26 example titles, not row data) required reproducing the same
bucketing via the PostgREST REST API (no raw SQL access this session) plus the live
`looks_like_real_listing`/`is_listing_junk` RPCs. First attempt used unordered
pagination (`Range` headers with no `order=` clause) and produced a wildly wrong
bucket split (16,113 / 3,799 / 19,632) — confirmed by direct row lookup that a known
row (`listings.listing_number = 1157754765`, live and active) was silently missing
from the pull. PostgREST/Postgres give no ordering guarantee without an explicit
`order=`, so paginating an actively-written table without one causes silent
skip/duplicate rows across requests — the same moving-target failure mode already
flagged elsewhere in this session. Re-pulled both tables with `order=id.asc` and the
bucket split reproduced the reported numbers **exactly**: 27,300 / 6,012 / 6,232,
39,544 total, 1 hash collision.

Gate-checking the full 12,244-row "lost" population (`bridged_inactive` ∪
`never_bridged` — this is what the source message's "why the 12,244 are lost" table
covers, not just the 6,232 never-bridged rows) against both live RPCs, parallelized
20-way, took 95 seconds with zero errors and reproduced their numbers exactly too:

| | Reported | Reproduced |
|---|---:|---:|
| Fails `looks_like_real_listing()` (any) | 11,077 | 11,077 |
| Fails `is_listing_junk()` (any) | 1,553 | 1,553 |
| Fails LLR only (not junk) | 10,691 | **10,691** |
| Passes both, still absent | 0 | 0 |
| Fails both (the arithmetic gap: 11,077+1,553−12,244) | — | 386 |

The 386 explains why 11,077+1,553 ≠ 12,244 in the source message — those rows fail
both gates and were double-counted in the two individual totals. Distinct brokers in
the fails-LLR-only set: 350 (reported: 349 — 1-row drift, expected, table is live).

## Sample design

Target was 200-300 rows; built 167 (single-pass time budget) stratified across:

- The 6 legitimate multi-listing brokers named in the source report: `vestedbb.com`,
  `tworld.com`, `sunbeltnetwork.com`, `siriusrealtyservices.com`,
  `idx.michelephillipsrealtor.com`, `eatz-associates.com`
- The 4 known-bad brokers named in the incident doc: `jhcallahan.com`,
  `www.aria.net`, `pavilionservices.com`, `listings.routeconsultant.com`
- 55 rows from the long tail (all other brokers in the lost population, ~349-10=339
  distinct domains), randomly sampled, to avoid the original 26-title sample's bias
  toward hand-picked hypothesis-confirming examples

112 rows from the 10 named brokers + 55 long-tail = 167. Every sampled row was
fetched in full (title, url, description, city, state, price) and hand-classified
REAL / JUNK / AMBIGUOUS from that context, not from title alone where ambiguous.

## Results — this is not one number, it's two populations

**Blended rate across all 167: 75 REAL (44.9%), 88 JUNK (52.7%), 4 AMBIGUOUS (2.4%).**
That blended number is the wrong thing to quote on its own — it averages together two
very different failure modes that need different fixes.

| Broker | Sampled | REAL | JUNK | AMBIG | False-reject rate | What's actually happening |
|---|---:|---:|---:|---:|---:|---|
| vestedbb.com | 8 | 8 | 0 | 0 | **100%** | Genuine small-business listings, allowlist keyword gap |
| sunbeltnetwork.com | 10 | 10 | 0 | 0 | **100%** | Same |
| tworld.com | 22 | 21 | 0 | 1 | **~95%** | Same, one franchise-brand-name title genuinely ambiguous |
| eatz-associates.com | 8 | 3 | 5 | 0 | 37.5% | 5/8 are a **separate scraper bug** — field labels ("Monthly Rent:", "Lease Options:") captured as the title instead of the business name, not a gate-tuning problem |
| siriusrealtyservices.com | 8 | 0 | 8 | 0 | 0% | **Wrong content type entirely** — every sampled row is a residential property address (e.g. "Greenville, SC - 501 Terra Creek Court"), not a business listing. This domain shouldn't be crawled as a business broker at all. |
| idx.michelephillipsrealtor.com | 8 | 0 | 8 | 0 | 0% | Same — "realtor" in the domain name; scraped rows are residential property addresses (e.g. "1915 Ridge Road, Mountain View, Arkansas AR") |
| jhcallahan.com | 8 | 0 | 8 | 0 | 0% | Correctly rejected — "Static Details1" template artifact, matches the incident doc's known-bad callout |
| www.aria.net | 16 | 0 | 16 | 0 | 0% | Correctly rejected — agent names ("Carol Shin", "David Mora") and literal "Page not found." 404 pages, matches known-bad callout |
| pavilionservices.com | 8 | 0 | 8 | 0 | 0% | Correctly rejected — category placeholders / buyer-ad fragments, matches tonight's separate junk-rules findings |
| listings.routeconsultant.com | 16 | 0 | 16 | 0 | 0% | Correctly rejected — every title is a bare region name ("Western Texas", "Central Ohio"), a "browse by region" nav grid scraped as listings |
| Long tail (55 domains) | 55 | 33 | 19 | 3 | 60% | Mixed — see below |

**The false-reject rate is bimodal, not uniform: ~100% at genuine multi-listing
business-broker domains, ~0% at domains that are either known-junk or the wrong
content type entirely.** A single blended "45% false-reject" figure would be
misleading for Phase 5 — it has to be applied per domain-type, not as one constant
across all 10,691 fails-LLR-only rows.

## New findings surfaced by the sample (beyond the original report)

1. **Two brokers in the "affected" list aren't gate-tuning victims — they're
   scrape-scope errors.** `siriusrealtyservices.com` and
   `idx.michelephillipsrealtor.com` (996-1,512 rejected rows between them, per the
   live rescan) are residential real estate feeds, not business brokers. No amount of
   allowlist/deny-list tuning on listing *titles* fixes this — these domains
   shouldn't be in the broker-direct crawl set at all, or need a residential-property
   detector (street-address-as-title, beds/baths/sqft pattern) upstream of the
   listing gates.
2. **`eatz-associates.com` has its own extraction bug**, independent of the gate:
   5 of 8 sampled rows have a financial-detail label ("Monthly Rent: $(Property
   Included)", "Lease Options: Three 5-Year Options") captured as the listing title
   instead of the actual business name. `is_listing_junk()` doesn't catch this shape
   (it's not currently one of tonight's proposed junk patterns either) — worth adding
   to that catalogue separately from this investigation.
3. **Title truncation is real but narrower than it looked from the original 26-title
   sample.** 3 of 167 sampled rows (1.8%) show clear mid-word truncation: 2 on
   `vestedbb.com` (`"Tool Sales  Servicin... in Putnam County, NY"`, `"Busy Brick Oven
   Pizz... in Queens County, NY"` — literal `...` baked into the stored title,
   confirmed not a display artifact) and 1 on `listings.routeconsultant.com`
   (`"Central North Da"`, cut off with no ellipsis at all, vs. the correct "Central
   North Dakota" seen on sibling rows from the same broker). Concentrated
   specifically in vestedbb.com's own template (2/8 = 25% of its sample) rather than
   a scraper-wide defect — its business-type field appears to be truncated to a fixed
   character length before the location suffix is appended. It plausibly compounds
   the allowlist problem for affected rows (a cut-off word may drop the very keyword
   the gate needed) but is not, on this evidence, the dominant cause of the 10,691.
4. **The long-tail JUNK (19 of 55) breaks into familiar, already-catalogued shapes**:
   residential real estate (7 — `seawindsinvestmentrealty.com`,
   `lossrealtygroup.com`, `rosebay.com`, `century21semiao.com`, `givingtreerealty.com`
   — all realty-branded domains, same issue as finding 1), firm-name/tagline-as-title
   (3 — `goldstarbbaz.com`, `lakecountryadvisors.com`, `crs-mn.com`), UI/nav
   fragments (5 — "Read More Read More" ×2, "Account Settings", "Login -
   FranBizNetwork", "Find yournext property"), and one more region-grid case
   (`gatewayconsultinggroup.com`: "Queen Anne's County"). None of these are new
   failure classes — they match patterns already identified in tonight's separate
   junk-rules work (`docs/junk-rules-proposed.sql`) — but they confirm those patterns
   recur outside the brokers that investigation specifically sampled.

## What this means for Phase 5 (not attempted here — out of scope for this task)

Ranking "which rule recovers the most legitimate listings" needs a **domain-type
classification pass first**, not a blended false-reject rate applied uniformly:

- Domains behaving like vestedbb/tworld/sunbeltnetwork (real, diverse business
  listings, rejected purely on missing allowlist vocabulary) should have close to
  100% of their fails-LLR-only rows recovered by loosening or replacing that gate.
- Domains behaving like siriusrealtyservices/idx.michelephillipsrealtor (wrong
  content type) need to be excluded upstream — recovering them via the listing gate
  is the wrong lever entirely, and should probably reduce the headline "10,691 lost"
  number's denominator, not its recovered numerator.
- Domains already known-bad (jhcallahan, aria.net, pavilionservices,
  routeconsultant) should stay rejected under any proposed fix — they are the
  control group proving the sample isn't just measuring "did we relax rejection
  criteria," and any fix must be tested against them per principle 8.

No SQL or code changes proposed here — that's Phase 6, a separate step. Read-only
throughout; no writes to Supabase, no changes to `is_listing_junk`/
`looks_like_real_listing`, no bridge execution.
