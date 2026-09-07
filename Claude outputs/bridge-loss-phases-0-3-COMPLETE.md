# Bridge-loss investigation — Phases 0–3 already complete

**Measured 2026-09-05 via direct SQL against `kqckuedsyyosmccushyd`.**
**Do not re-run Phases 0, 1, 2, or 3. They are answered below.**
**Start at Phase 4 (the stratified sample), which is the part that needs judgment.**

This replaces the plan's Phase 0–3 work. The proposed approach — pulling ~36k rows
via REST, recomputing `synth_num` in Python, and calling `/rpc/looks_like_real_listing`
and `/rpc/is_listing_junk` per row — is a faithful method and would produce the same
answer. It is just unnecessary: the gates are SQL functions callable inside a query,
so the entire bucketing is four queries and about ninety seconds. That also removes
the moving-target problem, because each query is a single consistent snapshot.

---

## Phase 1 — the population, exactly

```
listings_direct where status='active'                          39,544
listings where is_active and source='broker_direct'            27,300
gap                                                            12,244
```

The identity key is confirmed correct as documented:

```sql
900000000 + (('x' || substr(md5(id),1,7))::bit(28)::bigint)
```

Hash collisions across all 39,544 active rows: **1**. Not a material cause of loss,
but the bridge's `distinct on (synth_num)` is still required and must not be removed.

## Phase 3 — every row bucketed, zero remainder

| Bucket | Rows |
|---|---|
| Bridged and live in `listings` | 27,300 |
| Present in `listings` but deactivated | 6,012 |
| Never bridged at all | 6,232 |
| **Total** | **39,544** ✓ |

The plan's requirement — every row in exactly one bucket, no "unexplained remainder" —
is met.

## Why the 12,244 are lost

| Cause | Rows |
|---|---|
| Fails `looks_like_real_listing()` | 11,077 |
| Fails `is_listing_junk()` | 1,553 |
| **Passes both gates but still absent** | **0** |

Zero unexplained. (A 2026-09-03 run of this same query showed 143 unexplained; those
were rows created *after* that day's bridge fired, and they bridged normally on the
next run. Transient, not a defect.)

**`looks_like_real_listing()` is responsible for ~90% of all loss.**

## The finding that changes the conclusion

**10,691 rows fail `looks_like_real_listing()` while NOT being flagged as junk by
`is_listing_junk()`.** They are rejected solely for failing the first gate's keyword
requirement. 349 distinct brokers are affected.

`looks_like_real_listing()` requires the title to match an allowlist of business-type
words (`business|restaurant|shop|store|company|salon|service|practice|firm|...`).
Anything whose title does not happen to contain one of those ~80 words is rejected.

Sampled rejects, all from established brokers:

**vestedbb.com** (1,473 rejected of 3,631 active):
```
Books in York County, PA
Pest Control in Suffolk County, NY
Self Storage in Suffolk County, NY
Gas Station in Suffolk County, NY
Septic Pumping Servi... in Bristol County, MA
Bronx County Pharmac... in Bronx County, NY
```

**tworld.com and sunbeltnetwork.com** (686 rejected):
```
Caribbean Instrumentation Wholesaler Sale
Luxury Design Build Remodeling
National Wellness Coaching Biz
Tunnel Wash 2 Self Serve Bays
Window Treatments Flooring
Screen Printing Vehicle Wraps New Orleans
```

None of those 26 is junk. Every one is a real listing.

Two distinct mechanisms are visible:

1. **Missing vocabulary.** "Pest Control", "Self Storage", "Books", "Remodeling",
   "Window Treatments" are not on the allowlist. The list cannot enumerate every
   business type in the United States, so this failure mode is unbounded.
2. **Word-boundary brittleness.** The pattern uses `\mwholesale\M`, so
   **"Wholesaler" fails while "wholesale" passes.** Same class of bug as the
   `MONEY_TOKEN` regex found in the revenue investigation — a boundary assumption
   that holds for the test case and not for real text.

A third, separate issue is visible in the vestedbb sample: titles are being truncated
mid-word by the scraper (`Prime Home Health Ca... in Ocean County, NJ`). That is an
extraction defect independent of the gate, and it makes the allowlist problem worse
because truncation can remove the very keyword the gate is looking for.

## Top brokers by rejected rows

| Broker domain | Rejected |
|---|---|
| vestedbb.com | 1,473 |
| siriusrealtyservices.com | 1,248 |
| idx.michelephillipsrealtor.com | 996 |
| eatz-associates.com | 528 |
| tworld.com | 463 |
| jhcallahan.com | 450 |
| www.aria.net | 343 |
| pavilionservices.com | 242 |
| sunbeltnetwork.com | 223 |
| listings.routeconsultant.com | 221 |

Note the mix. `jhcallahan.com`, `aria.net` and `pavilionservices.com` are known-bad
(the Static Details1 / person-name / WooCommerce-buy-side cases) and are being
rejected correctly. `vestedbb.com`, `tworld.com` and `sunbeltnetwork.com` are major
legitimate brokers. Any stratified sample must cover both kinds or it will measure
the wrong thing.

---

## What is still genuinely open — start here

### Phase 4: stratified sample (the real work)

The 26 samples above are not a stratified sample. They came from four brokers and
were chosen to test a hypothesis, not to estimate a rate. Do this properly:

- 200–300 rejected rows, stratified by **broker** and by **which gate rejected them**
- Include known-bad brokers deliberately, so the sample can measure true rejects too
- Hand-classify each as REAL / JUNK / AMBIGUOUS
- Report the false-reject rate with its stratification, not as a single number

The prior from the 26: high. But that is a bet, not a measurement, and the whole
point of the exercise is to stop shipping bets.

### Phase 5: rank rules by recoverable listings

Rank by *legitimate listings recovered*, not by rows rejected. The allowlist clause
inside `looks_like_real_listing()` is almost certainly rank 1 at 10,691 rows.

### Phase 6: the fix is a shape change, not a longer list

Do not propose adding "pest control" and "self storage" to the allowlist. That moves
the boundary without changing the failure mode, and the next unlisted business type
hits it again.

The gate is inverted. A title should be rejected for **looking like navigation chrome,
a category label, or a CTA** — patterns that are enumerable and stable — not for
**failing to look like a business** — a category that is not enumerable. Deny-list the
junk shapes; accept everything else.

`is_listing_junk()` already does the deny-list job and already works: it flags only
1,553 rows and the tonight's junk-rules work shipped 5 new rules against it with zero
false rejects. The recommendation is to make `is_listing_junk()` the primary gate and
reduce `looks_like_real_listing()` to structural checks only (length bounds, not a
bare email or phone number, not a pure price string).

Whatever is proposed, principle 8 applies: test against input that **must pass**, and
a rule that rejects any known-good row does not ship.

### Phase 7: dry run, read-only

A dry run is fully doable in SQL — compute the bucket counts under a proposed gate and
diff against current. Do not run the real bridge as part of this. `bridge-direct-daily`
runs itself at 15:00 UTC daily and has succeeded three consecutive days
(2026-09-03 67.7s, 09-04 72.2s, 09-05 69.3s) since the timeout fix, so any accepted
change will take effect on the next scheduled run without anyone invoking it.

---

## Strategic note

BizQuest's live index is ~43,600 listings. `listings_direct` holds 39,544 active rows
today. 10,691 of them are being discarded by a keyword allowlist rather than by any
evidence of being junk.

Recovering even half of that puts DealLedger past the incumbent's index size without
crawling a single additional broker. The gap is not a coverage problem. It is this
filter.

## Method note

Everything above comes from queries run against `kqckuedsyyosmccushyd` on 2026-09-05,
except the 26 sampled titles which were pulled 2026-09-03 and are quoted verbatim.
Counts shift daily as the scraper runs — re-run before quoting any figure externally.
