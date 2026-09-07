# DOM contamination — quantified (2026-09-02)

Method: Supabase REST API only (`SUPABASE_URL`/`SUPABASE_SERVICE_KEY` from `.env`),
paginated GET against `listings_direct`, aggregated client-side. No direct Postgres
access was available this session (no DB connection string, no Docker for the
Supabase CLI's dump/inspect path) — see `claude/job-audit.md` for that constraint.
Raw pulls cached at `/tmp/listings_direct_active.json` (36,176 rows) for
re-verification; not committed.

**Caveat:** the incident report this queue item points to
(`claude/incident-2026-09-02-bridge-timeout.md`) was not available this session.
Everything below is derived fresh from the live table, not from that report.

## 1. `dom_source` — actual distinct values

`listings_direct`, `status='active'`, 36,176 rows total:

| dom_source | rows | has numeric `days_on_market` |
|---|---|---|
| `'first_seen'` | 23,429 | 13,043 yes / 10,386 no |
| `NULL` | 10,739 | 0 yes / 10,739 no |
| `'backtrack_unverified'` | 2,008 | 2,008 yes / 0 no |

Only **15,051 of 36,176 active rows (41.6%)** have any computed `days_on_market`
at all. `dom_source IS NULL` means no DOM was ever computed — it is not "DOM = 0",
it is "unknown", and **10,739 of those 10,824 null-DOM-adjacent rows (99.5%) have
`first_seen` in the four days Aug 29 – Sep 2** (see below). Any query that
`coalesce`s a null DOM to zero, rather than excluding it, will silently manufacture
a near-zero median out of these rows. That is almost certainly the mechanism behind
the "median DOM = 2 days" figure cited in the queue — see §3.

`dom_source = 'first_seen'` is not itself a contamination flag — it is the
baseline methodology for every broker-direct listing (there's no BBS-style
sequential ID to anchor against, so DOM defaults to `today − first_seen`). It only
becomes an artifact when `first_seen` itself is an artifact, i.e. when the broker
was crawled for the first time recently. `backtrack_unverified` is the pipeline's
attempt to recover an earlier true first-seen date; treat it as the closest thing
to "trustworthy" DOM currently computed, though still labeled unverified.

## 2. The Sept-2 backlog cohort — sized precisely, and smaller than assumed

`first_seen` date, all 36,176 active rows:

| date | rows |
|---|---|
| 2026-09-02 | 18,047 |
| 2026-09-01 | 3,797 |
| 2026-08-29 | 2,402 |
| 2026-07-26 | 2,226 |
| (all other dates) | ≤1,212/day |

The Aug 29 / Sep 1 / Sep 2 window totals **24,246 rows** — a clear, isolated spike
against a baseline of a few hundred to ~1,200/day. This lines up with the discovery
backlog push in commit `47ffe2f` ("broker_master merge... homepage-first
discovery").

**But the queue's framing — "18,047 rows from never-before-crawled brokers" —
overstates it.** Checking broker-level history within the currently-active set:

- 218 distinct brokers have a row first-seen in that 3-day window.
- Of those, only **141 brokers have zero other active rows outside the window**
  (i.e. their entire current footprint appeared in this push) — these are the
  brokers that are actually "never-before-crawled" by any reasonable definition.
- The other 77 brokers already had active listings before Aug 29; new rows from
  them in this window are far more likely to be genuinely new listings than
  crawl-start artifacts.
- Rows belonging to the 141 brand-new brokers: **6,393**, not 18,047 or 24,246.
- Cross-checked against `broker_discovery` (domain match found for 60 of the 218
  window brokers — the rest aren't in that table under a matching domain string):
  all 60 matched brokers show `attempts <= 1`, and 46 have `last_attempt_at =
  2026-09-02` — consistent with first-ever crawls, corroborating the broker-level
  split above for the subset it could check.

So: **the true "brand-new broker" backlog cohort is ~6,393 rows (141 brokers),
not 18,047.** The remaining ~17,850 window rows are new listings from
already-tracked brokers and shouldn't be discarded on that basis alone.

## 3. DOM median/distribution — as-is vs. cohort-excluded

| population | n (active) | n with numeric DOM | median | mean | p90 |
|---|---|---|---|---|---|
| All active rows, DOM nulls **coerced to 0** (reproduces the "~2 day" style figure) | 36,176 | 36,176 | **0** | — | — |
| All active rows, nulls **excluded** (i.e. `days_on_market IS NOT NULL`) | 36,176 | 15,051 | **44** | 52.0 | 122 |
| Excluding the 141-broker brand-new cohort (§2) | 29,783 | 14,778 | **44** | 52.0 | 122 |
| Only `dom_source = 'backtrack_unverified'` (strictest trustworthy subset) | 2,008 | 2,008 | **71** | 78.1 | 103 |
| `days_on_market IS NOT NULL` and `first_seen < 2026-08-28` (excludes entire discovery-push window, not just brand-new brokers) | — | 10,051 | **48** | 56.9 | 122 |

**Key finding: removing the backlog cohort barely moves the median at all (44 → 44),
because the backlog rows mostly don't have a computed DOM in the first place** — only
4,983 of the 24,246 window rows have any numeric `days_on_market`. The contamination
isn't "the backlog cohort drags the real median down" — a median computed correctly
(nulls excluded) is already 44 days and stable. **The contamination is entirely in
whatever aggregate is treating 10,739 null-DOM rows as zero instead of excluding
them.** Fix the null-handling and the "2-day" artifact disappears on its own; no
cohort-exclusion logic is actually required to get a correct top-line number.

## 4. Does the ~103-day sold-listing finding hold here?

**No — and it isn't testable against `listings_direct` in any meaningful way right now.**

`docs/METHODOLOGY.md` describes the DOM/`estimated_listed_date` methodology
(anchored interpolation off sequential listing IDs, ±12 days error) as applying to
the CoStar-sourced table (`listings`, BBS listing numbers) — a different table,
different source, different computation entirely from `listings_direct`'s
`dom_source`-based approach. The "~103 days sold / 17 days advertised" claim in
`CLAUDE.md` is drawn from that BBS population, not from
broker-direct data.

I checked anyway, in case `listings_direct` could independently corroborate it.
`listings_direct` does have 13,026 rows with `status='sold'` and a
`days_on_market_frozen` column that looks purpose-built for exactly this check.
In practice it's unusable:

- Only **232 of 13,026 sold rows (1.8%)** have a numeric `days_on_market_frozen`.
  Their median is **4 days**, mean 16.6 — implausibly fast, and the same shape as
  the backlog artifact (broker discovered, some listings already gone by first
  crawl, near-zero first-seen-to-sold interval).
- The non-frozen `days_on_market` field on the same sold rows fares slightly
  better (1,177 of 13,026 populated) but still gives a median of **32 days** —
  nowhere near 103, and 91% of sold rows have no DOM value of any kind.

**Conclusion: `listings_direct` cannot currently support or refute the 103-day
figure.** The sold-row sample is too sparse (98% missing) and what little exists is
itself contaminated by the same first-crawl artifact as the active rows. Don't try
to reproduce or defend the 103-day number from this table — it stands or falls on
the BBS-side methodology only.

## 5. Proposed publication gate

**Gate A (minimum, ships today):** `days_on_market IS NOT NULL`.
Result: n=15,051, median=44, mean=52.0, p90=122. This single condition already
excludes 99.5% of the actual backlog-artifact rows for free, since they never got
a DOM computed. No broker-history join required.

**Gate B (stricter, for any claim that leans on "clean, steady-state" data —
e.g. a tweet or trend line):** Gate A **and** `first_seen < 2026-08-28` (excludes
the entire discovery-push window, not just the subset from brand-new brokers).
Result: n=10,051, median=48, mean=56.9, p90=122.

Recommendation: use Gate A as the standing filter on any DOM aggregate exposed
anywhere (site, API, tweet). Use Gate B specifically when the claim is
time-sensitive to "what does the steady-state market look like" (e.g. before/after
comparisons that could be spoiled by a fresh discovery push landing mid-window).
Either way: **do not publish a DOM figure computed with `coalesce(days_on_market, 0)`
or any nulls-as-zero handling** — that's the specific bug that produces the ~2-day
number and it does not reflect the actual index.
