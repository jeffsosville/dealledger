# DOM contamination — quantified, and the inception-cohort gate (2026-09-03)

Method: Supabase REST API (`SUPABASE_URL`/`SUPABASE_SERVICE_KEY` from `.env`),
paginated GET against `listings_direct`/`crawl_runs`/`broker_discovery`/`broker_master`,
aggregated client-side. No direct Postgres access this session (no DB connection
string, no Docker for the Supabase CLI's dump path). This extends
`claude/dom-contamination.md` (same session, earlier pass) — read that file for the
full derivation of §1–4 below; this file adds the corrected backlog size, the
`bbs_listing_id` finding, and the inception-cohort gate design requested for tonight.

## 1. `dom_source` distribution — unchanged from the earlier pass

`listings_direct`, `status='active'`, 36,176 rows:

| dom_source | rows | has numeric `days_on_market` |
|---|---|---|
| `'first_seen'` | 23,429 | 13,043 yes / 10,386 no |
| `NULL` | 10,739 | 0 yes / 10,739 no |
| `'backtrack_unverified'` | 2,008 | 2,008 yes / 0 no |

Only 15,051 of 36,176 (41.6%) have any computed `days_on_market` at all. The
"~2-day median" figure is a null-handling bug (nulls coerced to 0 somewhere
downstream), not primarily a backlog-cohort artifact — see §3.

## 2. `bbs_listing_id` — the cross-match path supplies nothing today

**`bbs_listing_id` is NULL on all 36,176 active `listings_direct` rows. 0% coverage.**
This is worse than assumed going in — the "existing anchored interpolation via BBS
listing number" cross-match path referenced in `docs/bizquest-account-match.md` §3
currently has zero rows to work with. That path is entirely prospective; it doesn't
exist as data yet, only as an unused column and a plan.

## 3. Backlog cohort — corrected size

Two different, both-correct numbers, depending on what's being asked:

- **21,844 of 36,176 active rows (60.4%) have `first_seen >= 2026-09-01`.** This is
  the raw size of the recent-arrival window — verified this session, matches the
  figure circulating tonight.
- **6,393 of those rows (141 distinct broker domains) are from brokers whose entire
  active footprint is inside that window** — i.e., true first-contact brokers, not
  just recent listings from already-tracked brokers. This is the tighter number from
  the earlier pass (`claude/dom-contamination.md` §2) and is the one that actually
  represents "artifact of when we started looking," not just "recent."

Use 60.4% when describing how much of the table is recent; use 6,393/141 when the
claim is specifically about brand-new-broker contamination.

## 4. DOM median under different gates

| population | n (active) | n with DOM | median | mean | p90 |
|---|---|---|---|---|---|
| Gate A: `days_on_market IS NOT NULL` | 36,176 | 15,051 | 44 | 52.0 | 122 |
| Gate B: Gate A + `first_seen < 2026-08-28` | — | 10,051 | 48 | 56.9 | 122 |
| Gate C: Gate A + inception-cohort proxy (§5) | 29,792 (domain-level) | 12,975 | 39 | 52.2 | 122 |

Gate C's median (39) is *lower* than Gate A's (44), not higher — counter to the
intuition that filtering out "unseasoned" brokers should push DOM up. Reason: only
2,076 of the 6,384 single-first_seen-date-domain rows have a computed DOM at all
(32.5%, mostly via `backtrack_unverified`), and that small surviving subset skews
old (median 54) rather than young. The single-event cohort isn't pulling the
overall median down — it's mostly invisible to the DOM aggregate already, same
mechanism as §1. This is a caution against assuming cohort-exclusion is doing more
work than the null-exclusion is already doing; see the same conclusion in the
earlier pass (§3 there).

## 5. Inception-cohort gate — what exists to build it on

**No table tracks "times this broker has been successfully crawled" today.**
Checked three candidates:

- **`crawl_runs`** — 0 rows. Confirmed empty/orphaned (matches
  `docs/mission-doc-corrections.md` §5's note that this table isn't wired to the
  scraper).
- **`crawl_failures`** — has rows, but `broker_source_id` is null on the ones
  sampled, so failures can't be attributed to a specific broker record either. Only
  useful for global failure-type counts (e.g. `NO_PATTERN`), not per-broker history.
- **`broker_discovery.attempts`** — sampled rows show `attempts: 0` regardless of
  `status` or `last_attempt_at` being populated; the counter isn't incrementing in
  practice. Not usable as a crawl-count.
- **`broker_master.scraped_apr2026_at` / `active_listings_apr2026`** — a single
  static snapshot from an April backfill, not an ongoing counter.

**A real "second successful crawl" gate needs new infrastructure**: the scraper
would need to write one row per (broker, run) to something like `crawl_runs` with
an actual `broker_source_id`/domain and a success flag, incrementing a real count.
That doesn't exist and isn't buildable from data already on hand.

**What is buildable today, as a proxy, from `listings_direct` alone:** count
distinct `first_seen` dates among a broker's active rows. A broker with only one
distinct first_seen date has never contributed rows across two different scrape
events — the closest available approximation to "seen once." A broker with 2+
distinct first_seen dates has, by definition, appeared in at least two separate
observation events.

Result (515 distinct active `broker_domain` values):

- **348 domains (6,384 rows) have a single first_seen date** — proxy-"unseasoned."
- **167 domains (29,792 rows) have 2+ distinct first_seen dates** — proxy-"seasoned,"
  used as Gate C above.

**Caveats on this proxy, stated plainly:**
1. It only sees currently-*active* rows — a broker's earlier appearances that have
   since gone inactive are invisible, so a broker could look "single-event" today
   even if it was genuinely crawled multiple times with full turnover in between.
2. Multiple `first_seen` dates could arise from a backfill script running on two
   different calendar days for the same broker without a real second live crawl in
   between — this proxy can't distinguish "real second crawl" from "same batch,
   split across midnight" without crawl-run provenance, which doesn't exist (§5
   above).
3. It is domain-level, and `broker_domain` normalization has its own known issues
   (see `docs/bizquest-account-match.md` §4 — franchise sub-brands, relative BBS
   paths) that could misassign a listing to the wrong "broker" for this purpose.

**Recommendation:** ship Gate A now (it's free, already excludes 99.5% of the
backlog-artifact rows because they never got a DOM computed). Treat Gate C as
informative, not authoritative, until real crawl-run provenance exists — it doesn't
clearly outperform Gate A here and rests on a proxy with three named weaknesses.
Building the actual infrastructure (one written row per broker per crawl attempt,
with success/failure and a real domain key) is a prerequisite for a trustworthy
inception-cohort gate and should be scoped as its own small piece of work, not
bolted onto tonight's queue.

## 6. Does the ~103-day sold-listing finding hold here?

No change from the earlier pass: not testable against `listings_direct` (wrong
population — that figure is BBS sequential-ID interpolation on a different table
per `docs/METHODOLOGY.md`), and the table's own 13,026 `status='sold'` rows are 98%
missing a DOM value, so they can neither confirm nor refute it. See
`claude/dom-contamination.md` §4 for the full check.

## 7. Final recommended publication gate

**Ship:** `days_on_market IS NOT NULL` (Gate A). n=15,051, median=44, mean=52.0,
p90=122. Never publish a DOM aggregate that treats null as zero — that's the
specific, confirmed mechanism behind the ~2-day figure circulating tonight.

**Do not yet ship:** any inception-cohort claim. The concept is sound but nothing
in the current data can implement it reliably; Gate C above is the best available
approximation and it doesn't obviously beat Gate A. Build real per-broker crawl
provenance first.
