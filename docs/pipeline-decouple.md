# dealledger_pipeline.yml — decouple proposal + backlog quantification

**Refresh of an earlier pass this session** (previously `claude/pipeline-decouple.md` +
`claude/dealledger_pipeline.proposed.yml`). This version re-verifies everything live
rather than copying those numbers forward, per tonight's standing rule: don't restate
an inherited number as measured. Proposed workflow copied unchanged to
`docs/dealledger_pipeline.proposed.yml` (not applied, not pushed) — re-diffed against
the current `.github/workflows/dealledger_pipeline.yml` and it is byte-identical to
what it was earlier tonight, so the proposal itself needed no changes.

## What changed vs. the current file

1. **`schedule:` restored on its own trigger**, not the disabled BBS one. Old file's
   only `schedule:` block is commented out entirely, so the workflow only runs on
   `workflow_dispatch` — confirmed nobody has been triggering it:
   `gh run list --workflow=dealledger_pipeline.yml` shows last success
   `2026-07-16T14:01:18Z`, last attempt `2026-07-17T13:45:45Z` (failure), nothing
   since. New: `schedule: - cron: '0 7 * * *'`.
2. **`bbs_scrape` gated `if: workflow_dispatch` only; `needs: bbs_scrape` removed
   from `quality_score`.** BBS is permanently Akamai-blocked (also independently
   reconfirmed tonight via `docs/bizquest-account-match.md` — BizQuest/BBS scraping
   dead end, `account_id`-based identification is the live path instead). Leaving
   the `needs:` edge in place would keep `quality_score` (and everything chained
   after it) skipping forever even with the schedule restored.
3. **Assertion step added to each of the three surviving jobs** (`quality_score`,
   `relist_refresh`, `vertical_sync|`), each checking a condition the underlying
   script already computes and logs but never fails on. Unchanged from the earlier
   pass — see `docs/dealledger_pipeline.proposed.yml` inline comments for exact grep
   logic and the minimal script-side fix each stopgap points at.

## Backlog quantification — re-verified live, 2026-09-03

Method: Supabase REST, single-shot `Prefer: count=exact` / `Range: 0-0` requests
against `listings` (source .env), not multi-page client-side aggregation — see
methodology note at the bottom for why.

**Broker-direct active count has moved since the last pass, as expected**: the
bridge fix earlier tonight (`docs/incident-2026-09-02-bridge-timeout.md`) raised it
from 21,677 to **26,505**. Confirmed again just now via exact count.

**Tier distribution, active rows only (`is_active=true`, `listings`, all sources,
n=33,076) — unchanged from the earlier pass tonight, digit for digit:**

| tier | rows | % of active |
|---|---:|---:|
| Verified | 95 | 0.3% |
| Likely Real | 353 | 1.1% |
| Unverified | 29,359 | 88.8% |
| Likely Junk | 3,235 | 9.8% |
| *(null tier)* | 34 | 0.1% |
| **null `quality_score`** | **2,109** | 6.4% |

Unchanged because nothing has run `quality_scorer.py` between the two measurements —
consistent with the workflow still being disabled. This stability is itself evidence
the count-header method is reliable (see methodology note).

**The queue's framing still doesn't hold.** `Unverified` is a real score band
(40–59 in `scrapers/quality_scorer.py`), not "unscored." The genuinely unscored set
is `quality_score IS NULL` — 2,109 active rows, and every one has
`first_seen <= 2026-07-15`. None of them are the July-outage backlog; they were
excluded from scoring runs even before the outage, for a separate, uninvestigated
reason.

**Confirmed, more cleanly than before: stale `quality_score` is surviving upsert on
re-scraped rows.** Of the 26,505 active `broker_direct` rows, **5,993 have
`first_seen >= 2026-09-01`** — and **all 5,993 of them already carry a non-null
`quality_score`**. That's not possible via any code path that inserts a genuinely new
row (a brand-new row can only get a score from `quality_scorer.py`, which hasn't run
since July 16). The only consistent explanation: these are pre-existing rows that got
re-scraped/re-bridged after Sept 1, bumping `first_seen`/`last_seen` forward, while
the upsert left `quality_score`/`quality_tier` untouched — so a score computed against
whatever the row's title/price/category was before July 16 is still being served as
current. This is the same shape of bug already fixed twice this week on
`listings_direct` (`first_seen` baseline commit `bccb714`; "Fix upsert resetting
status on every re-scrape" commit `9cba758`) — worth checking whether `listings`'
own upsert path (in `scrapers/broker_scraper.py` / `dealledger_scraper_v6.py`, whichever
writes `broker_direct` rows into `listings`) has the same unconditional-overwrite or
column-omission issue. Not fixed tonight — flagged as a follow-up, same as the last
pass.

**Practical consequence unchanged:** there's no `quality_scored_at` column, so there's
no way to tell, for any given active row, whether its current tier reflects its
current content. Recommended follow-up (not done tonight): add
`quality_scored_at timestamptz`, set on every write by `upsert_scores()`.

**Best available answer to "what will the tier distribution look like after a full
pass":** still unknown in a rigorous sense, for the same reason as before — most
labels on recently-active rows are carryover, not current. The only number stated
with confidence: at least **2,109** active rows get a tier for the first time; the
5,993 recently-touched `broker_direct` rows are candidates for re-scoring to a
different tier once the pipeline actually runs, but by how much is not knowable from
the data that exists today.

## Methodology note — a live caveat worth carrying into every other item tonight

While re-measuring this, a **multi-page client-side pagination** query against
`listings` (fetching all active rows in 1,000-row windows via `Range` headers, no
explicit `order=`) returned a different `broker_direct` count on each of two
successive runs seconds apart — 24,879, then 24,348 — while a **single-shot
`Prefer: count=exact` request** against the identical filter returned **26,505** both
times, matching the number independently confirmed earlier tonight. The single-shot
count is authoritative here; the pagination drift is most likely because those
requests carried no explicit stable sort key, so Postgres/PostgREST doesn't guarantee
consistent page boundaries across repeated requests, and it's plausibly compounded by
concurrent writes to `listings` (a bridge or scrape job may be running right now).
Recommendation for the rest of tonight's items: prefer single-shot exact counts for
headline numbers, and if a full row pull is required, add an explicit
`order=id.asc` (or another stable key) to the pagination params — an un-ordered
paginated fetch against a live table is not a safe way to compute an aggregate.
