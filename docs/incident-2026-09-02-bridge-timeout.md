# Incident — dealledger.org frozen at 2026-08-27

**Detected:** 2026-09-02 ~23:00 ET (user report: "listings won't load")
**Resolved:** 2026-09-03 03:25 UTC
**Duration of silent failure:** 6 nights (Aug 28 – Sep 2)
**Data lost:** none. Everything was intact in `listings_direct` the whole time.

---

## What the user saw

Homepage table stuck showing `EST. LISTED = 2026-08-27` on every row, and slow to
render. Two separate causes, only one of them real.

## Root cause

`bridge_direct_to_listings()`, run nightly by the pg_cron job `bridge-direct-daily`
at 15:00 UTC, threw `canceling statement due to statement timeout` every night from
2026-08-28 onward. Last success was 2026-08-27 15:00 UTC.

The job stayed `active = true` and fired on schedule every night. It simply never
finished. Nothing asserted that it had.

The timeout was in the retire step:

```sql
update listings l set is_active = false
where l.source = 'broker_direct' and l.is_active
  and not exists (
    select 1 from listings_direct d
    where 900000000 + (('x' || substr(md5(d.id),1,7))::bit(28)::bigint) = l.listing_number
      and d.status = 'active'
      and looks_like_real_listing(d.title)
      and not is_listing_junk(d.title, d.url)
  );
```

Three compounding problems:

1. The join key is `md5(d.id)` computed inside a correlated subquery — no index can
   serve it.
2. `looks_like_real_listing()` and `is_listing_junk()` are called per inner row.
   Measured cost: **6.8 seconds per 3,000 rows** (~60 chained regex ops each). A
   plain `count(*)` applying both gates across the table exceeds 60s on its own.
3. 21,677 outer rows × 71,702 inner rows with two function calls each.

Both gate functions were added to this subquery by the 2026-08-27 migrations
(`structural_listing_gate`, `bridge_deactivate_stale_direct_rows`). The bridge
succeeded that same afternoon and never again.

Because the function is a single transaction, the timeout rolled back the upsert
too — so `listings` received nothing at all, not even a partial update.

## Why it surfaced when it did

`listings_direct` kept ingesting normally throughout. The backlog crawler had two
strong nights (Sept 1: 2,888 rows / 31 brokers; Sept 2: 21,258 rows / 197 brokers),
which widened the gap between the two tables until it was obvious on the site.

The scraper was never broken. Only the pipe between the two tables.

## Fix applied

Migration `bridge_direct_materialize_gate_set` (applied 2026-09-03 03:23 UTC).
Evaluates the gate set exactly once into an indexed temp table, then anti-joins
against it. Adds a function-scoped `statement_timeout` of 600s, since this is a batch
job and the default 120s left no headroom.

`distinct on (synth_num)` is retained deliberately — 28 bits over ~36k rows
collides, and that collision has bitten before.

**Result:** run completed in **73.8 seconds** (was hitting the 120s wall).
Active broker-direct listings on the site: 21,677 → 26,505.
Nightly job left on its normal schedule and now runs the fixed function.

A daily monitor now asserts the run succeeded and that `max(last_seen)` in
`listings` tracks `listings_direct` within one day. It also warns if runtime exceeds
240s, since the gates get slower as `listings_direct` grows.

---

## Corrections to earlier claims made during this investigation

Recorded per operating principle 11 (publish the correction).

- **`dealledger_listings` is not what the homepage reads.** It is down to 448 rows,
  which is a real problem, but it is not this one. The homepage queries
  `/rest/v1/listings` directly.
- **The `0d` DOM column was not a frontend bug.** It was an artifact of the stale
  bridge — those rows were written on Aug 27 with `first_seen` of that same day. No
  frontend change was needed. This was asserted before it was checked.

---

## Still open after this fix

### 1. `dealledger_pipeline.yml` has been disabled since 2026-07-15 — bigger than this incident

The workflow's cron is commented out: `# DISABLED — BBS Akamai-blocked 2026-07-15`.
Four jobs are chained off it by `needs:`:

```
bbs_scrape → quality_score → relist_refresh → vertical_sync
```

Disabling the BBS schedule killed all four. Consequences still live today:

- `quality_scorer.py` has not run since. Nothing has been scored `Verified` or
  `Likely Real` since **2026-07-16**. All 21,677 broker-direct rows in `listings`
  are `Unverified`, which is why `dealledger_listings` returns 448 rows.
- `relist_refresh.py` has not run — relist flags are frozen.
- `vertical_sync.py` has not run.

`workflow_dispatch` cannot rescue any of it, because `quality_score` still declares
`needs: bbs_scrape`, which now always fails on Akamai.

**This is also the true cause of the "vertical classifier stopped writing on 15 July"
story in DEALLEDGER_MISSION.md.** It was never a separate failure — it is this same
commented-out line. `classify_verticals.yml` was separately repaired to trigger off
`workflow_run` of the broker scrape, so vertical *tagging* works; `vertical_sync.py`
(the push to CleaningExits) is what remains dead.

**Fix:** decouple. `quality_score`, `relist_refresh` and `vertical_sync` need their
own schedule and must drop `needs: bbs_scrape`. BBS scraping is permanently dead and
nothing else should depend on it.

### 2. Junk from the Sept 1–2 crawl is now live

The gates blocked the worst of it (all 400 `Static Details1` from jhcallahan.com,
145 `Seller with` from pavilionservices.com, the aria.net person-names). What got
through: **2,045 rows, 7.7% of the direct index**, across 47 titles repeated more
than 15 times.

That 7.7% understates the visible damage. The homepage sorts by `estimated_listed_date
desc` and the junk all arrived on Sept 2, so it is concentrated on page one — roughly
**40% of the first screen**.

Largest single offender, and unambiguous:

**Pavilion Business Services — 959 active rows, 21 distinct titles.** Twenty are
generic category placeholders, each appearing ~48 times at one fixed round price:

| Title | Rows | Price |
|---|---|---|
| SAAS Company | 48 | $100,000,000 |
| Recurring Services Company | 49 | $100,000,000 |
| Glazing and Fenestration Company | 48 | $80,000,000 |
| Fenestration / Curtain Wall Company | 48 | $75,000,000 |
| Process Equipment Company | 48 | $60,000,000 |
| Food Processing Company | 48 | $50,000,000 |
| Facilities Management or Logistics Company | 48 | $45,000,000 |
| Distribution Company | 48 | $40,000,000 |
| Manufacturing Company | 48 | $25,000,000 |
| Telecom Company | 49 | $10,000,000 |

916 of 959 have no state. Every row has its own distinct URL, which is why dedupe
never caught it — the scraper walked a "what we sell" taxonomy page and wrote each
category once per listing card. No real listings are mixed in.

Reversible kill, pending review (nothing deleted, per principle 6):

```sql
update listings set is_active = false
where source='broker_direct' and broker_account='Pavilion Business Services';
```

Other confirmed junk still live: `SAG Hospitality Brokerage a UD Consulting Company`
(94 rows, firm name), execbb.com category labels (`General Services-Laundromat` 75,
`Food/Liquor-*` ~180), `Seller Distribution Buyer with funds up to $100M` (50, a
buyer-side ad).

Per principle 8, any new rule must be tested against known-good input before it goes
in — the last two guards written to reject junk rejected real listings instead.

### 3. Published DOM is about to be wrong

Median DOM on the direct index is now **2 days**, average 25. 18,047 rows landed on
Sept 2 from brokers never crawled before, so their `first_seen` records when we
started looking, not when the business was listed.

Publishing off this table this week would report roughly the inverse of the
103-day finding. DOM publication should be gated on `dom_source` so backlog-cohort
rows are excluded until anchored.

### 4. The daily broker scrape is erratic

Brokers producing per night: 12, 24, 65, **4**, **8**, 31, 197. The mission doc
assumes ~155 producing. Aug 30 and Aug 31 were near-total collapses that nobody
noticed. Worth a look on its own.

---

## The lesson, stated for the next session

Every failure in this incident was already described in DEALLEDGER_MISSION.md.

- Principle 1 (silent failure): a cron job marked `active` that fires and never
  finishes reports success by saying nothing.
- Principle 3 (check the call path first): `cron.job_run_details` held the exact
  error and the exact date. It was the second query run and it ended the search.
- Principle 8 (test a filter for what it costs, not just what it rejects): two gate
  functions were added to a hot correlated subquery and the cost was never measured.

**A scheduled job that can fail must assert that it succeeded.** `bridge-direct-daily`
now has a monitor. Nothing else in this system does.
