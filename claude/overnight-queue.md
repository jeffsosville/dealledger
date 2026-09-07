# Work queue — night of 2026-09-02

Context: `claude/incident-2026-09-02-bridge-timeout.md`. Read it first.

The bridge is fixed and the site is live as of 03:25 UTC. Everything below is what
that investigation surfaced and did not finish.

**Note (2026-09-02, session start):** the incident file this doc points to could
not be located anywhere in the repo, its git history, or any branch, and was not
supplied when asked. Items below proceed from this queue document alone; any place
where the incident report's specifics would have mattered is flagged inline in the
relevant deliverable rather than guessed at.

---

## Ground rules for unattended work

**Read-only against production unless the user has said otherwise in this session.**
Diagnose, quantify, and write the patch into this project as reviewable SQL or a
diff. Do not apply migrations, do not run scrapers, do not write to `listings` or
`listings_direct` while nobody is watching.

The one exception already granted: `CLAUDE.md` permits autonomous iteration on
scraper logic against a **single test broker**. Bulk runs still need a go-ahead.

Every item below ends by writing findings back into this project. A run that
concludes without writing anything down did not happen.

---

## 1. Audit every scheduled job for silent failure (highest value)

Two jobs were found dead by accident in one evening. Nobody has checked the rest.

```sql
-- pg_cron: every job, when it last succeeded, and whether it is lying
select j.jobname, j.schedule, j.active,
       max(d.start_time) filter (where d.status='succeeded') as last_success,
       max(d.start_time) as last_attempt,
       count(*) filter (where d.status<>'succeeded'
                          and d.start_time > now() - interval '14 days') as recent_failures
from cron.job j
left join cron.job_run_details d on d.jobid = j.jobid
group by 1,2,3
order by last_success nulls first;
```

For GitHub Actions, check each workflow in `.github/workflows/` for:

- a commented-out `schedule:` block (this is how `dealledger_pipeline.yml` died)
- `needs:` chains where an upstream job is known-dead — `quality_score` still
  declares `needs: bbs_scrape`, which always fails on Akamai
- last successful run per workflow via `gh run list --workflow=<name> --limit 20`

Deliverable: a table of every scheduled job — name, schedule, last success, whether
anything asserts it ran. Write it to `claude/job-audit.md`. Flag every job whose
last success is older than its schedule interval.

## 2. Decouple `dealledger_pipeline.yml`

`quality_score`, `relist_refresh` and `vertical_sync` have not run since
2026-07-15 because the BBS scrape they hang off was disabled.

Write the corrected workflow YAML into this project as a proposed diff. It needs:

- its own `schedule:` (not the disabled BBS one)
- `needs: bbs_scrape` removed from `quality_score`
- an assertion step per job: rows in vs rows written, and a non-zero exit when they
  disagree

Do not push it. Leave it for review.

Then quantify the backlog it will have to chew through: how many rows are
`Unverified` purely because the scorer has not run since July 16, and what the tier
distribution is expected to look like after a full scoring pass.

## 3. Draft `is_listing_junk` additions for the 2,045 live junk rows

Full list:

```sql
select header, count(*) as rows, min(direct_broker_url) as sample_url
from listings
where is_active and source='broker_direct'
group by 1 having count(*) > 15
order by 2 desc;
```

Known shapes: broker firm names ("SAG Hospitality Brokerage a UD Consulting
Company"), category labels ("General Services-Laundromat", "Food/Liquor-Restaurant"),
generic type placeholders priced $40–80M ("Manufacturing Company", "Telecom
Company"), and buyer-side ads ("Seller Distribution Buyer with funds up to $100M").

**Principle 8 is mandatory here.** Two guards written to reject junk have instead
rejected real listings, and a filter's false-reject rate is invisible. Before
proposing any rule:

1. Assemble a known-good set — say 500 rows currently passing that are unambiguously
   real listings.
2. Run the proposed rule against it and report how many it would newly reject.
3. A rule that rejects **any** known-good row does not ship. Report it and stop.

Watch the cost too. These functions are already the reason the bridge timed out —
6.8s per 3,000 rows. Any new clause must be measured, not just written. If the gate
gets slower, the fix is a stored boolean column maintained by trigger, not more
regex in the hot path.

Deliverable: `claude/junk-rules-proposed.sql` with the false-reject test results
inline as comments.

## 4. Quantify the DOM contamination

Median DOM on the direct index is 2 days because 18,047 rows arrived Sept 2 from
never-before-crawled brokers whose `first_seen` is an artifact of when crawling
started.

Work out:

- how many active rows have `dom_source` that makes their DOM trustworthy vs not
- what the median and distribution look like when the backlog cohort is excluded
- whether the ~103-day sold-listing finding still holds against the clean subset

Propose the publication gate. Nothing goes on the site or into a tweet off the
contaminated number. Write to `claude/dom-contamination.md`.

## 5. Explain the erratic broker scrape

Brokers producing per night: 12, 24, 65, **4**, **8**, 31, 197. Aug 30 and Aug 31
were near-total collapses nobody noticed. The mission doc assumes ~155.

Find out what happened on those two nights — GitHub Actions logs for
`broker_scrape.yml` first, per principle 3. Do not theorize before reading the run
log. Write findings to `claude/scrape-variance.md`.

---

## Not in scope tonight

- Applying any migration
- The 1,171 uncrawled brokers in `v_broker_crawl_candidates` — depth work, and the
  Sept 2 run just demonstrated it floods junk faster than the gates can catch it.
  Correctness first.
- Anything touching the ATM CRM project (`wgrmxhxozoyvcmvbfuxv`)
