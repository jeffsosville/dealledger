# Work queue — night of 2026-09-02

Read first, in this order:

1. `docs/incident-2026-09-02-bridge-timeout.md` — what broke and what was fixed
2. `docs/mission-doc-corrections.md` — **the mission doc's index figure is wrong by 3x**
3. `docs/bizquest-account-match.md` — the largest opportunity found tonight

The bridge is fixed and the site is live as of 03:25 UTC.

---

## Ground rules for unattended work

**Read-only against production unless the user has said otherwise in this session.**
Diagnose, quantify, and write the patch as reviewable SQL or a diff. Do not apply
migrations, do not run scrapers, do not write to `listings` or `listings_direct`
while nobody is watching.

The one exception already granted: `CLAUDE.md` permits autonomous iteration on
scraper logic against a **single test broker**. Bulk runs still need a go-ahead.

Every item ends by writing findings down. A run that concludes without writing
anything down did not happen.

**And the discipline that got broken repeatedly tonight: do not restate an inherited
number as a measured one.** Several figures in `DEALLEDGER_MISSION.md` were quoted
back as analysis this evening without being checked, and one of them (BizBuySell at
~133,000) was wrong by a factor of three and reversed the strategic conclusion. If you
did not run the query, say so.

---

## 0. Finish the BizQuest account_id match (highest value — new)

See `docs/bizquest-account-match.md` for the full finding. Summary: `account_id` is
populated on 34,472 of 43,629 active BizQuest rows and 29,166 join cleanly to
`broker_master.account`, naming 2,397 brokers — 67% of BizBuySell's live index —
each with a website already on file.

Two things block turning that into a ranked crawl queue:

**(a) Domain normalization.** The naive join of `broker_master.companyurl`/`url`
against `listings_direct.broker_domain` misses franchise sub-brands — it calls
Sunbelt Midwest and several Transworld regional offices "uncovered" while we hold
1,089 rows under `sunbeltnetwork.com` and 4,530 under `tworld.com`. Many
`broker_master` rows also store a relative BBS profile path
(`/business-broker/.../39998/`) instead of a real website.

Build proper normalization plus a franchise-parent roll-up, then produce: brokers
ranked by BBS listings held, with what we currently hold beside each. Write the view
definition to `docs/broker-coverage-view.sql` — **do not apply it**.

Report the corrected gap number. The first pass suggested 2,545 brokers / 21,842
listings uncovered; that is an upper bound and known to be inflated by the join bug.
Do not repeat it without recomputing.

**(b) The `is_fsbo` bug.** 43,044 of 43,629 rows are flagged FSBO, derived from an
empty `broker_company` that the parser never fills. Find where in the BizQuest
scraper `broker_company` should be captured and write the fix as a proposed diff.

## 1. Audit every scheduled job for silent failure

Two jobs were found dead by accident in one evening. Nobody has checked the rest.

```sql
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

Deliverable: `docs/job-audit.md` — every scheduled job, its schedule, last success,
and whether anything asserts it ran. Flag every job whose last success is older than
its schedule interval.

## 2. Decouple `dealledger_pipeline.yml`

`quality_score`, `relist_refresh` and `vertical_sync` have not run since 2026-07-15
because the BBS scrape they hang off was disabled.

Proposed diff only, do not push. It needs its own `schedule:`, `needs: bbs_scrape`
removed from `quality_score`, and an assertion step per job — rows in vs rows written,
non-zero exit when they disagree.

Then quantify the backlog: how many rows are `Unverified` purely because the scorer
has not run since July 16.

## 3. Draft `is_listing_junk` additions

```sql
select header, count(*) as rows, min(direct_broker_url) as sample_url
from listings where is_active and source='broker_direct'
group by 1 having count(*) > 15 order by 2 desc;
```

Largest offender already identified: **Pavilion Business Services**, 959 active rows
across 21 titles, 20 of them category placeholders appearing ~48 times each at one
fixed round price ("SAAS Company" ×48 at $100,000,000). 916 of 959 have no state.
Every row has a distinct URL, which is why dedupe never caught it. No real listings
mixed in.

Also live: `SAG Hospitality Brokerage a UD Consulting Company` (94, firm name),
execbb.com category labels (`General Services-Laundromat` 75, `Food/Liquor-*` ~180),
`Seller Distribution Buyer with funds up to $100M` (50, a buyer-side ad).

**Principle 8 is mandatory.** Before proposing any rule: assemble ~500 currently
passing rows that are unambiguously real listings, run the rule against them, report
how many it would newly reject. **A rule that rejects any known-good row does not
ship.**

Watch cost too — these gates are why the bridge timed out, at 6.8s per 3,000 rows.
Measure any new clause. If the gate gets slower, the answer is a stored boolean column
maintained by trigger, not more regex in the hot path.

Deliverable: `docs/junk-rules-proposed.sql`, false-reject results inline as comments.

## 4. DOM contamination

21,844 of 36,176 active rows (60%) were first seen Sept 1 or later, from brokers never
crawled before — their `first_seen` records when we started looking. `bbs_listing_id`
is null on all 36,176, so the one true-date path supplies nothing.

Recrawling cannot fix this; it only moves `last_seen`. Options are in
`docs/bizquest-account-match.md` §5 and the on-page-date route.

Quantify: trustworthy vs untrustworthy DOM by `dom_source`; the distribution with the
backlog cohort excluded; whether the ~103-day sold finding survives on the clean
subset. Propose an **inception cohort** gate — a broker enters the DOM panel only
after its second successful crawl, so `first_seen` means a real appearance rather than
first contact. Write to `docs/dom-contamination.md`.

## 5. Explain the erratic broker scrape

Brokers producing per night: 12, 24, 65, **4**, **8**, 31, 197. Aug 30 and Aug 31 were
near-total collapses nobody noticed. Read the GitHub Actions run logs for
`broker_scrape.yml` before forming any theory. Write to `docs/scrape-variance.md`.

---

## Not in scope

- Applying any migration
- Working `v_broker_crawl_candidates` blind — item 0 replaces it with a ranked queue.
  Sept 2 showed blind depth work floods junk faster than the gates catch it.
- Anything touching the ATM CRM project (`wgrmxhxozoyvcmvbfuxv`)
