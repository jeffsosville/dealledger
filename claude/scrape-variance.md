# Broker scrape variance — Aug 30 / Aug 31 2026

## What actually happened (from GitHub Actions logs, not theory)

Both nights the `scrape` job **never started at all**. It is not a scraper bug,
timeout, rate limit, or block — it's a GitHub billing failure on the account.

- **Aug 30**, run [33326684975](https://github.com/jeffsosville/dealledger/actions/runs/33326684975)
  (job 99297964816), duration 2s. Annotation:
  > The job was not started because recent account payments have failed or your
  > spending limit needs to be increased. Please check the 'Billing & plans'
  > section in your settings

- **Aug 31**, run [33432825186](https://github.com/jeffsosville/dealledger/actions/runs/33432825186)
  (job 99622062432), duration 3s. Identical annotation, word for word.

`gh run view --log` / `--log-failed` return "log not found" for both — expected,
since the job was rejected by the scheduler before a runner was ever assigned, so
no step logs exist to fetch.

**Same cause, both nights.** This is one billing lapse spanning two scheduled
runs, not two independent failures.

Runs resumed successfully starting **2026-09-01** (`33534927596`, `33553709615`,
both `conclusion: success`), so whatever billing issue existed was resolved
between 2026-08-31 evening and 2026-09-01.

## The "4" and "8" brokers-producing counts are unexplained by this log

If `broker_scrape.yml` never started on either night, it should have produced 0
new brokers, not 4 and 8. `specialized_scrape.yml` — the other workflow that
writes to the direct index — has no runs anywhere near Aug 30/31 in its last 40
runs (only two runs total, both 2026-09-02), so it isn't the source of that
trickle either.

**This is genuinely unresolved.** Possible explanations not yet checked: a
manual/local run outside CI, `last_seen` timestamps drifting forward from an
unrelated freshness-monitor touch, or the "producing" count in the mission doc
being computed on a rolling window that picks up stragglers from the prior
successful run. I did not have read access to `listings_direct` per-broker
`last_seen` for those two calendar dates to verify any of these, so this is
flagged rather than guessed at — do not treat 4/8 as scraper output counts
without confirming the source of that number.

## Is this still an active risk for tonight's run?

**Not the billing issue** — runs since Sep 1 are authenticating and starting
fine, including today's.

**But today's run (2026-09-02, `33662410364`) failed for a different, currently
live reason**: the `Run V6 scraper` step ran the full 5h50m and was killed by
`The job has exceeded the maximum execution time of 5h50m0s`, then the
`Check first_seen invariant` step also failed as a consequence, before
`Commit snapshot` / `Upload snapshot artifact` still ran (likely against
partial output). This is the run that produced the 18,047-row Sept 2 backlog
cohort referenced elsewhere in tonight's queue (item 4) — worth noting there,
since a run that hits the wall clock limit before finishing is a second,
independent source of an incomplete/partial snapshot, separate from the billing
outage. Out of scope for this item to fix; flagging so it isn't mistaken for
resolved.

## Bottom line

- Aug 30 + Aug 31 collapse: **one GitHub billing lapse**, not a scraper defect.
  No code change needed. If it recurs, `gh run list --workflow=broker_scrape.yml`
  will show the same "not started" annotation within seconds of the scheduled
  trigger — that's the tell, distinct from an actual scrape failure.
- The specific 4/8 producing-broker figures for those nights are not accounted
  for by any workflow log and should not be treated as verified until sourced.
- A real, currently-active failure mode exists as of today (2026-09-02): the
  scraper step hitting the 5h50m ceiling. Separate problem, separate fix.
