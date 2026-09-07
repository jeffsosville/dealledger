# Scheduled job audit — 2026-09-02

## Gap: pg_cron could not be checked

The queue's pg_cron query (`cron.job` / `cron.job_run_details`) requires a direct
Postgres connection. Nothing in this environment provides one:

- No `DATABASE_URL` / DB password in `.env` or the shell environment — only
  `SUPABASE_URL` + `SUPABASE_SERVICE_KEY` (PostgREST REST API + a fixed RPC list;
  `cron.job` is not exposed as a table/view/RPC).
- `supabase link --project-ref kqckuedsyyosmccushyd` succeeds, but `supabase db
  dump` shells out to a dockerized `pg_dump`, and Docker isn't installed/running
  here.
- No Management-API personal access token available to run the query via
  `POST /v1/projects/{ref}/database/query`.

**If pg_cron is in use at all on this project, it's a blind spot this audit cannot
close.** To close it, either: (a) run the query above in the Supabase SQL editor
for project `kqckuedsyyosmccushyd` and paste the result back, or (b) drop a
read-only Postgres connection string into the environment. Everything below is the
GitHub Actions side only.

---

## GitHub Actions — every workflow

| Workflow | Schedule | Last success | Last attempt | Failures (last 20 runs) | Asserts success? | Flag |
|---|---|---|---|---|---|---|
| `dealledger_pipeline.yml` (bbs_scrape, quality_score, relist_refresh, vertical_sync) | **disabled** — `schedule:` commented out 2026-07-15 | 2026-07-16 14:01 UTC | 2026-07-17 13:45 UTC (failure) | n/a — hasn't run since | No — no row-count assertion in any of the 4 jobs | 🔴 **48 days stale.** `quality_score`, `relist_refresh`, `vertical_sync` all hang off `needs: bbs_scrape`, which always fails on Akamai. Item 2 covers the fix. |
| `classify_verticals.yml` | none of its own — triggered by `workflow_run` on `broker_scrape.yml` completion | **never** (0 successful runs since the workflow was created) | 2026-09-02 23:30 UTC, conclusion `skipped` | 1 of 1 total runs, and that one was skipped, not failed | No | 🔴 **Highest-value finding.** The `if:` guard only fires the job when the upstream run's conclusion is exactly `success`. `broker_scrape.yml`'s last 5 runs: cancelled, success (manual), cancelled, cancelled, cancelled/failure. A scheduled `broker_scrape` run almost never concludes cleanly (see `scrape-variance.md` / concurrency behavior), so this workflow has **never once fired via automation** in its recorded history. This is the same class of failure as the vertical-classifier outage CLAUDE.md already documents — it's just the workflow wrapper, not the script, that's silently non-functional this time. |
| `broker_scrape.yml` | daily `0 14 * * *` | 2026-08-29 17:31 UTC (scheduled); 2026-09-01 20:10 UTC (manual dispatch) | 2026-09-02 17:39 UTC, `cancelled` | Aug 30 failure, Aug 31 failure, Sep 1 schedule cancelled, Sep 2 schedule cancelled | Partial — `first_seen_monitor.py` checks an invariant and fails the job, but there's no rows-in-vs-rows-written assertion | 🟡 The automated (schedule-triggered) path hasn't produced a clean success since **Aug 29** — 4 days, against a daily schedule. All subsequent scheduled attempts either failed or were cancelled; the only successes since are manual `workflow_dispatch` runs. See item 5 for Aug 30/31 root cause. |
| `regression_check.yml` | daily `0 15 * * *` | **no success in the last 20 runs** (oldest checked: 2026-08-14) | 2026-09-02 18:24 UTC, `failure` | 20 of 20 | **Yes** — this is the one job doing it right: it computes count vs. baseline floor per broker and exits 1 on any drop | 🟡 Different failure mode than the others: **the assertion works and has been correctly reporting a real regression for 3+ weeks** — `companysellers` has sat below its floor (386 vs. floor 398) since at least Aug 14, and nobody has either fixed the source or rebaselined it. A working alarm nobody is silencing is still a process gap, just not a code gap. |
| `tagging_monitor.yml` | daily `0 13:30 * * *` | **no success in the last 20 runs** (oldest checked: 2026-08-14) | 2026-09-02 17:13 UTC, `failure` | 20 of 20 | Yes, for tagging — checks trailing-7-day untagged rate against a 20% threshold and exits 1 | 🔴 Two issues stacked: (1) the 79% untagged spike on 2026-09-02 (the Sept 2 backlog cohort — see `dom-contamination.md`) correctly trips the gate, and the gate stays red for a full week afterward by design (trailing 7-day window), which explains the unbroken red streak; (2) **`RESEND_API_KEY` is not set as a repo secret at all** — the job logs `(not sending: dry_run=False, key_set=False)` every run. The check has never once emailed anyone, on any day, since it was written. It fails loudly in the Actions UI and nowhere else. |
| `freshness_monitor.yml` | daily `0 13 * * *` | 2026-09-02 16:56 UTC | 2026-09-02 16:56 UTC | 2 of last 20 (Aug 30, Aug 31 — correlates with the broker-scrape collapse those same two nights) | Yes — staleness thresholds per broker | 🔴 Same `RESEND_API_KEY` gap as `tagging_monitor.yml`: log shows `# DRY-RUN (RESEND_API_KEY unset) — email NOT sent.` on every run, including the two failure days. Repo secrets (`gh secret list`) confirm `RESEND_API_KEY` has never been set. This monitor has been running correctly and alerting *no one* since inception. |
| `bizquest_daily.yml` | daily `0 13 * * *` | 2026-09-02 16:54 UTC | 2026-09-02 16:54 UTC | 0 of 3 (short history — recently added) | No row-count assertion, just script exit code | 🟢 Healthy so far, but no assertion beyond exit code. |
| `bizquest_weekly_enrich.yml` | weekly `0 13 * * 0` (Sundays) | never run | never run | n/a | No | 🟢 Not actually overdue — file was added 2026-08-31, first Sunday due is 2026-09-06/07. Flagging only so it isn't forgotten if that date passes with no run. |
| `specialized_scrape.yml` | daily `0 8 * * *` | 2026-09-02 17:35 UTC (manual dispatch) | 2026-09-02 17:35 UTC | 0 of 2, but **both runs are `workflow_dispatch`, zero scheduled runs recorded** | Partial — same `first_seen_monitor.py` invariant check as `broker_scrape.yml` | 🟢 File created 2026-09-02 09:55 ET, same day — its first scheduled 8am UTC run hasn't come up yet (next is 2026-09-03). Not stale, just untested on its own trigger. |

---

## Commented-out schedules

Only one found: `dealledger_pipeline.yml` (line 4, disabled 2026-07-15, per the
inline comment). No other workflow has a disabled `schedule:` block.

## `needs:` chains on known-dead upstream jobs

Only `dealledger_pipeline.yml` internally (`quality_score` → `needs: bbs_scrape` →
`relist_refresh` → `vertical_sync`, a straight chain, so one dead root job kills
three downstream jobs). `classify_verticals.yml`'s `workflow_run` trigger is the
same *shape* of problem — a hard dependency on another workflow's exact success
state — but it isn't a `needs:` chain; see the row above.

## Jobs that assert nothing beyond exit code

`bizquest_daily.yml`, `bizquest_weekly_enrich.yml`, and all four jobs in
`dealledger_pipeline.yml` have no explicit rows-in vs. rows-written check — they
rely entirely on the underlying script's exit code. None of the four
`dealledger_pipeline.yml` jobs would have caught a script that ran, wrote nothing,
and exited 0.

## Summary — stale beyond schedule interval

1. `dealledger_pipeline.yml` — 48 days (disabled entirely)
2. `classify_verticals.yml` — never succeeded via automation, ever
3. `tagging_monitor.yml` / `freshness_monitor.yml` — alerting dead since inception (`RESEND_API_KEY` unset), independent of whether the checks themselves pass
4. `regression_check.yml` — correctly red for 3+ weeks on `companysellers`, unresolved
5. `broker_scrape.yml` — automated path hasn't cleanly succeeded since Aug 29 (4 days)
