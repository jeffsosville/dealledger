# Scheduled job audit — refreshed 2026-09-03 (early UTC)

Re-run of the 2026-09-02 audit (`claude/job-audit.md`) against current `gh run list`
output and `gh secret list`. No workflow has produced a run newer than 2026-09-02 as
of this refresh, so most findings are unchanged and re-verified rather than new;
differences from the prior pass are called out explicitly.

## Gap: pg_cron could not be checked — still true, and now confirmed costly

The queue's pg_cron query (`cron.job` / `cron.job_run_details`) requires a direct
Postgres connection. Nothing in this environment provides one: no `DATABASE_URL` /
DB password, `supabase db dump` needs Docker (not installed/running here), and no
Management-API personal access token for `POST /v1/projects/{ref}/database/query`.
`cron.job` is not exposed as a REST table/view/RPC.

**This blind spot is no longer theoretical.** `docs/incident-2026-09-02-bridge-timeout.md`
(pasted into this session, not independently verified against `cron.job` by me)
describes a pg_cron job, `bridge-direct-daily`, that ran `bridge_direct_to_listings()`
nightly at 15:00 UTC and silently failed every night from 2026-08-28 through
2026-09-02 with a statement-timeout error — six nights, `active=true` the whole
time, nothing asserting it had actually finished. That's exactly the failure mode
this audit exists to catch, on exactly the resource this audit cannot reach.

The doc further claims: fixed 2026-09-03 03:23 UTC via migration
`bridge_direct_materialize_gate_set`, runtime now 73.8s, and a new daily monitor
asserting the run succeeded and that `max(last_seen)` in `listings` tracks
`listings_direct` within a day, warning above 240s runtime.

**I have not confirmed any of that.** I have no way to query `cron.job`,
`cron.job_run_details`, or the migration history this session. Treat "fixed and
monitored" as an unverified claim from a pasted document, not a confirmed audit
result, until someone runs the original pg_cron query in the Supabase SQL editor for
`kqckuedsyyosmccushyd` or drops a read-only connection string into this environment.

| Job | Status per audit |
|---|---|
| `bridge-direct-daily` (pg_cron) | **Unauditable this session.** Claimed fixed 2026-09-03 + claimed newly monitored, per `docs/incident-2026-09-02-bridge-timeout.md`. Not verified against `cron.job`. |
| Any other pg_cron job | Unknown — entirely unaudited, no visibility at all. |

---

## GitHub Actions — every workflow (re-verified 2026-09-03)

| Workflow | Schedule | Last success | Last attempt | Failures (last 8-20 runs) | Asserts success? | Flag |
|---|---|---|---|---|---|---|
| `dealledger_pipeline.yml` (bbs_scrape, quality_score, relist_refresh, vertical_sync) | **disabled** — `schedule:`/`cron:` commented out on lines 4-5, tagged "DISABLED — BBS Akamai-blocked 2026-07-15" | 2026-07-16T14:01:18Z | 2026-07-17T13:45:45Z (failure) | Last 8 runs: failure/success/failure/cancelled/failure/failure/success/failure | No — no row-count assertion in any of the 4 chained jobs (`quality_score` needs `bbs_scrape`, `relist_refresh` needs `quality_score`, `vertical_sync` needs `relist_refresh`) | 🔴 **48 days stale, unchanged.** Item 2 covers the fix. |
| `classify_verticals.yml` | none of its own — `workflow_run` trigger keyed to `broker_scrape.yml` completion | **never** (only 1 run ever recorded) | 2026-09-02T23:30:13Z, conclusion `skipped` | 1 of 1 total | No | 🔴 **Unchanged, still the highest-value single finding.** The `if:` guard requires the upstream run to conclude exactly `success`; `broker_scrape.yml`'s scheduled runs essentially never do (see below). Zero automated executions in this workflow's entire history. |
| `broker_scrape.yml` | daily `0 14 * * *` | 2026-09-01T16:57:41Z (`workflow_dispatch`, manual); last **scheduled** success 2026-08-29T17:31Z per prior audit | 2026-09-02T17:39:16Z, `cancelled` (scheduled) | Last 8: cancelled(sched)/success(dispatch)/cancelled(dispatch)/cancelled(dispatch)/cancelled(sched)/success(dispatch)/failure(sched, Aug31)/failure(sched, Aug30) | Partial — `first_seen_monitor.py` invariant check, no rows-in-vs-rows-written assertion | 🟡 Automated (schedule-triggered) path has not produced a clean success since **Aug 29** — every subsequent scheduled run failed or was cancelled; all successes since are manual dispatches. Root cause for Aug 30/31 in `docs/scrape-variance.md`. |
| `regression_check.yml` | daily `0 15 * * *` | **no success in last 8 runs**, back to 2026-08-26 | 2026-09-02T18:24:44Z, `failure` | 8 of 8 checked (was 20/20 in prior audit — consistent, just a smaller window this pass) | **Yes** — computes count vs. baseline floor per broker, exits 1 on drop | 🟡 Unchanged: the assertion is doing its job correctly. `companysellers` still below floor, still unresolved, still nobody has acted on a real, correctly-firing alarm for 3+ weeks. |
| `tagging_monitor.yml` | daily `30 13 * * *` | no success in last 8 runs (back to 2026-08-26) | 2026-09-02T17:13:16Z, `failure` | 8 of 8 | Yes — trailing-7-day untagged-rate threshold, exits 1 above 20% | 🔴 Unchanged. `RESEND_API_KEY` reconfirmed **absent** from `gh secret list` (secrets present: `CLEANINGEXITS_ANON_KEY`, `CLEANINGEXITS_SERVICE_KEY`, `CLEANINGEXITS_SUPABASE_URL`, `PROXY_URL`, `SUPABASE_SERVICE_KEY`, `SUPABASE_URL` — no `RESEND_API_KEY`, no mail secret of any name). This check has never emailed anyone, ever. |
| `freshness_monitor.yml` | daily `0 13 * * *` | 2026-09-02T16:56:03Z | 2026-09-02T16:56:03Z | 2 of 8 (Aug 30, Aug 31 — same two nights as the broker-scrape collapse) | Yes — per-broker staleness thresholds | 🔴 Unchanged. Same confirmed-absent `RESEND_API_KEY`. Alerting dead since inception, independent of the check logic itself being correct. |
| `bizquest_daily.yml` | daily `0 13 * * *` | 2026-09-02T16:54:19Z | 2026-09-02T16:54:19Z | 0 of 3 | No row-count assertion beyond script exit code | 🟢 Healthy so far; thin assertion. |
| `bizquest_weekly_enrich.yml` | weekly `0 13 * * 0` (Sundays) | never run (0 runs recorded at all) | never run | n/a | No | 🟢 Not overdue — added 2026-08-31, first due date 2026-09-06/07. |
| `specialized_scrape.yml` | daily `0 8 * * *` | 2026-09-02T17:35:51Z (`workflow_dispatch`) | 2026-09-02T17:35:51Z | 0 of 2, but **both recorded runs are manual dispatches — zero scheduled runs have fired yet** | Partial — same `first_seen_monitor.py` check | 🟢 File is one day old; next scheduled 8am UTC run is 2026-09-03 (today) — unconfirmed as of this audit since no run had landed by refresh time. |

No workflow in this list produced a run dated 2026-09-03 as of this refresh — the
audit window (roughly one hour after the prior pass) simply hasn't crossed any
workflow's next scheduled fire time yet, `specialized_scrape.yml`'s included.

---

## Commented-out schedules

Unchanged: only `dealledger_pipeline.yml` (lines 4-5, disabled 2026-07-15).

## `needs:` chains on known-dead upstream jobs

Unchanged: `dealledger_pipeline.yml`'s `bbs_scrape → quality_score → relist_refresh →
vertical_sync` chain. `classify_verticals.yml`'s `workflow_run` gate is the same
*shape* of problem (hard dependency on another workflow's exact success state) but
isn't a `needs:` chain.

## Jobs that assert nothing beyond exit code

Unchanged: `bizquest_daily.yml`, `bizquest_weekly_enrich.yml`, and all four
`dealledger_pipeline.yml` jobs.

## Summary — stale beyond schedule interval

1. `dealledger_pipeline.yml` — 48 days (disabled entirely)
2. `classify_verticals.yml` — never succeeded via automation, ever
3. `tagging_monitor.yml` / `freshness_monitor.yml` — alerting dead since inception (`RESEND_API_KEY` unset, reconfirmed this pass)
4. `regression_check.yml` — correctly red for 3+ weeks on `companysellers`, unresolved
5. `broker_scrape.yml` — automated path hasn't cleanly succeeded since Aug 29
6. **New this pass:** `bridge-direct-daily` (pg_cron) — a real 6-night silent failure that this audit's own GitHub-Actions-only scope could never have caught, and whose claimed fix/monitor remains unverified because pg_cron access is still unavailable. This is the strongest argument yet for closing the pg_cron gap rather than continuing to scope around it.
