# Broker scrape variance — Aug 30 / Aug 31 2026 (refreshed 2026-09-03)

Re-verified from a fresh `gh run list`/`gh api` pull against `broker_scrape.yml`,
`specialized_scrape.yml`, and a fresh query of `listings_direct` and `crawl_runs`.
Starting point was `claude/scrape-variance.md` from earlier tonight; nothing in that
file's core conclusion changed, one new independent data point was added, and the
5h50m-ceiling risk was checked for recurrence.

## 1. Aug 30 / Aug 31 — billing lapse, reconfirmed

Both nights the `scrape` job never started. Re-pulled directly:

- **Aug 30**, run `33326684975`, job "scrape", `conclusion: failure`, 2s duration.
- **Aug 31**, run `33432825186`, job "scrape", `conclusion: failure`, 3s duration.

Both annotated: *"The job was not started because recent account payments have
failed or your spending limit needs to be increased."* No step logs exist for
either (job was rejected before a runner was assigned). Runs resumed successfully
Sep 1 (`33534927596`, `33553709615`, both `conclusion: success`). No change from
the earlier finding — this is one billing lapse, not a scraper defect.

## 2. The "4" and "8" brokers-producing figures — still unresolved, one new data point

Checked further this pass:

- **`specialized_scrape.yml`** has a defined schedule (`0 8 * * *`) but its entire
  run history is **two runs total, both created 2026-09-02, both
  `event: workflow_dispatch`** — its scheduled trigger has apparently never fired
  even once. It cannot be the source of anything on Aug 30/31; it didn't run then
  under any trigger type.
- **`crawl_runs`** (the table with `brokers_succeeded`/`brokers_failed` columns
  that looks purpose-built for exactly this metric) is **empty** — 0 rows,
  confirming the note elsewhere tonight that it's orphaned from the scraper. There
  is no stored per-night broker-count log anywhere in the database.
- No other workflow writes to `listings_direct` or touches broker counts.
- Checked for a local/manual run path: this machine has an empty `nohup.out` in
  the repo root, but it's dated 2026-09-02 18:50 local — three days after Aug
  30/31 and with zero content, so it's not evidence of anything on the nights in
  question. No other trace of a manual run on Aug 30/31 was found.
- **New data point**: computed distinct `broker_domain` by `first_seen` date
  directly from live `listings_direct` (this reflects each broker's *first ever*
  appearance, which the `first_seen`-preservation fix should keep stable across
  re-scrapes):

  | date | distinct new broker_domains |
  |---|---|
  | 2026-08-27 | 6 |
  | 2026-08-28 | 20 |
  | 2026-08-29 | 71 |
  | **2026-08-30** | **0** |
  | **2026-08-31** | **0** |
  | 2026-09-01 | 35 |
  | 2026-09-02 | 195 |

  This independently confirms Aug 30 and Aug 31 as genuine, complete collapses —
  zero new brokers either night, consistent with the job never starting. It does
  **not** reproduce the queue's cited "4" and "8" exactly (nor exactly reproduce
  12/24/65/31/197 for the surrounding nights either — shape is similar, values
  differ), so this is not the source table either; it's a different definition of
  "producing" than whatever generated the original sequence. Bottom line
  unchanged: **the specific figures "4" and "8" have no traceable source and
  should not be quoted as measured** — every path checked either returns zero
  (consistent with "job never started") or a different number.

## 3. Is the 5h50m execution-ceiling problem still live?

Confirmed via `gh run view 33662410364 --json jobs` and annotations: job `scrape`
ran 2026-09-02 17:39:18Z → 23:30:10Z (**5h50m52s**), annotation *"The job has
exceeded the maximum execution time of 5h50m0s"*, `conclusion: cancelled`
(GitHub's label for a time-limit kill, not `failure`). This matches the earlier
finding exactly — no correction needed there beyond noting the precise
`conclusion` value.

**No run has fired yet for today (2026-09-03)** — current time is 04:07 UTC and
the workflow's schedule is `0 14 * * *` (14:00 UTC / 10am ET), so tonight's run
is still ~10 hours out at the time of this check. Cannot yet confirm whether the
ceiling problem recurs tonight; this needs a follow-up check after 14:00 UTC
today. Treat it as an open, live risk until then, not as resolved.

## Bottom line

- Aug 30/31: one GitHub billing lapse, confirmed again from fresh log pulls. No
  code fix needed; watch for the "not started" annotation as the tell if it
  recurs.
- The 4/8 producing-broker figures remain unsourced after checking every
  automated write path and the two obvious candidate tables (`crawl_runs` empty,
  `specialized_scrape.yml` never scheduled-fired). Do not quote them as measured.
- The 5h50m execution-ceiling failure on the Sept 2 run is unchanged from the
  earlier finding; today's scheduled run (14:00 UTC) hasn't happened yet as of
  this check and needs to be watched, not assumed fixed.
