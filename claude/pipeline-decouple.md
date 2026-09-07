# dealledger_pipeline.yml — decouple proposal + backlog quantification

Proposed workflow: `claude/dealledger_pipeline.proposed.yml` (not applied, not pushed).

## What changed vs. the current file

Current file: `.github/workflows/dealledger_pipeline.yml`.

1. **`schedule:` restored, on its own trigger, not the disabled BBS one.**
   Old: the only `schedule:` block was commented out entirely (`# schedule:` /
   `# - cron: '0 12 * * *'`), so the whole workflow only ran on
   `workflow_dispatch` — nobody was triggering it, which is why nothing
   downstream has run since 2026-07-17. New: `schedule: - cron: '0 7 * * *'`
   (07:00 UTC daily), ahead of `specialized_scrape.yml` (08:00 UTC) and
   `broker_scrape.yml` (14:00 UTC), so scoring/relist/sync reflect a full prior
   day before the next scrape cycle starts landing rows.

2. **`bbs_scrape` gated to manual-only, nothing waits on it.**
   Added `if: github.event_name == 'workflow_dispatch'` to `bbs_scrape` so it
   never runs on the new schedule (Akamai blocks it — CLAUDE.md, permanent).
   `quality_score` no longer has `needs: bbs_scrape` at all. On the old file,
   even if the schedule were simply re-enabled without this change,
   `quality_score` would still declare `needs: bbs_scrape`, and since
   `bbs_scrape` will keep failing (Akamai), everything downstream would keep
   being skipped by GitHub Actions' default `needs` behavior. Removing the
   `needs:` edge is the actual fix, not just re-enabling the schedule.

3. **Assertion step added to each of the three surviving jobs**, each one
   checking a real, currently-swallowed condition:
   - `quality_score`: greps its own log for `Scoring N listings...` and
     `Scores written: X ok, Y errors`, fails the job if fetched≠written or
     errors>0. Today `scrapers/quality_scorer.py:upsert_scores()` logs errors
     but never calls `sys.exit(1)` on them, so a partial write currently shows
     green.
   - `relist_refresh`: greps for the `Count mismatch` line that
     `scrapers/relist_refresh.py` already computes
     (`matched_total != already_flagged + newly_flagged`) but only warns
     about — today `main()` returns `0` even when it detects that mismatch.
   - `vertical_sync`: greps for `Upsert error` lines that
     `scrapers/vertical_sync.py:sync_vertical()` already logs per failed
     batch but never fails on — the script only ever calls `sys.exit(1)` for
     a missing `SUPABASE_SERVICE_KEY`.

   None of these required editing the scripts tonight — each condition was
   already being computed and logged, just not asserted on. The comments in
   the proposed YAML note the minimal script-side fix (make each condition
   `sys.exit()` for real) that would make the workflow-level grep redundant;
   that's a follow-up, not done here.

## Backlog quantification

Queried live via Supabase REST (`SUPABASE_URL`/`SUPABASE_SERVICE_KEY` from
`.env`), read-only, against project `kqckuedsyyosmccushyd`. No raw SQL access
available (no DB connection string in this environment, no `cron`/`exec_sql`
RPC exposed, `supabase db dump` needs Docker which isn't installed here) — all
numbers below came from paginated REST queries against `listings`.

**Confirmed via `gh run list --workflow=dealledger_pipeline.yml`:** the
workflow's last *success* was `2026-07-16T14:01:18Z` (all 4 jobs green,
Quality Scoring ran 14:37:59–14:38:33 UTC that day). Every scheduled run after
that failed until the schedule was disabled outright on 2026-07-17. So
`scrapers/quality_scorer.py` has not executed since **2026-07-16**, not just
"since July 16" as a round number — that's the literal last timestamp.

**Tier distribution, active rows only (`is_active=true`, n=33,076):**

| tier | rows | % of active |
|---|---:|---:|
| Verified | 95 | 0.3% |
| Likely Real | 353 | 1.1% |
| Unverified | 29,359 | 88.8% |
| Likely Junk | 3,235 | 9.8% |
| *(null tier)* | 34 | 0.1% |

**Rows genuinely never scored** (`quality_score is null`, active): **2,109**.

### The queue's framing doesn't hold — reporting the correction, not the assumption

The queue asked "how many rows are `Unverified` purely because the scorer has
not run since July 16." That framing conflates two different things, and the
data says they're different:

- `Unverified` is not "not yet scored." It's a real score band —
  `scrapers/quality_scorer.py` buckets score 40–59 as `Unverified`,
  60–79 as `Likely Real`, 80+ as `Verified`, and <40 as `Likely Junk`
  (`scrapers/quality_scorer.py:162-165`). 29,359 active rows carrying that
  tier were *scored into it* at some point — they are not sitting in limbo.
- The rows that are actually unscored — `quality_score is null` — number
  **2,109**, and every single one has `first_seen <= 2026-07-15`. None of
  them have `first_seen` after the outage started. So this set is not "stuck
  since the scorer died" either; these rows were apparently excluded from
  *every* scoring run, including ones before July 16 — a separate,
  pre-existing gap (possibly the scorer's fetch query filters something these
  966 `broker_direct` + rest `bizbuysell`-sourced rows lack — not
  investigated further here, out of scope for this item).

**More important, unrequested finding:** rows in `listings` with
`source='broker_direct'` and `first_seen` as recent as **2026-09-02** (today)
already carry a non-null `quality_score`/`quality_tier` — despite the scorer
not having run since July 16. Neither `scrapers/broker_scraper.py` nor
`scrapers/dealledger_scraper_v6.py` (the writers of `broker_direct` rows into
`listings`) set `quality_score` themselves — grepped both, no hits. The
only explanation consistent with the evidence is that these are **stale
scores carried over by upsert-on-conflict semantics**: when an existing row
is re-scraped, the upsert doesn't touch `quality_score`/`quality_tier` at
all, so whatever value was last written (before July 16) survives untouched,
while `first_seen`/`last_seen` get bumped by the re-scrape. This repo has
just fixed an analogous bug twice this week on `listings_direct`
(`first_seen` baseline commit `bccb714`, "Fix upsert resetting status on
every re-scrape" commit `9cba758`) — this may be the same shape of problem
on the separate `listings` table's upsert path, unconfirmed and **not
investigated further tonight**, flagged for a follow-up session.

**Practical consequence:** there is no reliable way, with the columns that
exist today, to say how many active rows' current tier reflects their
*current* content versus a score computed against a title/price/category
that has since changed. `listings` has no `quality_scored_at` (or similar)
timestamp. **Recommended follow-up (not done tonight):** add a
`quality_scored_at timestamptz` column, set by `upsert_scores()` on every
write, so this question has a real answer instead of an inferred one next
time.

**Best available answer to "what will the tier distribution look like after a
full scoring pass":** unknown in a rigorous sense, for the reason above — most
of today's `Unverified`/`Likely Real`/`Verified`/`Likely Junk` labels on
recently-active rows are carryover from a scoring pass against older content,
not a live reflection of what's on the site now. The only number that can be
stated with confidence is the floor: at least **2,109** active rows will get
a tier for the first time, plus however many of the 17,874 active rows with
`first_seen > 2026-07-15` turn out, on inspection, to have been rescored
copies of pre-existing rows rather than genuinely new ones (not distinguished
here — see above).
