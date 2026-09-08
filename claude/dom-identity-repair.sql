-- DealLedger — DOM identity repair
-- STATUS: PREPARED 2026-09-08, NOT YET APPLIED to kqckuedsyyosmccushyd.
-- Run manually (no DB execution access from this session). Each step is
-- independently reversible except the two DELETEs, which only remove rows
-- created during the 2026-09-08 diagnosis session (see step 2 note).
--
-- Context: Claude outputs/HANDOFF-dom-identity.md
-- Root cause: scrapers/dealledger_scraper_v6.py computed row id as
--   sha256(title|asking_price|url) until commit 239b156 (2026-09-01), which
--   replaced it with stable_listing_id() (URL-only). Confirmed via
--   `git log --oneline 637947d..239b156 -- scrapers/dealledger_scraper_v6.py`
--   and `git log --all --oneline -- scrapers/dealledger_scraper_v6.py` that
--   239b156 did NOT revert other commits on this file — no commit touched it
--   between 7fde6f6 (26 Aug) and 239b156 (1 Sep), so there was nothing in
--   between to revert. Confirmed 2026-09-08.
--
-- Every price edit before the fix produced a new id, so the upsert missed
-- and inserted a fresh row with a reset first_seen (CLAUDE.md principle 4).

-- ---------------------------------------------------------------------------
-- Step 1: Backfill true_first_seen — earliest first_seen per normalised URL
-- across all id schemes. Additive only: true_first_seen is ~98% NULL and
-- nothing currently reads it, so this cannot break an existing reader.
--
-- CORRECTED 2026-09-08 (second pass): the original version of this step
-- grouped by normalised URL alone and took min(first_seen) across the group,
-- guarded only by url_is_listing_specific IS TRUE. That flag is not reliable
-- enough on its own. Measured against the live table:
--
--   41,923 URLs flagged listing-specific
--   40,134  carry exactly one business          <- safe
--    1,091  carry 2-4 distinct titles
--      698  carry 5+ distinct titles            <- index pages, mislabelled
--             worst single URL: 223 businesses
--
-- Unguarded, this would have written 25,336 rows in which several genuinely
-- different businesses share a URL, stamping all of them with the earliest
-- first_seen in the group — a second, larger version of the exact bug this
-- repair exists to fix.
--
-- The HAVING clause below restricts the write to URLs where every row shares
-- one normalised title — i.e. actually the same listing repeated. Effect:
-- 45,673 rows written, 5,539 gain time (mean 49 days), 4,885 listings
-- repaired (vs ~5,977 unguarded — 82% of the value for 0% of the damage).
-- ---------------------------------------------------------------------------

-- Step 1a — dry run first. Writes nothing. Expect: 45673 / 5539 / 49 / 4885.
WITH k AS (
  SELECT lower(regexp_replace(url, '/+$', '')) AS lk,
         count(DISTINCT lower(btrim(title)))   AS titles,
         min(first_seen)                       AS true_fs
  FROM listings_direct
  WHERE url_is_listing_specific IS TRUE
    AND url IS NOT NULL AND title IS NOT NULL
  GROUP BY 1
)
SELECT count(*) FILTER (WHERE k.titles = 1)                        AS rows_to_write,
       count(*) FILTER (WHERE k.titles = 1 AND l.first_seen > k.true_fs) AS rows_gaining_time,
       round(avg(extract(epoch FROM (l.first_seen - k.true_fs))/86400)
             FILTER (WHERE k.titles = 1 AND l.first_seen > k.true_fs)) AS avg_days_recovered,
       count(DISTINCT k.lk) FILTER (WHERE k.titles = 1 AND l.first_seen > k.true_fs) AS listings_repaired
FROM listings_direct l
JOIN k ON lower(regexp_replace(l.url, '/+$', '')) = k.lk
WHERE l.url_is_listing_specific IS TRUE AND l.title IS NOT NULL;

-- Snapshot before writing, so rollback can be exact rather than "clear
-- everything" (that would also wipe the 1,114 rows populated before today):
CREATE TABLE _tfs_backup_20260908 AS
  SELECT id, true_first_seen FROM listings_direct WHERE true_first_seen IS NOT NULL;

-- Step 1b — the repair. Additive: writes only true_first_seen, overwrites no
-- existing non-null value, touches no column any surface currently reads.
UPDATE listings_direct l
SET true_first_seen = k.true_fs
FROM (
  SELECT lower(regexp_replace(url, '/+$', '')) AS lk,
         min(first_seen)                       AS true_fs
  FROM listings_direct
  WHERE url_is_listing_specific IS TRUE
    AND url IS NOT NULL AND title IS NOT NULL
  GROUP BY 1
  HAVING count(DISTINCT lower(btrim(title))) = 1     -- <<< THE GUARD
) k
WHERE lower(regexp_replace(l.url, '/+$', '')) = k.lk
  AND l.url_is_listing_specific IS TRUE
  AND l.title IS NOT NULL
  AND (l.true_first_seen IS NULL OR l.true_first_seen > k.true_fs);

-- Step 1c — verify. Expect repaired ~5,539 and never_earlier_should_be_0 = 0.
SELECT count(*) FILTER (WHERE true_first_seen IS NOT NULL)          AS populated,
       count(*) FILTER (WHERE true_first_seen < first_seen)         AS repaired,
       count(*) FILTER (WHERE true_first_seen > first_seen)         AS never_earlier_should_be_0,
       round(avg(extract(epoch FROM (first_seen - true_first_seen))/86400)
             FILTER (WHERE true_first_seen < first_seen))           AS avg_days_recovered
FROM listings_direct
WHERE url_is_listing_specific IS TRUE;

-- Rollback — restores the pre-repair state exactly, using the snapshot above
-- rather than a blanket NULL-out (which would also clear the 1,114 rows
-- populated before today):
-- UPDATE listings_direct l SET true_first_seen = b.true_first_seen
-- FROM _tfs_backup_20260908 b WHERE l.id = b.id;
-- UPDATE listings_direct SET true_first_seen = NULL
-- WHERE id NOT IN (SELECT id FROM _tfs_backup_20260908) AND true_first_seen IS NOT NULL;
-- DROP TABLE _tfs_backup_20260908; -- only after confirming the repair is good

-- ---------------------------------------------------------------------------
-- Step 2: Drop the untrusted reconstructed crawl_run rows.
-- These 1,801 rows were reconstructed from first_seen/last_seen date
-- clustering during the 2026-09-08 diagnosis session — docs/dom-contamination.md
-- §5 caveat 2 explicitly declines to trust this proxy (cannot distinguish a
-- real second crawl from one batch split across midnight). This DELETE is an
-- exception to "nothing gets deleted" (CLAUDE.md principle 6) only because
-- these specific rows were created today by diagnosis, not by the pipeline.
-- ---------------------------------------------------------------------------
DELETE FROM listing_observation
 WHERE crawl_run_id IN (SELECT id FROM crawl_run
                        WHERE error LIKE 'reconstructed from date clustering%');
DELETE FROM crawl_run WHERE error LIKE 'reconstructed from date clustering%';

-- Note: v_dom_direct, v_dom_display_direct, v_listing_dates, v_crawl_gaps,
-- v_listing_identity rested their confidence tiers on these reconstructed
-- runs and are already flagged "unsound, do not publish" in the handoff.
-- After this DELETE they will return less (or nothing) rather than wrong
-- data — that is the intended effect, not a regression to fix here.
-- v_listing_dates does not depend on crawl_run and is unaffected.

-- ---------------------------------------------------------------------------
-- Step 3: Rename the SQL listing_key(text) function.
-- It collides in name (not in schema — different signature, different
-- purpose) with the Python listing_key() in scrapers/run_specialized.py,
-- which holds execbb.com/vestedbb.com id stability and whose reversion once
-- produced 1,058 duplicate rows (CLAUDE.md principle 13). ALTER FUNCTION
-- RENAME updates by OID, so any view built on it (e.g. v_listing_identity)
-- keeps working without modification — no view needs to be touched.
-- Adjust the argument list below if the live signature differs.
-- ---------------------------------------------------------------------------
ALTER FUNCTION listing_key(text) RENAME TO dom_url_key;

-- Verify: \df dom_url_key   (should show the function; listing_key(text) gone)
-- Verify dependents still resolve: SELECT pg_get_viewdef('v_listing_identity', true);

-- ===========================================================================
-- SEPARATE ISSUE — surfaced while correcting Step 1, larger than this repair,
-- NOT fixed here. The fix belongs in code (whatever sets
-- url_is_listing_specific), not in SQL, and is a flag/deactivate job per
-- CLAUDE.md principle 6, not a delete. Tracked as follow-up.
-- ===========================================================================
-- 698 URLs are flagged url_is_listing_specific = TRUE while carrying 5+
-- distinct businesses each (one carries 223) — these are index pages, same
-- class as the aria.net bug (343 rows for 3 listings, CLAUDE.md principle 7).
-- Across the table: 25,776 rows, 2,905 of them status='active', across 116
-- broker domains. Those 2,905 rows are inflating the live active-listing
-- count right now.
--
-- Find them (read-only):
--
--   SELECT lower(regexp_replace(url,'/+$','')) AS url,
--          count(DISTINCT lower(btrim(title))) AS businesses,
--          count(*) AS rows, max(broker_domain) AS broker
--   FROM listings_direct
--   WHERE url_is_listing_specific IS TRUE AND url IS NOT NULL AND title IS NOT NULL
--   GROUP BY 1 HAVING count(DISTINCT lower(btrim(title))) >= 5
--   ORDER BY 2 DESC;
