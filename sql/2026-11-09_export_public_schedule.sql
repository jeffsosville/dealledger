-- Restore the daily public export (approved by Jeff 2026-11-09, on condition the
-- export is verified before it is published — see scripts/verify_public_export.py).
--
-- Run ONLY after:
--   1. the export patch is on main, and
--   2. one manual run passed:  select public.dispatch_workflow('export_public.yml');
--      then check the Actions run: "Verify (gate)" green, and
--      data/public/verification.json on main shows "passed": true.
--
-- 15:55 UTC: after removal-rule-daily (15:10), refresh-homepage-v2 (15:32, builds
-- listing_age_v2) and refresh_seo_listings (15:45).

select cron.schedule('gh-export-public', '55 15 * * *',
  $$select public.dispatch_workflow('export_public.yml')$$);

-- Check
select jobname, schedule, command, active from cron.job where jobname = 'gh-export-public';

-- Rollback
-- select cron.unschedule('gh-export-public');
