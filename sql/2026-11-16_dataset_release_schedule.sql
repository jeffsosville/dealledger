-- Monthly dataset release (GitHub release -> Zenodo DOI, Hugging Face, Kaggle).
-- Run ONLY after gh-export-public is scheduled and has passed at least once
-- (sql/2026-11-09_export_public_schedule.sql). 16:40 UTC on the 1st, after the
-- 15:55 export. The workflow refuses to publish unless today's export passed.
select cron.schedule('gh-dataset-release', '40 16 1 * *',
  $$select public.dispatch_workflow('dataset_release.yml')$$);
select jobname, schedule, active from cron.job where jobname = 'gh-dataset-release';
-- Rollback: select cron.unschedule('gh-dataset-release');
