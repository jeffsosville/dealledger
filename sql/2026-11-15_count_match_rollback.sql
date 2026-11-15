-- Rollback for 2026-11-15_count_match.sql. Nothing else reads these objects;
-- the scraper falls back automatically if the columns disappear.
begin;
drop view if exists public.v_count_match_summary;
drop view if exists public.v_count_match_scorecard;
alter table public.crawl_run drop constraint if exists crawl_run_displayed_source_check;
alter table public.crawl_run
  drop column if exists count_complete,
  drop column if exists broker_displayed_note,
  drop column if exists broker_displayed_count_source,
  drop column if exists broker_displayed_count;
commit;
