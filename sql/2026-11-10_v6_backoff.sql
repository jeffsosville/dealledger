-- 2026-11-10  Weekly backoff for brokers V6 keeps failing on.
--
-- In the 7 days to Nov 10, 1,544 of 5,658 V6 runs (27%) went to 297 brokers
-- that never once succeeded. ORDER=weekly's "fill the budget" step appends every
-- never-scraped broker and the whole producer tail with no failure cooldown, so
-- a broken broker is retried every night.
--
-- v6_backoff_domains() returns the bare domains to skip tonight:
--   3+ failed V6 runs in the last 21 days,
--   no ok V6 run in the last 14 days,
--   no published active listings on the site (brokers holding live listings
--     keep their nightly crawl - the removal rule depends on it),
--   already attempted in the last 6 days.
-- So each such broker is still tried about once a week, never dropped.
-- 248 brokers qualify today (~1,100 runs/week); weekly retries cut that to ~250.
--
-- Read-only. scrapers/dealledger_scraper_v6.py calls it; if the function is
-- missing or errors, the scraper logs a warning and crawls as before.
-- Status, stage and listings are never changed by this.
--
-- Rollback: drop function public.v6_backoff_domains(int);

begin;

create or replace function public.v6_backoff_domains(p_retry_days int default 7)
returns table (bare_domain text)
language sql
stable
security definer
set search_path = public
as $$
  with r as (
    select regexp_replace(lower(broker_domain), '^www\.', '') as d,
           max(started_at) as last_try,
           max(started_at) filter (where status = 'ok') as last_ok,
           count(*) filter (where status = 'failed') as fails
    from crawl_run
    where source = 'v6'
      and started_at > now() - interval '21 days'
    group by 1
  ), live as (
    select distinct regexp_replace(lower(broker_domain), '^www\.', '') as d
    from listings_direct
    where status = 'active' and published
  )
  select r.d
  from r
  where r.fails >= 3
    and (r.last_ok is null or r.last_ok < now() - interval '14 days')
    and r.last_try > now() - make_interval(days => greatest(p_retry_days - 1, 1))
    and not exists (select 1 from live where live.d = r.d)
$$;

revoke all on function public.v6_backoff_domains(int) from public, anon, authenticated;
grant execute on function public.v6_backoff_domains(int) to service_role;

-- Test: expect ~197 domains to skip tonight.
select count(*) as skip_tonight from public.v6_backoff_domains();

commit;
