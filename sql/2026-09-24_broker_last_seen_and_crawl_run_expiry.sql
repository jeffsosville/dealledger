-- 2026-09-24  Applied to kqckuedsyyosmccushyd via the Supabase MCP.
--
-- 1. broker_last_seen(): per-broker staleness, grouped server-side.
--    Replaces the client-side page-through of all of listings_direct
--    (77k rows, 78 round-trips) in dealledger_scraper_v6._order_by_staleness.
--    www. is stripped here; the scraper strips it on its side too, so
--    "www.x.com" in the CSV and "x.com" in listings_direct meet.
create or replace function public.broker_last_seen()
returns table (bare_domain text, last_seen timestamptz, n bigint)
language sql
stable
set search_path = public
as $$
  select regexp_replace(lower(btrim(broker_domain)), '^www\.', '') as bare_domain,
         max(last_seen)                                            as last_seen,
         count(*)                                                  as n
  from public.listings_direct
  where broker_domain is not null and btrim(broker_domain) <> ''
  group by 1
  order by 1
$$;

revoke all on function public.broker_last_seen() from public, anon, authenticated;
grant execute on function public.broker_last_seen() to service_role;

-- 2. expire_stale_crawl_runs(): a crawl_run row still 'running' after 6h is a
--    process that died without closing it. Mark it failed/timeout so it stops
--    counting as a zombie in v_coverage_scorecard.zombie_runs.
create or replace function public.expire_stale_crawl_runs(max_age interval default interval '6 hours')
returns integer
language sql
volatile
set search_path = public
as $$
  with expired as (
    update public.crawl_run
       set status      = 'failed',
           error       = coalesce(error, 'timeout'),
           finished_at = coalesce(finished_at, now())
     where status = 'running'
       and started_at < now() - max_age
    returning 1
  )
  select count(*)::int from expired
$$;

revoke all on function public.expire_stale_crawl_runs(interval) from public, anon, authenticated;
grant execute on function public.expire_stale_crawl_runs(interval) to service_role;

-- Hourly sweep, off minute 0 like the other crons.
select cron.schedule('expire-stale-crawl-runs', '37 * * * *',
                     $$select public.expire_stale_crawl_runs()$$);
