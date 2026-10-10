-- 2026-10-10 public_stats: one row of coverage figures for every public page.
--
-- Why: the review found four different coverage figures across the site,
-- dataset metadata, methodology and essay (475 / 535 / ~770 / 1,700+). Each
-- described a different stage of the pipeline under the same word, and most
-- were typed by hand. Pages should read these numbers, never hard-code them.
--
-- Scope: the listings and broker figures count what dealledger.org SHOWS —
-- mv_listings_page (the homepage table, bridged listings only) and
-- broker_directory (the /brokers page). The pipeline-funnel figures come from
-- broker_sources and crawl_run. Record start date is deliberately left out
-- until the Feb 18 vs Mar 24 question is settled (lib/brokerRegistry.ts says
-- March 24, 2026; listing_sighting starts 2026-02-18).
--
-- Refresh: inside refresh_broker_registry() (cron 15:35 UTC), right after
-- broker_directory, so both pages and these figures agree on each day's data.
--
-- Also: drops broker_registry, the superseded first draft from
-- sql/2026-10-08_broker_registry.sql ("unused by the site. Safe to drop").
-- It was briefly rebuilt on listings_direct on 2026-10-10 in error. Derived
-- view only; no data is lost. Nothing in the repo or database referenced it.

drop materialized view if exists public.broker_registry;
drop materialized view if exists public.public_stats;

create materialized view public.public_stats as
select
  -- What the site shows
  (select count(*) from mv_listings_page where source = 'broker_direct')       as listings_on_site,
  (select count(*) from broker_directory where active_count > 0)              as broker_sites_on_site,
  -- Pipeline funnel (broker_sources.discovery_stage; see CLAUDE.md)
  (select count(*) from broker_sources
     where discovery_stage in ('2_listings_url_known','3_crawlable','4_producing')) as broker_sites_in_pipeline,
  (select count(*) from broker_sources where discovery_stage = '4_producing') as broker_sites_producing,
  (select count(distinct lower(regexp_replace(broker_domain, '^www\.', '')))
     from crawl_run
     where status = 'ok' and finished_at > now() - interval '7 days')          as broker_sites_crawled_7d,
  -- Freshness
  (select max(finished_at) from crawl_run where status = 'ok')                as last_successful_crawl_at,
  now()                                                                       as as_of;

create unique index public_stats_as_of_idx on public.public_stats (as_of);
grant select on public.public_stats to anon, authenticated;

create or replace function public.refresh_broker_registry()
returns void language sql security definer set search_path = public as $$
  refresh materialized view concurrently public.broker_directory;
  refresh materialized view public.public_stats;
$$;
revoke execute on function public.refresh_broker_registry() from public, anon, authenticated;
