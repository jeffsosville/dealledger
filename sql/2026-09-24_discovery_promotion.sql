-- 2026-09-24  Applied to kqckuedsyyosmccushyd via the Supabase MCP.
--
-- Broker discovery -> crawl, end to end through broker_sources.
-- The nightly scrape now builds its list from broker_sources
-- (discovery_stage 3_crawlable / 4_producing), not from a committed CSV, so
-- discovery has to land a trusted result at 3_crawlable. It used to stop at
-- 2_listings_url_known and rely on agents/export_discovered_ok.py appending
-- to data/brokers_clean.csv, which the scrape no longer reads.

-- 1. The hand-verified junk list lived only in export_discovered_ok.py.
--    Move it to broker_block, which V6 reads at startup and
--    v_discovery_queue already excludes. cgkbusinesssales.com was blocked
--    nowhere else.
insert into public.broker_block (broker_domain, reason, updated_at)
select d, r, now()
from (values
  ('pavilionservices.com',        'WP blog fallback read as listings; buyer-side placeholders (verified by hand)'),
  ('cgkbusinesssales.com',        'WP blog fallback read as listings (verified by hand)'),
  ('sagbrokerage.com',            'site header / category pages, not listings (2026-09-08)'),
  ('businessesforsale.nebba.com', 'pagination loop / boilerplate titles (2026-09-08)'),
  ('jhcallahan.com',              'real estate, not business-for-sale (CRE_LEASE_DOMAINS)')
) v(d, r)
where not exists (select 1 from public.broker_block k
                  where regexp_replace(lower(k.broker_domain), '^www\.', '') = v.d);

update public.broker_sources b
   set discovery_stage = '0_blocked', updated_at = now()
 where discovery_stage <> '0_blocked'
   and exists (select 1 from public.broker_block k
               where regexp_replace(lower(k.broker_domain), '^www\.', '')
                   = regexp_replace(lower(b.domain), '^www\.', ''));

-- 2. Backfill: discovery already verified these (broker_discovery.status='ok',
--    listings URL known) but they never reached the crawl.
update public.broker_sources b
   set discovery_stage = '3_crawlable',
       strategy_status = 'ready',
       updated_at      = now()
  from public.broker_discovery d
 where d.domain = b.domain
   and d.status = 'ok'
   and b.discovery_stage in ('1_needs_discovery', '2_listings_url_known')
   and b.listing_url is not null;

-- 3. 3_crawlable -> 4_producing once the broker has live rows in
--    listings_direct (seen in the last 30 days). Bookkeeping only: both
--    stages are crawled. Daily after the nightly scrape.
create or replace function public.promote_producing_brokers()
returns integer
language sql
volatile
set search_path = public
as $$
  with promoted as (
    update public.broker_sources b
       set discovery_stage = '4_producing', updated_at = now()
     where b.discovery_stage in ('2_listings_url_known', '3_crawlable')
       and exists (
         select 1 from public.listings_direct l
          where regexp_replace(lower(l.broker_domain), '^www\.', '')
              = regexp_replace(lower(b.domain), '^www\.', '')
            and l.last_seen > now() - interval '30 days')
    returning 1
  )
  select count(*)::int from promoted
$$;

revoke all on function public.promote_producing_brokers() from public, anon, authenticated;
grant execute on function public.promote_producing_brokers() to service_role;

select cron.schedule('promote-producing-brokers', '17 21 * * *',
                     $$select public.promote_producing_brokers()$$);
