-- 2026-10-08 Clean location + category layer for the SEO pages.
--
-- The public record (mv_listings_page) carries whatever state text each broker
-- site printed: "NY", "New York", "Las Vegas", or nothing. Geo pages built on
-- that leak listings. This file adds:
--
--   dl_state_code(text)  -> two-letter code from a code, a full name, or a
--                           trailing ", XX" in a location string; else null
--   mv_seo_listings      -> one row per public listing with state_code,
--                           vertical (from listings_direct), price, cash flow
--                           and the same DOM the homepage shows
--   mv_seo_page_stats    -> per-page stats (count, brokers, new in 30 days, median price,
--                           median price/cash-flow) for every state, vertical
--                           and vertical x state with enough listings
--
-- Source columns are not rewritten (scrapers would overwrite them); the clean
-- values live only here. Refreshed daily by refresh_seo_listings() at 15:45
-- UTC, after the broker directory refresh.
--
-- APPLIED to production on 2026-10-08.

create or replace function public.dl_state_code(s text)
returns text language sql stable as $$
  with t as (select upper(btrim(coalesce(s, ''), ' .,')) u)
  select coalesce(
    (select code from public.state_map, t where code = t.u limit 1),
    (select code from public.state_map, t where upper(raw) = t.u limit 1),
    (select code from public.state_map, t
      where t.u ~ (',\s*' || code || '(\s+\d{5}(-\d{4})?)?$') limit 1),
    (select code from public.state_map, t
      where t.u ~ (',\s*' || upper(raw) || '(\s+\d{5}(-\d{4})?)?$') limit 1)
  )
$$;

create materialized view public.mv_seo_listings as
with d as (
  select distinct on (url) url, vertical, state_norm, state, location_raw, city, cash_flow, revenue
  from public.listings_direct
  where url is not null
  order by url, last_seen desc nulls last
)
select m.listing_number,
       m.title,
       m.source_url,
       public.dl_bare_host(m.source_url)                       as broker_domain,
       coalesce(public.dl_state_code(m.state),
                public.dl_state_code(d.state_norm),
                public.dl_state_code(d.state),
                public.dl_state_code(d.location_raw))          as state_code,
       nullif(btrim(coalesce(d.city, l.city)), '')             as city,
       coalesce(nullif(d.vertical, ''), 'other')               as vertical,
       m.price,
       coalesce(l.cash_flow, d.cash_flow::bigint)              as cash_flow,
       d.revenue::bigint                                       as revenue,
       m.dom_days_eff,
       m.dom_basis,
       m.dom_display,
       m.estimated_listed_date,
       m.relisted
from public.mv_listings_page m
join public.listings l using (listing_number)
left join d on d.url = m.source_url;

create unique index mv_seo_listings_pk on public.mv_seo_listings (listing_number);
create index mv_seo_listings_state on public.mv_seo_listings (state_code);
create index mv_seo_listings_vertical on public.mv_seo_listings (vertical, state_code);

grant select on public.mv_seo_listings to anon, authenticated;

-- Page-level stats. scope: 'state' | 'vertical' | 'vertical_state'.
-- No median DOM here: ~2,400 "observed" listings share one first-seen date
-- (the bulk broker onboarding, ~84 days before 2026-10-08), which pins every
-- median at 84. Pages show "new in the last 30 days" instead, and per-listing
-- DOM with floors labeled. (An earlier draft, mv_seo_stats, had median_dom;
-- it is unused and can be dropped: drop materialized view public.mv_seo_stats;)
create materialized view public.mv_seo_page_stats as
with base as (
  select *,
         case when cash_flow > 0 and price > 0 then price::numeric / cash_flow end as multiple
  from public.mv_seo_listings
),
g as (
  select 'state'::text as scope, null::text as page_vertical, state_code as page_state, b.* from base b where state_code is not null
  union all
  select 'vertical', vertical, null, b.* from base b where vertical <> 'other'
  union all
  select 'vertical_state', vertical, state_code, b.* from base b where vertical <> 'other' and state_code is not null
)
select scope,
       page_vertical,
       page_state,
       count(*)                                                                   as listings,
       count(distinct broker_domain)                                              as brokers,
       percentile_cont(0.5) within group (order by price) filter (where price > 0)            as median_price,
       percentile_cont(0.5) within group (order by cash_flow) filter (where cash_flow > 0)    as median_cash_flow,
       percentile_cont(0.5) within group (order by multiple) filter (where multiple between 0.3 and 20) as median_multiple,
       count(*) filter (where dom_basis = 'observed' and dom_days_eff between 0 and 30)       as new_30d,
       count(*) filter (where relisted)                                                        as relisted,
       count(*) filter (where cash_flow > 0)                                                   as with_cash_flow
from g
group by 1, 2, 3;

create unique index mv_seo_page_stats_pk on public.mv_seo_page_stats (scope, coalesce(page_vertical, ''), coalesce(page_state, ''));
grant select on public.mv_seo_page_stats to anon, authenticated;

create or replace function public.refresh_seo_listings()
returns void language plpgsql security definer set search_path = public as $$
begin
  refresh materialized view concurrently public.mv_seo_listings;
  refresh materialized view concurrently public.mv_seo_page_stats;
end $$;

select cron.schedule('refresh_seo_listings', '45 15 * * *', 'select public.refresh_seo_listings()');
