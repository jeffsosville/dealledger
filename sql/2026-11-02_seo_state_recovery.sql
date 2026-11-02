-- 2026-11-02 Recover missing states on the SEO layer, add an as-of date.
--
-- About 4,500 of ~21,500 public listings had no state_code, so they never
-- reached a state or state x type page. Two conservative fallbacks, used only
-- when every printed location field is empty or unparseable:
--
--   title     a full state name in the title ("... for Sale in Lansing Michigan"),
--             or a ", XX" postal code ("Bar & Grill Near Springfield, MO").
--             "Washington" is skipped when followed by DC.
--   broker    the broker's home state, when at least 90% of that broker's
--             located listings (and at least 5) are in one state.
--
-- state_source records which rule set the state: reported | title | broker.
-- mv_seo_page_stats gains refreshed_at so pages can print "Data as of".
--
-- Replaces the 2026-10-08 views. Also drops the unused mv_seo_stats draft.
-- APPLIED to production on 2026-11-02.

drop materialized view if exists public.mv_seo_stats;
drop materialized view if exists public.mv_seo_page_stats;
drop materialized view if exists public.mv_seo_listings;

create or replace function public.dl_state_from_title(t text)
returns text language sql stable as $$
  select coalesce(
    (select sm.code from public.state_map sm
      where length(sm.raw) > 2
        and t ~* ('\m' || sm.raw || '\M')
        and not (sm.raw = 'Washington' and t ~* 'washington,?\s*d\.?\s*c\.?\M')
      order by length(sm.raw) desc
      limit 1),
    (select sm.code from public.state_map sm
      where t ~ (',\s*' || sm.code || '(\s+\d{5}(-\d{4})?)?\M')
      limit 1)
  )
$$;

create materialized view public.mv_seo_listings as
with d as (
  select distinct on (url) url, vertical, state_norm, state, location_raw, city, cash_flow, revenue
  from public.listings_direct
  where url is not null
  order by url, last_seen desc nulls last
),
base as (
  select m.listing_number,
         m.title,
         m.source_url,
         public.dl_bare_host(m.source_url)                       as broker_domain,
         coalesce(public.dl_state_code(m.state),
                  public.dl_state_code(d.state_norm),
                  public.dl_state_code(d.state),
                  public.dl_state_code(d.location_raw))          as reported_state,
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
  left join d on d.url = m.source_url
),
broker_home as (
  select broker_domain,
         (array_agg(reported_state order by n desc))[1] as home_state,
         max(n)::numeric / sum(n)                       as share,
         sum(n)                                         as located
  from (select broker_domain, reported_state, count(*) n
        from base where reported_state is not null
        group by 1, 2) x
  group by 1
),
resolved as (
  select b.*,
         case when b.reported_state is null then public.dl_state_from_title(b.title) end as title_state,
         case when h.share >= 0.9 and h.located >= 5 then h.home_state end              as broker_state
  from base b
  left join broker_home h using (broker_domain)
)
select listing_number,
       title,
       source_url,
       broker_domain,
       coalesce(reported_state, title_state, broker_state) as state_code,
       case when reported_state is not null then 'reported'
            when title_state    is not null then 'title'
            when broker_state   is not null then 'broker' end as state_source,
       city,
       vertical,
       price,
       cash_flow,
       revenue,
       dom_days_eff,
       dom_basis,
       dom_display,
       estimated_listed_date,
       relisted
from resolved;

create unique index mv_seo_listings_pk on public.mv_seo_listings (listing_number);
create index mv_seo_listings_state on public.mv_seo_listings (state_code);
create index mv_seo_listings_vertical on public.mv_seo_listings (vertical, state_code);
grant select on public.mv_seo_listings to anon, authenticated;

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
       count(*) filter (where cash_flow > 0)                                                   as with_cash_flow,
       now()                                                                                   as refreshed_at
from g
group by 1, 2, 3;

create unique index mv_seo_page_stats_pk on public.mv_seo_page_stats (scope, coalesce(page_vertical, ''), coalesce(page_state, ''));
grant select on public.mv_seo_page_stats to anon, authenticated;

-- refresh_seo_listings() and its 15:45 UTC cron job are unchanged.
