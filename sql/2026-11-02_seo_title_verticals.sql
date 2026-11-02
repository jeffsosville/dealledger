-- 2026-11-02 Classify "other" listings by title; split car washes out of automotive.
--
-- ~3,800 public listings (18%) carried vertical 'other' and reached no type
-- page. dl_vertical_from_title() assigns a vertical from title keywords, in
-- priority order. It applies only where the scraper gave no vertical or
-- 'other', except car washes, which always become 'car_wash'.
--
-- New verticals: car_wash, salon_spa, gas_station, childcare, pet,
-- liquor_store, accounting, fitness, sign_print, trucking.
--
-- vertical_source records which rule set it: scraper | title.
-- APPLIED to production on 2026-11-02.

create or replace function public.dl_vertical_from_title(t text)
returns text language sql immutable as $$
  select case
    when t is null then null
    when t ~* 'car ?wash' then 'car_wash'
    when t ~* '\matm (route|portfolio|business)|\matms\M' then 'atm'
    when t ~* 'pest control|extermina|termite|mosquito control' then 'pest_control'
    when t ~* 'laundromat|coin laundry|wash ?(&|and) ?fold' then 'laundromat'
    when t ~* '\mvending|micro.?market' then 'vending'
    when t ~* 'janitorial|commercial cleaning|cleaning (service|company|business|franchise)|maid service|carpet clean|window clean' then 'cleaning'
    when t ~* '\mpets?\M|grooming|kennel|veterinar|animal hospital|dog (daycare|boarding|training)' then 'pet'
    when t ~* 'adult day|home (care|health)|senior care|assisted living' then 'healthcare'
    when t ~* 'day ?care|child ?care|preschool|early learning|montessori|learning center' then 'childcare'
    when t ~* '\m(hair )?salon|barber|\mspa\M|med ?spa|medical spa|\mnail|\mlash|waxing|massage|tanning' then 'salon_spa'
    when t ~* 'gas station|c-store|convenience store|fuel station|truck stop' then 'gas_station'
    when t ~* '\mliquor|wine (shop|store)|package store|bottle shop|beer (store|distributor)' then 'liquor_store'
    when t ~* '\mcpa\M|accounting|bookkeeping|\mtax (prep|practice|preparation|service|resolution)' then 'accounting'
    when t ~* '\mgym\M|fitness|yoga|pilates|crossfit|martial arts|\mdojo\M|boxing studio' then 'fitness'
    when t ~* '\msign (shop|company|business)|\msigns\M|\mprinting|print shop|screen ?print|embroidery' then 'sign_print'
    when t ~* 'trucking|freight|\mltl\M|logistics' then 'trucking'
    when t ~* '\mhvac\M|heating (and|&) (air|cooling)|air conditioning' then 'hvac'
    when t ~* 'plumbing|plumber' then 'plumbing'
    when t ~* 'landscap|lawn (care|service)|tree service' then 'landscaping'
    when t ~* 'roofing' then 'roofing'
    when t ~* 'pool (service|route|cleaning)' then 'pool_service'
    when t ~* 'electrical contractor|electrician' then 'electrical'
    when t ~* 'e-?commerce|amazon|\mfba\M|shopify|\msaas\M' then 'technology'
  end
$$;

drop materialized view if exists public.mv_seo_page_stats;
drop materialized view if exists public.mv_seo_listings;


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
         case when m.title ~* 'car ?wash' then 'car_wash'
              when coalesce(nullif(d.vertical, ''), 'other') = 'other'
                then coalesce(public.dl_vertical_from_title(m.title), 'other')
              else d.vertical end                                as vertical,
         case when m.title ~* 'car ?wash' and coalesce(d.vertical, '') <> 'car_wash' then 'title'
              when coalesce(nullif(d.vertical, ''), 'other') = 'other'
                   and public.dl_vertical_from_title(m.title) is not null then 'title'
              else 'scraper' end                                 as vertical_source,
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
       vertical_source,
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

create unique index mv_seo_page_stats_pk on public.mv_seo_page_stats (scope, page_vertical, page_state) nulls not distinct;
grant select on public.mv_seo_page_stats to anon, authenticated;

