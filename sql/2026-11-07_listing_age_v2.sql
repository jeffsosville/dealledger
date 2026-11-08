-- 2026-11-07  Listing-age evidence hierarchy. Record of what was applied to
-- kqckuedsyyosmccushyd (already live; do not re-run blindly).
--
-- Public age is shown from the strongest evidence available, never from the
-- seeded / backdated listings.first_seen:
--   window   "Listed between A and B"        complete crawls of the broker saw it appear (≤45 days wide)
--   observed "Observed by DealLedger since"  first dated sighting in our crawl / committed snapshots
--   in_db    "In DealLedger since"           date the row was first written to listings_direct
--   (none)   no age shown
-- Level 1 (broker-reported publication date) and level 3 (archive capture) slot in
-- ahead of these when captured.
--
-- Objects
--   refresh_listing_dates_v2(45) -> listing_dates_v2   (first_sighting, first_insert,
--                                    seed_first_seen, listed_after, window_days kept apart)
--   v_listing_age                -> listing_age_v2     (one row per page listing_number)
--   mv_listings_page_v2          homepage / listing pages (anon select)
--   mv_seo_listings_v2           SEO pages / browse CSV  (anon select)
--   refresh_listings_page_v2()   rebuilds all of the above; pg_cron refresh-homepage-v2 15:32 UTC
-- The legacy mv_listings_page / mv_seo_listings / listing_dates keep refreshing until
-- nothing reads them (public_stats and scripts/export_public_listings.py still do).

create table if not exists public.listing_dates_v2 (like public.listing_dates including all);
alter table public.listing_dates_v2
  add column if not exists seed_first_seen date, add column if not exists window_days int,
  add column if not exists first_sighting date, add column if not exists first_insert date;
alter table public.listing_dates_v2 enable row level security;

-- refresh_listing_dates_v2(p_max_window int): same identity rules as
-- refresh_listing_dates(); first_seen = min(crawl / archive / observation sightings,
-- listings_direct.created_at); seed_* sightings never count; window up to p_max_window
-- days, refused when a seed date predates the complete crawl that missed the listing.
-- (Full body: \sf public.refresh_listing_dates_v2)

create or replace view public.v_listing_age as
with x as (
  select distinct on (a.synth) a.*
  from (select (900000000 + (('x'||substr(md5(r.row_id),1,7))::bit(28))::bigint) synth, ld.*
        from listing_dates_v2 ld cross join lateral unnest(ld.row_ids) r(row_id)) a
  order by a.synth, a.first_seen)
select synth as listing_number,
  case when listed_on_basis = 'observed' then 'window'
       when first_sighting is not null and (first_insert is null or first_sighting <= first_insert) then 'observed'
       when first_insert is not null then 'in_db' end as age_basis,
  case when listed_on_basis = 'observed' then first_seen
       when first_sighting is not null and (first_insert is null or first_sighting <= first_insert) then first_sighting
       else first_insert end as age_date,
  case when listed_on_basis = 'observed' then listed_after end as listed_after,
  first_sighting, first_insert
from x;

create table if not exists public.listing_age_v2 (
  listing_number bigint primary key, age_basis text, age_date date, listed_after date,
  first_sighting date, first_insert date, built_at timestamptz default now());
alter table public.listing_age_v2 enable row level security;

create materialized view if not exists public.mv_listings_page_v2 as
with base as (
  select l.listing_number, l.header as title, l.slug, l.state, l.price, l.url as source_url, l.source,
         l.listing_views, coalesce(l.relisted, false) as relisted,
         a.age_basis, a.age_date, a.listed_after
  from listings l
  left join listing_age_v2 a on a.listing_number = l.listing_number
  where l.is_active and not is_publicly_hidden(l.header, l.url)
)
select listing_number, title, coalesce(slug, listing_number::text) as slug, state, price, source_url, source,
  listing_views, relisted,
  age_basis as dom_basis,
  age_date as estimated_listed_date,
  listed_after,
  case when age_date is not null then current_date - age_date end as dom_days_eff,
  case when age_basis = 'window' then current_date - listed_after end as dom_days_max,
  case age_basis
    when 'window'   then 'Listed between ' || to_char(listed_after, 'Mon FMDD, YYYY') || ' and ' || to_char(age_date, 'Mon FMDD, YYYY')
    when 'observed' then 'Observed by DealLedger since ' || to_char(age_date, 'Mon FMDD, YYYY')
    when 'in_db'    then 'In DealLedger since ' || to_char(age_date, 'Mon FMDD, YYYY')
  end as dom_display,
  case age_basis
    when 'window'   then 'Appearance window from complete crawls of the broker''s site'
    when 'observed' then 'First dated sighting in a DealLedger crawl'
    when 'in_db'    then 'Date the record was first written to the DealLedger database'
  end as age_source,
  lower(coalesce(title, '')) as title_lc
from base
with no data;
-- indexes: pk (listing_number), estimated_listed_date desc, state, price, dom_days_eff, dom_basis
grant select on public.mv_listings_page_v2 to anon, authenticated;

-- mv_seo_listings_v2: the mv_seo_listings definition reading mv_listings_page_v2,
-- plus listed_after, dom_days_max, age_source. Indexes as mv_seo_listings. anon select.

-- refresh_listings_page_v2(): refresh_listing_dates_v2(45) -> listing_age_v2 ->
-- mv_listings_page_v2 (asserts >=1000 rows and >=90% with an age basis) -> mv_seo_listings_v2.
-- select cron.schedule('refresh-homepage-v2', '32 15 * * *',
--   $$set statement_timeout='10min'; select public.refresh_listings_page_v2()$$);
