-- 2026-11-16  eXp Commercial syndicated listings on crebbgroup.com
--
-- On 2026-11-16 the broker scraper picked up 96 listings on
-- crebbgroup.com/opportunities/business-brokerage-listings/expb-<id>/.
-- CREBB Group is an eXp Commercial team; the expb-<id> pages are eXp's
-- network-wide business inventory (id = expcommercial.com listing id), not
-- CREBB's own deals. All 96 entered as "Observed by DealLedger since Nov 16"
-- and took over the homepage.
--
-- 1. listing_sighting_evidence: an earlier dated sighting of the SAME listing
--    by a DealLedger crawl under another URL. v_listing_age takes the earlier
--    date as first_sighting, and a sighting older than a listing's appearance
--    window voids the window (the window only shows when this URL appeared).
--      exp_corporate   same eXp id seen on expcommercial.com/listings (Oct 2026)
--      bizbuysell      same state + same asking price + title similarity >= 0.5
--                      against our BizBuySell crawl (first_seen = our crawl date,
--                      never BizBuySell's own listed date)
-- 2. listing_public_holds: URLs kept off public pages until the eXp corporate
--    scraper attributes them to the listing advisor. Release = delete the row
--    and run select public.refresh_listings_page_v2().
-- 3. is_publicly_hidden() checks the hold table (now STABLE: it reads a table).

create table if not exists public.listing_sighting_evidence (
  listing_number bigint primary key,
  sighting_date  date not null,
  evidence       text not null check (evidence in ('exp_corporate', 'bizbuysell', 'exp_corporate+bizbuysell')),
  source_ref     text,
  note           text,
  added_at       timestamptz not null default now()
);
alter table public.listing_sighting_evidence enable row level security;

create table if not exists public.listing_public_holds (
  url           text primary key,
  reason        text not null,
  held_at       timestamptz not null default now(),
  release_when  text
);
alter table public.listing_public_holds enable row level security;

-- Evidence for the crebbgroup expb- listings
with c as (
  select listing_number, header, state, price, substring(url from 'expb-([0-9]+)') expid
  from public.listings where url ~* 'crebbgroup\.com/.*/expb-[0-9]+'
),
corp as (
  select distinct on (substring(url from '-([0-9]+)/?$'))
         substring(url from '-([0-9]+)/?$') expid, first_seen::date d, url
  from public.listings where url ilike '%expcommercial.com/listings/%'
  order by substring(url from '-([0-9]+)/?$'), first_seen
),
bbs as (
  select distinct on (c.listing_number) c.listing_number, b.first_seen::date d, b.listing_number bbs_no
  from c join public.listings b
    on b.source = 'bizbuysell' and b.state = c.state and b.price = c.price and c.price is not null
   and similarity(lower(c.header), lower(b.header)) >= 0.5
  order by c.listing_number, b.first_seen
),
ev as (
  select c.listing_number, corp.d cd, corp.url curl, bbs.d bd, bbs.bbs_no
  from c left join corp using (expid) left join bbs using (listing_number)
  where corp.d is not null or bbs.d is not null
)
insert into public.listing_sighting_evidence (listing_number, sighting_date, evidence, source_ref, note)
select listing_number,
       least(cd, bd),
       case when cd is not null and bd is not null then 'exp_corporate+bizbuysell'
            when cd is not null then 'exp_corporate' else 'bizbuysell' end,
       concat_ws(' | ', curl, case when bbs_no is not null then 'bizbuysell #' || bbs_no end),
       'eXp Commercial syndicated listing on crebbgroup.com; see sql/2026-11-16_syndicated_exp_anchors.sql'
from ev
on conflict (listing_number) do nothing;

-- Hold the rest until the eXp corporate scraper lands
insert into public.listing_public_holds (url, reason, release_when)
select l.url,
       'eXp Commercial syndicated listing on crebbgroup.com with no earlier sighting; first_seen would read as new',
       'eXp corporate scraper attributes it to the listing advisor with a dated sighting'
from public.listings l
where l.url ~* 'crebbgroup\.com/.*/expb-[0-9]+'
  and not exists (select 1 from public.listing_sighting_evidence e where e.listing_number = l.listing_number)
on conflict (url) do nothing;

create or replace function public.is_publicly_hidden(t text, u text)
 returns boolean
 language sql
 stable
 set search_path to 'public'
as $function$
  select coalesce(public.is_listing_junk(t,u) or public.is_real_estate_junk(t) or public.is_cre_listing(t,u)
                  or exists (select 1 from public.listing_public_holds h where h.url = u), false)
$function$;

create or replace view public.v_listing_age as
with x as (
  select distinct on (a.synth) a.synth, a.listing_key, a.broker_domain, a.row_ids, a.first_seen, a.last_seen,
         a.listed_on_basis, a.listed_after, a.status, a.published, a.ended_on, a.built_at, a.seed_first_seen,
         a.window_days, a.first_sighting, a.first_insert
  from (select 900000000 + (('x'::text || substr(md5(r.row_id), 1, 7)))::bit(28)::bigint as synth,
               ld.listing_key, ld.broker_domain, ld.row_ids, ld.first_seen, ld.last_seen, ld.listed_on_basis,
               ld.listed_after, ld.status, ld.published, ld.ended_on, ld.built_at, ld.seed_first_seen,
               ld.window_days, ld.first_sighting, ld.first_insert
        from listing_dates_v2 ld cross join lateral unnest(ld.row_ids) r(row_id)) a
  order by a.synth, a.first_seen
),
y as (
  select x.*,
         case when e.sighting_date is not null
              then least(coalesce(x.first_sighting, e.sighting_date), e.sighting_date)
              else x.first_sighting end as fs,
         (x.listed_on_basis = 'observed' and (e.sighting_date is null or e.sighting_date >= x.listed_after)) as win
  from x left join public.listing_sighting_evidence e on e.listing_number = x.synth
)
select synth as listing_number,
  case when win then 'window'
       when fs is not null and (first_insert is null or fs <= first_insert) then 'observed'
       when first_insert is not null then 'in_db' end as age_basis,
  case when win then first_seen
       when fs is not null and (first_insert is null or fs <= first_insert) then fs
       else first_insert end as age_date,
  case when win then listed_after end as listed_after,
  fs as first_sighting,
  first_insert
from y;
