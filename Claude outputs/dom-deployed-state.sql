-- DealLedger — anchored DOM interpolation
-- STATUS: APPLIED to kqckuedsyyosmccushyd on 2026-09-05.
-- This file is a record of what is live, for the repo. Do not re-run
-- to "install" — it is already installed. Re-run only to modify.
--
-- Migrations applied:
--   dom_anchored_interpolation
--   dom_confidence_recalibration
--
-- Both are additive: they create views and functions. No existing table
-- was altered and no row was modified.
--
-- Published method: dealledger.org/dom-method.html

-- ---------------------------------------------------------------------------
-- Anchor segments
-- ---------------------------------------------------------------------------
create or replace view v_dom_segments as
select
  listing_number as from_num,
  anchor_date    as from_date,
  confidence     as from_conf,
  lead(listing_number) over (order by listing_number) as to_num,
  lead(anchor_date)    over (order by listing_number) as to_date,
  lead(confidence)     over (order by listing_number) as to_conf,
  (lead(anchor_date) over (order by listing_number) - anchor_date) as segment_days,
  (lead(listing_number) over (order by listing_number) - listing_number) as segment_numbers
from dom_anchors;

-- ---------------------------------------------------------------------------
-- Listing number -> estimated listed date.
-- Returns NULL below the earliest anchor. That is deliberate: extrapolating
-- past the anchor set is the one thing that would make this indefensible.
-- ---------------------------------------------------------------------------
create or replace function estimated_listed_date(p_listing_number bigint)
returns date language sql stable as $$
  select case
    when p_listing_number < (select min(listing_number) from dom_anchors) then null
    when p_listing_number >= (select max(listing_number) from dom_anchors)
      then (select anchor_date from dom_anchors order by listing_number desc limit 1)
    else (
      select s.from_date
           + ((p_listing_number - s.from_num)::numeric
              / nullif(s.segment_numbers,0) * s.segment_days)::int
      from v_dom_segments s
      where p_listing_number >= s.from_num and p_listing_number < s.to_num
      limit 1)
  end;
$$;

-- ---------------------------------------------------------------------------
-- Confidence tier. Keyed on anchor quality first, segment width second.
--   high   : both bracketing anchors are real observations, segment <= 45d
--   medium : both real, <= 150d (or a narrow segment with a sample edge)
--   low    : a 'sample' anchor at an edge of a wide segment
--   floor  : below the earliest anchor
-- ---------------------------------------------------------------------------
create or replace function dom_confidence(p_listing_number bigint)
returns text language sql stable as $$
  select case
    when p_listing_number < (select min(listing_number) from dom_anchors) then 'floor'
    when p_listing_number >= (select max(listing_number) from dom_anchors) then 'high'
    else coalesce((
      select case
        when s.from_conf <> 'sample' and s.to_conf <> 'sample' and s.segment_days <= 45  then 'high'
        when s.from_conf <> 'sample' and s.to_conf <> 'sample' and s.segment_days <= 150 then 'medium'
        when s.segment_days <= 45 then 'medium'
        else 'low'
      end
      from v_dom_segments s
      where p_listing_number >= s.from_num and p_listing_number < s.to_num
      limit 1), 'low')
  end;
$$;

-- ---------------------------------------------------------------------------
-- The view the site reads. anon has SELECT.
-- dom_display is pre-rendered and safe to print directly — it never emits a
-- bare number for a listing that cannot be dated.
-- ---------------------------------------------------------------------------
create or replace view v_listing_dom as
select
  b.listing_number, b.header, b.state, b.city, b.price, b.url,
  estimated_listed_date(b.listing_number) as est_listed_date,
  dom_confidence(b.listing_number)        as dom_confidence,
  case when estimated_listed_date(b.listing_number) is null then null
       else current_date - estimated_listed_date(b.listing_number) end as dom_days,
  case when estimated_listed_date(b.listing_number) is null
       then current_date - (select min(anchor_date) from dom_anchors) end as dom_floor_days,
  case when estimated_listed_date(b.listing_number) is null
       then 'listed before ' || to_char((select min(anchor_date) from dom_anchors),'DD Mon YYYY')
       else (current_date - estimated_listed_date(b.listing_number))::text || ' days'
  end as dom_display
from bizquest_listings b
where b.is_active;

grant select on v_listing_dom, v_dom_segments to anon;


-- ===========================================================================
-- VERIFICATION — all passed 2026-09-05. Re-run after adding anchors.
-- ===========================================================================

-- A. Anchors must be strictly monotonic in BOTH number and date.
--    Any row returned invalidates the entire method. Returned 0.
--
-- select a.listing_number, a.anchor_date, b.listing_number, b.anchor_date
-- from dom_anchors a join dom_anchors b on b.listing_number > a.listing_number
-- where b.anchor_date <= a.anchor_date;

-- B. No estimate may be in the future or before the floor. Both returned 0.
--
-- select count(*) from v_listing_dom
-- where est_listed_date > current_date
--    or est_listed_date < (select min(anchor_date) from dom_anchors);

-- C. Coverage and distribution. 2026-09-05:
--      43,609 live / 40,750 datable / 2,859 floored / median 128 days
--
-- select count(*) total, count(dom_days) datable,
--        count(*) filter (where dom_days is null) floored,
--        percentile_cont(0.5) within group (order by dom_days)::int median
-- from v_listing_dom;

-- D. Confidence mix. 2026-09-05:
--      high 11,369 (median 29d) | medium 23,345 (137d)
--      low   6,036 (median 398d) | floor 2,859
--    80% at medium or better. If 'low' starts to dominate, add anchors
--    before publishing anything new.
--
-- select dom_confidence, count(*),
--        percentile_cont(0.5) within group (order by dom_days)::int
-- from v_listing_dom group by 1 order by 2 desc;


-- ===========================================================================
-- THE ONE THING THAT WOULD IMPROVE THIS MOST
-- ===========================================================================
-- The anchor set starts 2024-12-12, so 2,859 live listings (6.6%) can only be
-- floored, and every "2+ years listed" claim is an extrapolation below the
-- anchor set rather than an interpolation within it.
--
-- One anchor from 2023 and one from 2021-22 would convert all 2,859 into dated
-- listings and turn the 2-year claim into a measurement. A Wayback Machine
-- snapshot of a marketplace search page gives exactly the observation needed:
-- a date, and the highest listing number visible on that date.
--
-- insert into dom_anchors (anchor_date, listing_number, confidence)
-- values ('2023-mm-dd', <highest number seen>, 'sample');
