-- market_multiple_by_segment
-- Median asking-price-to-SDE multiples by sub-segment within a vertical,
-- for the vertical sites' valuation pages (e.g. CleaningExits
-- /valuation/residential, /valuation/commercial).
--
-- Same filters as market_multiple_by_sde_band: active listings with a
-- positive asking price and cash flow, multiple between 0.5x and 10x.
-- Segments come from title/category keywords. Order matters: the first
-- match wins, so "commercial janitorial" lands in commercial, not other.
-- Pages must suppress any segment below their minimum sample size.

create or replace view public.market_multiple_by_segment as
with base as (
  select
    vertical,
    asking_price,
    cash_flow,
    asking_price / nullif(cash_flow, 0) as mult,
    lower(coalesce(title, '') || ' ' || coalesce(category, '')) as t
  from public.listings_direct
  where vertical is not null
    and status = 'active'
    and cash_flow > 0
    and asking_price > 0
    and asking_price / nullif(cash_flow, 0) between 0.5 and 10
),
seg as (
  select *,
    case
      when vertical = 'cleaning' and t ~ '(janitorial|commercial clean|office clean|custodial)' then 'commercial'
      when vertical = 'cleaning' and t ~ '(residential|maid|house ?clean|home clean|housekeep)' then 'residential'
      when vertical = 'cleaning' and t ~ '(carpet|upholster)' then 'carpet'
      when vertical = 'cleaning' and t ~ 'window' then 'window'
      when vertical = 'cleaning' and t ~ '(pressure|power wash|soft wash)' then 'pressure-washing'
      when vertical = 'cleaning' and t ~ '(restoration|remediation|water damage|mold)' then 'restoration'
      else 'other'
    end as segment
  from base
)
select
  vertical,
  segment,
  count(*) as sample_size,
  round((percentile_cont(0.5)  within group (order by mult::float8))::numeric, 2) as median_multiple,
  round((percentile_cont(0.25) within group (order by mult::float8))::numeric, 2) as p25_multiple,
  round((percentile_cont(0.75) within group (order by mult::float8))::numeric, 2) as p75_multiple,
  percentile_cont(0.5) within group (order by asking_price::float8) as median_asking_price,
  percentile_cont(0.5) within group (order by cash_flow::float8)    as median_sde
from seg
group by vertical, segment;

grant select on public.market_multiple_by_segment to anon, authenticated;
