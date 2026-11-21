-- 2026-11-21 — API v1: price changes joined to public listing metadata, so state/type
-- filters and counts run in the database (was: newest 1,000 rows fetched, then filtered).
-- Read-only, additive. Applied 2026-11-21. Rollback: drop view public.api_price_changes;
create or replace view public.api_price_changes
with (security_invoker = true, security_barrier = true) as
select p.listing_number,
       p.prev_price, p.next_price, p.pct_change,
       p.prev_last_seen::date  as changed_after,
       p.next_first_seen::date as changed_on_or_before,
       p.window_days,
       m.title, m.city, m.state_code, m.vertical, m.price as current_price,
       m.broker_domain, m.source_url
from public.price_history p
join public.mv_seo_listings_v2 m on m.listing_number = p.listing_number
where p.prev_price > 0 and p.next_price > 0
  and p.next_first_seen > p.prev_last_seen;

grant select on public.api_price_changes to anon, authenticated;
