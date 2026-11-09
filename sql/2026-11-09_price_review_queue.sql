-- Price v7 validation: read-only review queue for agents/price_review_check.py.
-- Lists crawl-origin price changes still pending, with the evidence a reviewer
-- needs. Changes nothing. service_role only (same pattern as
-- public.price_detection_summary). Rollback at the bottom.

create or replace function public.price_review_queue(p_only_auto_eligible boolean default false)
returns table (
  identity_id integer, listing_id text, broker_domain text, listing_status text, url text,
  title text, prev_price numeric, next_price numeric, pct_change numeric,
  prev_last_seen date, next_first_seen date, next_last_seen date,
  screen_result text, screen_reasons text, review_ref text,
  prev_source text, next_source text, cash_flow numeric, revenue numeric,
  auto_eligible boolean, prev_from_crawl boolean
)
language sql stable security definer
set search_path = public, private
as $$
  select c.identity_id, c.listing_id, l.broker_domain, l.status, l.url, l.title,
         c.prev_price, c.next_price, c.pct_change,
         c.prev_last_seen, c.next_first_seen, c.next_last_seen,
         c.screen_result, c.screen_reasons, c.review_ref,
         c.prev_source, c.next_source, l.cash_flow, l.revenue,
         coalesce(c.review_ref, '') like 'auto-eligible:%' as auto_eligible,
         c.prev_source like 'crawl %' as prev_from_crawl
  from private.price_change c
  join public.listings_direct l on l.id = c.listing_id
  where c.origin = 'crawl' and c.review_status = 'pending'
    and (not p_only_auto_eligible or coalesce(c.review_ref, '') like 'auto-eligible:%')
  order by (coalesce(c.review_ref, '') like 'auto-eligible:%') desc, c.next_first_seen, c.listing_id;
$$;

revoke all on function public.price_review_queue(boolean) from public, anon, authenticated;
grant execute on function public.price_review_queue(boolean) to service_role;

-- Check (expect today's pending crawl changes; nothing is modified):
--   select listing_id, broker_domain, prev_price, next_price, auto_eligible from public.price_review_queue();
--   select has_function_privilege('anon', 'public.price_review_queue(boolean)', 'execute');  -- false

-- Rollback:
--   drop function if exists public.price_review_queue(boolean);
