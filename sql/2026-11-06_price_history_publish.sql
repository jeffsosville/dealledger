-- 2026-11-06: publish approved historical price changes. SEPARATE APPROVAL REQUIRED.
-- Exposes only private.v_price_history_public (approved + passed screen + published
-- listing), as public.price_history, read-only, to anon/authenticated.
-- Rollback: drop view public.price_history (also in _rollback.sql).

begin;
create or replace view public.price_history
with (security_barrier = true) as
select listing_number, prev_price, prev_last_seen, next_price, next_first_seen, pct_change, window_days
from private.v_price_history_public;
revoke all on public.price_history from public, anon, authenticated;
grant select on public.price_history to anon, authenticated;
commit;
