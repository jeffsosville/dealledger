-- Rollback for 2026-11-06_price_history.sql (and _publish.sql). Safe to run more than once.
-- Removes only objects the price-history migrations created. Touches nothing else.
-- The committed snapshot files are the source; the backfill can be rebuilt and reloaded.

begin;
do $$ begin
  if exists (select 1 from pg_namespace where nspname = 'cron') then
    if exists (select 1 from cron.job where jobname = 'detect-price-changes') then
      perform cron.unschedule('detect-price-changes');
    end if;
  end if;
end $$;
drop view if exists public.price_history;
drop trigger if exists trg_listings_direct_0_price_observation on public.listings_direct;
drop trigger if exists trg_listings_direct_price_observation on public.listings_direct;
drop function if exists public.price_history_load(text, jsonb);
drop function if exists private.record_price_observation();
drop function if exists private.detect_price_changes();
drop view if exists private.v_price_history_public;
drop table if exists private.price_change;
drop table if exists private.price_evidence;
drop table if exists private.price_identity;
drop table if exists private.price_source_file;
drop function if exists private.identity_url(text);
commit;
