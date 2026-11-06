-- 2026-11-06: close public read access to marketplace-era broker profiles.
-- STATUS: PROPOSED — not applied. Needs Jeff's approval.
--
-- What is exposed today through the public (anon) key:
--   broker_master              table  8,123 rows (7,343 with phone, 1,559 with email)
--   broker_firms               view   3,896 rows (647 with email), aggregates broker_master
--   firm_velocity              view   2,584 rows, every broker_master column incl. email/phone
--   v_broker_crawl_candidates  view   2,185 rows, broker_master counts per domain
-- The views are owned by postgres without security_invoker, so they bypass
-- broker_master's RLS: revoking the table alone would leave all three views open.
--
-- What still needs public access: pages/broker/[slug].tsx reads
-- broker_firms.companyurl by slug to 301 retired broker slugs. That route keeps
-- working through a column-level grant on (slug, companyurl) only.
-- Pipeline readers (agents/discover_backlog.py, scrapers/unified_scraper.py) use
-- the service key and are unaffected. No function or cron job references these objects.

begin;

revoke all on public.broker_master             from anon, authenticated;
revoke all on public.firm_velocity             from anon, authenticated;
revoke all on public.v_broker_crawl_candidates from anon, authenticated;
revoke all on public.broker_firms              from anon, authenticated;

-- Retired-slug redirects only: the broker's website by slug, nothing else.
grant select (slug, companyurl) on public.broker_firms to anon, authenticated;

-- Inert once the grants are gone; dropped so a future blanket grant
-- doesn't silently re-expose the table.
drop policy if exists "public read"             on public.broker_master;
drop policy if exists "anon_read_broker_master" on public.broker_master;

commit;
