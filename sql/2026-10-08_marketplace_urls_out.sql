-- 2026-10-08 Keep marketplace URLs out of the public record.
--
-- Cause: some brokers list a deal on their own site but link the card to its
-- marketplace copy (bizbuysell.com, loopnet.com). The generic scraper recorded
-- that outbound link as the listing URL, so 76 bizbuysell.com rows (and one
-- loopnet.com row) were live on the site, from springfieldstrategies.com,
-- papadop.com, lonestarba.com, districtbusinessbrokers.com, pbabroker.com,
-- mdrma.com, rbbnc.com and others. broker_block only held
-- broker.bizbuysell.com, so www.bizbuysell.com URLs passed the bridge.
--
-- APPLIED to production on 2026-10-08. The scraper now drops such cards
-- (scrapers/dealledger_scraper_v6.py, is_marketplace_url) and the snapshot
-- merge drops anything that mentions a marketplace (scripts/merge_snapshot_shards.py).

-- 1. Block marketplace domains. The bridge already skips listings whose URL
--    host is in broker_block, and sync_blocked_brokers moves matching
--    broker_sources rows to 0_blocked so they are never crawled.
insert into broker_block (broker_domain, reason, updated_at)
select d, 'MARKETPLACE_NO_BBS', now() from unnest(array[
  'bizbuysell.com','bizquest.com','loopnet.com','businessesforsale.com','businessbroker.net',
  'bizben.com','dealstream.com','flippa.com',
  'us.businessesforsale.com','uk.businessesforsale.com','canada.businessesforsale.com',
  'india.businessesforsale.com','newzealand.businessesforsale.com','singapore.businessesforsale.com',
  'france.businessesforsale.com','greece.businessesforsale.com','spain.businessesforsale.com',
  'uae.businessesforsale.com','australia.businessesforsale.com','businessesforsale.nuwireinvestor.com']) d
where not exists (select 1 from broker_block b where lower(b.broker_domain) = d);

-- 2. Take the current rows off the site now (the next bridge run would too).
update listings l set is_active = false
where l.source = 'broker_direct' and l.is_active
  and lower(regexp_replace(substring(l.url from '^https?://([^/:?#]+)'), '^www\.', ''))
      ~ '(^|\.)(bizbuysell\.com|bizquest\.com|loopnet\.com|businessesforsale\.com|businessbroker\.net|bizben\.com|dealstream\.com|flippa\.com)$';

-- 3. Belt and braces: the public API never returns a listing on a marketplace
--    URL, active or historical, whatever the pipeline does.
create or replace function public.is_marketplace_url(u text)
returns boolean language sql immutable set search_path = public as $$
  select coalesce(lower(regexp_replace(substring(u from '^https?://([^/:?#]+)'), '^www\.', ''))
    ~ '(^|\.)(bizbuysell\.com|bizquest\.com|loopnet\.com|businessesforsale\.com|businessbroker\.net|bizben\.com|dealstream\.com|flippa\.com)$', false)
$$;

create policy "public never sees marketplace urls" on public.listings
  as restrictive for select to anon, authenticated
  using (not public.is_marketplace_url(url));

-- 4. Rebuild what the site reads.
select refresh_listings_page();
select refresh_broker_registry();

-- Verify (expect 0):
-- set role anon; select count(*) from listings where is_marketplace_url(url); reset role;
-- select count(*) from mv_listings_page where is_marketplace_url(source_url);
