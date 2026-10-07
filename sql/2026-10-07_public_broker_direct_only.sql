-- 2026-10-07: the public API exposes broker-direct data only.
-- Legacy marketplace-era rows stay in the database (service_role/postgres only)
-- until they are exported to the private archive repo and dropped.
-- Pipeline and cron jobs use service_role/postgres and are unaffected.
-- The site reads listings (now broker_direct only), mv_listings_page,
-- broker_firms, broker_master, sba_inquiries; listing_history errors are tolerated.

begin;

drop policy if exists "public read" on public.listings;
create policy "public read broker_direct" on public.listings
  for select to anon, authenticated using (source = 'broker_direct');

revoke select on
  public.bizquest_listings, public.listing_history, public.listing_views_history,
  public.views_history_load, public.dom_snapshot_20260908, public.dom_snapshot_20260908b,
  public.dom_anchors, public.listings_broker_archived_2026_04_27, public.unmatched_listings,
  public.listing_matches, public.match_labels, public.broker_classifications,
  public.unified_listings, public.dealledger_listings,
  public.v_label_queue, public.v_label_queue_top, public.v_discovery_queue
from anon, authenticated;

commit;

-- Verify (should be 0, then false):
-- set role anon; select count(*) from public.listings where source <> 'broker_direct'; reset role;
-- select has_table_privilege('anon', 'public.bizquest_listings', 'SELECT');
