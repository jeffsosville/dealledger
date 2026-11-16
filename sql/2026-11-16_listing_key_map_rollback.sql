-- Rollback for 2026-11-16_listing_key_map.sql: restores every saved definition, drops the
-- trigger and lookup function. listing_key_map is kept (renamed) for reference; nothing deleted.
begin;
set local statement_timeout = '900s';
drop trigger if exists trg_dl_assign_listing_number on public.listings_direct;
do $$
declare d text; i text; s text;
begin
  execute (select def from private.keymap_saved_defs_20261116 where obj = 'public.bridge_direct_to_listings()');
  execute (select def from private.keymap_saved_defs_20261116 where obj = 'public.mcp_listing(bigint)');
  execute (select def from private.keymap_saved_defs_20261116 where obj = 'private.remap_synth(text)');
  execute 'create or replace view public.v_direct_dom as '  || (select def from private.keymap_saved_defs_20261116 where obj = 'public.v_direct_dom');
  execute 'create or replace view public.v_listing_age as ' || (select def from private.keymap_saved_defs_20261116 where obj = 'public.v_listing_age');
  drop materialized view if exists public.mv_seo_page_stats;
  drop materialized view if exists public.mv_seo_listings_v2;
  select def, extra into d, i from private.keymap_saved_defs_20261116 where obj = 'public.mv_seo_listings_v2';
  execute 'create materialized view public.mv_seo_listings_v2 as ' || d;
  foreach s in array string_to_array(i, ';') loop execute s; end loop;
  select def, extra into d, i from private.keymap_saved_defs_20261116 where obj = 'public.mv_seo_page_stats';
  execute 'create materialized view public.mv_seo_page_stats as ' || d;
  foreach s in array string_to_array(i, ';') loop execute s; end loop;
  grant select on public.mv_seo_listings_v2, public.mv_seo_page_stats to anon, authenticated;
end $$;
drop function if exists public.dl_listing_number(text);
drop function if exists private.dl_assign_listing_number();
alter table public.listing_key_map rename to listing_key_map_rolled_back_20261116;
commit;
-- Then: select refresh_listings_page_v2(); select bridge_direct_to_listings();
-- and revert scripts/public_scope.py, export_public_listings.py, verify_public_export.py.
