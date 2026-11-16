-- 2026-11-16 — Stored listing numbers (listing_key_map). Priority 2/3: permanent IDs, no shared ages.
--
-- Problem: public listing numbers were computed on the fly as
--   900000000 + first 28 bits of md5(listings_direct.id)
-- 20 numbers are shared by two different rows (40 rows; 1 pair both active/published on Nov 16).
-- Ages (listing_age_v2), the bridge (distinct on number), SEO pages and /listing/{n} then mix
-- the two listings. Permanent IDs are also needed before citations / the Zenodo DOI.
--
-- Fix: listing_key_map(listing_id -> listing_number), unique on both.
--   * Every existing row keeps its current number, except one row per collision, which gets the
--     next free number. Winner = the row the site already shows under that number (legacy
--     listings.url match), else the earliest first_seen.
--   * New rows get a number at insert (trigger), probing past any number already taken.
--   * public.dl_listing_number(id) replaces the formula in: bridge_direct_to_listings(),
--     mcp_listing(bigint), private.remap_synth(text), v_direct_dom, v_listing_age,
--     mv_seo_listings_v2 (+ dependent mv_seo_page_stats, recreated unchanged).
-- Nothing is deleted. Rollback: 2026-11-16_listing_key_map_rollback.sql (restores the saved
-- definitions from private.keymap_saved_defs_20261116).

begin;
set local statement_timeout = '900s';

-- 0. Save every definition we change -----------------------------------------------------------
create table if not exists private.keymap_saved_defs_20261116 (obj text primary key, kind text, def text, extra text);
insert into private.keymap_saved_defs_20261116 values
  ('public.bridge_direct_to_listings()', 'function', pg_get_functiondef('public.bridge_direct_to_listings()'::regprocedure), null),
  ('public.mcp_listing(bigint)',          'function', pg_get_functiondef('public.mcp_listing(bigint)'::regprocedure), null),
  ('private.remap_synth(text)',           'function', pg_get_functiondef('private.remap_synth(text)'::regprocedure), null),
  ('public.v_direct_dom',                 'view',     pg_get_viewdef('public.v_direct_dom'::regclass), null),
  ('public.v_listing_age',                'view',     pg_get_viewdef('public.v_listing_age'::regclass), null),
  ('public.mv_seo_listings_v2',           'matview',  pg_get_viewdef('public.mv_seo_listings_v2'::regclass),
     (select string_agg(pg_get_indexdef(indexrelid), ';' ) from pg_index where indrelid = 'public.mv_seo_listings_v2'::regclass)),
  ('public.mv_seo_page_stats',            'matview',  pg_get_viewdef('public.mv_seo_page_stats'::regclass),
     (select string_agg(pg_get_indexdef(indexrelid), ';' ) from pg_index where indrelid = 'public.mv_seo_page_stats'::regclass))
on conflict (obj) do nothing;

-- 1. The map --------------------------------------------------------------------------------------
create or replace function public.dl_hash_number(id text) returns bigint
language sql immutable parallel safe as
$$ select 900000000 + (('x' || substr(md5(id), 1, 7))::bit(28))::bigint $$;

create table if not exists public.listing_key_map (
  listing_id     text primary key,
  listing_number bigint not null unique,
  assigned_at    timestamptz not null default now(),
  reason         text not null default 'hash'   -- 'hash' = the old formula; 'collision' = reassigned
);
alter table public.listing_key_map enable row level security;
drop policy if exists listing_key_map_read on public.listing_key_map;
create policy listing_key_map_read on public.listing_key_map for select to anon, authenticated using (true);
grant select on public.listing_key_map to anon, authenticated;

-- Backfill: winners keep the hash number.
with h as (
  select d.id, public.dl_hash_number(d.id) n, d.first_seen, d.url from listings_direct d
), ranked as (
  select h.*, row_number() over (
           partition by n
           order by (exists (select 1 from listings l where l.listing_number = h.n and l.source = 'broker_direct' and l.url = h.url)) desc,
                    first_seen nulls last, id) rk
  from h
)
insert into public.listing_key_map (listing_id, listing_number, reason)
select id, n, 'hash' from ranked where rk = 1
on conflict (listing_id) do nothing;

-- Losers: every number any row hashes to is reserved, so start probing past the whole hash set.
create temp table _all_hash on commit drop as select distinct public.dl_hash_number(id) n from listings_direct;
create index on _all_hash (n);
do $$
declare r record; v bigint;
begin
  for r in
    select d.id, public.dl_hash_number(d.id) h from listings_direct d
    where not exists (select 1 from public.listing_key_map k where k.listing_id = d.id)
    order by d.first_seen nulls last, d.id
  loop
    v := r.h + 1;
    loop
      if v >= 900000000 + 268435456 then v := 900000000; end if;
      exit when not exists (select 1 from public.listing_key_map where listing_number = v)
            and not exists (select 1 from _all_hash where _all_hash.n = v);
      v := v + 1;
    end loop;
    insert into public.listing_key_map (listing_id, listing_number, reason) values (r.id, v, 'collision');
  end loop;
end $$;

-- New rows: assign at insert.
create or replace function private.dl_assign_listing_number() returns trigger
language plpgsql security definer set search_path = public, private as $$
declare n bigint := public.dl_hash_number(new.id);
begin
  if exists (select 1 from public.listing_key_map where listing_id = new.id) then return new; end if;
  loop
    begin
      insert into public.listing_key_map (listing_id, listing_number, reason)
      values (new.id, n, case when n = public.dl_hash_number(new.id) then 'hash' else 'collision' end);
      return new;
    exception when unique_violation then
      if exists (select 1 from public.listing_key_map where listing_id = new.id) then return new; end if;
      n := case when n + 1 >= 900000000 + 268435456 then 900000000 else n + 1 end;
    end;
  end loop;
end $$;
drop trigger if exists trg_dl_assign_listing_number on public.listings_direct;
create trigger trg_dl_assign_listing_number after insert on public.listings_direct
  for each row execute function private.dl_assign_listing_number();

-- The lookup everything uses. Falls back to the hash only for an id not yet in the map.
create or replace function public.dl_listing_number(id text) returns bigint
language sql stable parallel safe as
$$ select coalesce((select k.listing_number from public.listing_key_map k where k.listing_id = $1),
                   public.dl_hash_number($1)) $$;

-- 2. Swap the formula in functions and views --------------------------------------------------------
do $$
declare d text; o text;
  f_view  text := '(900000000 + (((''x''::text || substr(md5(%s), 1, 7)))::bit(28))::bigint)';
begin
  -- bridge
  d := pg_get_functiondef('public.bridge_direct_to_listings()'::regprocedure);
  o := d;
  d := replace(d, '900000000 + ((''x'' || substr(md5(d.id), 1, 7))::bit(28)::bigint)', 'public.dl_listing_number(d.id)');
  if d = o then raise exception 'bridge: formula not found'; end if;
  execute d;

  -- mcp_listing reverse lookup
  d := pg_get_functiondef('public.mcp_listing(bigint)'::regprocedure);
  o := d;
  d := replace(d, '900000000 + ((''x'' || substr(md5(x.id), 1, 7))::bit(28))::bigint = ln',
                  'x.id = (select k.listing_id from public.listing_key_map k where k.listing_number = ln)');
  if d = o then raise exception 'mcp_listing: formula not found'; end if;
  execute d;

  -- views
  d := pg_get_viewdef('public.v_direct_dom'::regclass); o := d;
  d := replace(d, format(f_view, 'd.id'), 'public.dl_listing_number(d.id)');
  if d = o then raise exception 'v_direct_dom: formula not found'; end if;
  execute 'create or replace view public.v_direct_dom as ' || d;

  d := pg_get_viewdef('public.v_listing_age'::regclass); o := d;
  d := replace(d, format(f_view, 'r.row_id'), 'public.dl_listing_number(r.row_id)');
  if d = o then raise exception 'v_listing_age: formula not found'; end if;
  execute 'create or replace view public.v_listing_age as ' || d;
end $$;

-- remap_synth: same name, now the stored number (was IMMUTABLE; the lookup is STABLE).
create or replace function private.remap_synth(id text) returns bigint
language sql stable parallel safe as $$ select public.dl_listing_number(id) $$;

-- 3. Matviews: rebuild mv_seo_listings_v2 with the lookup, and mv_seo_page_stats unchanged on top ----
do $$
declare d1 text; d2 text; i1 text; i2 text; s text;
begin
  select def, extra into d1, i1 from private.keymap_saved_defs_20261116 where obj = 'public.mv_seo_listings_v2';
  select def, extra into d2, i2 from private.keymap_saved_defs_20261116 where obj = 'public.mv_seo_page_stats';
  s := replace(d1, '(900000000 + (((''x''::text || substr(md5(ld.id), 1, 7)))::bit(28))::bigint)', 'public.dl_listing_number(ld.id)');
  if s = d1 then raise exception 'mv_seo_listings_v2: formula not found'; end if;
  drop materialized view public.mv_seo_page_stats;
  drop materialized view public.mv_seo_listings_v2;
  execute 'create materialized view public.mv_seo_listings_v2 as ' || s;
  foreach s in array string_to_array(i1, ';') loop execute s; end loop;
  execute 'create materialized view public.mv_seo_page_stats as ' || d2;
  foreach s in array string_to_array(i2, ';') loop execute s; end loop;
  grant select on public.mv_seo_listings_v2, public.mv_seo_page_stats to anon, authenticated;
end $$;

-- 4. Checks (any failure aborts the whole migration) -------------------------------------------------
do $$
declare a bigint; b bigint; c bigint;
begin
  select count(*) into a from listings_direct;
  select count(*) into b from public.listing_key_map;
  if a <> b then raise exception 'map rows % <> listings_direct rows %', b, a; end if;
  select count(*) into c from public.listing_key_map where reason = 'collision';
  if c > 40 then raise exception 'too many reassigned rows: %', c; end if;
  if exists (select 1 from listings_direct d join public.listing_key_map k on k.listing_id = d.id
             where k.reason = 'hash' and k.listing_number <> public.dl_hash_number(d.id))
  then raise exception 'a non-collision row changed number'; end if;
  raise notice 'listing_key_map: % rows, % reassigned', b, c;
end $$;

commit;

-- After commit (outside the transaction):
--   select refresh_listings_page_v2();   -- rebuilds listing_age_v2 / mv_listings_page_v2 with split ages
--   select bridge_direct_to_listings();  -- reassigned listings get their own legacy row
