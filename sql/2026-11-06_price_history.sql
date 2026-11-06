-- 2026-11-06: historical price history (PRIVATE). Additive only.
--
-- New objects only. Nothing existing is altered: no change to listings_direct
-- rows, first_seen, DOM, listing_sighting, listing_observation, listing_dates,
-- legacy listings, or any existing trigger. One new BEFORE trigger is added to
-- listings_direct; it returns every row unchanged, writes only to the new tables,
-- and can never fail or alter a crawl.
--
-- Nothing here is readable by anon/authenticated. Publishing is a separate
-- migration (2026-11-06_price_history_publish.sql) with its own approval.
--
-- The originals stay where they are: the committed snapshot files in git.
-- Every evidence row records the file, the commit that first added it, and the
-- row number, so any observation can be traced back to the exact source line.
--
-- Rules and validation: claude/price-history-validation-2026-11-06.md.

begin;

create schema if not exists private;

-- Same normalization as scripts/price_history_build.py identity_url(): like
-- public.norm_listing_url() but keeps the query string (it distinguishes
-- listings, e.g. currentlistings.aspx?businessid=25).
create or replace function private.identity_url(u text)
returns text language sql immutable parallel safe as $$
  select case when q <> '' then lower(regexp_replace(p, '/+$', '')) || '?' || lower(q)
              else lower(regexp_replace(p, '/+$', '')) end
  from (select split_part(s, '?', 1) as p,
               case when position('?' in s) > 0 then substr(s, position('?' in s) + 1) else '' end as q
        from (select regexp_replace(split_part(regexp_replace(coalesce(u, ''), '^[\s\u00a0]+|[\s\u00a0]+$', '', 'g'), '#', 1),
                                    '^https?://(www\.)?', '', 'i') as s) a) b
$$;

-- 1. Source files ------------------------------------------------------------------
create table if not exists private.price_source_file (
  file_id       smallint primary key,                  -- 0 = live crawl (trigger)
  path          text not null unique,
  first_commit  text not null default '',              -- git commit that first added the file
  file_version  text not null default '',              -- 'working_tree' | 'first_commit' (4 rewritten files)
  snapshot_date date,
  row_count     int
);
insert into private.price_source_file (file_id, path, file_version) values (0, 'crawl', 'live')
on conflict (file_id) do nothing;

-- 2. Identity: normalized URL -> current listings_direct id -------------------------
create table if not exists private.price_identity (
  identity_id     int generated always as identity primary key,
  identity_url    text not null unique,
  listing_id      text not null,                       -- listings_direct.id
  identity_status text not null check (identity_status in ('ok','ambiguous')),
  link_basis      text not null,                       -- 'same id' | 'sequential id change' | ambiguity reason
  ids_seen        int  not null default 1,
  source          text not null default 'snapshot_backfill'
);
create index if not exists price_identity_listing_idx on private.price_identity (listing_id);

-- 3. Evidence: every dated observation, eligible and excluded --------------------
create table if not exists private.price_evidence (
  file_id         smallint not null references private.price_source_file,
  row_index       int not null,                        -- row in the file; 0 for crawl rows
  observed_on     date not null,
  listing_id_seen text not null default '',            -- id exactly as recorded then
  identity_id     int references private.price_identity,
  asking_price    numeric,
  cash_flow       numeric,
  revenue         numeric,
  title_head      text,                                -- first 120 chars, eligible rows only
  exclusion       text not null default ''             -- '' = eligible
);
create unique index if not exists price_evidence_file_row_uq on private.price_evidence (file_id, row_index) where file_id > 0;
create unique index if not exists price_evidence_crawl_uq on private.price_evidence (observed_on, listing_id_seen) where file_id = 0;
create index if not exists price_evidence_identity_idx on private.price_evidence (identity_id, observed_on) where exclusion = '';

-- 4. Changes: between two consecutive dated observations --------------------------
create table if not exists private.price_change (
  identity_id       int not null references private.price_identity,
  listing_id        text not null,
  prev_price        numeric not null,
  prev_first_seen   date not null,
  prev_last_seen    date not null,                     -- last observation at the old price
  next_price        numeric not null,
  next_first_seen   date not null,                     -- first observation at the new price
  next_last_seen    date not null,
  pct_change        numeric not null,
  prev_source       text not null,                     -- file@commit#row
  next_source       text not null,
  screen_result     text not null check (screen_result in ('pass','anomaly','identity')),
  screen_reasons    text not null default '',
  rules_version     text not null,
  review_status     text not null check (review_status in ('approved','rejected','pending','not_eligible')),
  reviewer          text not null default '',
  review_ref        text not null default '',
  origin            text not null check (origin in ('snapshot_backfill','crawl')),
  primary key (identity_id, next_first_seen),
  check (next_first_seen > prev_last_seen),
  check (prev_price <> next_price)
);
create index if not exists price_change_listing_idx on private.price_change (listing_id);

-- 5. Forward collection: each crawl that sees a listing writes one evidence row per
--    listing per day, priced or not, changed or not. Idempotent within a day.
--    BEFORE trigger, named to fire first, so it records the price the crawl actually
--    saw: trg_listings_direct_keep_filled later substitutes the old price when the
--    scrape returns none, which would otherwise look like a fresh observation.
--    Always returns NEW unchanged: it can never alter or cancel the crawl's write.
create or replace function private.record_price_observation()
returns trigger language plpgsql security definer set search_path = public, private, pg_temp as $$
declare v_url text; v_identity int;
begin
  if new.last_seen is null or new.url is null then return new; end if;
  if tg_op = 'UPDATE' and new.last_seen is not distinct from old.last_seen
     and (new.asking_price is null or new.asking_price is not distinct from old.asking_price) then
    return new;
  end if;
  v_url := private.identity_url(new.url);
  insert into private.price_identity (identity_url, listing_id, identity_status, link_basis, source)
  values (v_url, new.id, 'ok', 'same id', 'crawl')
  on conflict (identity_url) do nothing;
  select identity_id into v_identity from private.price_identity where identity_url = v_url;
  insert into private.price_evidence (file_id, row_index, observed_on, listing_id_seen, identity_id,
                                      asking_price, cash_flow, revenue, title_head, exclusion)
  values (0, 0, new.last_seen::date, new.id, v_identity, new.asking_price, new.cash_flow, new.revenue,
          left(new.title, 120), case when new.asking_price is null then 'no price' else '' end)
  on conflict (observed_on, listing_id_seen) where file_id = 0
  do update set asking_price = excluded.asking_price, exclusion = excluded.exclusion
  where private.price_evidence.asking_price is null and excluded.asking_price is not null;  -- fill a gap; never overwrite
  return new;
exception when others then
  raise warning 'record_price_observation(%): %', new.id, sqlerrm;            -- never break a crawl
  return new;
end $$;

drop trigger if exists trg_listings_direct_price_observation on public.listings_direct;
drop trigger if exists trg_listings_direct_0_price_observation on public.listings_direct;
create trigger trg_listings_direct_0_price_observation
  before insert or update of last_seen, asking_price on public.listings_direct
  for each row execute function private.record_price_observation();

-- 6. Nightly detector: new crawl observations -> new candidate changes, always
--    'pending' (private) until reviewed. Backfill changes are never re-derived.
create or replace function private.detect_price_changes()
returns integer language plpgsql security definer set search_path = public, private, pg_temp as $$
declare v_rows integer;
begin
  with daily as (
    select distinct on (e.identity_id, e.observed_on)
           e.identity_id, e.observed_on, e.asking_price, e.cash_flow, e.revenue,
           f.path || '@' || left(f.first_commit, 12) || '#' || e.row_index as src
    from private.price_evidence e
    join private.price_identity i on i.identity_id = e.identity_id and i.identity_status = 'ok'
    join private.price_source_file f on f.file_id = e.file_id
    where e.exclusion = '' and e.asking_price >= 1000
    order by e.identity_id, e.observed_on, (e.file_id = 0)          -- snapshot row preferred on the same day
  ), runs as (
    select *, sum(case when asking_price is distinct from prev_p then 1 else 0 end)
                over (partition by identity_id order by observed_on) as run_no
    from (select *, lag(asking_price) over (partition by identity_id order by observed_on) as prev_p from daily) x
  ), bounds as (
    select identity_id, run_no, min(asking_price) as price, min(observed_on) as first_seen, max(observed_on) as last_seen,
           (array_agg(src order by observed_on))[1] as first_src, (array_agg(src order by observed_on desc))[1] as last_src,
           (array_agg(cash_flow order by observed_on))[1] as first_cf, (array_agg(revenue order by observed_on))[1] as first_rev
    from runs group by identity_id, run_no
  ), pairs as (
    select b.identity_id, a.price as prev_price, a.first_seen as prev_first_seen, a.last_seen as prev_last_seen,
           b.price as next_price, b.first_seen as next_first_seen, b.last_seen as next_last_seen,
           a.last_src as prev_source, b.first_src as next_source, b.first_cf, b.first_rev,
           exists (select 1 from bounds c where c.identity_id = b.identity_id and c.run_no < b.run_no - 1 and c.price = b.price) as flip
    from bounds a join bounds b on b.identity_id = a.identity_id and b.run_no = a.run_no + 1
  )
  insert into private.price_change (identity_id, listing_id, prev_price, prev_first_seen, prev_last_seen, next_price,
         next_first_seen, next_last_seen, pct_change, prev_source, next_source, screen_result, screen_reasons,
         rules_version, review_status, origin)
  select p.identity_id, i.listing_id, p.prev_price, p.prev_first_seen, p.prev_last_seen, p.next_price,
         p.next_first_seen, p.next_last_seen, round((p.next_price - p.prev_price) / p.prev_price * 100, 1),
         p.prev_source, p.next_source,
         case when r.reasons = '' then 'pass' else 'anomaly' end, r.reasons, 'crawl.2026-11-06.v1',
         case when r.reasons = '' then 'pending' else 'not_eligible' end, 'crawl'
  from pairs p
  join private.price_identity i on i.identity_id = p.identity_id
  cross join lateral (select concat_ws('; ',
      case when p.next_price / p.prev_price < 0.2 or p.next_price / p.prev_price > 5 then 'ratio outside 0.2-5x' end,
      case when greatest(p.prev_price, p.next_price) >= 1e8 then 'placeholder or implausible value' end,
      case when p.flip then 'price returns to an earlier value (flip-flop)' end,
      case when abs(p.next_price - p.prev_price) / p.prev_price < 0.005 then 'change under 0.5%' end,
      case when abs(p.next_price - p.prev_price) / p.prev_price > 0.6 then 'change over 60% (needs external evidence)' end,
      case when p.next_price in (p.first_cf, p.first_rev) then 'price equals revenue or cash flow on the same row' end
    ) as reasons) r
  where p.next_first_seen > coalesce((select max(c.next_first_seen) from private.price_change c
                                      where c.identity_id = p.identity_id and c.origin = 'snapshot_backfill'),
                                     '-infinity'::date)
  on conflict (identity_id, next_first_seen) do nothing;
  get diagnostics v_rows = row_count;
  return v_rows;
end $$;

-- 7. What would be public: approved after review, passed the screen, and the
--    listing has a public page (in the registry) and is not quarantined.
create or replace view private.v_price_history_public as
select l.listing_number, c.prev_price, c.prev_last_seen, c.next_price, c.next_first_seen,
       c.pct_change, (c.next_first_seen - c.prev_last_seen) as window_days
from private.price_change c
join public.listings_direct d on d.id = c.listing_id
join public.listings l on l.source = 'broker_direct'
 and l.listing_number = 900000000 + (('x' || substr(md5(c.listing_id), 1, 7))::bit(28)::bigint)
where c.review_status = 'approved' and c.screen_result = 'pass'
  and d.status not like 'quarantined%' and d.status <> 'parse_error';

-- 8. Backfill loader, called by scripts/price_history_load.py with the service key.
--    In public only because PostgREST serves only that schema; EXECUTE is
--    service_role only. Idempotent: source/identity/evidence rows are never
--    rewritten; for backfill changes only screen/review fields are refreshed.
create or replace function public.price_history_load(p_kind text, p_rows jsonb)
returns integer language plpgsql security definer set search_path = public, private, pg_temp as $$
declare v_rows integer;
begin
  if p_kind = 'file' then
    insert into private.price_source_file (file_id, path, first_commit, file_version, snapshot_date, row_count)
    select file_id, path, first_commit, file_version, snapshot_date, row_count
    from jsonb_to_recordset(p_rows) as r(file_id smallint, path text, first_commit text, file_version text,
                                         snapshot_date date, row_count int)
    on conflict (file_id) do nothing;
  elsif p_kind = 'identity' then
    insert into private.price_identity (identity_url, listing_id, identity_status, link_basis, ids_seen)
    select identity_url, listing_id, identity_status, link_basis, ids_seen
    from jsonb_to_recordset(p_rows) as r(identity_url text, listing_id text, identity_status text, link_basis text, ids_seen int)
    on conflict (identity_url) do nothing;
  elsif p_kind = 'evidence' then
    insert into private.price_evidence (file_id, row_index, observed_on, listing_id_seen, identity_id,
                                        asking_price, cash_flow, revenue, title_head, exclusion)
    select r.file_id, r.row_index, r.snapshot_date, r.listing_id, i.identity_id, r.asking_price, r.cash_flow, r.revenue,
           case when r.exclusion = '' then left(r.title, 120) end, r.exclusion
    from jsonb_to_recordset(p_rows) as r(file_id smallint, row_index int, snapshot_date date, listing_id text,
         identity_url text, asking_price numeric, cash_flow numeric, revenue numeric, title text, exclusion text)
    left join private.price_identity i on i.identity_url = r.identity_url
    on conflict (file_id, row_index) where file_id > 0 do nothing;
  elsif p_kind = 'change' then
    insert into private.price_change (identity_id, listing_id, prev_price, prev_first_seen, prev_last_seen, next_price,
           next_first_seen, next_last_seen, pct_change, prev_source, next_source, screen_result, screen_reasons,
           rules_version, review_status, reviewer, review_ref, origin)
    select i.identity_id, r.listing_id, r.prev_price, r.prev_first_seen, r.prev_last_seen, r.next_price,
           r.next_first_seen, r.next_last_seen, r.pct_change, r.prev_source, r.next_source, r.screen_result,
           r.screen_reasons, r.rules_version, r.review_status, r.reviewer, r.review_ref, 'snapshot_backfill'
    from jsonb_to_recordset(p_rows) as r(listing_id text, identity_url text, prev_price numeric, prev_first_seen date,
         prev_last_seen date, next_price numeric, next_first_seen date, next_last_seen date, pct_change numeric,
         prev_source text, next_source text, screen_result text, screen_reasons text, rules_version text,
         review_status text, reviewer text, review_ref text)
    join private.price_identity i on i.identity_url = r.identity_url
    on conflict (identity_id, next_first_seen) do update
      set screen_result = excluded.screen_result, screen_reasons = excluded.screen_reasons,
          rules_version = excluded.rules_version, review_status = excluded.review_status,
          reviewer = excluded.reviewer, review_ref = excluded.review_ref
      where private.price_change.origin = 'snapshot_backfill'
        and (private.price_change.screen_result, private.price_change.screen_reasons, private.price_change.rules_version,
             private.price_change.review_status, private.price_change.reviewer, private.price_change.review_ref)
            is distinct from (excluded.screen_result, excluded.screen_reasons, excluded.rules_version,
                              excluded.review_status, excluded.reviewer, excluded.review_ref);
  else
    raise exception 'unknown kind %', p_kind;
  end if;
  get diagnostics v_rows = row_count;
  return v_rows;
end $$;

-- 9. Access: nothing for the API roles.
revoke all on all tables in schema private from public, anon, authenticated;
revoke all on function private.record_price_observation(), private.detect_price_changes(),
                       private.identity_url(text), public.price_history_load(text, jsonb)
  from public, anon, authenticated;
grant usage on schema private to service_role;
grant execute on function private.detect_price_changes(), public.price_history_load(text, jsonb) to service_role;
grant select on private.price_source_file, private.price_identity, private.price_evidence, private.price_change,
                private.v_price_history_public to service_role;

commit;

-- Nightly detection, scheduled separately once approved (after the scrapes, before the page refresh):
-- select cron.schedule('detect-price-changes', '55 14 * * *', 'select private.detect_price_changes()');
