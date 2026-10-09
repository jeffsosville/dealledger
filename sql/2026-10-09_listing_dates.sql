-- 2026-10-09 Listing dates: one model.
-- Every day a crawl sees a listing is a row in listing_sighting (append-only).
-- listing_dates (rebuilt nightly 14:50 UTC) derives first_seen, last_seen,
-- observed/floor and ended_on from it. Method: docs/METHODOLOGY.md#date-recovery.
--
-- Parts 1 and 2 were applied Oct 9 in the SQL editor. Part 3 switches the site.

-- ===== Part 1: sightings (applied) =====
create table if not exists public.listing_sighting (
  row_id  text not null,
  seen_on date not null,
  source  text not null check (source in ('crawl','archive','observation','seed_first_seen','seed_last_seen')),
  primary key (row_id, seen_on)
);
alter table public.listing_sighting enable row level security;
revoke all on public.listing_sighting from anon, authenticated;

create or replace function public.record_listing_sighting()
returns trigger language plpgsql security definer set search_path = public, pg_temp as $$
begin
  if new.last_seen is not null
     and (tg_op = 'INSERT' or new.last_seen is distinct from old.last_seen) then
    insert into public.listing_sighting(row_id, seen_on, source)
    values (new.id, new.last_seen::date, 'crawl')
    on conflict do nothing;
  end if;
  return null;
end $$;

drop trigger if exists trg_listings_direct_record_sighting on public.listings_direct;
create trigger trg_listings_direct_record_sighting
  after insert or update of last_seen on public.listings_direct
  for each row execute function public.record_listing_sighting();

-- One-time seeds (applied): listing_observation, then first_seen and last_seen of every row,
-- then data/snapshots via scripts/backfill_sightings_archive.py.

-- ===== Part 2: listing_dates (applied) =====
create table if not exists public.listing_dates (
  listing_key     text primary key,
  broker_domain   text not null,
  row_ids         text[] not null,
  first_seen      date not null,
  last_seen       date not null,
  listed_on_basis text not null check (listed_on_basis in ('observed','floor')),
  listed_after    date,
  status          text not null,
  published       boolean not null,
  ended_on        date,
  built_at        timestamptz not null default now()
);
alter table public.listing_dates enable row level security;
revoke all on public.listing_dates from anon, authenticated;

create or replace function public.refresh_listing_dates()
returns integer language plpgsql security definer set search_path = public, pg_temp as $$
declare v_rows integer;
begin
  create temp table t_ident on commit drop as
  with base as (
    select id, regexp_replace(lower(broker_domain), '^www\.', '') dom, status, published, delisted_at,
           url, title, asking_price, norm_listing_url(url) nu, coalesce(url_is_listing_specific, true) spec
    from listings_direct
    where broker_domain is not null and status in ('active','sold','inactive','superseded')
  ), shared as (
    select dom, nu from base where nu is not null group by 1,2 having count(distinct lower(title)) > 1
  )
  select b.*, b.dom || '|' || case
           when b.nu is not null and b.spec and s.nu is null then 'u:' || b.nu
           else 't:' || lower(regexp_replace(coalesce(b.title,''), '[^a-zA-Z0-9]+', '', 'g'))
                || ':' || coalesce(round(b.asking_price)::text, '') end lkey
  from base b left join shared s on s.dom = b.dom and s.nu = b.nu;
  create index on t_ident (id);

  create temp table t_sight on commit drop as
    select i.lkey, i.dom, s.seen_on, s.source from listing_sighting s join t_ident i on i.id = s.row_id;

  create temp table t_k on commit drop as
    select lkey, dom, min(seen_on) fs, max(seen_on) ls from t_sight group by 1,2;

  create temp table t_cday on commit drop as
    select dom, d, max(n) n from (
      select dom, seen_on d, count(distinct lkey) n
        from t_sight where source in ('crawl','archive','observation') group by 1,2
      union all
      select regexp_replace(lower(broker_domain), '^www\.', ''), started_at::date, max(listings_seen)
        from crawl_run where status = 'ok' and listings_seen > 0 group by 1,2
    ) x group by 1,2;

  create temp table t_cc on commit drop as
    select dom, d from (
      select *, n >= greatest(3, 0.8 * max(n) over (partition by dom order by d
                 range between interval '30 days' preceding and interval '30 days' following)) ok
      from t_cday) z
    where ok;
  create index on t_cc (dom, d);

  create temp table t_d on commit drop as
    select k.*, nc.new_n, cd.n day_n,
           (select max(c.d) from t_cc c where c.dom = k.dom and c.d < k.fs and c.d >= k.fs - 14) prev_complete
    from t_k k
    join (select dom, fs, count(*) new_n from t_k group by 1,2) nc on nc.dom = k.dom and nc.fs = k.fs
    left join t_cday cd on cd.dom = k.dom and cd.d = k.fs;

  truncate public.listing_dates;
  insert into public.listing_dates
    (listing_key, broker_domain, row_ids, first_seen, last_seen, listed_on_basis, listed_after,
     status, published, ended_on)
  select d.lkey, d.dom, r.row_ids, d.fs, d.ls,
         case when d.prev_complete is not null and d.new_n <= greatest(10, 0.2 * coalesce(d.day_n, 0))
              then 'observed' else 'floor' end,
         case when d.prev_complete is not null and d.new_n <= greatest(10, 0.2 * coalesce(d.day_n, 0))
              then d.prev_complete end,
         r.status, r.published, case when r.status <> 'active' then r.ended_on end
  from t_d d
  join (
    select lkey, array_agg(id order by id) row_ids,
           case when bool_or(status = 'active') then 'active'
                when bool_or(status = 'sold') then 'sold'
                when bool_or(status = 'inactive') then 'inactive' else 'superseded' end status,
           bool_or(status = 'active' and published) published,
           max(delisted_at)::date ended_on
    from t_ident group by lkey
  ) r using (lkey);
  get diagnostics v_rows = row_count;
  return v_rows;
end $$;

select public.refresh_listing_dates();

select cron.schedule('refresh-listing-dates', '50 14 * * *',
  $$set statement_timeout = '10min'; select public.refresh_listing_dates();$$);


-- ===== Part 3: site reads listing_dates; DOM views off the public API =====
-- mv_listings_page takes listed_on and dom_basis from v_direct_dom. Same columns, new source.
create or replace view public.v_direct_dom as
select distinct on (synth) synth, observed_first_seen, broker_onboarded, basis
from (
  select 900000000 + (('x' || substr(md5(d.id), 1, 7)))::bit(28)::bigint as synth,
         ld.first_seen as observed_first_seen,
         b.broker_onboarded,
         case when ld.listed_on_basis = 'observed' then 'observed' else 'floor' end as basis
  from public.listing_dates ld
  cross join lateral unnest(ld.row_ids) as r(row_id)
  join public.listings_direct d on d.id = r.row_id
  join public.v_broker_onboarded b on b.broker_domain = d.broker_domain
  where d.status = 'active'
) s
order by synth, observed_first_seen;

revoke select on public.dom_daily, public.v_direct_dom, public.v_dom_direct, public.v_dom_display_direct,
  public.v_listing_dom_direct, public.v_broker_dom, public.v_dom_coverage from anon, authenticated;
revoke execute on function public.snapshot_dom_daily() from anon, authenticated, public;

-- Rebuild the homepage view now instead of waiting for 15:25 UTC (~2-3 min).
set statement_timeout = '10min';
select refresh_listings_page();

select dom_basis, count(*) from mv_listings_page group by 1 order by 2 desc;
