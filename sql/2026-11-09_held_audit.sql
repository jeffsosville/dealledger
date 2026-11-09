-- 2026-11-09_held_audit.sql — support for agents/held_audit.py
-- Settles listings the removal rule holds (broker's candidates > 30% of its
-- active listings) without hand counts. Additive; rollback at the bottom.
--
-- Verdicts written by apply_held_audit():
--   observed      own page live AND linked from the broker's site  -> last_seen = today,
--                 sighting source 'page_visit' (no price observation is recorded)
--   gone_404 / gone_redirect                                       -> inactive (as stale_check)
--   sold_text     broker's own sold label on the page              -> sold (as stale_check)
--   unlisted      page live, but not linked anywhere on a site
--                 whose links cover >=80% of the current crawl     -> inactive, "left observation";
--                 restored automatically if a crawl sees it again (observation_exit)

create table if not exists private.held_audit (
  run_at        timestamptz not null,
  listing_id    text not null,
  broker_domain text not null,
  verdict       text not null,
  detail        text not null default '',
  primary key (run_at, listing_id)
);

create table if not exists private.held_audit_broker (
  run_at         timestamptz not null,
  broker_domain  text not null,
  listing_url    text,
  site_links     int,
  crawl_recent   int,
  coverage       numeric,
  held           int,
  observed       int,
  gone           int,
  unlisted       int,
  unresolved     int,
  new_urls       int,
  new_url_sample text[],
  exception      text not null default '',
  primary key (run_at, broker_domain)
);

-- Sightings carry their source; a page visit is not a crawl.
alter table public.listing_sighting drop constraint if exists listing_sighting_source_check;
alter table public.listing_sighting add constraint listing_sighting_source_check
  check (source = any (array['crawl','archive','observation','seed_first_seen','seed_last_seen','page_visit']));
create or replace function public.record_listing_sighting()
 returns trigger language plpgsql security definer set search_path to 'public', 'pg_temp'
as $function$
begin
  if new.last_seen is not null
     and (tg_op = 'INSERT' or new.last_seen is distinct from old.last_seen) then
    insert into public.listing_sighting(row_id, seen_on, source)
    values (new.id, new.last_seen::date,
            coalesce(nullif(current_setting('dealledger.sighting_source', true), ''), 'crawl'))
    on conflict do nothing;
  end if;
  return null;
end $function$;

-- Price observations come from crawls only; a page visit re-reads no price.
create or replace function private.record_price_observation()
 returns trigger language plpgsql security definer set search_path to 'public', 'private', 'pg_temp'
as $function$
declare v_url text; v_identity int;
begin
  if coalesce(current_setting('dealledger.skip_price_obs', true), '') = 'on' then return new; end if;
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
                                      asking_price, cash_flow, revenue, title_head, exclusion, city, state)
  values (0, 0, new.last_seen::date, new.id, v_identity, new.asking_price, new.cash_flow, new.revenue,
          left(new.title, 300), case when new.asking_price is null then 'no price' else '' end,
          nullif(trim(new.city), ''), nullif(upper(trim(new.state)), ''))
  on conflict (observed_on, listing_id_seen) where file_id = 0
  do update set asking_price = excluded.asking_price, exclusion = excluded.exclusion
  where private.price_evidence.asking_price is null and excluded.asking_price is not null;
  return new;
exception when others then
  raise warning 'record_price_observation(%): %', new.id, sqlerrm;
  return new;
end $function$;

-- Held listings: removal-rule candidates on brokers over the 30% guard.
create or replace function public.held_audit_queue(p_domain text default null,
                                                   p_offset int default 0, p_limit int default 1000)
returns table (listing_id text, broker_domain text, url text, title text, listing_url text)
language sql volatile security definer set search_path to 'public','private','pg_temp' as $$
  with c as (select a.*, regexp_replace(lower(a.broker_domain), '^www\.', '') dom
             from private.absence_candidates() a),
       cc as (select dom, count(*) cand from c group by dom),
       act as (select regexp_replace(lower(l.broker_domain), '^www\.', '') dom, count(*) act
               from listings_direct l where l.status = 'active' group by 1),
       held as (select cc.dom from cc join act using (dom) where cc.cand > 0.30 * act.act)
  select c.listing_id, c.broker_domain, l.url, l.title,
         (select s.listing_url from broker_sources s
           where regexp_replace(lower(s.domain), '^www\.', '') = c.dom and s.listing_url is not null
           order by (s.discovery_stage = '4_producing') desc limit 1)
  from c join held using (dom)
  join listings_direct l on l.id = c.listing_id
  where p_domain is null or c.dom = regexp_replace(lower(p_domain), '^www\.', '')
  order by c.dom, c.listing_id
  offset p_offset limit p_limit
$$;

-- One call per run: items = [{id, broker_domain, verdict, detail}], brokers = [{...summary}].
create or replace function public.apply_held_audit(p_items jsonb, p_brokers jsonb default '[]')
returns jsonb language plpgsql security definer set search_path to 'public','private','pg_temp' as $$
declare v_run timestamptz := now(); v_obs int := 0; v_gone int := 0; v_sold int := 0; v_unl int := 0;
begin
  create temp table _i on commit drop as
  select x->>'id' id, x->>'broker_domain' dom, x->>'verdict' verdict, left(coalesce(x->>'detail', ''), 300) detail
  from jsonb_array_elements(p_items) x;

  insert into private.held_audit (run_at, listing_id, broker_domain, verdict, detail)
  select v_run, id, dom, verdict, detail from _i on conflict do nothing;

  insert into private.held_audit_broker (run_at, broker_domain, listing_url, site_links, crawl_recent, coverage,
         held, observed, gone, unlisted, unresolved, new_urls, new_url_sample, exception)
  select v_run, b->>'broker_domain', b->>'listing_url', (b->>'site_links')::int, (b->>'crawl_recent')::int,
         (b->>'coverage')::numeric, (b->>'held')::int, (b->>'observed')::int, (b->>'gone')::int,
         (b->>'unlisted')::int, (b->>'unresolved')::int, (b->>'new_urls')::int,
         array(select jsonb_array_elements_text(coalesce(b->'new_url_sample', '[]'))),
         coalesce(b->>'exception', '')
  from jsonb_array_elements(p_brokers) b on conflict do nothing;

  -- observed: a dated observation from the listing's own live, linked page
  perform set_config('dealledger.sighting_source', 'page_visit', true);
  perform set_config('dealledger.skip_price_obs', 'on', true);
  update listings_direct l set last_seen = v_run, stale_checked_at = v_run, stale_result = 'live_linked'
    from _i where _i.id = l.id and _i.verdict = 'observed' and l.status = 'active';
  get diagnostics v_obs = row_count;
  perform set_config('dealledger.sighting_source', '', true);
  perform set_config('dealledger.skip_price_obs', '', true);

  -- gone / broker sold label: same writes as agents/stale_check.py
  insert into quarantine_log (id, prior_status, new_status, reason, title, url, at)
  select l.id, 'active', case when _i.verdict = 'sold_text' then 'sold' else 'inactive' end,
         'held audit: ' || _i.verdict || coalesce(' ' || nullif(_i.detail, ''), ''), l.title, l.url, v_run
  from _i join listings_direct l on l.id = _i.id
  where _i.verdict in ('gone_404', 'gone_redirect', 'sold_text') and l.status = 'active';
  update listings_direct l set status = 'inactive', delisted_at = v_run, stale_checked_at = v_run, stale_result = _i.verdict
    from _i where _i.id = l.id and _i.verdict in ('gone_404', 'gone_redirect') and l.status = 'active';
  get diagnostics v_gone = row_count;
  update listings_direct l set status = 'sold', delisted_at = v_run, stale_checked_at = v_run, stale_result = 'sold_text'
    from _i where _i.id = l.id and _i.verdict = 'sold_text' and l.status = 'active';
  get diagnostics v_sold = row_count;

  -- unlisted: left observation, restorable if a crawl sees it again
  insert into private.observation_exit (listing_id, broker_domain, last_seen, run_ids, run_counts, max_45d)
  select a.listing_id, a.broker_domain, a.last_seen, a.run_ids, a.run_counts, a.max_45d
  from private.absence_candidates() a join _i on _i.id = a.listing_id and _i.verdict = 'unlisted'
  on conflict do nothing;
  insert into quarantine_log (id, prior_status, new_status, reason, title, url, at)
  select l.id, 'active', 'inactive',
         'left observation: absent from 3 complete crawls and not linked from the broker''s site (held audit)'
         || coalesce('; ' || nullif(_i.detail, ''), ''), l.title, l.url, v_run
  from _i join listings_direct l on l.id = _i.id
  where _i.verdict = 'unlisted' and l.status = 'active';
  update listings_direct l set status = 'inactive', stale_checked_at = v_run, stale_result = 'unlisted'
    from _i where _i.id = l.id and _i.verdict = 'unlisted' and l.status = 'active';
  get diagnostics v_unl = row_count;

  return jsonb_build_object('run_at', v_run, 'observed', v_obs, 'gone', v_gone, 'sold_label', v_sold, 'unlisted', v_unl);
end $$;

revoke all on function public.held_audit_queue(text, int, int) from public, anon, authenticated;
revoke all on function public.apply_held_audit(jsonb, jsonb) from public, anon, authenticated;
grant execute on function public.held_audit_queue(text, int, int) to service_role;
grant execute on function public.apply_held_audit(jsonb, jsonb) to service_role;

-- Rollback (run by hand):
--   drop function public.apply_held_audit(jsonb, jsonb);
--   drop function public.held_audit_queue(text, int, int);
--   restore record_listing_sighting / record_price_observation from their
--   pre-2026-11-09 definitions (source literal 'crawl'; no skip_price_obs check);
--   drop table private.held_audit_broker; drop table private.held_audit;
--   Listings set by a run: select * from private.held_audit where run_at = '<run>';
