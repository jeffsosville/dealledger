-- 2026-11-09 mcp_listing(ln) — read-only, anon-executable listing record for the DealLedger MCP.
-- Approved by Jeff Nov 9: RPC only; MCP route and directory submission on hold.
-- Scope: published, active, broker-direct listings_direct rows only. No legacy `listings`,
-- no marketplace data, no interpolated ages, no relist fields, no removal history (published = active only today).
-- Rollback: drop function if exists public.mcp_listing(bigint);

create or replace function public.mcp_listing(ln bigint)
returns jsonb
language plpgsql
stable
security definer
set search_path = public, pg_temp
as $fn$
declare
  marketplace constant text[] := array[
    'bizbuysell.com','bizquest.com','businessesforsale.com','loopnet.com','dealstream.com',
    'bizben.com','businessbroker.net','globalbx.com','empireflippers.com','quietlight.com',
    'flippa.com','interexo.com','businessmart.com','bizmls.com','commercialmls.com'];
  ids   text[];
  d     record;
  a     record;
  obs   record;
  age   jsonb;
  bdate jsonb;
  steps jsonb;
begin
  if ln is null or ln < 900000000 or ln >= 900000000 + 268435456 then
    return jsonb_build_object('found', false, 'listing_number', ln);
  end if;

  select array_agg(x.id) into ids
  from listings_direct x
  where x.published is true
    and x.status = 'active'
    and x.source = 'broker_direct'
    and x.url is not null
    and regexp_replace(lower(coalesce(x.broker_domain, '')), '^www\.', '') <> all (marketplace)
    and not exists (select 1 from broker_block b
                    where regexp_replace(lower(b.broker_domain), '^www\.', '')
                        = regexp_replace(lower(x.broker_domain), '^www\.', ''))
    and 900000000 + (('x' || substr(md5(x.id), 1, 7))::bit(28))::bigint = ln;

  if ids is null or array_length(ids, 1) <> 1 then
    return jsonb_build_object('found', false, 'listing_number', ln);
  end if;

  select x.id, x.title, x.url, x.broker_domain, x.city, x.state_norm, x.vertical,
         x.asking_price, x.cash_flow, x.revenue, x.last_seen,
         x.broker_published_on, x.broker_date_source, x.broker_date_first_recorded
    into d
  from listings_direct x where x.id = ids[1];

  -- Listing age: evidence hierarchy from listing_age_v2 (levels 2 and 4 today).
  select la.age_basis, la.age_date, la.listed_after into a
  from listing_age_v2 la where la.listing_number = ln;

  age := case a.age_basis
    when 'window' then jsonb_build_object(
      'basis', 'appearance_window',
      'listed_after', a.listed_after, 'listed_by', a.age_date,
      'label', 'Listed between ' || to_char(a.listed_after, 'Mon FMDD, YYYY') || ' and ' || to_char(a.age_date, 'Mon FMDD, YYYY'),
      'method', 'Appearance window from complete crawls of the broker''s site')
    when 'observed' then jsonb_build_object(
      'basis', 'dealledger_first_observed',
      'date', a.age_date,
      'label', 'Observed by DealLedger since ' || to_char(a.age_date, 'Mon FMDD, YYYY'),
      'method', 'First dated sighting in a DealLedger crawl; the listing may be older')
    when 'in_db' then jsonb_build_object(
      'basis', 'dealledger_first_recorded',
      'date', a.age_date,
      'label', 'In DealLedger since ' || to_char(a.age_date, 'Mon FMDD, YYYY'),
      'method', 'Date the record was first written to the DealLedger database; the listing may be older')
    else null end;

  -- Level 1: broker-stated date, reported separately and never merged into listing_age.
  if d.broker_published_on is not null then
    bdate := jsonb_build_object(
      'date', d.broker_published_on,
      'source', d.broker_date_source,
      'first_recorded', d.broker_date_first_recorded,
      'label', 'Broker-reported publication date; not verified by DealLedger');
  end if;

  -- Dated observations: DealLedger crawls and observations only (no seeded or archive rows).
  select min(s.seen_on) first_on, max(s.seen_on) last_on, count(distinct s.seen_on) days
    into obs
  from listing_sighting s
  where s.row_id = d.id and s.source in ('crawl', 'observation');

  -- Asking-price changes: same publication filter as the public price_history view.
  select coalesce(jsonb_agg(jsonb_build_object(
           'from_price', c.prev_price,
           'to_price', c.next_price,
           'pct_change', c.pct_change,
           'changed_after', c.prev_last_seen,
           'changed_by', c.next_first_seen,
           'basis', 'Change observed between two DealLedger crawls of the broker''s page')
         order by c.next_first_seen), '[]'::jsonb)
    into steps
  from private.price_change c
  where c.listing_id = d.id
    and c.review_status = 'approved'
    and c.screen_result in ('pass', 'review');

  return jsonb_build_object(
    'found', true,
    'listing_number', ln,
    'status', 'active',
    'title', d.title,
    'broker_domain', regexp_replace(lower(d.broker_domain), '^www\.', ''),
    'broker_url', d.url,
    'city', nullif(btrim(d.city), ''),
    'state', d.state_norm,
    'vertical', nullif(d.vertical, ''),
    'asking_price', d.asking_price,
    'cash_flow', d.cash_flow,
    'revenue', d.revenue,
    'listing_age', age,
    'broker_stated_date', bdate,
    'observation', jsonb_build_object(
      'first_observed', obs.first_on,
      'last_observed', obs.last_on,
      'distinct_days_observed', obs.days),
    'price_history', steps,
    'dealledger_url', 'https://dealledger.org/listing/' || ln,
    'source', 'DealLedger broker-direct index',
    'license', 'CC0-1.0',
    'as_of', now());
end;
$fn$;

revoke all on function public.mcp_listing(bigint) from public;
grant execute on function public.mcp_listing(bigint) to anon, authenticated;

comment on function public.mcp_listing(bigint) is
  'DealLedger MCP: one published active broker-direct listing. Read-only. Age from listing_age_v2 evidence hierarchy; broker-stated date separate; observations from crawl/observation sightings; price steps approved only. Added 2026-11-09.';
