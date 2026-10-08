-- 2026-10-08 Broker registry rebuilt on broker-direct observations.
--
-- The old /brokers page read `broker_firms` (a legacy firm table) and counted
-- listings by `firm_key`, which no broker-direct listing carries. Every count
-- was stale, and PostgREST's 1,000-row cap truncated both the firm list and
-- the counts. The new pages read `broker_directory`: one row per broker
-- website whose published listings we have observed.
--
-- APPLIED to production on 2026-10-08, in this order:
--   1. broker_registry_overrides table + seed rows   (below)
--   2. broker_registry materialized view             (first draft; SUPERSEDED,
--      unused by the site. Safe to drop:  drop materialized view public.broker_registry;)
--   3. broker_directory materialized view            (what the site reads)
--   4. refresh_broker_registry() refreshes broker_directory; cron 15:35 UTC
--
-- Floors: broker_directory.tracking_since is the first day we read each broker's
-- site. The pages treat any listing first seen within 14 days of that date as a
-- floor ("listed by"), because a site's existing inventory trickles in during
-- the first crawls. (The view's own active_floored / median_days_observed
-- columns use a strict "after tracking_since" rule and are not displayed.)
--
-- ── Manual corrections (display name, HQ, network flag, outreach hold) ──────
create table if not exists public.broker_registry_overrides (
  domain        text primary key,      -- bare host, no www
  display_name  text,
  hq_city       text,
  hq_state      text,
  is_network    boolean not null default false,  -- franchise/association domain
  outreach_hold boolean not null default false,  -- never include in outreach
  about         text,                  -- short factual description, from their own site
  about_source  text,                  -- URL the description came from
  updated_at    timestamptz not null default now()
);
alter table public.broker_registry_overrides enable row level security;

insert into public.broker_registry_overrides (domain, display_name, is_network, outreach_hold) values
  ('tworld.com',           'Transworld Business Advisors',            true, true),
  ('sunbeltnetwork.com',   'Sunbelt Business Brokers',                true, true),
  ('murphybusiness.com',   'Murphy Business Sales',                   true, true),
  ('fcbb.com',             'First Choice Business Brokers',           true, true),
  ('linkbusiness.com',     'LINK Business',                           true, true),
  ('gabb.org',             'Georgia Association of Business Brokers', true, true)
on conflict (domain) do nothing;

-- ── Directory (what the site reads) ─────────────────────────────────────────
create materialized view if not exists public.broker_directory as
with onb as (
  select lower(regexp_replace(broker_domain, '^www\.', '')) as domain, min(broker_onboarded) as tracking_since
  from public.v_broker_onboarded group by 1),
l0 as (
  select lower(regexp_replace(substring(url from '^https?://([^/:?#]+)'), '^www\.', '')) as domain,
    is_active, first_seen, last_seen, price, upper(state) as state
  from public.listings where source = 'broker_direct' and url is not null),
l as (
  select l0.*, o.tracking_since,
    (o.tracking_since is not null and l0.first_seen::date > o.tracking_since) as observed_start
  from l0 left join onb o using (domain)),
agg as (
  select domain, min(tracking_since) as tracking_since,
    count(*) filter (where is_active) as active_count,
    count(*) filter (where not is_active and last_seen > now() - interval '180 days') as removed_180d,
    count(*) as lifetime_count,
    min(first_seen) as first_observed, max(last_seen) as last_observed,
    percentile_cont(0.5) within group (order by price) filter (where is_active and price > 0) as median_asking,
    percentile_cont(0.5) within group (order by (current_date - first_seen::date)) filter (where is_active and observed_start) as median_days_observed,
    count(*) filter (where is_active and observed_start) as active_with_observed_start,
    count(*) filter (where is_active and not observed_start) as active_floored
  from l group by domain),
states as (
  select domain, string_agg(state, ', ' order by n desc, state) as states_listed
  from (select domain, state, count(*) n from l
        where is_active and state = any (array['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','PR','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'])
        group by 1, 2) s group by domain),
src as (
  select distinct on (lower(regexp_replace(domain, '^www\.', '')))
    lower(regexp_replace(domain, '^www\.', '')) as domain, company_name, broker_name, city, state, homepage_url, listing_url,
    coalesce(ibba_member, false) as ibba_member
  from public.broker_sources where domain is not null
  order by lower(regexp_replace(domain, '^www\.', '')), (discovery_stage = '4_producing') desc, updated_at desc)
select regexp_replace(a.domain, '[^a-z0-9]+', '-', 'g') as slug, a.domain,
  coalesce(o.display_name, nullif(s.company_name, ''), nullif(s.broker_name, ''), a.domain) as firm_name,
  (o.display_name is not null) as name_verified,
  case when o.is_network then null else coalesce(o.hq_city, nullif(nullif(lower(s.city), 'unknown'), '')) end as hq_city,
  case when o.is_network then null else upper(coalesce(o.hq_state, nullif(nullif(lower(s.state), 'unknown'), ''))) end as hq_state,
  coalesce(o.is_network, false) as is_network,
  coalesce(nullif(s.homepage_url, ''), 'https://' || a.domain) as homepage_url,
  s.listing_url as listings_page_url, coalesce(s.ibba_member, false) as ibba_member,
  o.about, o.about_source,
  a.tracking_since,
  a.active_count, a.removed_180d, a.lifetime_count, a.first_observed, a.last_observed, a.median_asking,
  round(a.median_days_observed::numeric) as median_days_observed, a.active_with_observed_start, a.active_floored,
  st.states_listed, now() as refreshed_at
from agg a
left join src s on s.domain = a.domain
left join public.broker_registry_overrides o on o.domain = a.domain
left join states st on st.domain = a.domain
where a.domain is not null
  and not exists (select 1 from public.broker_block b where lower(regexp_replace(b.broker_domain, '^www\.', '')) = a.domain);

create unique index if not exists broker_directory_slug_idx on public.broker_directory (slug);
create index if not exists broker_directory_active_idx on public.broker_directory (active_count desc);
grant select on public.broker_directory to anon, authenticated;

-- ── Nightly refresh, after the bridge (15:00) and homepage refresh (15:25) ──
create or replace function public.refresh_broker_registry()
returns void language sql security definer set search_path = public as $$
  refresh materialized view concurrently public.broker_directory;
$$;
revoke execute on function public.refresh_broker_registry() from public, anon, authenticated;

select cron.schedule('refresh-broker-registry', '35 15 * * *',
  $$set statement_timeout = '10min'; select public.refresh_broker_registry();$$);
