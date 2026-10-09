-- 2026-10-08 Outbound click tracking.
--
-- Goal: count buyers DealLedger sends to broker websites (target: 1,000 broker
-- click-outs per month) and give each broker a monthly "we sent you N buyers".
--
-- Every outbound link on dealledger.org points at /go/{listing_number} or
-- /go/b/{slug}. Those Next.js routes call dl_click / dl_click_broker, which
-- log one row and return the destination. The destination always comes from
-- the database, never from the request, so /go cannot be used as an open
-- redirect.
--
-- listing_clicks is private (RLS on, no policies). Only the two SECURITY
-- DEFINER functions write to it. Bots and link previewers are logged with
-- is_bot = true and excluded from every count, as are rows with
-- source_page = 'test' (two verification rows written 2026-10-08).
--
-- APPLIED to production on 2026-10-08.

create table if not exists public.listing_clicks (
  id              bigserial primary key,
  clicked_at      timestamptz not null default now(),
  listing_number  bigint,                 -- null for broker-site clicks
  broker_domain   text,                   -- bare host, no www
  target_url      text not null,
  kind            text not null,          -- 'listing' | 'broker_home' | 'broker_listings'
  source_page     text,                   -- 'home' | 'listing' | 'broker' | 'seo' ...
  referrer_host   text,
  is_bot          boolean not null default false
);
alter table public.listing_clicks enable row level security;

create index if not exists listing_clicks_at_idx     on public.listing_clicks (clicked_at);
create index if not exists listing_clicks_domain_idx on public.listing_clicks (broker_domain, clicked_at);
create index if not exists listing_clicks_listing_idx on public.listing_clicks (listing_number);

create or replace function public.dl_bare_host(u text)
returns text language sql immutable as $$
  select nullif(lower(regexp_replace(split_part(regexp_replace(coalesce(u, ''), '^[a-z]+://', '', 'i'), '/', 1),
                                     '^www\.|:\d+$', '', 'gi')), '')
$$;

create or replace function public.dl_is_bot(ua text)
returns boolean language sql immutable as $$
  select ua is null or ua = '' or ua ~* '(bot|crawl|spider|slurp|preview|facebookexternalhit|embedly|curl|wget|python|httpclient|headless|lighthouse|monitor|scan)'
$$;

-- Listing click-out. Returns the broker's listing URL, or null if unknown.
create or replace function public.dl_click(
  p_listing_number bigint,
  p_source text default null,
  p_referrer text default null,
  p_ua text default null
) returns text
language plpgsql security definer set search_path = public as $$
declare
  v_url text;
begin
  select coalesce(m.source_url, l.url) into v_url
  from public.listings l
  left join public.mv_listings_page m on m.listing_number = l.listing_number
  where l.listing_number = p_listing_number
  limit 1;

  if v_url is null or v_url !~* '^https?://' then
    return null;
  end if;

  insert into public.listing_clicks (listing_number, broker_domain, target_url, kind, source_page, referrer_host, is_bot)
  values (p_listing_number, public.dl_bare_host(v_url), v_url, 'listing',
          left(p_source, 32), public.dl_bare_host(p_referrer), public.dl_is_bot(p_ua));

  return v_url;
end $$;

-- Broker-site click-out from a broker page. p_kind: 'home' or 'listings'.
create or replace function public.dl_click_broker(
  p_slug text,
  p_kind text default 'home',
  p_source text default null,
  p_referrer text default null,
  p_ua text default null
) returns text
language plpgsql security definer set search_path = public as $$
declare
  v_domain text;
  v_url    text;
begin
  select d.domain,
         case when p_kind = 'listings' then coalesce(d.listings_page_url, d.homepage_url, 'https://' || d.domain)
              else coalesce(d.homepage_url, 'https://' || d.domain) end
    into v_domain, v_url
  from public.broker_directory d
  where d.slug = lower(p_slug)
  limit 1;

  if v_url is null or v_url !~* '^https?://' then
    return null;
  end if;

  insert into public.listing_clicks (broker_domain, target_url, kind, source_page, referrer_host, is_bot)
  values (public.dl_bare_host(coalesce(v_domain, v_url)), v_url,
          case when p_kind = 'listings' then 'broker_listings' else 'broker_home' end,
          left(p_source, 32), public.dl_bare_host(p_referrer), public.dl_is_bot(p_ua));

  return v_url;
end $$;

revoke all on function public.dl_click(bigint, text, text, text) from public;
revoke all on function public.dl_click_broker(text, text, text, text, text) from public;
grant execute on function public.dl_click(bigint, text, text, text) to anon, authenticated;
grant execute on function public.dl_click_broker(text, text, text, text, text) to anon, authenticated;

-- Reporting (private; read with the service key or the SQL editor).
create or replace view public.v_clicks_monthly as
select date_trunc('month', clicked_at)::date as month,
       count(*)                                  as clicks,
       count(distinct broker_domain)             as brokers_reached,
       count(*) filter (where kind = 'listing')  as listing_clicks
from public.listing_clicks
where not is_bot and coalesce(source_page, '') <> 'test'
group by 1
order by 1 desc;

create or replace view public.v_clicks_by_broker_month as
select date_trunc('month', clicked_at)::date as month,
       broker_domain,
       count(*)                       as clicks,
       count(distinct listing_number) as listings_clicked
from public.listing_clicks
where not is_bot and coalesce(source_page, '') <> 'test'
group by 1, 2
order by 1 desc, 3 desc;

revoke all on public.v_clicks_monthly, public.v_clicks_by_broker_month from anon, authenticated;
