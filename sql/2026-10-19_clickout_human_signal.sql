-- 2026-10-19 Click-out tracking: count only real human clicks.
--
-- Problem: /go logged ~10,000 "non-bot" click-outs a day (Oct 14-19), spread
-- across ~8,000 different listings, mostly with no referrer. robots.txt
-- already disallows /go/ and the redirect sends X-Robots-Tag: noindex,
-- nofollow, so these are crawlers that ignore robots and send a browser-like
-- user agent. dl_is_bot() only reads the user agent, so it let them through
-- and the 1,000/month goal could not be measured.
--
-- Fix: the /go routes now pass p_human, true only when the request carries the
-- headers a real browser sends for a user-initiated click on dealledger.org:
--   Sec-Fetch-User: ?1   and   Sec-Fetch-Site: same-origin | same-site
-- Crawlers fetching /go URLs from HTML don't send these. A click counts only
-- if human = true and the user agent isn't a known bot.
--
-- Rows from before this change have human = false: there is no way to tell
-- which of them were people, so they drop out of the counts rather than
-- inflate them. The user agent is now stored (truncated) so the next
-- misclassification can be diagnosed from the table instead of guessed.
--
-- APPLIED to production on 2026-10-19.

alter table public.listing_clicks add column if not exists ua    text;
alter table public.listing_clicks add column if not exists human boolean not null default false;

-- Drop + recreate (not create or replace): adding a parameter to an existing
-- function would leave two overloads, and calls with named args would become
-- ambiguous. p_human defaults to null, so the old 4/5-argument calls from the
-- currently deployed site keep working (and count as not human) until the
-- new code is live.
drop function if exists public.dl_click(bigint, text, text, text);
drop function if exists public.dl_click_broker(text, text, text, text, text);

create function public.dl_click(
  p_listing_number bigint,
  p_source text default null,
  p_referrer text default null,
  p_ua text default null,
  p_human boolean default null
) returns text
language plpgsql security definer set search_path = public as $$
declare
  v_url text;
  v_bot boolean := public.dl_is_bot(p_ua);
begin
  select coalesce(m.source_url, l.url) into v_url
  from public.listings l
  left join public.mv_listings_page m on m.listing_number = l.listing_number
  where l.listing_number = p_listing_number
  limit 1;

  if v_url is null or v_url !~* '^https?://' then
    return null;
  end if;

  insert into public.listing_clicks
    (listing_number, broker_domain, target_url, kind, source_page, referrer_host, is_bot, ua, human)
  values (p_listing_number, public.dl_bare_host(v_url), v_url, 'listing',
          left(p_source, 32), public.dl_bare_host(p_referrer), v_bot,
          left(p_ua, 300), coalesce(p_human, false) and not v_bot);

  return v_url;
end $$;

create function public.dl_click_broker(
  p_slug text,
  p_kind text default 'home',
  p_source text default null,
  p_referrer text default null,
  p_ua text default null,
  p_human boolean default null
) returns text
language plpgsql security definer set search_path = public as $$
declare
  v_domain text;
  v_url    text;
  v_bot    boolean := public.dl_is_bot(p_ua);
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

  insert into public.listing_clicks
    (broker_domain, target_url, kind, source_page, referrer_host, is_bot, ua, human)
  values (public.dl_bare_host(coalesce(v_domain, v_url)), v_url,
          case when p_kind = 'listings' then 'broker_listings' else 'broker_home' end,
          left(p_source, 32), public.dl_bare_host(p_referrer), v_bot,
          left(p_ua, 300), coalesce(p_human, false) and not v_bot);

  return v_url;
end $$;

revoke all on function public.dl_click(bigint, text, text, text, boolean) from public;
revoke all on function public.dl_click_broker(text, text, text, text, text, boolean) from public;
grant execute on function public.dl_click(bigint, text, text, text, boolean) to anon, authenticated, service_role;
grant execute on function public.dl_click_broker(text, text, text, text, text, boolean) to anon, authenticated, service_role;

-- Reporting views now count human clicks only.
create or replace view public.v_clicks_monthly as
select date_trunc('month', clicked_at)::date as month,
       count(*)                                  as clicks,
       count(distinct broker_domain)             as brokers_reached,
       count(*) filter (where kind = 'listing')  as listing_clicks
from public.listing_clicks
where human and coalesce(source_page, '') <> 'test'
group by 1
order by 1 desc;

create or replace view public.v_clicks_by_broker_month as
select date_trunc('month', clicked_at)::date as month,
       broker_domain,
       count(*)                       as clicks,
       count(distinct listing_number) as listings_clicked
from public.listing_clicks
where human and coalesce(source_page, '') <> 'test'
group by 1, 2
order by 1 desc, 3 desc;

-- Daily check: human clicks vs everything else, so the filter can be watched.
create or replace view public.v_clicks_daily as
select clicked_at::date as day,
       count(*) filter (where human)                    as human_clicks,
       count(distinct listing_number) filter (where human) as human_listings,
       count(*) filter (where not human and not is_bot) as unverified,
       count(*) filter (where is_bot)                   as known_bots
from public.listing_clicks
where coalesce(source_page, '') <> 'test'
group by 1
order by 1 desc;

revoke all on public.v_clicks_monthly, public.v_clicks_by_broker_month, public.v_clicks_daily from anon, authenticated;
