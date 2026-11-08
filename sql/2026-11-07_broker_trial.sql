-- 2026-11-07  Broker trial: let the nightly scraper verify stage-2 brokers, with their
-- inventory held private until broker-level and listing-level checks pass.
--
-- Why: 412 stage-2 'proposed' brokers have a known listings URL but failed the raw-HTML
-- discovery gate (most showed 0 prices because the page is built by JavaScript). The
-- nightly scraper renders pages and uses the proxy, so it is the better verifier.
--
-- Flow
--   1. A cohort of brokers moves to 3_crawlable / strategy_status 'trial'. The nightly
--      export (stages 3+4) picks them up; V6 crawls never-scraped brokers first.
--   2. Every listing a trial broker produces is inserted with needs_review = true, so
--      is_publishable() keeps it off the site (needs_review is sticky against upserts).
--   3. private.judge_broker_trials() (daily, before promote_producing_brokers) decides:
--        pass         -> release the held listings, strategy_status 'ready'; promotion to
--                        4_producing then happens as for any producing broker.
--        fail_quality -> listings stay held; broker back to its prior stage, 'trial_failed'.
--        no_listings  -> 3+ crawl attempts, nothing usable; back to prior stage.
--   4. promote_producing_brokers() skips brokers still on trial.
--
-- Rollback: section at the bottom.

begin;

create table if not exists private.broker_trial (
  domain            text primary key,
  cohort            int not null,
  prior_stage       text,
  prior_status      text,
  listing_url       text,
  started_at        timestamptz not null default now(),
  verdict           text,            -- null = running; pass / fail_quality / no_listings
  verdict_reason    text,
  metrics           jsonb,
  decided_at        timestamptz
);

-- Cohort 1: 50 of the stage-2 'proposed' brokers, drawn at random (seeded, reproducible)
-- from those with a listings URL on their own domain, no human decision, no review verdict,
-- not blocked, no listings yet, and a URL that is not an article or guide page.
select setseed(0.1107);
insert into private.broker_trial (domain, cohort, prior_stage, prior_status, listing_url)
select domain, 1, discovery_stage, strategy_status, listing_url
from (
  select b.*, random() r
  from broker_sources b
  where b.discovery_stage = '2_listings_url_known'
    and b.strategy_status = 'proposed'
    and norm_domain(b.listing_url) = b.domain
    and coalesce(b.decided_by, '') <> 'human'
    and b.listing_url !~* '/(post|posts|blog|news|article|articles|resources|insights)/|valuation|how-to|/sell-?(ing)?-'
    and not exists (select 1 from broker_review_applied r where r.domain = b.domain)
    and not exists (select 1 from broker_block k where k.broker_domain = b.domain)
    and not exists (select 1 from listings_direct l
                     where regexp_replace(lower(l.broker_domain), '^www\.', '') = b.domain)
  order by r
  limit 50
) c
on conflict (domain) do nothing;

update broker_sources b
   set discovery_stage = '3_crawlable', strategy_status = 'trial',
       decided_by = 'auto', decided_at = now(),
       decision_reason = 'trial cohort 1: crawl privately, publish only if checks pass',
       updated_at = now()
  from private.broker_trial t
 where t.domain = b.domain and t.verdict is null and t.cohort = 1;

-- 2. Hold every new listing from a broker on trial.
create or replace function private.trial_hold()
returns trigger language plpgsql security definer set search_path to 'public','private','pg_temp' as $$
begin
  if exists (select 1 from private.broker_trial t
              where t.verdict is null
                and t.domain = regexp_replace(lower(new.broker_domain), '^www\.', '')) then
    new.needs_review := true;
  end if;
  return new;
end $$;

drop trigger if exists trg_listings_direct_b_trial_hold on public.listings_direct;
create trigger trg_listings_direct_b_trial_hold
  before insert on public.listings_direct
  for each row execute function private.trial_hold();

-- 3. Judge.
create or replace function private.judge_broker_trials()
returns jsonb language plpgsql security definer set search_path to 'public','private','pg_temp' as $$
declare t record; m jsonb; v text; why text; out jsonb := '[]'::jsonb;
  n int; attempts int;
begin
  for t in select * from private.broker_trial where verdict is null loop
    select count(*) into attempts from crawl_run c
     where regexp_replace(lower(c.broker_domain), '^www\.', '') = t.domain and c.started_at >= t.started_at;

    with rows as (
      select distinct on (l.url) l.*
        from listings_direct l
       where regexp_replace(lower(l.broker_domain), '^www\.', '') = t.domain
         and l.status = 'active' and l.created_at >= t.started_at
       order by l.url, l.last_seen desc nulls last)
    select jsonb_build_object(
      'listings',      count(*),
      'crawl_attempts', attempts,
      'publishable',   round(avg((is_publishable(title, asking_price, cash_flow, status, false))::int), 3),
      'with_price',    round(avg((asking_price is not null)::int), 3),
      'url_specific',  round(avg((coalesce(url_is_listing_specific, true))::int), 3),
      'junk',          round(avg((is_listing_junk(title, url) or is_real_estate_junk(title))::int), 3),
      'distinct_titles', round(count(distinct lower(title))::numeric / nullif(count(*), 0), 3))
    into m from rows;
    n := (m->>'listings')::int;

    if n = 0 then
      if attempts >= 3 then v := 'no_listings'; why := attempts || ' crawl attempts, no listings'; else continue; end if;
    elsif n < 3 then
      if attempts >= 3 then v := 'fail_quality'; why := 'fewer than 3 listings after ' || attempts || ' attempts'; else continue; end if;
    elsif (m->>'publishable')::numeric >= 0.8 and (m->>'with_price')::numeric >= 0.7
      and (m->>'url_specific')::numeric >= 0.8 and (m->>'junk')::numeric <= 0.1
      and (m->>'distinct_titles')::numeric >= 0.9 then
      v := 'pass'; why := 'all checks passed';
    else
      v := 'fail_quality';
      why := concat_ws('; ',
        case when (m->>'publishable')::numeric < 0.8 then 'publishable ' || (m->>'publishable') end,
        case when (m->>'with_price')::numeric < 0.7 then 'with price ' || (m->>'with_price') end,
        case when (m->>'url_specific')::numeric < 0.8 then 'listing-specific URLs ' || (m->>'url_specific') end,
        case when (m->>'junk')::numeric > 0.1 then 'junk/real estate ' || (m->>'junk') end,
        case when (m->>'distinct_titles')::numeric < 0.9 then 'distinct titles ' || (m->>'distinct_titles') end);
    end if;

    update private.broker_trial set verdict = v, verdict_reason = why, metrics = m, decided_at = now()
     where domain = t.domain;

    if v = 'pass' then
      perform set_config('dealledger.release_review', 'on', true);
      update listings_direct l set needs_review = false
       where regexp_replace(lower(l.broker_domain), '^www\.', '') = t.domain
         and l.created_at >= t.started_at and l.needs_review;
      perform set_config('dealledger.release_review', 'off', true);
      update broker_sources set strategy_status = 'ready', decided_by = 'auto', decided_at = now(),
             decision_reason = 'trial passed: ' || m::text, updated_at = now()
       where domain = t.domain;
    else
      update broker_sources set discovery_stage = coalesce(t.prior_stage, '2_listings_url_known'),
             strategy_status = 'trial_failed', decided_by = 'auto', decided_at = now(),
             decision_reason = 'trial ' || v || ': ' || why, updated_at = now()
       where domain = t.domain;
    end if;
    out := out || jsonb_build_object('domain', t.domain, 'verdict', v, 'why', why, 'metrics', m);
  end loop;
  return out;
end $$;

-- 4. Promotion skips brokers still on trial.
create or replace function public.promote_producing_brokers()
 returns integer language sql set search_path to 'public' as $function$
  with promoted as (
    update public.broker_sources b
       set discovery_stage = '4_producing', updated_at = now()
     where b.discovery_stage in ('2_listings_url_known', '3_crawlable')
       and coalesce(b.strategy_status, '') not in ('trial', 'trial_failed')
       and exists (
         select 1 from public.listings_direct l
          where regexp_replace(lower(l.broker_domain), '^www\.', '')
              = regexp_replace(lower(b.domain), '^www\.', '')
            and l.last_seen > now() - interval '30 days')
    returning 1
  )
  select count(*)::int from promoted
$function$;

-- Daily, before promote-producing-brokers (21:17 UTC).
select cron.schedule('judge-broker-trials', '5 21 * * *', 'select private.judge_broker_trials()');

-- Read-out for the triage report.
create or replace function public.broker_trial_summary()
returns jsonb language sql security definer set search_path to 'public','private' as $$
  select jsonb_build_object(
    'brokers', count(*),
    'running', count(*) filter (where verdict is null),
    'crawled', count(*) filter (where exists (select 1 from crawl_run c
                 where regexp_replace(lower(c.broker_domain), '^www\.', '') = t.domain and c.started_at >= t.started_at)),
    'passed', count(*) filter (where verdict = 'pass'),
    'failed_quality', count(*) filter (where verdict = 'fail_quality'),
    'no_listings', count(*) filter (where verdict = 'no_listings'),
    'held_listings', (select count(*) from listings_direct l join private.broker_trial t2
                        on t2.domain = regexp_replace(lower(l.broker_domain), '^www\.', '')
                       where l.created_at >= t2.started_at and l.needs_review and l.status = 'active'),
    'released_listings', (select count(*) from listings_direct l join private.broker_trial t2
                        on t2.domain = regexp_replace(lower(l.broker_domain), '^www\.', '')
                       where t2.verdict = 'pass' and l.created_at >= t2.started_at and l.published))
  from private.broker_trial t
$$;
revoke all on function public.broker_trial_summary() from public, anon, authenticated;
grant execute on function public.broker_trial_summary() to service_role;

commit;

-- Rollback
-- begin;
-- select cron.unschedule('judge-broker-trials');
-- drop trigger if exists trg_listings_direct_b_trial_hold on public.listings_direct;
-- update broker_sources b set discovery_stage = t.prior_stage, strategy_status = t.prior_status, updated_at = now()
--   from private.broker_trial t where t.domain = b.domain;
-- (restore promote_producing_brokers from sql history; held listings stay needs_review = true)
-- commit;
