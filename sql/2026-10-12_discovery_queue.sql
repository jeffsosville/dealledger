-- 2026-10-12: the discovery queue never runs dry.
--
-- 1. broker_sources.last_discovery_probe_at, written by discover_backlog.py
--    on every real probe.
-- 2. v_discovery_queue serves new brokers first (queue_rank 0), then earlier
--    failures due for another look (no listings page, dead, failed probe:
--    rank 1; blocked: rank 2) once their last probe is 21+ days old.
--    Never re-checked: human decisions, franchise-covered, duplicates,
--    rejected, block list, and review verdicts realestate / aggregator /
--    not_broker / duplicate / dead / captcha.
-- 3. discovery_benchmark table + discovery_benchmark_key() for
--    agents/discovery_benchmark.py.
-- 4. Seeds the IBBA members whose domain was never added to broker_sources
--    (realtors, banks, lenders, CPAs, ISPs, franchise subdomains and
--    marketplaces filtered out).
-- 5. A second daily discovery run at 18:23 UTC.
--
-- Safe to re-run.

begin;

alter table broker_sources add column if not exists last_discovery_probe_at timestamptz;
comment on column broker_sources.last_discovery_probe_at is
  'Set by agents/discover_backlog.py on every real probe. v_discovery_queue re-checks failures once this is older than 21 days.';

create or replace view v_discovery_queue as
with ibba as (
  select norm_domain(ibba_staging.domain) as domain, count(*)::integer as members
  from ibba_staging
  where norm_domain(ibba_staging.domain) is not null
  group by norm_domain(ibba_staging.domain)
), q as (
  select b.*, 'new'::text as queue_reason, 0 as queue_rank
  from broker_sources b
  where b.discovery_stage = '1_needs_discovery'
    and coalesce(b.consecutive_failures, 0) < 5
  union all
  select b.*, 'recheck'::text,
         case when b.discovery_stage = '0_blocked' then 2 else 1 end
  from broker_sources b
  where b.discovery_stage in ('0_unusable', '0_blocked')
    and coalesce(b.decided_by, '') <> 'human'
    and coalesce(b.strategy_status, '') <> 'blocked'
    and coalesce(b.last_error_type, '') not in
        ('FRANCHISE_COVERED', 'DUPLICATE_LISTINGS_URL', 'REJECTED')
    and coalesce(b.last_discovery_probe_at, '-infinity'::timestamptz)
        < now() - interval '21 days'
    and not exists (select 1 from broker_discovery d
                    where d.domain = b.domain
                      and d.status in ('ok', 'rejected', 'auth_required'))
    and not exists (select 1 from broker_review_applied r
                    where r.domain = b.domain
                      and r.verdict in ('realestate', 'aggregator', 'not_broker',
                                        'duplicate', 'dead', 'captcha'))
)
select q.domain, q.company_name, q.homepage_url, q.state,
  q.active_listings_est as bbs_active_hint, q.sold_listings_est as bbs_sold_hint,
  q.ibba_member, q.seed_source, q.strategy_status, q.consecutive_failures,
  q.last_failure_at, q.last_error_type,
  (coalesce(i.members, case when q.ibba_member then 1 else 0 end) * 10
   + case when q.ibba_member then 5 else 0 end
   + case when q.email ~ '@' then 1 else 0 end) as priority_score,
  coalesce(i.members, case when q.ibba_member then 1 else 0 end) as ibba_members,
  q.queue_reason, q.queue_rank, q.last_discovery_probe_at
from q
left join ibba i on i.domain = q.domain
where not exists (select 1 from broker_block k where k.broker_domain = q.domain)
order by q.queue_rank, 13 desc, q.domain;

create table if not exists discovery_benchmark (
  id bigserial primary key,
  run_id text not null,
  domain text not null,
  key_source text,
  expected_url text,
  found_url text,
  match text,
  candidates jsonb,
  reasons jsonb,
  error text,
  secs numeric,
  at timestamptz not null default now()
);
alter table discovery_benchmark enable row level security;
drop policy if exists discovery_benchmark_read on discovery_benchmark;
create policy discovery_benchmark_read on discovery_benchmark for select to anon using (true);
create index if not exists discovery_benchmark_run on discovery_benchmark(run_id);

create or replace function discovery_benchmark_key(n int default 100)
returns table(domain text, expected_url text, key_source text)
language sql stable
set search_path = public
as $$
  (select b.domain, b.listing_url, 'producing'
     from broker_sources b
    where b.discovery_stage = '4_producing'
      and b.last_success_at > now() - interval '14 days'
      and b.listing_url ~* '^https?://'
      and norm_domain(b.listing_url) = b.domain
      and b.listing_url !~* '^https?://[^/]+/?$'
    order by md5(b.domain)
    limit n)
  union
  (select distinct on (r.domain) r.domain,
          coalesce(r.new_listing_url, r.prior_listing_url), 'review_verdict'
     from broker_review_applied r
    where r.verdict in ('broker_ok', 'broker_fixed')
      and coalesce(r.new_listing_url, r.prior_listing_url) ~* '^https?://'
    order by r.domain, r.reviewed_at desc)
$$;
revoke all on function discovery_benchmark_key(int) from public, anon, authenticated;

-- IBBA members never added to broker_sources.
with m as (
  select regexp_replace(lower(norm_domain(domain)), '^[^a-z0-9]+|^www\.', '', 'g') as d,
         max(company) as company, max(state) as st, max(email) as email
  from ibba_staging
  where norm_domain(domain) is not null
  group by 1
)
insert into broker_sources (domain, listing_url, homepage_url, company_name, state,
                            email, seed_source, ibba_member, discovery_stage,
                            strategy_status)
select d, 'https://' || d, 'https://' || d, company, st, email,
       'ibba_backfill_2026_10_12', true, '1_needs_discovery', 'pending'
from m
where d ~ '^[a-z0-9][a-z0-9.-]*\.[a-z]{2,}$'
  and d !~ '^www'
  and d !~* '(realt|realestate|real-estate|homes|hometeam|showings|landgroup|propert|commercialre|\mcre\M|bank|bancorp|mtg|mortgage|lending|loan|funding|\mcpa|cpas?\.|accountant|forensic|tax|law|legal|insur|wealth|morganstanley|edwardjones|exprealty|kw\.com|coldwell|keyes|bhhs|remax|century21|compass\.com|svn\.com|ccim|nrtllc|tinyurl|bit\.ly|linkedin|facebook|google|gmail|yahoo|outlook|icloud|hotmail|aol\.|pacbell|frontier\.com|softcom|midcoast\.com|kacy\.net|northwest\.com|sbcglobal|comcast|verizon|earthlink|bizbuysell|bizquest|loopnet|crexi|mls|fcbb|tworld|t\.world|transworld|sunbelt|murphybusiness|vrbb|hedgestone|linkbusiness|wpengine|\.ae$|\.ca$|\.uk$|\.fr$|\.in$|\.cm$)'
  and not exists (select 1 from broker_block k where k.broker_domain = m.d)
  and not exists (select 1 from broker_sources b where b.domain = m.d)
on conflict do nothing;

commit;

-- Second daily discovery run (the first is gh-broker-discovery, 06:23 UTC).
select cron.unschedule('gh-broker-discovery-pm')
where exists (select 1 from cron.job where jobname = 'gh-broker-discovery-pm');
select cron.schedule('gh-broker-discovery-pm', '23 18 * * *',
                     $$select public.dispatch_workflow('broker_discovery.yml')$$);

-- Check:
select queue_reason, count(*) from v_discovery_queue group by 1;
