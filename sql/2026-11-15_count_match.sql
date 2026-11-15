-- W6: broker displayed-count columns + count-match scorecard
-- Applied 2026-11-15 as migration count_match_2026_11_15. Priority 1 + 2.
-- Scrapers already send these fields (scrapers/crawl_run_log.py _displayed_fields);
-- before this, the columns were missing and the counts were dropped.
-- Does NOT change absence_candidates() / the removal rule.

begin;

alter table public.crawl_run
  add column if not exists broker_displayed_count integer,
  add column if not exists broker_displayed_count_source text,
  add column if not exists broker_displayed_note text,
  add column if not exists count_complete boolean;

alter table public.crawl_run drop constraint if exists crawl_run_displayed_source_check;
alter table public.crawl_run
  add constraint crawl_run_displayed_source_check
  check (broker_displayed_count_source is null or broker_displayed_count_source in ('stated','counted'));

-- Backfill the mismatches already logged as text (Nov 13 onward)
update public.crawl_run r
   set broker_displayed_count = (x.m)[2]::int,
       broker_displayed_count_source = (x.m)[3],
       count_complete = false
  from (select id, regexp_match(error, 'COUNT_MISMATCH: saw (\d+), broker displays (\d+) \((stated|counted)\)') m
          from public.crawl_run where error like 'COUNT_MISMATCH%') x
 where r.id = x.id and x.m is not null and r.broker_displayed_count is null;

-- Per-broker scorecard. Tolerance 90-110% (scrapers/displayed_count.py).
create or replace view public.v_count_match_scorecard
with (security_invoker = true) as
with prod as (
  select distinct lower(regexp_replace(domain, '^www\.', '')) as dom
    from public.broker_sources
   where discovery_stage = '4_producing' and domain is not null
),
runs as (
  select lower(regexp_replace(broker_domain, '^www\.', '')) as dom,
         started_at, status, listings_seen,
         broker_displayed_count as displayed, broker_displayed_count_source as src,
         count_complete,
         row_number() over (partition by lower(regexp_replace(broker_domain, '^www\.', ''))
                            order by started_at desc) as rn
    from public.crawl_run
   where broker_displayed_count is not null and status in ('ok','partial')
),
db as (
  select lower(regexp_replace(broker_domain, '^www\.', '')) as dom,
         count(distinct url) filter (where status = 'active' and not coalesce(needs_review, false)) as active_urls
    from public.listings_direct
   where source = 'broker_direct'
   group by 1
),
j as (
  select p.dom,
         coalesce(d.active_urls, 0) as active_urls,
         r1.displayed, r1.src, r1.listings_seen as seen_last, r1.count_complete as last_complete,
         r1.started_at::date as last_count_on,
         r2.displayed as displayed_prev, r2.count_complete as prev_complete
    from prod p
    left join db d on d.dom = p.dom
    left join runs r1 on r1.dom = p.dom and r1.rn = 1
    left join runs r2 on r2.dom = p.dom and r2.rn = 2
)
select dom as broker_domain,
       displayed as broker_count,
       src as broker_count_source,
       active_urls,
       active_urls - displayed as gap,
       seen_last,
       last_count_on,
       (last_complete is true and prev_complete is true) as crawl_matched_twice,
       (displayed is not null and active_urls between 0.9 * displayed and 1.1 * displayed) as db_matches,
       case
         when displayed is null then 'no_count'
         when last_complete is true and prev_complete is true
              and active_urls between 0.9 * displayed and 1.1 * displayed then 'monitored'
         when last_complete is true and prev_complete is not true
              and active_urls between 0.9 * displayed and 1.1 * displayed then 'matched_once'
         when seen_last < 0.9 * displayed then 'crawl_short'
         when seen_last > 1.1 * displayed then 'crawl_over'
         when active_urls > 1.1 * displayed then 'db_extra'
         else 'db_short'
       end as verdict
  from j;

create or replace view public.v_count_match_summary
with (security_invoker = true) as
select now()::date as as_of,
       count(*) as producing_brokers,
       count(*) filter (where verdict <> 'no_count') as with_broker_count,
       count(*) filter (where verdict = 'monitored') as monitored,
       count(*) filter (where verdict = 'matched_once') as matched_once,
       count(*) filter (where verdict = 'crawl_short') as crawl_short,
       count(*) filter (where verdict = 'crawl_over') as crawl_over,
       count(*) filter (where verdict = 'db_extra') as db_extra,
       count(*) filter (where verdict = 'db_short') as db_short,
       count(*) filter (where verdict = 'no_count') as no_count
  from public.v_count_match_scorecard;

revoke all on public.v_count_match_scorecard, public.v_count_match_summary from anon, authenticated;

commit;
