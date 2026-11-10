-- 2026-11-10  Put stage-2 brokers back in the discovery queue.
--
-- v_discovery_queue served only 1_needs_discovery (now 0 rows) and 0_unusable /
-- 0_blocked rechecks, so the twice-daily discovery job has had nothing to do
-- since 2026-11-05 while 709 brokers sat at 2_listings_url_known waiting for a
-- human. This adds a third branch, queue_reason 'restage', queue_rank 1:
--
--   stage 2, status proposed / needs_work / pending / fingerprinted / failed,
--   not decided by a human, not held by the realty guard, not in a trial,
--   not rejected in broker_review_applied, not blocked,
--   last probe older than 14 days (or never).
--
-- 512 brokers qualify today (386 never probed, 154 IBBA). At ~1,400 probes a
-- day the first pass finishes in about a day. A re-probe can move a broker to
-- 3_crawlable (passes every gate), keep it at stage 2 with a fresh reason, or
-- move it to 0_unusable on evidence (dead site, no page) - the same outcomes as
-- any discovery probe. Human-decided rows are never touched.
--
-- Needs agents/discover_backlog.py from the same patch so 'restage' rows are
-- always probed (treated like 'recheck') and every stage-2 outcome records why.
--
-- Rollback: sql/2026-11-10_discovery_requeue_stage2_rollback.sql

begin;

create or replace view public.v_discovery_queue as
with ibba as (
  select norm_domain(ibba_staging.domain) as domain, count(*)::integer as members
  from ibba_staging
  where norm_domain(ibba_staging.domain) is not null
  group by norm_domain(ibba_staging.domain)
), q as (
  select b.id, b.account_id, b.broker_name, b.company_name, b.listing_url, b.homepage_url,
         b.city, b.state, b.email, b.phone, b.active_listings_est, b.sold_listings_est,
         b.leaderboard_score, b.is_featured, b.priority, b.vertical_hint, b.notes, b.domain,
         b.strategy_type, b.render_mode, b.proxy_mode, b.pagination_mode, b.detail_mode,
         b.strategy_status, b.strategy_confidence, b.active_listings_source, b.seed_source,
         b.pagination_param, b.listings_per_page, b.total_pages_est, b.last_fingerprinted_at,
         b.last_success_at, b.last_failure_at, b.last_http_status, b.last_error_type,
         b.last_error_message, b.last_listing_count, b.consecutive_failures, b.created_at,
         b.updated_at, b.ibba_member, b.discovery_stage, b.first_seeded_at, b.decided_at,
         b.decided_by, b.decision_reason, b.last_discovery_probe_at,
         'new'::text as queue_reason, 0 as queue_rank
  from broker_sources b
  where b.discovery_stage = '1_needs_discovery'
    and coalesce(b.consecutive_failures, 0) < 5
  union all
  select b.id, b.account_id, b.broker_name, b.company_name, b.listing_url, b.homepage_url,
         b.city, b.state, b.email, b.phone, b.active_listings_est, b.sold_listings_est,
         b.leaderboard_score, b.is_featured, b.priority, b.vertical_hint, b.notes, b.domain,
         b.strategy_type, b.render_mode, b.proxy_mode, b.pagination_mode, b.detail_mode,
         b.strategy_status, b.strategy_confidence, b.active_listings_source, b.seed_source,
         b.pagination_param, b.listings_per_page, b.total_pages_est, b.last_fingerprinted_at,
         b.last_success_at, b.last_failure_at, b.last_http_status, b.last_error_type,
         b.last_error_message, b.last_listing_count, b.consecutive_failures, b.created_at,
         b.updated_at, b.ibba_member, b.discovery_stage, b.first_seeded_at, b.decided_at,
         b.decided_by, b.decision_reason, b.last_discovery_probe_at,
         'restage'::text, 1
  from broker_sources b
  where b.discovery_stage = '2_listings_url_known'
    and coalesce(b.strategy_status, '') = any (array['proposed','needs_work','pending','fingerprinted','failed'])
    and coalesce(b.decided_by, '') <> 'human'
    and coalesce(b.decision_reason, '') !~* '^(held|trial)'
    and coalesce(b.last_discovery_probe_at, '-infinity'::timestamptz) < now() - interval '14 days'
    and not exists (select 1 from private.broker_trial t where t.domain = b.domain)
    and not exists (select 1 from broker_review_applied r
                    where r.domain = b.domain
                      and r.verdict = any (array['realestate','aggregator','not_broker','duplicate','dead','captcha']))
  union all
  select b.id, b.account_id, b.broker_name, b.company_name, b.listing_url, b.homepage_url,
         b.city, b.state, b.email, b.phone, b.active_listings_est, b.sold_listings_est,
         b.leaderboard_score, b.is_featured, b.priority, b.vertical_hint, b.notes, b.domain,
         b.strategy_type, b.render_mode, b.proxy_mode, b.pagination_mode, b.detail_mode,
         b.strategy_status, b.strategy_confidence, b.active_listings_source, b.seed_source,
         b.pagination_param, b.listings_per_page, b.total_pages_est, b.last_fingerprinted_at,
         b.last_success_at, b.last_failure_at, b.last_http_status, b.last_error_type,
         b.last_error_message, b.last_listing_count, b.consecutive_failures, b.created_at,
         b.updated_at, b.ibba_member, b.discovery_stage, b.first_seeded_at, b.decided_at,
         b.decided_by, b.decision_reason, b.last_discovery_probe_at,
         'recheck'::text,
         case when b.discovery_stage = '0_blocked' then 2 else 1 end
  from broker_sources b
  where b.discovery_stage = any (array['0_unusable','0_blocked'])
    and coalesce(b.decided_by, '') <> 'human'
    and coalesce(b.strategy_status, '') <> 'blocked'
    and coalesce(b.last_error_type, '') <> all (array['FRANCHISE_COVERED','DUPLICATE_LISTINGS_URL','REJECTED'])
    and coalesce(b.last_discovery_probe_at, '-infinity'::timestamptz) < now() - interval '21 days'
    and not exists (select 1 from broker_discovery d
                    where d.domain = b.domain and d.status = any (array['ok','rejected','auth_required']))
    and not exists (select 1 from broker_review_applied r
                    where r.domain = b.domain
                      and r.verdict = any (array['realestate','aggregator','not_broker','duplicate','dead','captcha']))
)
select q.domain, q.company_name, q.homepage_url, q.state,
       q.active_listings_est as bbs_active_hint,
       q.sold_listings_est as bbs_sold_hint,
       q.ibba_member, q.seed_source, q.strategy_status, q.consecutive_failures,
       q.last_failure_at, q.last_error_type,
       coalesce(i.members, case when q.ibba_member then 1 else 0 end) * 10
         + case when q.ibba_member then 5 else 0 end
         + case when q.email ~ '@' then 1 else 0 end as priority_score,
       coalesce(i.members, case when q.ibba_member then 1 else 0 end) as ibba_members,
       q.queue_reason, q.queue_rank, q.last_discovery_probe_at
from q
left join ibba i on i.domain = q.domain
where not exists (select 1 from broker_block k where k.broker_domain = q.domain)
order by q.queue_rank,
         coalesce(i.members, case when q.ibba_member then 1 else 0 end) * 10
           + case when q.ibba_member then 5 else 0 end
           + case when q.email ~ '@' then 1 else 0 end desc,
         q.domain;

revoke all on public.v_discovery_queue from anon, authenticated;

-- Test: expect ~512 'restage' rows today, the other branches unchanged (0 rows).
select queue_reason, count(*) from public.v_discovery_queue group by 1 order by 1;

commit;
