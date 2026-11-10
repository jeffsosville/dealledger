-- 2026-11-10  Backfill why each stage-2 broker is parked.
--
-- 404 of 709 broker_sources rows at 2_listings_url_known have no
-- last_error_type, because discover_backlog.py wrote a reason only for
-- 0_unusable / 0_blocked outcomes. The reason was kept in broker_discovery
-- (status, raw->gate->fails, notes). This copies it onto broker_sources so the
-- stage-2 queue can be triaged. Fills only rows whose last_error_type is null;
-- touches no stage, status, URL or decision column.
--
-- Expected today: 360 rows filled (GATE_PROPOSED 109, FAILED 104,
-- NO_LISTINGS_PAGE 60, WEAK_YIELD 34, JS_ONLY 25, NEEDS_RECHECK 22, OK 5,
-- BLOCKED 1). 44 have no broker_discovery row and stay null until re-probed.
--
-- Rollback: sql/2026-11-10_stage2_reason_backfill_rollback.sql

begin;

create table if not exists private.stage2_reason_backfill_20261110 (
  domain text primary key,
  old_last_error_type text,
  old_last_error_message text,
  new_last_error_type text,
  new_last_error_message text,
  at timestamptz default now()
);

with d as (
  select distinct on (domain) domain, status, raw, notes
  from broker_discovery
  order by domain, updated_at desc nulls last
), src as (
  select b.domain,
         b.last_error_type as old_type,
         b.last_error_message as old_msg,
         case d.status when 'proposed' then 'GATE_PROPOSED'
                       when 'weak' then 'WEAK_YIELD'
                       else upper(d.status) end as new_type,
         left('backfilled 2026-11-10 from broker_discovery: ' || coalesce(
                (select string_agg(x, '; ') from jsonb_array_elements_text(d.raw->'gate'->'fails') x),
                d.notes, d.status), 300) as new_msg
  from broker_sources b
  join d on d.domain = b.domain
  where b.discovery_stage = '2_listings_url_known'
    and b.last_error_type is null
    and d.status is not null
)
insert into private.stage2_reason_backfill_20261110
  (domain, old_last_error_type, old_last_error_message, new_last_error_type, new_last_error_message)
select domain, old_type, old_msg, new_type, new_msg from src
on conflict (domain) do nothing;

update broker_sources b
   set last_error_type = k.new_last_error_type,
       last_error_message = k.new_last_error_message
  from private.stage2_reason_backfill_20261110 k
 where k.domain = b.domain
   and b.last_error_type is null;

-- Test: expect ~360 backfilled, and stage-2 rows with no reason down to ~44.
select (select count(*) from private.stage2_reason_backfill_20261110) as backfilled,
       (select count(*) from broker_sources
         where discovery_stage = '2_listings_url_known' and last_error_type is null) as still_null;

commit;
