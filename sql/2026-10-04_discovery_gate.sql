-- Task 3: discovery gate. Record who promoted (or rejected) each broker and why.
-- Review before applying. Safe to re-run.
--
-- decided_by: 'auto'  = passed every GATE rule in agents/discover_backlog.py
--             'human' = --decide crawlable|reject
-- Feeds the Task 4 scoreboard (auto_promotions, human_decisions, auto_survival_7d).

alter table public.broker_sources
  add column if not exists decided_at      timestamptz,
  add column if not exists decided_by      text,
  add column if not exists decision_reason text;

do $$ begin
  alter table public.broker_sources
    add constraint broker_sources_decided_by_chk
    check (decided_by is null or decided_by in ('auto', 'human'));
exception when duplicate_object then null; end $$;

create index if not exists broker_sources_decided_at_idx
  on public.broker_sources (decided_at) where decided_at is not null;

-- The human review queue: candidates the gate would not auto-promote.
create or replace view public.v_discovery_proposed as
select d.domain,
       d.listings_url,
       (d.raw -> 'gate' ->> 'prices')::int        as prices,
       (d.raw -> 'gate' ->> 'items_parsed')::int  as items_parsed,
       (d.raw -> 'gate' ->> 're_hits')::int       as re_hits,
       (d.raw -> 'gate' ->> 'biz_hits')::int      as biz_hits,
       d.raw -> 'gate' ->> 'final_url'            as final_url,
       d.raw -> 'gate' -> 'fails'                 as fails,
       d.last_attempt_at
from public.broker_discovery d
where d.status = 'proposed'
order by d.last_attempt_at;

-- Keep broker_block and broker_sources in step every day. The 2026-09-24
-- backfill only ran once, so domains blocked later stayed at 4_producing and
-- inflated the producing-broker count (148 found on 2026-10-04, synced by hand).
create or replace function public.sync_blocked_brokers()
returns integer
language sql
volatile
set search_path = public
as $$
  with s as (
    update public.broker_sources b
       set discovery_stage = '0_blocked', updated_at = now()
     where b.discovery_stage <> '0_blocked'
       and exists (select 1 from public.broker_block k
                    where regexp_replace(lower(k.broker_domain), '^www\.', '')
                        = regexp_replace(lower(b.domain), '^www\.', ''))
    returning 1)
  select count(*)::int from s
$$;

revoke all on function public.sync_blocked_brokers() from public, anon, authenticated;
grant execute on function public.sync_blocked_brokers() to service_role;

select cron.schedule('sync-blocked-brokers', '11 21 * * *',
                     $$select public.sync_blocked_brokers()$$);
