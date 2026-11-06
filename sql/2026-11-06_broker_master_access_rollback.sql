-- Rollback for 2026-11-06_broker_master_access.sql. Safe to run more than once.
-- Restores the read access that existed before (SELECT only; the old write
-- grants were never needed and are not restored).
begin;

grant select on public.broker_master, public.broker_firms, public.firm_velocity,
                public.v_broker_crawl_candidates to anon, authenticated;

drop policy if exists "public read" on public.broker_master;
create policy "public read" on public.broker_master
  for select to public using (true);

drop policy if exists "anon_read_broker_master" on public.broker_master;
create policy "anon_read_broker_master" on public.broker_master
  for select to anon using (true);

commit;

-- Check after rollback (expect true, true, 2):
-- select has_table_privilege('anon', 'public.broker_master', 'SELECT'),
--        has_table_privilege('anon', 'public.broker_firms', 'SELECT'),
--        (select count(*) from pg_policies where schemaname = 'public' and tablename = 'broker_master');
