-- Rollback for 2026-11-06_broker_master_access.sql. Restores the read access that
-- existed before (SELECT only; the old write grants were never needed and are not restored).
begin;

grant select on public.broker_master, public.broker_firms, public.firm_velocity,
                public.v_broker_crawl_candidates to anon, authenticated;

create policy "public read" on public.broker_master
  for select to public using (true);
create policy "anon_read_broker_master" on public.broker_master
  for select to anon using (true);

commit;
