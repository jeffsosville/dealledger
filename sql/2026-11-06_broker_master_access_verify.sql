-- Tests for 2026-11-06_broker_master_access.sql. Read-only; run after applying.
-- Each block raises an exception on failure; success prints nothing.

-- 1. Privileges as recorded in the catalog.
do $$
begin
  if has_table_privilege('anon', 'public.broker_master', 'SELECT')             then raise exception 'anon can still select broker_master'; end if;
  if has_table_privilege('anon', 'public.firm_velocity', 'SELECT')             then raise exception 'anon can still select firm_velocity'; end if;
  if has_table_privilege('anon', 'public.v_broker_crawl_candidates', 'SELECT') then raise exception 'anon can still select v_broker_crawl_candidates'; end if;
  if has_table_privilege('anon', 'public.broker_firms', 'SELECT')              then raise exception 'anon has full select on broker_firms'; end if;
  if not has_column_privilege('anon', 'public.broker_firms', 'slug', 'SELECT')       then raise exception 'redirect lookup broken: slug'; end if;
  if not has_column_privilege('anon', 'public.broker_firms', 'companyurl', 'SELECT') then raise exception 'redirect lookup broken: companyurl'; end if;
  if has_column_privilege('anon', 'public.broker_firms', 'email', 'SELECT')          then raise exception 'anon can read broker_firms.email'; end if;
  if has_column_privilege('anon', 'public.broker_firms', 'telephone', 'SELECT')      then raise exception 'anon can read broker_firms.telephone'; end if;
  if has_table_privilege('anon', 'public.broker_master', 'TRUNCATE')           then raise exception 'anon can truncate broker_master'; end if;
  if exists (select 1 from pg_policies where tablename = 'broker_master')      then raise exception 'broker_master read policies still present'; end if;
end $$;

-- 2. Behaviour as the anon role (what the public key actually gets).
begin read only;
set local role anon;
do $$
declare n int;
begin
  begin perform 1 from public.broker_master limit 1;             raise exception 'broker_master readable';
  exception when insufficient_privilege then null; end;
  begin perform 1 from public.firm_velocity limit 1;             raise exception 'firm_velocity readable';
  exception when insufficient_privilege then null; end;
  begin perform 1 from public.v_broker_crawl_candidates limit 1; raise exception 'v_broker_crawl_candidates readable';
  exception when insufficient_privilege then null; end;
  begin perform email from public.broker_firms limit 1;          raise exception 'broker_firms.email readable';
  exception when insufficient_privilege then null; end;
  -- The exact query /broker/[slug] makes still works.
  select count(*) into n from public.broker_firms where slug is not null and companyurl is not null;
  if n = 0 then raise exception 'redirect lookup returns no rows'; end if;
end $$;
rollback;

-- 3. Service role (pipeline) unaffected.
begin read only;
set local role service_role;
select count(*) as service_role_candidates from public.v_broker_crawl_candidates;  -- expect ~2,185
rollback;

-- 4. After applying, over HTTP with the public key (from the Mac):
--   curl -s -o /dev/null -w '%{http_code}\n' "$SUPABASE_URL/rest/v1/broker_master?select=account&limit=1" -H "apikey: $ANON"            # expect 401
--   curl -s "$SUPABASE_URL/rest/v1/broker_firms?select=companyurl&slug=eq.moxie-brokerage-group-moxie-select" -H "apikey: $ANON"     # expect one row
--   curl -s -o /dev/null -w '%{http_code}\n' "$SUPABASE_URL/rest/v1/broker_firms?select=email&limit=1" -H "apikey: $ANON"          # expect 401
--   and load one retired broker slug on dealledger.org: expect a 301 to the current broker page.
