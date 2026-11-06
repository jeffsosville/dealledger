-- Tests for 2026-11-06_broker_master_access.sql. Run after applying.
-- Every check raises an exception on failure; a clean run ends with
-- "broker_master access: all checks passed".
--
-- Read-only except section 4, which attempts writes that match no rows
-- (WHERE false) inside a transaction that is rolled back; they must fail on
-- privileges before touching anything.
--
-- The redirect slug is a production broker_firms slug that is unique and has a
-- companyurl (checked 2026-11-06), so the lookup must return exactly one URL.

select set_config('verify.redirect_slug', '123bsc-com', false);

-- 1. Catalog: anon and authenticated, including PUBLIC and inherited privileges.
--    has_table_privilege / has_column_privilege account for PUBLIC grants and
--    role membership, so these cover inherited access too.
do $$
declare
  r text; obj text; col text; priv text;
  restricted text[] := array['broker_master','firm_velocity','v_broker_crawl_candidates','dealledger_listings'];
  table_privs text[] := array['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER'];
begin
  if current_setting('server_version_num')::int >= 170000 then
    table_privs := table_privs || 'MAINTAIN'::text;
  end if;

  foreach r in array array['anon','authenticated'] loop
    -- No table-level privilege of any kind on the four objects.
    foreach obj in array restricted || 'broker_firms'::text loop
      foreach priv in array table_privs loop
        if has_table_privilege(r, 'public.' || obj, priv) then
          raise exception '% has % on %', r, priv, obj;
        end if;
      end loop;
    end loop;

    -- No column of the restricted objects is readable or writable.
    foreach obj in array restricted loop
      for col in select attname from pg_attribute
                 where attrelid = ('public.' || obj)::regclass and attnum > 0 and not attisdropped loop
        foreach priv in array array['SELECT','INSERT','UPDATE','REFERENCES'] loop
          if has_column_privilege(r, 'public.' || obj, col, priv) then
            raise exception '% has % on %.%', r, priv, obj, col;
          end if;
        end loop;
      end loop;
    end loop;

    -- broker_firms: SELECT on slug and companyurl only; nothing else on any column.
    for col in select attname from pg_attribute
               where attrelid = 'public.broker_firms'::regclass and attnum > 0 and not attisdropped loop
      if col in ('slug','companyurl') then
        if not has_column_privilege(r, 'public.broker_firms', col, 'SELECT') then
          raise exception 'redirect lookup broken: % cannot select broker_firms.%', r, col;
        end if;
      else
        if has_column_privilege(r, 'public.broker_firms', col, 'SELECT') then
          raise exception '% can select broker_firms.%', r, col;
        end if;
      end if;
      foreach priv in array array['INSERT','UPDATE','REFERENCES'] loop
        if has_column_privilege(r, 'public.broker_firms', col, priv) then
          raise exception '% has % on broker_firms.%', r, priv, col;
        end if;
      end loop;
    end loop;
  end loop;

  -- Nothing granted to PUBLIC, at table or column level.
  if exists (select 1 from pg_class c, aclexplode(c.relacl) a
             where c.oid in ('public.broker_master'::regclass, 'public.broker_firms'::regclass,
                             'public.firm_velocity'::regclass, 'public.v_broker_crawl_candidates'::regclass)
               and a.grantee = 0) then
    raise exception 'PUBLIC holds a table privilege on a restricted object';
  end if;
  if exists (select 1 from pg_attribute t, aclexplode(t.attacl) a
             where t.attrelid in ('public.broker_master'::regclass, 'public.broker_firms'::regclass,
                                  'public.firm_velocity'::regclass, 'public.v_broker_crawl_candidates'::regclass)
               and a.grantee = 0) then
    raise exception 'PUBLIC holds a column privilege on a restricted object';
  end if;

  -- Read policies gone.
  if exists (select 1 from pg_policies where schemaname = 'public' and tablename = 'broker_master') then
    raise exception 'broker_master policies still present';
  end if;
end $$;

-- 2. No alternate path: no function anywhere references the restricted objects,
--    and no security-definer function that anon/authenticated can execute runs dynamic SQL.
do $$
declare f text;
begin
  select string_agg(n.nspname || '.' || p.proname, ', ') into f
  from pg_proc p join pg_namespace n on n.oid = p.pronamespace
  where p.prosrc ~* '(broker_master|broker_firms|firm_velocity|v_broker_crawl_candidates)';
  if f is not null then raise exception 'functions reference restricted objects: %', f; end if;

  select string_agg(n.nspname || '.' || p.proname, ', ') into f
  from pg_proc p join pg_namespace n on n.oid = p.pronamespace
  where p.prosecdef
    and (has_function_privilege('anon', p.oid, 'EXECUTE') or has_function_privilege('authenticated', p.oid, 'EXECUTE'))
    and p.prosrc ~* '\mexecute\M';
  if f is not null then raise exception 'public security-definer functions with dynamic SQL: %', f; end if;

  select string_agg(schemaname || '.' || matviewname, ', ') into f from pg_matviews
  where definition ~* '(broker_master|broker_firms|firm_velocity|v_broker_crawl_candidates)';
  if f is not null then raise exception 'matviews reference restricted objects: %', f; end if;

  select string_agg(schemaname || '.' || viewname, ', ') into f from pg_views
  where definition ~* '(broker_master|broker_firms|firm_velocity|v_broker_crawl_candidates)'
    and (schemaname, viewname) not in (('public','broker_firms'),('public','firm_velocity'),
                                       ('public','v_broker_crawl_candidates'),('public','dealledger_listings'));
  if f is not null then raise exception 'other views reference restricted objects: %', f; end if;
end $$;

-- 3. Reads as each public role, in the shape PostgREST sends them.
--    PostgREST: role switched with set_config inside a read-only transaction,
--    then a CTE over the requested columns with the filter as a bind parameter.
begin isolation level read committed read only;
select set_config('role', 'anon', true), set_config('request.jwt.claims', '{"role":"anon"}', true);
do $$
declare n int; body json; s text := current_setting('verify.redirect_slug');
begin
  -- The exact request /broker/[slug] makes: GET /broker_firms?select=companyurl&slug=eq.<slug>
  with pgrst_source as (
    select "public"."broker_firms"."companyurl" from "public"."broker_firms"
    where "public"."broker_firms"."slug" = s
  )
  select pg_catalog.count(t), coalesce(json_agg(t), '[]') into n, body from (select * from pgrst_source) t;
  if n <> 1 or (body -> 0 ->> 'companyurl') is null then
    raise exception 'anon redirect lookup for % returned % row(s): %', s, n, body;
  end if;
  raise notice 'anon redirect lookup ok: %', body;

  begin perform 1 from public.broker_master limit 1;              raise exception 'anon: broker_master readable';
  exception when insufficient_privilege then null; end;
  begin perform 1 from public.firm_velocity limit 1;              raise exception 'anon: firm_velocity readable';
  exception when insufficient_privilege then null; end;
  begin perform 1 from public.v_broker_crawl_candidates limit 1;  raise exception 'anon: v_broker_crawl_candidates readable';
  exception when insufficient_privilege then null; end;
  begin perform 1 from public.dealledger_listings limit 1;        raise exception 'anon: dealledger_listings readable';
  exception when insufficient_privilege then null; end;
  begin perform * from public.broker_firms limit 1;               raise exception 'anon: select * on broker_firms works';
  exception when insufficient_privilege then null; end;
  begin perform email from public.broker_firms limit 1;           raise exception 'anon: broker_firms.email readable';
  exception when insufficient_privilege then null; end;
  begin perform telephone from public.broker_firms limit 1;       raise exception 'anon: broker_firms.telephone readable';
  exception when insufficient_privilege then null; end;
  begin perform companyname from public.broker_firms limit 1;     raise exception 'anon: broker_firms.companyname readable';
  exception when insufficient_privilege then null; end;
end $$;
rollback;

begin isolation level read committed read only;
select set_config('role', 'authenticated', true), set_config('request.jwt.claims', '{"role":"authenticated"}', true);
do $$
declare n int; body json; s text := current_setting('verify.redirect_slug');
begin
  with pgrst_source as (
    select "public"."broker_firms"."companyurl" from "public"."broker_firms"
    where "public"."broker_firms"."slug" = s
  )
  select pg_catalog.count(t), coalesce(json_agg(t), '[]') into n, body from (select * from pgrst_source) t;
  if n <> 1 or (body -> 0 ->> 'companyurl') is null then
    raise exception 'authenticated redirect lookup for % returned % row(s): %', s, n, body;
  end if;

  begin perform 1 from public.broker_master limit 1;              raise exception 'authenticated: broker_master readable';
  exception when insufficient_privilege then null; end;
  begin perform 1 from public.firm_velocity limit 1;              raise exception 'authenticated: firm_velocity readable';
  exception when insufficient_privilege then null; end;
  begin perform 1 from public.v_broker_crawl_candidates limit 1;  raise exception 'authenticated: v_broker_crawl_candidates readable';
  exception when insufficient_privilege then null; end;
  begin perform 1 from public.dealledger_listings limit 1;        raise exception 'authenticated: dealledger_listings readable';
  exception when insufficient_privilege then null; end;
  begin perform email from public.broker_firms limit 1;           raise exception 'authenticated: broker_firms.email readable';
  exception when insufficient_privilege then null; end;
end $$;
rollback;

-- 4. Writes as each public role. WHERE false: no row can match, and the
--    transaction is rolled back regardless. Each must fail on privileges.
begin;
do $$
declare r text;
begin
  foreach r in array array['anon','authenticated'] loop
    perform set_config('role', r, true);
    begin insert into public.broker_master (account) select 1 where false;
          raise exception '%: insert into broker_master allowed', r;
    exception when insufficient_privilege then null; end;
    begin update public.broker_master set email = email where false;
          raise exception '%: update broker_master allowed', r;
    exception when insufficient_privilege then null; end;
    begin delete from public.broker_master where false;
          raise exception '%: delete from broker_master allowed', r;
    exception when insufficient_privilege then null; end;
    -- broker_firms is not an updatable view, so Postgres may refuse this
    -- before the privilege check; either refusal is a pass. Section 1 already
    -- proves no UPDATE privilege on any of its columns.
    begin update public.broker_firms set companyurl = companyurl where false;
          raise exception '%: update broker_firms allowed', r;
    exception when insufficient_privilege or feature_not_supported or object_not_in_prerequisite_state then null; end;
    perform set_config('role', 'none', true);
  end loop;
end $$;
rollback;

-- 5. service_role (pipeline) unaffected.
do $$
declare obj text;
begin
  foreach obj in array array['broker_master','broker_firms','firm_velocity','v_broker_crawl_candidates','dealledger_listings'] loop
    if not has_table_privilege('service_role', 'public.' || obj, 'SELECT') then
      raise exception 'service_role lost SELECT on %', obj;
    end if;
  end loop;
end $$;
begin isolation level read committed read only;
select set_config('role', 'service_role', true);
do $$
declare a bigint; b bigint; c bigint; d bigint;
begin
  select count(*) into a from public.broker_master;
  select count(*) into b from public.broker_firms;
  select count(*) into c from public.firm_velocity;
  select count(*) into d from public.v_broker_crawl_candidates;
  if a = 0 or b = 0 or d = 0 then
    raise exception 'service_role sees empty data: broker_master=% broker_firms=% firm_velocity=% candidates=%', a, b, c, d;
  end if;
  raise notice 'service_role rows: broker_master=% broker_firms=% firm_velocity=% candidates=%', a, b, c, d;
end $$;
rollback;

select 'broker_master access: all checks passed' as result;

-- 6. After applying in production, over HTTP with the public key (from the Mac):
--   curl -s -o /dev/null -w '%{http_code}\n' "$SUPABASE_URL/rest/v1/broker_master?select=account&limit=1" -H "apikey: $ANON"            # expect 401
--   curl -s "$SUPABASE_URL/rest/v1/broker_firms?select=companyurl&slug=eq.123bsc-com" -H "apikey: $ANON"     # expect one row
--   curl -s -o /dev/null -w '%{http_code}\n' "$SUPABASE_URL/rest/v1/broker_firms?select=email&limit=1" -H "apikey: $ANON"          # expect 401
--   curl -s -o /dev/null -w '%{http_code}\n' "$SUPABASE_URL/rest/v1/firm_velocity?limit=1" -H "apikey: $ANON"                       # expect 401
--   curl -s -o /dev/null -w '%{http_code}\n' "$SUPABASE_URL/rest/v1/v_broker_crawl_candidates?limit=1" -H "apikey: $ANON"           # expect 401
--   and load one retired broker slug on dealledger.org: expect a 301 to the current broker page.
