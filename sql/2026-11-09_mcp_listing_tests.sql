-- Tests for public.mcp_listing(bigint). Run as postgres; section B switches to anon inside a rolled-back transaction.
-- Results 2026-11-09: all pass. See claude/mcp-listing-rpc-2026-11-09.md.

-- A. Function properties and privileges (expect: t,t,search_path pinned, no PUBLIC in acl, t, t, f,f,f,f)
select p.prosecdef security_definer, p.provolatile = 's' stable, p.proconfig::text config, p.proacl::text acl,
       p.prosrc !~* '\m(insert|update|delete|truncate|alter|drop|grant|revoke|create|copy|execute)\M' no_dml_in_body,
       has_function_privilege('anon', p.oid, 'EXECUTE') anon_exec,
       has_table_privilege('anon','public.listings_direct','SELECT') anon_listings_direct,
       has_table_privilege('anon','public.listing_sighting','SELECT') anon_listing_sighting,
       has_table_privilege('anon','private.price_change','SELECT') anon_price_change,
       has_schema_privilege('anon','private','USAGE') anon_private_usage
from pg_proc p where p.oid = 'public.mcp_listing(bigint)'::regprocedure;

-- B. Behaviour as anon. Listing numbers picked 2026-11-09 (re-pick if data changes):
--    3 per non-eligible status, active-unpublished, the one marketplace-domain row, the one hash collision,
--    bad inputs, and 3 per age basis / broker date / approved price change.
begin;
set local role anon;
with t(cat, ln, expect) as (values
 ('active_unpublished',900042156::bigint,false),('active_unpublished',900093354,false),('active_unpublished',900103202,false),
 ('inactive',900013145,false),('inactive',900027118,false),('inactive',900046700,false),
 ('sold',900012675,false),('sold',900029993,false),('sold',900041058,false),
 ('superseded',900014720,false),('superseded',900018910,false),('superseded',900020445,false),
 ('parse_error',900053574,false),('parse_error',900092167,false),('parse_error',900103156,false),
 ('q_blocked',900010530,false),('q_blocked',900014022,false),('q_blocked',900045545,false),
 ('q_nonlisting',900045331,false),('q_nonlisting',900209811,false),('q_nonlisting',900805363,false),
 ('q_platform',900380389,false),('q_platform',900618187,false),('q_platform',901867596,false),
 ('q_residential',900004045,false),('q_residential',900031995,false),('q_residential',900040245,false),
 ('marketplace_domain',984151948,false),('collision',1081745256,false),
 ('null',null,false),('zero',0,false),('negative',-1,false),('huge',9223372036854775807,false),
 ('age_in_db',900112083,true),('age_in_db',900123609,true),('age_in_db',900205555,true),
 ('age_observed',900059527,true),('age_observed',900066790,true),('age_observed',900079610,true),
 ('age_window',900572722,true),('age_window',900582324,true),('age_window',900584546,true),
 ('broker_date',900387753,true),('broker_date',900468485,true),('broker_date',900692603,true),
 ('price_change',900853153,true),('price_change',901413667,true),('price_change',906129876,true)),
r as (select cat, ln, expect, public.mcp_listing(ln) j from t)
select cat, ln,
  (j->>'found')::boolean = expect as pass_found,
  case when expect then j->>'status' = 'active' else true end as pass_status,
  j::text !~* '"(first_seen|true_first_seen|days_on_market|dom_|contact_name|contact_phone|description|relist|bbs_listing_id|profile_views|seed_)' as pass_no_forbidden_keys,
  j->'listing_age'->>'basis' age_basis, j->'listing_age'->>'label' age_label,
  j->'broker_stated_date'->>'label' broker_date_label,
  jsonb_array_length(j->'price_history') price_steps,
  j->'observation'->>'distinct_days_observed' obs_days
from r order by expect, cat;
rollback;

-- C. Consistency sample against public views (40 random on-site listings; ~130 ms per call)
with s as (select listing_number ln from mv_seo_listings_v2 order by md5(listing_number::text) limit 40),
r as (select ln, public.mcp_listing(ln) j from s)
select count(*) sampled,
 count(*) filter (where (j->>'found')::boolean) found,
 count(*) filter (where (j->>'found')::boolean and j->>'status' <> 'active') bad_status,
 count(*) filter (where (j->>'found')::boolean and j->'listing_age'->>'basis' is null) no_age,
 count(*) filter (where (j->>'found')::boolean and (j->'observation'->>'distinct_days_observed')::int = 0) zero_obs,
 count(*) filter (where (j->>'found')::boolean and jsonb_array_length(j->'price_history') <> (select count(*) from price_history p where p.listing_number=r.ln)) price_mismatch,
 count(*) filter (where (j->>'found')::boolean and (j->'listing_age'->>'basis') is distinct from
   (case (select age_basis from listing_age_v2 a where a.listing_number=r.ln)
      when 'window' then 'appearance_window' when 'observed' then 'dealledger_first_observed' when 'in_db' then 'dealledger_first_recorded' end)) age_mismatch
from r;
