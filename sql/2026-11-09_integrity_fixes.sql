-- 2026-11-09 — fixes from the weekly integrity check (Nov 9)
-- Run each PART on its own, in order, after its dry-run SELECT looks right.
-- Rollback: sql/2026-11-09_integrity_fixes_rollback.sql
-- Every status change is logged in quarantine_log with its reason.
-- Every cleared financial value is copied to private.financials_cleared first.

------------------------------------------------------------------------------------------
-- PART 1  [P1] Non-broker trial domains out (Jeff, Nov 9: "fix all of this")
--   github.com, gov.texas.gov, zeebiz.com (news)      -> quarantined_nonlisting
--   realselectintl.com, homesmart.com (real estate)   -> quarantined_residential
--   match-entreprises.com (France, not US)            -> quarantined_nonlisting
--   laundromatmarketplace.com, headoversea.com        -> NOT included; Jeff's call
------------------------------------------------------------------------------------------
-- dry run
select regexp_replace(lower(broker_domain), '^www\.', '') d, status, count(*)
from listings_direct
where regexp_replace(lower(broker_domain), '^www\.', '') in
      ('github.com','gov.texas.gov','zeebiz.com','realselectintl.com','homesmart.com','match-entreprises.com')
group by 1, 2 order by 1, 2;

begin;
create temp table _p1(d text, new_status text, why text) on commit drop;
insert into _p1 values
  ('github.com',            'quarantined_nonlisting',  'not a broker: code host (trial cohort 1)'),
  ('gov.texas.gov',         'quarantined_nonlisting',  'not a broker: government site (trial cohort 1)'),
  ('zeebiz.com',            'quarantined_nonlisting',  'not a broker: news site (trial cohort 1)'),
  ('realselectintl.com',    'quarantined_residential', 'real estate brokerage (trial cohort 1)'),
  ('homesmart.com',         'quarantined_residential', 'real estate brokerage (trial cohort 1)'),
  ('match-entreprises.com', 'quarantined_nonlisting',  'outside US scope: French broker (trial cohort 1)');

insert into broker_block (broker_domain, reason, updated_at)
select d, why || ' — Jeff 2026-11-09', now() from _p1
on conflict (broker_domain) do update set reason = excluded.reason, updated_at = now();

insert into quarantine_log (id, prior_status, new_status, reason, title, url, at)
select l.id, l.status, p.new_status, p.why, l.title, l.url, now()
from listings_direct l join _p1 p on regexp_replace(lower(l.broker_domain), '^www\.', '') = p.d
where l.status = 'active';

update listings_direct l set status = p.new_status
from _p1 p
where regexp_replace(lower(l.broker_domain), '^www\.', '') = p.d and l.status = 'active';

update private.broker_trial t set verdict = coalesce(t.verdict, 'fail_quality'),
       verdict_reason = coalesce(t.verdict_reason || '; ', '') || 'blocked by Jeff 2026-11-09: ' || p.why,
       decided_at = coalesce(t.decided_at, now())
from _p1 p where t.domain = p.d;

update broker_sources b set discovery_stage = '0_blocked', strategy_status = 'trial_failed',
       decided_by = 'human', decided_at = now(), decision_reason = 'Jeff 2026-11-09: ' || p.why, updated_at = now()
from _p1 p where b.domain = p.d;
commit;

------------------------------------------------------------------------------------------
-- PART 2  [P1] Reusable trial picker with a domain sanity filter (needed before cohort 2)
------------------------------------------------------------------------------------------
create or replace function private.pick_trial_cohort(p_cohort int, p_n int default 50, p_seed float8 default 0.1107)
returns int language plpgsql security definer set search_path to 'public','private','pg_temp' as $$
declare n int;
begin
  perform setseed(p_seed);
  insert into private.broker_trial (domain, cohort, prior_stage, prior_status, listing_url)
  select domain, p_cohort, discovery_stage, strategy_status, listing_url
  from (
    select b.*, random() r
    from broker_sources b
    where b.discovery_stage = '2_listings_url_known'
      and b.strategy_status = 'proposed'
      and norm_domain(b.listing_url) = b.domain
      and coalesce(b.decided_by, '') <> 'human'
      and b.listing_url !~* '/(post|posts|blog|news|article|articles|resources|insights)/|valuation|how-to|/sell-?(ing)?-'
      -- sanity filter (cohort 1 let in github, a .gov site, a news site, realtors, a French broker)
      and b.domain !~* '\.(gov|edu|mil|org)$|\.(gov|edu)\.[a-z]{2}$'
      and b.domain !~* '\.(fr|de|uk|ca|au|es|it|nl|be|ch|in|br|mx)$'
      and b.domain !~* '(^|\.)(github|gitlab|wikipedia|medium|substack|wordpress|blogspot|linkedin|facebook|youtube|yelp|reddit|zeebiz|forbes|inc|entrepreneur|bizjournals)\.'
      and b.domain !~* '(realt|homes?smart|idx|mls|properties|property|apartments|loopnet|crexi|kw\.com|remax|coldwell|century21|sothebys|compass\.com|exp ?realty)'
      and b.domain !~* '(bizbuysell|bizquest|businessesforsale|businessbroker\.net|bizben|dealstream|flippa|empireflippers|quietlight|acquire\.com|microacquire|websiteclosers|feinternational|marketplace)'
      and not exists (select 1 from broker_review_applied r where r.domain = b.domain)
      and not exists (select 1 from broker_block k where regexp_replace(lower(k.broker_domain), '^www\.', '') = b.domain)
      and not exists (select 1 from listings_direct l
                       where regexp_replace(lower(l.broker_domain), '^www\.', '') = b.domain)
      and not exists (select 1 from private.broker_trial t where t.domain = b.domain)
    order by r
    limit p_n
  ) c
  on conflict (domain) do nothing;
  get diagnostics n = row_count;

  update broker_sources b
     set discovery_stage = '3_crawlable', strategy_status = 'trial',
         decided_by = 'auto', decided_at = now(),
         decision_reason = 'trial cohort ' || p_cohort || ': crawl privately, publish only if checks pass',
         updated_at = now()
    from private.broker_trial t
   where t.domain = b.domain and t.verdict is null and t.cohort = p_cohort;
  return n;
end $$;
revoke all on function private.pick_trial_cohort(int, int, float8) from public, anon, authenticated;
-- Cohort 2 is NOT started here. Preview what it would pick (read-only):
--   select domain, listing_url from broker_sources b where ... (same where-clause) order by random() limit 50;
-- Start it, once Jeff says go:  select private.pick_trial_cohort(2, 50, 0.1109);

------------------------------------------------------------------------------------------
-- PART 3  [P1/P2] Stale www.* duplicates of brokers that are crawled OK daily
--   The broker is crawled successfully under its bare domain every day; the old www.*
--   rows are never refreshed and still say active. Removal rule: the broker's crawl
--   succeeds with a normal count and the listing is absent from >= 3 consecutive
--   successful crawls.
--     * same URL live under the bare key -> superseded (duplicate)
--     * otherwise                        -> inactive (left observation), reason logged
------------------------------------------------------------------------------------------
create temp table _p3 as
with o as (
  select l.id, l.broker_domain, l.url, l.title, l.last_seen,
         regexp_replace(lower(l.broker_domain), '^www\.', '') bare,
         regexp_replace(lower(l.url), '^https?://(www\.)?', '') nurl
  from listings_direct l
  where l.status = 'active' and l.broker_domain ~* '^www\.' and l.last_seen < current_date - 7
), ok as (
  select o.id,
         (select count(*) from crawl_run c
           where c.broker_domain = o.bare and c.status = 'ok' and c.started_at::date > o.last_seen
             and c.listings_seen >= 0.5 * (select max(c2.listings_seen) from crawl_run c2
                                            where c2.broker_domain = o.bare and c2.status = 'ok')) good_runs
  from o
)
select o.*, ok.good_runs,
       exists (select 1 from listings_direct f where f.broker_domain = o.bare and f.status = 'active'
               and regexp_replace(lower(f.url), '^https?://(www\.)?', '') = o.nurl) live_dup
from o join ok using (id)
where ok.good_runs >= 3;

-- dry run
select broker_domain, count(*) n, count(*) filter (where live_dup) dup, min(good_runs) min_runs from _p3 group by 1 order by 2 desc;

begin;
insert into quarantine_log (id, prior_status, new_status, reason, title, url, at)
select id, 'active', case when live_dup then 'superseded' else 'inactive' end,
       case when live_dup then 'duplicate of live row under ' || bare
            else 'left observation: absent from ' || good_runs || ' successful crawls of ' || bare || ' since ' || last_seen end,
       title, url, now()
from _p3;
update listings_direct l set status = case when p.live_dup then 'superseded' else 'inactive' end
from _p3 p where l.id = p.id and l.status = 'active';
commit;

------------------------------------------------------------------------------------------
-- PART 4  [P4] Sticky wrong financials
--   V6's _sane_financials() already drops impossible values on every new scrape, but
--   trg_listings_direct_keep_filled coalesces NULL back to the old value, so the wrong
--   numbers written before Sep 17 never clear. Add an explicit bypass, then clear
--   cash_flow where it is impossible (= asking price, = revenue, or >= revenue).
--   asking_price is NOT touched (price history is unaffected).
--   revenue = asking_price (2,000+ active rows) is NOT cleared here: on some sites the
--   asking price itself is the revenue figure ("$33M Roofing Company", price 33M), so
--   those rows are held from price-change auto-publish (PART 5) until detail maps fix them.
------------------------------------------------------------------------------------------
create or replace function public.listings_direct_keep_filled()
returns trigger language plpgsql as $function$
begin
  -- Explicit repair: set local dealledger.clear_fields = 'on' to let NULLs through.
  if coalesce(current_setting('dealledger.clear_fields', true), '') <> 'on' then
    new.asking_price := coalesce(new.asking_price, old.asking_price);
    new.cash_flow := coalesce(new.cash_flow, old.cash_flow);
    new.revenue := coalesce(new.revenue, old.revenue);
    new.city := coalesce(new.city, old.city);
    new.state := coalesce(new.state, old.state);
  end if;
  -- broker-reported dates: keep the last reported value when a scrape omits it
  new.broker_published_on := coalesce(new.broker_published_on, old.broker_published_on);
  new.broker_modified_on := coalesce(new.broker_modified_on, old.broker_modified_on);
  new.broker_date_source := coalesce(new.broker_date_source, old.broker_date_source);
  new.broker_date_first_recorded := coalesce(old.broker_date_first_recorded,
    case when new.broker_published_on is not null then current_date end);
  return new;
end $function$;

create table if not exists private.financials_cleared (
  id text, broker_domain text, asking_price numeric, cash_flow numeric, revenue numeric,
  rule text, cleared_at timestamptz default now());

-- dry run (expect ~1,156 rows, 0 would unpublish)
select count(*) n, count(distinct broker_domain) domains,
       count(*) filter (where status = 'active' and published) active_pub,
       count(*) filter (where status = 'active' and published
                        and not is_publishable(title, asking_price, null, status, false)) would_unpublish
from listings_direct
where status not like 'quarantined%' and cash_flow is not null
  and (cash_flow = asking_price or cash_flow = revenue or (revenue > 0 and cash_flow >= revenue));

begin;
create temp table _p4 on commit drop as
select id, broker_domain, asking_price, cash_flow, revenue,
       case when cash_flow = asking_price then 'cf=price' when cash_flow = revenue then 'cf=revenue' else 'cf>=revenue' end rule
from listings_direct
where status not like 'quarantined%' and cash_flow is not null
  and (cash_flow = asking_price or cash_flow = revenue or (revenue > 0 and cash_flow >= revenue));
insert into private.financials_cleared (id, broker_domain, asking_price, cash_flow, revenue, rule)
select id, broker_domain, asking_price, cash_flow, revenue, rule from _p4;
set local dealledger.clear_fields = 'on';
update listings_direct l set cash_flow = null from _p4 p where p.id = l.id and l.cash_flow = p.cash_flow;
commit;

------------------------------------------------------------------------------------------
-- PART 5  [P4] Hold price-change auto-publish where the asking price may be misread
--   (auto-publish is globally off today; this keeps these out when it is switched on)
------------------------------------------------------------------------------------------
create table if not exists private.price_auto_hold (
  domain text primary key, reason text not null, added_at timestamptz default now());
insert into private.price_auto_hold (domain, reason) values
  ('commercialroutesforsale.com', 'financials majority-corrupt (51/96) — detail map pending'),
  ('americanbusinessbrokers.com', 'financials majority-corrupt (39/48) — detail map pending')
on conflict (domain) do nothing;

-- detect_price_changes(): identical to the live v7 function except the marked block.
create or replace function private.detect_price_changes()
 returns jsonb language plpgsql security definer set search_path to 'public', 'private', 'pg_temp'
as $function$
declare v_cutoff date; v_run bigint; v_ins int := 0; v_upd int := 0; v_wd int := 0; v_cand int; v_on boolean;
begin
  select max(snapshot_date) into v_cutoff from private.price_source_file where file_id > 0;
  select coalesce((select auto_publish from private.price_detector_config where id), false) into v_on;
  insert into private.price_detection_run (status, live_cutoff) values ('running', v_cutoff) returning run_id into v_run;
  drop table if exists _cand;
  create temp table _cand on commit drop as
  select c.*, (select string_agg(r.id::text, ',') from public.crawl_run r
               where r.broker_domain in (split_part(i.identity_url, '/', 1), 'www.' || split_part(i.identity_url, '/', 1))
                 and r.status in ('ok', 'partial') and r.started_at::date = c.next_first_seen) as next_runs
  from private.price_change_candidates(v_cutoff) c
  join private.price_identity i on i.identity_id = c.identity_id;
  select count(*) into v_cand from _cand;

  -- >>> 2026-11-09 hold: domain under repair, or listing's asking price equals its revenue
  update _cand c set auto_publish = false,
         auto_blockers = concat_ws('; ', nullif(c.auto_blockers, ''),
           case when h.domain is not null then 'domain hold: ' || h.reason end,
           case when d.revenue = d.asking_price then 'asking price equals revenue (possible misread)' end)
    from private.price_identity i
    join public.listings_direct d on d.id = i.listing_id
    left join private.price_auto_hold h on h.domain = regexp_replace(lower(d.broker_domain), '^www\.', '')
   where i.identity_id = c.identity_id and c.auto_publish
     and (h.domain is not null or d.revenue = d.asking_price);
  -- <<<

  alter table _cand add column eff_status text, add column eff_reviewer text, add column eff_ref text;
  update _cand set
    eff_status   = case when screen_result = 'hard' then 'not_eligible'
                        when auto_publish and v_on then 'approved' else 'pending' end,
    eff_reviewer = case when auto_publish and v_on then 'auto:v7' else '' end,
    eff_ref      = case when auto_publish then 'auto-eligible:v7' else '' end;
  insert into private.price_change (identity_id, listing_id, prev_price, prev_first_seen, prev_last_seen, next_price,
         next_first_seen, next_last_seen, pct_change, prev_source, next_source, screen_result, screen_reasons,
         rules_version, review_status, reviewer, review_ref, origin, detected_at, decided_at)
  select c.identity_id, c.listing_id, c.prev_price, c.prev_first_seen, c.prev_last_seen, c.next_price,
         c.next_first_seen, c.next_last_seen, c.pct_change, c.prev_source,
         c.next_source || coalesce(' run=' || c.next_runs, ''), c.screen_result,
         concat_ws(' | ', nullif(c.screen_reasons, ''), case when not c.auto_publish then 'auto: ' || c.auto_blockers end),
         'crawl.2026-11-07.v7',
         c.eff_status, c.eff_reviewer, c.eff_ref, 'crawl', now(),
         case when c.eff_status <> 'pending' then now() end
  from _cand c
  where not exists (select 1 from private.price_change x
                    where x.identity_id = c.identity_id and x.next_first_seen = c.next_first_seen);
  get diagnostics v_ins = row_count;
  update private.price_change x
     set next_last_seen = c.next_last_seen, screen_result = c.screen_result,
         screen_reasons = concat_ws(' | ', nullif(c.screen_reasons, ''), case when not c.auto_publish then 'auto: ' || c.auto_blockers end),
         rules_version = 'crawl.2026-11-07.v7',
         review_status = c.eff_status, reviewer = c.eff_reviewer, review_ref = c.eff_ref,
         decided_at = case when c.eff_status is distinct from x.review_status then now() else x.decided_at end
    from _cand c
   where x.identity_id = c.identity_id and x.next_first_seen = c.next_first_seen
     and x.origin = 'crawl' and (x.reviewer = '' or x.reviewer like 'auto:%')
     and (x.review_ref = '' or x.review_ref like 'auto-eligible:%')
     and (x.next_last_seen, x.screen_result, x.review_status, x.reviewer, x.review_ref)
         is distinct from (c.next_last_seen, c.screen_result, c.eff_status, c.eff_reviewer, c.eff_ref);
  get diagnostics v_upd = row_count;
  update private.price_change x
     set review_status = 'not_eligible', reviewer = '', decided_at = now(),
         screen_reasons = concat_ws(' | ', nullif(x.screen_reasons, ''), 'withdrawn: no longer supported by the evidence')
   where x.origin = 'crawl' and (x.reviewer = '' or x.reviewer like 'auto:%')
     and (x.review_ref = '' or x.review_ref like 'auto-eligible:%') and x.review_status <> 'not_eligible'
     and x.next_first_seen > v_cutoff
     and not exists (select 1 from _cand c where c.identity_id = x.identity_id and c.next_first_seen = x.next_first_seen);
  get diagnostics v_wd = row_count;
  update private.price_detection_run r set finished_at = now(), status = 'ok', candidates = v_cand,
         inserted = v_ins, updated = v_upd, withdrawn = v_wd,
         auto_published = (select count(*) from private.price_change where origin = 'crawl' and reviewer = 'auto:v7' and review_status = 'approved'),
         note = case when v_on then '' else 'auto-publish off: eligible changes kept private' end,
         pending = (select count(*) from private.price_change where origin = 'crawl' and review_status = 'pending'),
         hard = (select count(*) from private.price_change where origin = 'crawl' and review_status = 'not_eligible')
   where r.run_id = v_run;
  return jsonb_build_object('run_id', v_run, 'cutoff', v_cutoff, 'candidates', v_cand,
                            'inserted', v_ins, 'updated', v_upd, 'withdrawn', v_wd);
end $function$;
