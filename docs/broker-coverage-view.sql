-- Ranked broker crawl queue: BBS (BizQuest) listings held per broker vs.
-- DealLedger listings_direct coverage for that same broker, with domain
-- normalization and a franchise-parent roll-up.
--
-- NOT APPLIED. Proposed for review only.
--
-- Status: this SQL has NOT been executed against the live database. The join
-- logic, franchise map, and resulting numbers below were validated by
-- replicating the same logic in Python against data pulled via the PostgREST
-- API (no direct SQL/DB connection was available this session — see
-- docs/job-audit.md for that constraint). Before applying, run this once by
-- hand and diff its output against docs/broker-coverage-findings.md's numbers
-- (334 covered domains / 13,784 BBS listings; 735 uncovered domains / 6,490
-- BBS listings; 1,007 accounts / 8,892 listings with no resolvable domain at
-- all). If they don't match, trust neither until reconciled.
--
-- Known gaps in this version (see docs/broker-coverage-findings.md §"What's
-- still approximate"):
--   * Franchise roll-up only covers Transworld and Sunbelt, the two the queue
--     named. Other multi-office brands are not rolled up and will show as
--     fragmented/uncovered until someone identifies them the same way.
--   * broker_master.url is a relative BBS profile path in 100% of rows sampled
--     (e.g. /business-broker/matt-millsaps/hedgestone/41794/) and is never a
--     usable website — this view does not attempt to resolve it. The 1,007
--     accounts / 8,892 BBS listings behind broker_master rows with no usable
--     companyurl are left as "domain unknown", not "uncovered" — some of them
--     may already be covered under a domain we can't currently derive from
--     broker_master.
--   * www.sunbeltnetwork.com vs sunbeltnetwork.com in listings_direct.broker_domain
--     is itself a normalization bug on the ingestion side, not just this view's
--     problem — worth a separate fix so broker_domain is written normalized at
--     write time.

create or replace view v_broker_bbs_domain_norm as
-- Normalize broker_master's website field and roll franchise sub-brands up to
-- the domain DealLedger actually crawls centrally for that brand.
select
  bm.account,
  bm.companyname,
  bm.companyurl,
  case
    when bm.companyname ilike '%transworld%' then 'tworld.com'
    when bm.companyname ilike '%sunbelt%'     then 'sunbeltnetwork.com'
    when bm.companyurl is null or btrim(bm.companyurl) = '' then null
    when btrim(bm.companyurl) like '/%' then null  -- relative BBS profile path, not a website
    else regexp_replace(
           regexp_replace(lower(btrim(bm.companyurl)), '^https?://', ''),
           '^www\.', ''
         )
  end as canonical_domain
from broker_master bm;

create or replace view v_listings_direct_domain_counts as
-- Same normalization applied to the domain DealLedger already holds rows for,
-- so both sides of the join are comparable. Also fixes the www.-prefix
-- duplication seen live (www.sunbeltnetwork.com vs sunbeltnetwork.com).
select
  regexp_replace(lower(btrim(broker_domain)), '^www\.', '') as canonical_domain,
  count(*) as dealledger_active_listings
from listings_direct
where status = 'active'
  and broker_domain is not null
group by 1;

create or replace view v_broker_bbs_coverage as
with bbs_by_account as (
  select
    nullif(regexp_replace(b.account_id, '[^0-9]', '', 'g'), '')::bigint as account,
    count(*) as bbs_listings_held
  from bizquest_listings b
  where b.is_active
    and b.account_id is not null
  group by 1
),
joined as (
  select
    n.account,
    n.companyname,
    n.canonical_domain,
    bbs.bbs_listings_held
  from bbs_by_account bbs
  join v_broker_bbs_domain_norm n on n.account = bbs.account
)
select
  j.canonical_domain,
  -- pick a representative company name for display; a domain can span
  -- multiple broker_master accounts (individual agents, or a franchise
  -- rolled up from many offices)
  max(j.companyname) filter (where j.canonical_domain is not null) as sample_company_name,
  count(distinct j.account) as broker_master_accounts,
  sum(j.bbs_listings_held) as bbs_listings_held,
  coalesce(ldc.dealledger_active_listings, 0) as dealledger_active_listings,
  greatest(sum(j.bbs_listings_held) - coalesce(ldc.dealledger_active_listings, 0), 0) as estimated_gap,
  case
    when j.canonical_domain is null then 'unknown_domain'
    when coalesce(ldc.dealledger_active_listings, 0) > 0 then 'covered'
    else 'uncovered'
  end as coverage_state
from joined j
left join v_listings_direct_domain_counts ldc
  on ldc.canonical_domain = j.canonical_domain
group by j.canonical_domain, ldc.dealledger_active_listings
order by bbs_listings_held desc;

-- Ranked crawl queue: uncovered/under-covered brokers only, highest value first.
create or replace view v_broker_crawl_queue_ranked as
select *
from v_broker_bbs_coverage
where coverage_state in ('uncovered', 'unknown_domain')
order by bbs_listings_held desc;
