-- 2026-10-12: reconcile Eatz & Associates against their full listings page.
-- Tom Traina (Principal) asked us on Oct 12 to take the listings from
-- eatz-associates.com. Read in full that day: 49 For Sale, 11 Under Contract,
-- 5 For Lease (65). Nothing is deleted; every status change is logged in
-- quarantine_log with the reason.
begin;

-- 1. Listing-index pages stored as listings (?fwp_status=..., /page/N/): 52 rows
with t as (
  update listings_direct set status='quarantined_nonlisting', updated_at=now()
  where broker_domain ilike '%eatz%' and status='active' and url ~ 'fwp_status|/page/[0-9]'
  returning id, title, url)
insert into quarantine_log (prior_status, new_status, reason, title, url, at)
select 'active','quarantined_nonlisting','Eatz reconcile 2026-10-12: listings index page stored as a listing', title, url, now() from t;

-- 2. Duplicate rows for the same listing URL (older generic-crawl copies,
--    many with asking price = gross sales): keep the current specialized row.
with r as (
  select id, row_number() over (partition by url order by (id like 'spec:%') desc, last_seen desc) rn
  from listings_direct where broker_domain ilike '%eatz%' and status='active'),
t as (
  update listings_direct d set status='superseded', updated_at=now()
  from r where d.id=r.id and r.rn>1
  returning d.id, d.title, d.url)
insert into quarantine_log (prior_status, new_status, reason, title, url, at)
select 'active','superseded','Eatz reconcile 2026-10-12: duplicate of the current row for the same URL', title, url, now() from t;

-- 3. Space for lease / property only (no business sale): real estate rule.
with t as (
  update listings_direct set status='quarantined_nonlisting', updated_at=now()
  where broker_domain ilike '%eatz%' and status='active'
    and url ~ '(fully-built-out-equipped-restaurant-downtown-bartlett-il-space-for-lease|freestanding-restaurant-w-drive-thru-kenosha-wi-property-for-sale)'
  returning id, title, url)
insert into quarantine_log (prior_status, new_status, reason, title, url, at)
select 'active','quarantined_nonlisting','Eatz reconcile 2026-10-12: broker lists it as space for lease / property for sale, not a business', title, url, now() from t;

-- 4. Under Contract listings wrongly marked sold on Oct 6 (the Eatz scraper
--    only read For Sale + New Listing). Back to active with the broker's figures.
with v(id, price, rev, cf, city, st, loc) as (values
  ('spec:249593a869195cb5', 230000, 426404, 85842, null, 'IL', 'Western Suburb of Chicago, IL'),
  ('spec:811d78f1a2c03535', 265000, null, null, 'Wonder Lake', 'IL', 'Wonder Lake, IL'),
  ('spec:01cdb69c00da5e0c', 159000, 665462, 111348, 'Belgrade', 'MT', 'Belgrade, MT'),
  ('spec:3b30733c123edea7', 209000, 440967, 94959, 'Lynchburg', 'VA', 'Lynchburg, VA'),
  ('spec:885e406cbd9ff029', 375000, 847215, 262238, 'Plainfield', 'IN', 'Plainfield, IN'),
  ('spec:acb0e9f952f69662', 1100000, 1496582, 264523, null, 'IL', 'Far South Suburb of Chicago, IL'),
  ('spec:412564704a0b2335', 449000, 674100, 163650, 'West Lafayette', 'IN', 'West Lafayette & Lafayette, IN'),
  ('spec:2f8969454feec5f1', 150000, null, null, null, 'IL', 'Western Suburb of Chicago, IL'),
  ('spec:640b04a413118119', 1100000, 781185, 137271, 'Chicago', 'IL', 'Chicago, IL'),
  ('spec:9a24cf746b3002ee', 4500000, 4369975, 989719, 'Coconut Creek', 'FL', 'Coconut Creek, Coral Springs, Miami Lakes, Plantation, Pompano Beach, FL'),
  ('spec:1221e7d833fd9661', 3999000, 5277215, 1530448, 'Chicago', 'IL', 'Chicago, IL')),
t as (
  update listings_direct d set status='active', asking_price=v.price, revenue=coalesce(v.rev, d.revenue),
    cash_flow=coalesce(v.cf, d.cash_flow), city=coalesce(v.city, d.city), state=coalesce(v.st, d.state),
    location_raw=coalesce(d.location_raw, v.loc), last_seen=now(), delisted_at=null, updated_at=now()
  from v where d.id=v.id and d.status='sold'
  returning d.id, d.title, d.url)
insert into quarantine_log (prior_status, new_status, reason, title, url, at)
select 'sold','active','Eatz reconcile 2026-10-12: broker page shows Under Contract (still listed); was wrongly marked sold Oct 6', title, url, now() from t;

commit;

select status, published, count(*) from listings_direct where broker_domain ilike '%eatz%' group by 1,2 order by 3 desc;
