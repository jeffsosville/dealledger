-- 2026-11-19 — feldmanbrokerage.com moves from the V6 generic crawl to
-- scrapers/feldman_scraper.py (run_specialized key 'feldman').
-- V6 stored each text line ("Net 96,305", "Selling price. $1,299,000") as a
-- listing under #dl-<slug> URLs. The new rows use #fb-<title>-<county>.
-- Run ONCE, after the first specialized run has written the #fb- rows.

-- 1. Check first: the new rows exist (expect ~64).
select count(*) as new_rows
from listings_direct
where broker_domain = 'feldmanbrokerage.com' and url like '%#fb-%';

-- 2. Retire the V6 rows.
update listings_direct
set status = 'superseded'
where broker_domain = 'feldmanbrokerage.com'
  and url not like '%#fb-%'
  and status in ('active', 'parse_error', 'sold');
