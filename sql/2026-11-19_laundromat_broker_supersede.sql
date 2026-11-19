-- 2026-11-19 — laundromat-broker.com moves from the V6 generic crawl to
-- scrapers/laundromat_broker_scraper.py (run_specialized key 'laundromatbroker').
--
-- The V6 rows used index URLs and #dl-<slug> fragments built from body text;
-- the new rows use #lb-<heading number>. Run this ONCE, after the first
-- specialized run has written the #lb- rows, so the old rows stop showing as
-- active. Same 'superseded' status the earlier re-key used.

-- 1. Check first: the new rows exist (expect ~154).
select count(*) as new_rows
from listings_direct
where broker_domain = 'laundromat-broker.com' and url like '%#lb-%';

-- 2. Retire the V6 rows.
update listings_direct
set status = 'superseded'
where broker_domain = 'laundromat-broker.com'
  and url not like '%#lb-%'
  and status in ('active', 'parse_error', 'sold');
