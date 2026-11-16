-- Rollback for 2026-11-16_site_page_hold.sql
begin;
set local dealledger.release_review = 'on';
update listings_direct d set needs_review = b.needs_review, updated_at = now()
from private.site_page_hold_20261116 b where d.id = b.id;
insert into quarantine_log (id, prior_status, new_status, reason, title, url, at)
select b.id, 'active (needs_review hold)', b.status, 'Rollback of 2026-11-16 site-page hold', b.title, b.url, now()
from private.site_page_hold_20261116 b;
commit;
