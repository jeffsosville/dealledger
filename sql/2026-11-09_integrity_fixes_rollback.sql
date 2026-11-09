-- Rollback for sql/2026-11-09_integrity_fixes.sql. Run only the PARTs you need to undo.
-- Set :day to the date the fixes were applied, e.g. '2026-11-09'.

-- PART 1 — trial domains
begin;
set local dealledger.release_quarantine = 'on';
update listings_direct l set status = q.prior_status
  from quarantine_log q
 where q.id = l.id and q.at::date = date '2026-11-09' and q.reason like '%(trial cohort 1)'
   and l.status = q.new_status;
delete from broker_block where reason like '%(trial cohort 1) — Jeff 2026-11-09';
update broker_sources b set discovery_stage = coalesce(t.prior_stage, '2_listings_url_known'),
       strategy_status = 'trial_failed', decision_reason = 'rollback of 2026-11-09 block', updated_at = now()
  from private.broker_trial t
 where t.domain = b.domain and b.decision_reason like 'Jeff 2026-11-09:%';
commit;

-- PART 2 — trial picker
drop function if exists private.pick_trial_cohort(int, int, float8);

-- PART 3 — www.* duplicates
begin;
update listings_direct l set status = 'active'
  from quarantine_log q
 where q.id = l.id and q.at::date = date '2026-11-09'
   and (q.reason like 'duplicate of live row under %' or q.reason like 'left observation: absent from % successful crawls of %')
   and l.status = q.new_status;
commit;
-- note: superseded -> active is blocked by trg_listings_direct_lock_superseded; if any must come back,
-- disable that trigger for the statement:  alter table listings_direct disable trigger trg_listings_direct_lock_superseded; ... enable

-- PART 4 — cleared cash_flow
begin;
update listings_direct l set cash_flow = c.cash_flow
  from private.financials_cleared c
 where c.id = l.id and c.cleared_at::date = date '2026-11-09' and l.cash_flow is null;
commit;
-- keep_filled bypass is inert unless the setting is used; to revert the function exactly,
-- re-create it without the `if coalesce(current_setting('dealledger.clear_fields' ...` guard.

-- PART 5 — price auto-publish hold
-- Re-run the detect_price_changes() definition from claude/price-detector-v7-2026-11-07.sql, then:
drop table if exists private.price_auto_hold;

-- PART 6 — misread-price exclusions
update private.price_evidence set exclusion = ''
 where exclusion = 'misread: revenue read as asking price (fixed 2026-11-09)';
-- stored prices written by --fix-price: the old values are in the backfill output; restore by id if needed.
