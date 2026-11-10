-- Rollback for 2026-11-10_stage2_reason_backfill.sql.
-- Restores last_error_type / last_error_message on rows the backfill filled,
-- but only where nothing has written a newer reason since (a later discovery
-- probe's value is kept).

begin;

update broker_sources b
   set last_error_type = k.old_last_error_type,
       last_error_message = k.old_last_error_message
  from private.stage2_reason_backfill_20261110 k
 where k.domain = b.domain
   and b.last_error_type is not distinct from k.new_last_error_type
   and b.last_error_message is not distinct from k.new_last_error_message;

select count(*) as restored_rows_remaining_with_backfill_text
  from broker_sources
 where last_error_message like 'backfilled 2026-11-10 from broker_discovery:%';

drop table private.stage2_reason_backfill_20261110;

commit;
