"""
relist_refresh.py — DealLedger sticky relist flagging.

Calls the `refresh_relisted()` Postgres function (defined in
scripts/relist_refresh.sql) and logs the result. Sticky: only flips
false → true, never the reverse.

Pipeline position: run after quality_scorer.py, before vertical_sync.py,
so verticals inherit the latest relist flags.

Env:
  SUPABASE_URL           — Supabase project URL
  SUPABASE_SERVICE_KEY   — service-role key (needed to update listings)

Exit codes:
  0 — success
  1 — config / connection error
  2 — RPC error (function missing, SQL error, etc.)
"""

import logging
import os
import sys

from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")


def main() -> int:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        log.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in environment.")
        return 1

    try:
        sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    except Exception as e:
        log.error("Failed to create Supabase client: %s", e)
        return 1

    log.info("Calling refresh_relisted()...")

    try:
        result = sb.rpc("refresh_relisted").execute()
    except Exception as e:
        log.error("RPC refresh_relisted failed: %s", e)
        log.error("If this is the first run, apply scripts/relist_refresh.sql "
                  "to the database to create the function.")
        return 2

    data = result.data or []
    if not data:
        log.warning("refresh_relisted returned no rows — unexpected.")
        return 2

    row = data[0]
    matched_total   = row.get("matched_total", 0)
    newly_flagged   = row.get("newly_flagged", 0)
    already_flagged = row.get("already_flagged", 0)

    log.info("Relist refresh complete:")
    log.info("  matched_total   = %s (active listings currently matching)", matched_total)
    log.info("  already_flagged = %s (sticky carry-over from prior runs)",  already_flagged)
    log.info("  newly_flagged   = %s (flipped false -> true this run)",     newly_flagged)

    # Sanity-check: matched_total should equal already_flagged + newly_flagged.
    if matched_total != (already_flagged + newly_flagged):
        log.warning(
            "Count mismatch: matched_total (%s) != already_flagged (%s) + newly_flagged (%s). "
            "Check for race conditions or manual flag flips.",
            matched_total, already_flagged, newly_flagged,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
