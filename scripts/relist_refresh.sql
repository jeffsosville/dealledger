-- ============================================================================
-- relist_refresh.sql
--
-- Sticky relist flagging. An active listing is marked relisted = true when
-- a matching record exists in listing_history under a *different* listing
-- number, sharing all of:
--   - state (history.region = listings.state)
--   - exact asking price (cast to numeric)
--   - first 20 chars of header (case-insensitive)
--   - price > $50,000 (filters noise / placeholder prices)
--
-- Sticky: once relisted = true, this script never flips it back to false.
-- Idempotent: re-running only flips false → true for newly-matched listings.
--
-- Produces the canonical 1,212 figure (April 2026 baseline) against the
-- listing_history snapshots imported June 2025 (42,467 rows) and
-- November 2025 (49,960 rows).
--
-- Canonical case: 11 FedEx Ground Routes, Ellenwood GA, broker account 48273
--   listing_numbers 2194203, 2343010, 2370957, 2496031 — same $800K price,
--   same $395,384 cash flow, 4 IDs over 2+ years.
-- ============================================================================

-- Requires pg_trgm (already enabled per index on listings.header).
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ----------------------------------------------------------------------------
-- Function: refresh_relisted()
-- Flips listings.relisted to true (sticky) for active listings that match
-- any record in listing_history under a different listing number.
-- Returns counts so callers can log them.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.refresh_relisted()
RETURNS TABLE (
    matched_total   bigint,  -- total active listings currently matching the rule
    newly_flagged   bigint,  -- rows this run flipped false → true
    already_flagged bigint   -- rows that were already true (sticky carry-over)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_matched_total   bigint := 0;
    v_newly_flagged   bigint := 0;
    v_already_flagged bigint := 0;
BEGIN
    -- Build the set of currently-matching active listing_numbers once,
    -- so we can both update and report against the same snapshot.
    CREATE TEMP TABLE _relist_matches ON COMMIT DROP AS
    SELECT DISTINCT b.listing_number
    FROM   listing_history a
    JOIN   listings        b
      ON   a.region                       = b.state
      AND  a.price::numeric               = b.price::numeric
      AND  LEFT(LOWER(a.header), 20)      = LEFT(LOWER(b.header), 20)
      AND  a.listnumber::bigint           <> b.listing_number
    WHERE  a.price IS NOT NULL
      AND  b.price IS NOT NULL
      AND  a.price::numeric > 50000
      AND  b.is_active = true;

    SELECT count(*) INTO v_matched_total FROM _relist_matches;

    -- Count how many of those are already flagged (sticky carry-over).
    SELECT count(*)
      INTO v_already_flagged
      FROM listings l
      JOIN _relist_matches m USING (listing_number)
     WHERE l.relisted = true;

    -- Flip only the rows that haven't been flagged yet.
    WITH updated AS (
        UPDATE listings l
           SET relisted = true
          FROM _relist_matches m
         WHERE l.listing_number = m.listing_number
           AND (l.relisted IS DISTINCT FROM true)
        RETURNING l.listing_number
    )
    SELECT count(*) INTO v_newly_flagged FROM updated;

    RETURN QUERY SELECT v_matched_total, v_newly_flagged, v_already_flagged;
END;
$$;

-- ----------------------------------------------------------------------------
-- One-time manual run (for ad-hoc refresh from psql / Supabase SQL editor):
--
--   SELECT * FROM public.refresh_relisted();
--
-- Expected output shape:
--   matched_total | newly_flagged | already_flagged
--   --------------+---------------+----------------
--           1235  |            23 |            1212
-- ----------------------------------------------------------------------------
