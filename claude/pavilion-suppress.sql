-- DealLedger — suppress the 2026-09-02 discovery-sweep junk domains
-- STATUS: PREPARED 2026-09-08, NOT YET APPLIED to kqckuedsyyosmccushyd.
-- Run manually (no DB execution access from this session).
--
-- CORRECTED 2026-09-08: the original version of this file set
-- status = 'removed'. 'removed' is not a live value on listings_direct — the
-- seven real ones are active, sold, superseded, parse_error, inactive,
-- quarantined_platform, quarantined_residential. 'removed' only exists in
-- ledger_ingestion.py (a diff key) and vertical_sync.py (writing to the
-- vertical marketplace tables) — neither is listings_direct. Running the
-- original version would have added an eighth status nothing in this table
-- filters for, so every row it touched would have silently vanished from any
-- reader that filters by an explicit status list rather than by
-- status != 'active'. Fixed below to use two statuses that already exist and
-- already have precedent for exactly this kind of row.
--
-- Five domains, ~2,157 currently-active junk rows, all first_seen 2026-09-02
-- (the same discovery sweep). Two distinct defects, not one:
--
--   pavilionservices.com, www.aria.net, www.sagbrokerage.com,
--   businessesforsale.nebba.com  ->  status = 'parse_error'
--     "Broken, not grown" (CLAUDE.md principle 7): a handful of distinct
--     titles carrying a large row count — index pages, blog placeholder
--     posts, or a pagination loop re-crawling the same cards as new.
--     Extraction produced garbage; these were never real listings, so
--     'inactive' would misstate what happened. Confirmed by eyeballing
--     cached local snapshot samples (no live DB read available this
--     session — see per-domain notes below):
--       pavilionservices.com — category-placeholder blog posts, e.g.
--         "Manufacturing Company" / "Distribution Company" repeated
--         verbatim with identical round prices ($25M, $100M).
--       www.aria.net — CLAUDE.md principle 7's own cited case: paginated
--         index URLs written as listings, 343 rows for 3 real listings.
--       www.sagbrokerage.com — data/latest.json + snapshots show 2 titles
--         across 55 sampled rows: "SAG Hospitality Brokerage a UD
--         Consulting Company" (site header text, 40x) and "Buy or Sell a
--         Liquor License" (a service/category page, 14x). Neither is a
--         listing.
--       businessesforsale.nebba.com — the worst locally: 6,318 sampled rows
--         collapse to a handful of literal titles repeated ~950x each
--         ("SG 11713 Ohio Dayton Piqua Vacant Unbranded Gas Station C
--         Store Develo", "6-Unit Apartment building and Retail Space") —
--         the same card re-crawled as new on every page, aria's exact
--         pattern.
--
--   jhcallahan.com  ->  status = 'quarantined_residential'
--     A DIFFERENT defect — do not read this as more of the same. Live-fetched
--     2026-09-08: this is a residential/commercial REAL ESTATE brokerage
--     (Twin Cities), not a business-for-sale broker. Its 450 rows likely ARE
--     450 distinct real property listings (each carries a distinct
--     ListingId in its URL) — they just aren't businesses, and its site
--     template gives every detail page the same generic title ("Listings
--     Details - JH Callahan"), which is what produced the misleading
--     "2 titles, 225 rows/title" signature. The fix applied in code is scope
--     exclusion (CRE_LEASE_DOMAINS), not a duplicate/junk-title finding.
--     'quarantined_residential' already holds exactly this bucket — same
--     precedent as davesmithrealty, cushmanwakefield, bhhsneproperties,
--     nextwavecommercial, daltonwade. Exact match, not an improvisation.
--
-- Code-side fixes already applied (2026-09-08, this session), so none of
-- this recurs going forward:
--   scrapers/dealledger_scraper_v6.py
--     — pavilionservices.com added to BLOCKLIST_DOMAINS (aria.net was
--       already there — principle 7's own case, proof the blocklist alone
--       doesn't touch data already written, which is why this file exists).
--     — jhcallahan.com added to CRE_LEASE_DOMAINS (same mechanism already
--       built for malonecb.com); CRE_LEASE_DOMAINS is now also checked at
--       broker-load time, not just per-card, so it stops wasting crawl
--       budget on either domain.
--     — title_repetition_gate(): a write-time check on rows/distinct-title
--       ratio (threshold 15, calibrated against real brokers that MUST
--       pass — execbb.com 5.4, restaurantrealty.com 6.2,
--       www.calhouncompanies.com 5.6, corbettrestaurantgroup.com 6.9 — and
--       confirmed junk that MUST reject — aria 35.7, sagbrokerage 48.0,
--       nebba 50.8; see scrapers/test_yield_gate.py). Catches the next
--       pavilion/aria/nebba without anyone needing to notice it first.
--     — is_blocked(domain) now gates the --broker single-URL path too, and
--       --broker forces Supabase off unless --write is passed (CLAUDE.md
--       principle 16) — this is how these four reached production in the
--       first place, bypassing every filter above.
--   agents/export_discovered_ok.py
--     — pavilionservices.com, cgkbusinesssales.com, www.sagbrokerage.com,
--       businessesforsale.nebba.com and jhcallahan.com all in
--       KNOWN_BAD_DOMAINS, so a status='ok' row in broker_discovery for any
--       of them is never exported into data/brokers_clean.csv again.
-- None of that touches rows already written. This file is that part.
--
-- Per CLAUDE.md principle 6 (nothing gets deleted): flag/deactivate, not
-- DELETE. Both statuses used below are existing, live values on
-- listings_direct — see the correction note above.

-- ---------------------------------------------------------------------------
-- Step 1: check scope before writing anything. Eyeball a few actual rows
-- per domain here, live, before running Step 2 — the notes above are from
-- cached local snapshot data, not this table.
-- ---------------------------------------------------------------------------
SELECT broker_domain, status, count(*)
FROM listings_direct
WHERE broker_domain ILIKE ANY (ARRAY[
        '%pavilionservices.com%',
        '%jhcallahan.com%',
        '%aria.net%',
        '%sagbrokerage.com%',
        '%nebba.com%'
      ])
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- SELECT title, asking_price, url, first_seen FROM listings_direct
-- WHERE broker_domain ILIKE '%<domain>%' AND status = 'active' LIMIT 10;

-- ---------------------------------------------------------------------------
-- Step 2: quarantine active rows in listings_direct.
-- Two separate UPDATEs, not one combined list, because the two groups get
-- different statuses for different reasons — keeping them apart also makes
-- each one individually reviewable/reversible.
-- ---------------------------------------------------------------------------
UPDATE listings_direct
SET status = 'parse_error',
    updated_at = now()
WHERE broker_domain ILIKE ANY (ARRAY[
        '%pavilionservices.com%',
        '%aria.net%',
        '%sagbrokerage.com%',
        '%nebba.com%'
      ])
  AND status = 'active';

UPDATE listings_direct
SET status = 'quarantined_residential',
    updated_at = now()
WHERE broker_domain ILIKE '%jhcallahan.com%'
  AND status = 'active';

-- ---------------------------------------------------------------------------
-- Step 3: same for the bridged `listings` table, IF any of these rows made
-- it across (public/sitemap-brokers.xml has a broker page for
-- pavilionservices.com, so at least some did at some point). VERIFY both the
-- column name AND the status vocabulary before running this — `listings` is
-- a different table from listings_direct and this session has no confirmed
-- read of its actual status values (docs/SCHEMA.md documents 'removed' for
-- some table, but that's exactly the claim that turned out wrong for
-- listings_direct — don't assume it transfers here uninspected).
-- ---------------------------------------------------------------------------
-- SELECT broker_domain, status, count(*) FROM listings
-- WHERE broker_domain ILIKE ANY (ARRAY[
--         '%pavilionservices.com%', '%jhcallahan.com%', '%aria.net%',
--         '%sagbrokerage.com%', '%nebba.com%'
--       ])
-- GROUP BY 1, 2;
--
-- -- Adjust the status values below to whatever Step 3's SELECT actually shows.
-- UPDATE listings
-- SET status = '<verified-inactive-equivalent>'
-- WHERE broker_domain ILIKE ANY (ARRAY[
--         '%pavilionservices.com%', '%aria.net%',
--         '%sagbrokerage.com%', '%nebba.com%'
--       ])
--   AND status = 'active';
--
-- UPDATE listings
-- SET status = '<verified-quarantine-equivalent>'
-- WHERE broker_domain ILIKE '%jhcallahan.com%'
--   AND status = 'active';

-- ---------------------------------------------------------------------------
-- Step 4: verify.
-- ---------------------------------------------------------------------------
SELECT broker_domain, status, count(*)
FROM listings_direct
WHERE broker_domain ILIKE ANY (ARRAY[
        '%pavilionservices.com%',
        '%jhcallahan.com%',
        '%aria.net%',
        '%sagbrokerage.com%',
        '%nebba.com%'
      ])
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
-- Expect 0 rows with status = 'active' for any of the five; jhcallahan.com
-- all 'quarantined_residential', the other four all 'parse_error'.

-- Rollback (undo step 2 only — restores every row this statement touched
-- back to 'active'; harmless to run even if nothing needed it):
-- UPDATE listings_direct SET status = 'active'
-- WHERE broker_domain ILIKE ANY (ARRAY[
--         '%pavilionservices.com%', '%aria.net%',
--         '%sagbrokerage.com%', '%nebba.com%'
--       ])
--   AND status = 'parse_error';
-- UPDATE listings_direct SET status = 'active'
-- WHERE broker_domain ILIKE '%jhcallahan.com%' AND status = 'quarantined_residential';
