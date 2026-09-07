-- ============================================================
-- is_listing_junk ADDITIONS — proposed, night of 2026-09-02
-- Item 3 of claude/overnight-queue.md
--
-- Method (CLAUDE.md principle 8, mandatory):
--   1. Reproduced the queue's grouping query live via REST (26,505 active
--      broker_direct rows as of this run).
--   2. Drafted one candidate predicate per junk shape.
--   3. Built a 500-row known-good sample (random draw from the 23,929 active
--      rows NOT in a >15-count duplicate-header group), then, per the
--      methodology note in the queue, scrubbed the sampling POOL of any row
--      already matching a candidate rule before drawing the 500 — see the
--      "Automotive/Transportation-Gas Station Only" note below for why that
--      scrub step mattered.
--   4. Ran every candidate against the 500-row sample AND, as extra
--      diligence, the full 23,418-row scrubbed pool. Zero false rejects
--      required to ship.
--   5. Timed the shipped patterns client-side: 4 anchored regexes x 10 passes
--      x 26,505 headers = 0.11s total, ~10.6ms per full-table pass. These are
--      cheap (all anchored with ^, no backtracking) — nothing like the
--      6.8s/3,000-row cost that caused the bridge timeout. No trigger/column
--      needed for these five; flagged again at the bottom for what WOULD need
--      one.
--
-- IMPORTANT — found, not part of this task, but too load-bearing to omit:
-- The deployed is_listing_junk(t,u) does NOT match the repo's
-- junk_filter_rules.sql. Live-tested via /rest/v1/rpc/is_listing_junk:
--   is_listing_junk('Land for Sale - Saunders Real Estate', ...)  -> false
--     (repo file's rule 9, real-estate-not-business, should catch this)
--   is_listing_junk('$860,000 Gross Revenue', ...)                -> false
--     (repo file's rule 4, financial fragment, should catch this)
-- Rules 2, 3, and 7 (nav words, status words, privacy/terms) ARE live.
-- Rules 4, 6, and 9 from the checked-in file appear never deployed. That gap
-- is separate from tonight's additions below and should be reconciled on its
-- own — deploying the existing file may already clear some of the "General
-- Services-*" and "Land for Sale" rows before any of the below is even
-- applied. Not verified further tonight (read-only, no function changes
-- applied).
-- ============================================================


-- ----------------------------------------------------------------
-- SHAPE 1 — category label leaked into title (SHIP)
-- Broker: "Executive Business Brokers (Larry Bodner)" only. Their real
-- listings are titled "<listingID> <Category>-<Subcategory>" (e.g.
-- "13826107DPB4 General Services-Laundromat" — confirmed present and
-- untouched by this rule because of the anchor). A subset of their rows
-- lost the ID prefix during extraction and are left as bare
-- "General Services-Laundromat", "Food/Liquor-Restaurant", etc.
--
-- Pattern:      ^(General Services|General Retail|Food/Liquor|Automotive/Transportation)-
-- Matches:      957 of 26,505 active rows (654 were already >15-count groups;
--               303 more are the same shape at lower per-string duplicate
--               counts, e.g. "General Services-Adult Day Care",
--               "Automotive/Transportation-Gas/Repair" — same broker, same
--               defect, just less repetitive category text).
-- False rejects: 0 / 500 known-good sample. 0 / 23,418 full scrubbed pool.
--
-- NOTE: the sampling pool initially included
-- "Automotive/Transportation-Gas Station Only" (price 50000, same broker,
-- unique count) picked up by unscrubbed random sampling — it is NOT a real
-- title, it's the identical defect at a category string that happens to be
-- rare. It was removed from the known-good pool before the false-reject
-- count above was taken, per the queue's "filter out anything matching your
-- candidate junk patterns already" instruction. Leaving it in would have
-- been a false "known-good" row, not a false reject by the rule.
--
-- VERDICT: SHIP
select header, count(*) from listings
where is_active and source='broker_direct'
  and header ~* '^(general services|general retail|food/liquor|automotive/transportation)-'
group by 1 order by 2 desc;
-- (957 rows total; run above to re-verify before deploying)


-- ----------------------------------------------------------------
-- SHAPE 2 — broker tagline / firm description used as title (SHIP)
-- Four exact strings, each repeated 38-64 times, all firm marketing copy,
-- not a listing title. Exact-match (not substring) to keep blast radius at
-- zero — these are long, specific, and not the kind of thing that appears
-- inside a real title as a substring.
--
-- Includes "SD Business Advisors | The business selling experts" — note
-- this is a DIFFERENT string than what junk_filter_rules.sql rule 6 already
-- targets (rule 6 requires length < 45 chars and the title to *end* with
-- "the business selling experts"; this string is 51 chars because of the
-- "SD Business Advisors | " prefix, so rule 6 — even if deployed — would
-- still miss it). Exact-match sidesteps that length trap entirely.
--
-- Matches: 249 of 26,505 active rows.
-- False rejects: 0 / 500 known-good. 0 / 23,418 full scrubbed pool.
--
-- VERDICT: SHIP
select header, count(*) from listings
where is_active and source='broker_direct'
  and lower(trim(regexp_replace(header, '\s+', ' ', 'g'))) = any(array[
        'sag hospitality brokerage a ud consulting company',
        'specializing in the resale of franchise businesses since 1978',
        'sd business advisors | the business selling experts',
        'connecting your businesswith extraordinary opportunity'  -- yes, missing space is in the source data
      ])
group by 1 order by 2 desc;
-- (249 rows total)


-- ----------------------------------------------------------------
-- SHAPE 3 — buyer-side ad, not a listing (SHIP)
-- "Seller Distribution Buyer with funds up to $100M to invest
--  $ 100,000,000.00" and variants. This is a buyer looking for deals, not a
-- business for sale — shouldn't be in the index as a "listing" at all.
--
-- Pattern:       buyer with funds  (case-insensitive substring)
-- Matches:       58 of 26,505 active rows.
-- False rejects: 0 / 500 known-good. 0 / 23,418 full scrubbed pool.
--
-- VERDICT: SHIP
select header, count(*) from listings
where is_active and source='broker_direct'
  and header ~* 'buyer with funds'
group by 1 order by 2 desc;
-- (58 rows total)


-- ----------------------------------------------------------------
-- SHAPE 4 — "Year Established: ..." metadata fragment as title (SHIP)
-- Scraper grabbed a detail-page metadata line instead of the title. All 25
-- distinct variants found are the SAME shape end to end — "Year
-- Established: <year> (Current Owner(s)/Location <year>)" and nothing else
-- appended (checked every distinct string, longest is 77 chars and is still
-- pure metadata, no business name riding along).
--
-- Pattern:       ^Year Established\s*:   (anchored to start)
-- Matches:       409 of 26,505 active rows — much larger than the 27-row
--                group the queue's >15 query surfaced, because the
--                "(Current Owner YYYY)" suffix makes almost every row's full
--                string unique, so most instances never crossed the >15
--                threshold despite being the identical defect.
-- False rejects: 0 / 500 known-good. 0 / 23,418 full scrubbed pool.
--
-- VERDICT: SHIP
select header, count(*) from listings
where is_active and source='broker_direct'
  and header ~* '^year established\s*:'
group by 1 order by 2 desc;
-- (409 rows total)


-- ----------------------------------------------------------------
-- SHAPE 5 — "Business Type ..." filter-widget dropdown scraped as title (SHIP)
-- "Business Type Agriculture Automotive Business Support & Supplies
--  Computers & Electronics Contractors..." — a category-filter dropdown's
-- full option list, scraped as the listing title. Broker: matt sadati.
--
-- Pattern:       ^Business Type\s
-- Matches:       20 of 26,505 active rows (all identical string).
-- False rejects: 0 / 500 known-good. 0 / 23,418 full scrubbed pool.
--
-- VERDICT: SHIP
select header, count(*) from listings
where is_active and source='broker_direct'
  and header ~* '^business type\s'
group by 1 order by 2 desc;
-- (20 rows total)


-- ============================================================
-- PROPOSED PATCH — add to is_listing_junk(t text, u text), section 6
-- (broker firm name as title) and as new sections. Additive only; existing
-- clauses (1-9 in junk_filter_rules.sql) are untouched. Whether this lands
-- on top of the currently-deployed function or the full checked-in file is
-- a separate decision — see the deployment-gap note at the top.
-- ============================================================

-- Add into the WHERE-style OR chain of is_listing_junk:

    -- 10. CATEGORY LABEL LEAKED INTO TITLE (broker extraction defect)
    or t ~* '^(general services|general retail|food/liquor|automotive/transportation)-'

    -- 11. BROKER TAGLINE AS TITLE (exact strings only — see SHAPE 2 note
    --     on why this doesn't reuse the existing rule-6 regex)
    or lower(trim(regexp_replace(t, '\s+', ' ', 'g'))) = any(array[
          'sag hospitality brokerage a ud consulting company',
          'specializing in the resale of franchise businesses since 1978',
          'sd business advisors | the business selling experts',
          'connecting your businesswith extraordinary opportunity'
        ])

    -- 12. BUYER-SIDE AD, NOT A LISTING
    or t ~* 'buyer with funds'

    -- 13. METADATA FRAGMENT: "Year Established: ..." grabbed instead of title
    or t ~* '^year established\s*:'

    -- 14. NAV FRAGMENT: category filter dropdown grabbed instead of title
    or t ~* '^business type\s'

-- Combined coverage of sections 10-14, de-duplicated across rows that could
-- match more than one clause: 1,693 of 26,505 active broker_direct rows
-- (6.4%). All five are anchored or exact-match; combined cost measured at
-- ~10.6ms per full-table pass client-side, negligible next to the 6.8s/3,000
-- row incident. No stored-column/trigger needed for these five.


-- ============================================================
-- REJECTED — do not ship (Principle 8: any known-good false reject kills it)
-- ============================================================

-- REJECTED CANDIDATE: generic "<industry> Company" / "<industry> Reseller"
-- title, meant to catch the ~19-string Pavilion Business Services placeholder
-- set ("Manufacturing Company", "Distribution Company", "Telecom Company",
-- "SAAS Company", "Technology Company", etc. — 45 broker rows total, each an
-- exact string repeated with an identical, suspiciously round asking price
-- like $25,000,000.00 or $100,000,000.00 across every row).
--
--   -- REJECTED: t ~* '(company|reseller)$'
--
-- False-reject test: against the full 23,418-row scrubbed known-good pool,
-- 607 real, unambiguous listings end in "Company" or "Reseller", e.g.:
--   "Established 40-year Towing Company & Roadside Service Company" ($3.5M)
--   "Premier Custom Pool Construction and Concrete Remodeling Company" ($500K)
--   "High-Quality Metal Finishing Company" ($9,923,101 — a non-round price,
--     i.e. clearly a real appraisal, not a template)
--   "New York Trucking Company" ($1.7M)
-- "Company" is simply how a huge share of real trade/service business names
-- end. This rule does not ship in any form.
--
-- The Pavilion placeholder set is real but its actual signature isn't the
-- word "Company" — it's an EXACT (title, price) pair repeated verbatim
-- across many rows for one broker, with round-number prices ($X,000,000.00).
-- is_listing_junk only receives (t, u) and has no visibility into price or
-- per-broker duplication, so it is the wrong function for this shape.
-- passes_listing_gate(t, u, price, dup_titles, dup_prices, url_specific,
-- descr) already takes dup_titles/dup_prices as inputs and is the right
-- place to add a "same title AND same price repeated N+ times for this
-- broker" clause instead. Not drafted tonight — flagging as a follow-up
-- item rather than forcing a title-only heuristic that isn't safe.


-- ============================================================
-- NOT JUNK — flagged so nobody "fixes" these by mistake
-- ============================================================
-- Several >15-count header groups from the queue's own query are REAL,
-- distinct titles that happen to repeat because the same listing appears to
-- have been ingested/synced multiple times (or, in one case, a franchise
-- concept resold at many locations under one template title) — this is a
-- DEDUPE problem, not a junk-title problem, and none of it belongs in
-- is_listing_junk:
--   "Prime Downtown SF FiDi Opportunity"                              (49x, same price every row — likely a literal duplicate ingest)
--   "Established Pasta Manufacturing Company + Real Estate"           (39x, same price every row)
--   "Profitable Pizza Restaurant with Full Kitchen in Prime Corner..."(38x, same price every row)
--   "Newly Remodeled & Fully Equipped Sonoma County Restaurant"       (36x, same price every row)
--   "Restaurant and Bar with Beer Garden in Affluent Sacramento..."   (35x, same price every row)
--   "SBA Loan approved! Historic Cafe with Beer & Wine License..."   (27x, same price every row)
--   "Bring Your Concept OR Continue Dessert Quick Serve Restaurant..."(33x, PRICES DIFFER $99K-$250K — genuinely distinct listings sharing a franchise-template title, not duplicates at all)
--   "Pizza Restaurant" / "Indian Restaurant"                          (16x each, prices vary — generic but plausible real titles, too generic to safely pattern-match anyway)
-- All broker "steven d zimmerman..." except the last two (matt sadati).
-- Recommend: if these get addressed, it's via row-level dedupe (same broker +
-- same title + same price + same/near URL) or a broker-specific ingestion
-- fix, not a is_listing_junk title rule.
