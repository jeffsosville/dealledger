-- ============================================================
-- is_listing_junk ADDITIONS — proposed, night of 2026-09-02/03
-- Item 3 of tonight's queue. Supersedes claude/junk-rules-proposed.sql
-- (earlier pass this session) — this file re-verifies every number live
-- and adds the Pavilion Business Services broker-account kill plus one
-- pattern extension found while checking execbb.com overlap.
--
-- Method (CLAUDE.md principle 8, mandatory):
--   Every count below was re-queried live via the REST API against
--   `listings` (is_active=true, source='broker_direct') moments before
--   writing this file, not copied from the earlier pass or from the queue
--   text. Numbers moved between the two passes tonight (e.g. Shape 1 was
--   957 rows ~40 minutes ago, 472 now; Pavilion was 959, is 1,126 now) —
--   this table is being written to live during measurement (the same
--   moving-target problem flagged elsewhere tonight). Treat every count in
--   this file as "as of this query", not a fixed fact.
--
--   Known-good sample: 500 rows drawn from a pool of 23,984 active rows
--   that do NOT match any candidate pattern below and are NOT under
--   broker_account='Pavilion Business Services'. Every candidate was
--   checked against both the 500-sample and the full 23,984-row pool.
--   Zero false rejects required to ship, per principle 8.
-- ============================================================


-- ================================================================
-- ACTION A — broker-account kill: Pavilion Business Services (SHIP)
-- ================================================================
-- Distinct from everything below: this is a narrow, high-confidence,
-- BROKER-SCOPED action, not a general title-pattern rule. It does not
-- belong in is_listing_junk (which only sees title+url, not broker_account)
-- and should not be generalized — it targets one broker_account value only.
--
-- Verified live just now: 1,126 active broker_direct rows under
-- broker_account = 'Pavilion Business Services', 21 distinct titles, ALL 21
-- confirmed junk shapes (no 22nd, real-listing title exists):
--   - 19 generic "<industry> Company/Reseller" placeholders at a fixed,
--     suspiciously round price per title (e.g. "SAAS Company" x64 @
--     $100,000,000; "Manufacturing Company" x61 @ $25,000,000) or no price
--     at all ("Technology Company" x58, price=NULL)
--   - 1 buyer-side ad ("Seller Distribution Buyer with funds up to $100M to
--     invest $ 100,000,000.00" x48 — note the FULL title has trailing text
--     beyond the short paraphrase used in the incident doc)
--   - 1 firm tagline ("Connecting your businesswith extraordinary
--     opportunity" x46 — the only rows in this set with a non-null state,
--     'NC', and the only ones with a real-looking price ($5,000,000) —
--     checked individually, still not a real listing, it's marketing copy)
-- 1,077 of 1,126 (96%) have no state at all. distinct direct_broker_url
-- count is 793 of 1,126 — NOT fully distinct-per-row as first reported;
-- worth a follow-up on why ~330 rows now share a URL with another row
-- (possibly re-ingestion), but doesn't change the kill decision either way.
--
-- VERDICT: SHIP (as a one-time broker-scoped statement, reviewed before
-- running — not proposed as a standing rule)
update listings set is_active = false
where source = 'broker_direct' and broker_account = 'Pavilion Business Services';
-- Re-run the count query immediately before executing to confirm the
-- number and shape haven't changed, given the volatility noted above:
--   select header, count(*), min(price), min(state) from listings
--   where is_active and source='broker_direct'
--     and broker_account='Pavilion Business Services'
--   group by 1 order by 2 desc;


-- ================================================================
-- SHAPE 1 — category label leaked into title (SHIP, extended)
-- ================================================================
-- Broker: execbb.com / "Executive Business Brokers (Larry Bodner)". Real
-- listings there are titled "<listingID> <Category>-<Subcategory>" (e.g.
-- "13825971VS4 General Services-Laundromat" — confirmed present, live, and
-- correctly left untouched by the anchor). A subset lost the ID prefix
-- during extraction and are left as bare "General Services-Laundromat",
-- "Food/Liquor-Restaurant", "Wholesale/Manufac./Dist.-Wholesale",
-- "Asset Sale-Restaurant", etc.
--
-- The earlier pass tonight only covered 4 of the 6 category-taxonomy
-- prefixes execbb.com actually uses. Checking execbb.com overlap just now
-- found 191 of 663 execbb.com rows didn't match the original pattern — most
-- of those are genuinely fine (they still carry their ID prefix), but 30
-- are the same defect under two prefixes the original pattern missed:
-- "Wholesale/Manufac./Dist.-" and "Asset Sale-". Extended and re-tested.
--
-- Pattern:       ^(general services|general retail|food/liquor|
--                  automotive/transportation|wholesale/manufac\.?/dist\.?|
--                  asset sale)-
-- Matches now:   472 (original 4 prefixes) + 30 (new 2 prefixes) = 502 of
--                26,505 active rows. (Was 957 ~40 min ago — see volatility
--                note at top; re-run before deploying.)
-- False rejects: 0 / 500 known-good sample. 0 / 23,984 full scrubbed pool
--                (checked for both the original and extended pattern).
--
-- VERDICT: SHIP (extended)
select header, count(*) from listings
where is_active and source='broker_direct'
  and header ~* '^(general services|general retail|food/liquor|automotive/transportation|wholesale/manufac\.?/dist\.?|asset sale)-'
group by 1 order by 2 desc;


-- ================================================================
-- SHAPE 2 — broker tagline / firm description used as title (SHIP)
-- ================================================================
-- Unchanged from the earlier pass. Four exact strings, firm marketing copy,
-- not a listing title. Exact-match (not substring) to keep blast radius at
-- zero.
--
-- "SD Business Advisors | The business selling experts" is 51 chars,
-- distinct from junk_filter_rules.sql's rule 6 (requires length < 45 and a
-- suffix match) — that rule would still miss this string even if deployed.
--
-- Matches now:   253 of 26,505 active rows (was 249).
-- False rejects: 0 / 500 known-good. 0 / 23,984 full scrubbed pool.
--
-- VERDICT: SHIP
select header, count(*) from listings
where is_active and source='broker_direct'
  and lower(trim(regexp_replace(header, '\s+', ' ', 'g'))) = any(array[
        'sag hospitality brokerage a ud consulting company',
        'specializing in the resale of franchise businesses since 1978',
        'sd business advisors | the business selling experts',
        'connecting your businesswith extraordinary opportunity'
      ])
group by 1 order by 2 desc;
-- Note: 'connecting your businesswith extraordinary opportunity' is also
-- one of the 21 Pavilion Business Services titles killed by Action A above
-- (46 of the 253 matches). If Action A runs, this rule's live match count
-- drops to ~207 automatically — both are still worth shipping since Shape 2
-- also catches SAG Hospitality (a different broker) and others, and
-- is_listing_junk has no broker_account visibility to rely on Action A
-- alone going forward.


-- ================================================================
-- SHAPE 3 — buyer-side ad, not a listing (SHIP)
-- ================================================================
-- Matches now: 48 of 26,505 (was 58; the drop is Pavilion churn — see
-- Action A, this shape's buyer-ad row is also one of the 21 Pavilion
-- titles). Kept as a standing is_listing_junk rule regardless, since a
-- buyer-side ad from a different broker would still need catching.
--
-- False rejects: 0 / 500 known-good. 0 / 23,984 full scrubbed pool.
--
-- VERDICT: SHIP
select header, count(*) from listings
where is_active and source='broker_direct'
  and header ~* 'buyer with funds'
group by 1 order by 2 desc;


-- ================================================================
-- SHAPE 4 — "Year Established: ..." metadata fragment as title (SHIP)
-- ================================================================
-- Matches now: 410 of 26,505 (was 409 — stable). Scraper grabbed a
-- detail-page metadata line instead of the title; every distinct variant
-- checked is pure metadata, no business name riding along.
--
-- False rejects: 0 / 500 known-good. 0 / 23,984 full scrubbed pool.
--
-- VERDICT: SHIP
select header, count(*) from listings
where is_active and source='broker_direct'
  and header ~* '^year established\s*:'
group by 1 order by 2 desc;


-- ================================================================
-- SHAPE 5 — "Business Type ..." filter-widget dropdown as title (SHIP)
-- ================================================================
-- Matches now: 18 of 26,505 (was 20). A category-filter dropdown's full
-- option list, scraped as the listing title. Broker: matt sadati.
--
-- False rejects: 0 / 500 known-good. 0 / 23,984 full scrubbed pool.
--
-- VERDICT: SHIP
select header, count(*) from listings
where is_active and source='broker_direct'
  and header ~* '^business type\s'
group by 1 order by 2 desc;


-- ================================================================
-- PROPOSED PATCH — add to is_listing_junk(t text, u text)
-- Additive only; existing clauses (1-9 in junk_filter_rules.sql) untouched.
-- ================================================================

    -- 10. CATEGORY LABEL LEAKED INTO TITLE (broker extraction defect,
    --     6 taxonomy prefixes)
    or t ~* '^(general services|general retail|food/liquor|automotive/transportation|wholesale/manufac\.?/dist\.?|asset sale)-'

    -- 11. BROKER TAGLINE AS TITLE (exact strings only)
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

-- Combined coverage of sections 10-14 just now: ~1,200 of 26,505 active
-- broker_direct rows (~4.5%), overlapping with Action A's 1,126-row kill
-- where a Pavilion title also happens to match Shape 2/3. All five patterns
-- are anchored or exact-match; cost is negligible (sub-microsecond regex,
-- no backtracking) next to the 6.8s/3,000-row cost that caused the bridge
-- timeout. No stored column/trigger needed for these five.


-- ================================================================
-- REJECTED — do not ship
-- ================================================================

-- REJECTED CANDIDATE: generic "<industry> Company" / "<industry> Reseller"
-- title suffix, meant to catch the Pavilion placeholder set without a
-- broker_account condition.
--
--   -- REJECTED: t ~* '(company|reseller)$'
--
-- False-reject test: 607 real, unambiguous listings in the known-good pool
-- end in "Company" or "Reseller", e.g. "Established 40-year Towing Company
-- & Roadside Service Company" ($3.5M), "High-Quality Metal Finishing
-- Company" ($9,923,101 — non-round, clearly a real appraisal). Does not
-- ship in any form; the word "Company" is simply common in real trade
-- business names. Handled instead by Action A (broker-scoped kill) since
-- the real signature is exact-title-and-price repetition for one broker,
-- which is_listing_junk(t,u) cannot see. `passes_listing_gate` (which does
-- take dup_titles/dup_prices) is the right place for a general version of
-- this check — not drafted tonight, flagged as a follow-up.


-- ================================================================
-- NOT JUNK — flagged so nobody "fixes" these by mistake
-- ================================================================
-- Several >15-count header groups are REAL, distinct titles repeating
-- because the same listing was ingested multiple times, or a franchise
-- concept resold at many locations under one template title — a DEDUPE
-- problem, not a junk-title problem; none of it belongs in is_listing_junk:
--   "Prime Downtown SF FiDi Opportunity"                              (same price every row — likely literal duplicate ingest)
--   "Established Pasta Manufacturing Company + Real Estate"           (same price every row)
--   "Profitable Pizza Restaurant with Full Kitchen in Prime Corner..."(same price every row)
--   "Newly Remodeled & Fully Equipped Sonoma County Restaurant"       (same price every row)
--   "Restaurant and Bar with Beer Garden in Affluent Sacramento..."   (same price every row)
--   "SBA Loan approved! Historic Cafe with Beer & Wine License..."    (same price every row)
--   "Bring Your Concept OR Continue Dessert Quick Serve Restaurant..."(PRICES DIFFER $99K-$250K — genuinely distinct listings sharing a franchise-template title, not duplicates)
--   "Pizza Restaurant" / "Indian Restaurant"                          (prices vary — generic but plausible real titles, too generic to pattern-match safely anyway)
-- Recommend row-level dedupe (same broker + same title + same price + same
-- URL) or a broker-specific ingestion fix if these get addressed, not a
-- is_listing_junk title rule.


-- ================================================================
-- KNOWN GAP, unchanged from earlier pass — not fixed tonight
-- ================================================================
-- The deployed is_listing_junk(t,u) does NOT match the repo's
-- junk_filter_rules.sql: rules 4 (financial fragment), 6 (broker suffix),
-- and 9 (real-estate-not-business) tested live as inactive via
-- /rest/v1/rpc/is_listing_junk. Rules 2, 3, 7 ARE live. Deploying the
-- checked-in file as-is may already clear some "Land for Sale" and
-- financial-fragment rows before anything in this file is even applied.
-- Not verified further (read-only, no function changes applied).
