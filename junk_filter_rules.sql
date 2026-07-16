-- ============================================================
-- DEALLEDGER JUNK FILTER — direct listing quality gate
-- Based on 100-row sample analysis (~49% junk in categories below).
-- A listing is JUNK if is_listing_junk(title, url) returns true.
-- Used by: (1) the bridge function, (2) periodic cleanup.
-- ============================================================

create or replace function is_listing_junk(t text, u text)
returns boolean
language sql
immutable
as $$
  select
    -- 1. EMPTY / too-short title (nav fragments, blanks)
    t is null
    or length(trim(coalesce(t,''))) < 6

    -- 2. NAV / UI ELEMENTS (buttons, chips, page furniture)
    or lower(trim(t)) = any(array[
        'more details','add to favorites','business listings','newsletter sign up',
        'create account','recent posts','medical spa','real estate','environmental svcs',
        'view all','load more','sign in','log in','learn more','read more','click here',
        'view listing','view details','see details','favorites','next','previous',
        'our listings','all listings','featured listings','search','filter','home',
        'contact us','about us','get started','subscribe','menu'])

    -- 3. STATUS WORDS / error & challenge pages
    or lower(trim(t)) = any(array[
        'sold','pending','under contract','coming soon','new listing','new','featured',
        'checking your browser','403 - forbidden','403 forbidden','404','not found',
        'access denied','page not found','error'])
    or lower(t) like 'checking your browser%'
    or lower(t) like '403%forbidden%'

    -- 4. FINANCIAL FRAGMENTS (grabbed a price/metric, not a name)
    or lower(trim(t)) ~ '^(net|gross|asking price|revenue|cash flow|sde|ebitda|price)\M'
    or lower(t) ~ 'gross (revenue|sales)'
    or lower(t) ~ 'net op(erating)? inc'
    or trim(t) ~ '^\$?[\d,]+(\.\d+)?\s*$'                    -- title is just a number
    or trim(t) ~ '^\$[\d,]+.{0,4}(for sale|gross|revenue)'   -- "$860,000 Gross Revenue"

    -- 5. CTA / MARKETING copy
    or lower(t) like 'join the%'
    or lower(t) like 'sell your business%'
    or lower(t) like '%exit planning circle%'
    or lower(t) like 'it''s the easiest thing%'
    or lower(t) like '%the easiest thing in the world%'

    -- 6. BROKER FIRM NAME as title (short + firm keyword)
    or (length(trim(t)) < 45 and lower(t) ~ '(business advisors|business brokers|commercial real estate brokerage|the business selling experts)$')

    -- 7. PRIVACY / TERMS / LEGAL
    or lower(t) like 'privacy policy%'
    or lower(t) like 'terms %'
    or lower(t) like '%terms of service%'
    or lower(t) like 'google maps'

    -- 8. BAD URLs (non-listing destinations)
    or u is null
    or u like 'javascript:%'
    or u like 'mailto:%'
    or u like '%maps.app.goo.gl%'
    or u like '%/privacy%'
    or u like '%/terms%'
    or u like '%/author/%'
    or u like '%addtofavorites%'
    or u like '%/newsletter%'

    -- 9. REAL ESTATE / LAND (CRE + residential, not a business)
    or (lower(t) ~ '(commercial real estate for lease|residential land|land for sale|for lease in)' and lower(t) !~ 'business')
$$;

-- ============================================================
-- USAGE 1 — CLEANUP: remove junk currently live on the site
-- ============================================================
-- Preview count first:
--   select count(*) from listings where source='broker_direct' and is_active=true
--     and is_listing_junk(header, url);
-- Then delete:
--   delete from listings where source='broker_direct' and is_active=true
--     and is_listing_junk(header, url);

-- ============================================================
-- USAGE 2 — BRIDGE: reject junk at insert time
-- In bridge_direct_to_listings(), replace the inline "not (...)" block with:
--     and not is_listing_junk(title, url)
-- (cleaner + single source of truth than the inline array checks)
-- ============================================================

-- ============================================================
-- NOTE ON UPSTREAM FIX (the real solution):
-- These are scraper extraction failures — the scraper grabbed a nav link,
-- price string, or status badge instead of the business name/title.
-- The filter hides them, but the durable fix is improving title extraction
-- in the scraper (dealledger_scraper_v6.py) so it doesn't capture these in
-- the first place. Filter = symptom control; better extraction = cure.
-- ============================================================
