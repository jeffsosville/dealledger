-- 2026-11-14 — applied (migration price_floor_and_liquor_license_title_rule; the
-- listings trigger was run by hand in the SQL editor).
--
-- 1. Price floor: no business sells for under $1,000. Sub-$1K asking prices were parse
--    errors ("$725K" read as 725, "$2M" as 2) or "$1" placeholders. BEFORE INSERT/UPDATE
--    triggers null them on both tables, for every writer:
--      trg_listings_direct_0a_price_floor -> listings_direct_price_floor()  (asking_price)
--      trg_listings_0a_price_floor        -> listings_price_floor()         (price)
--    Existing sub-$1K values were cleared on both tables.
--
-- 2. dl_vertical_from_title(): license-only titles ("C33 Liquor License") -> 'other'
--    (the liquor-licenses niche in lib/niches.ts picks them up); a store/bar/restaurant
--    that comes with a license keeps liquor_store. ATM rule widened to "ATM processing
--    route", "84 ATM route", "11 ATMs", "bitcoin ATM". The scraper got the same rules
--    (scrapers/dealledger_scraper_v6.py _title_rule_vertical, classify_verticals ATM rule).

create or replace function public.listings_price_floor()
 returns trigger language plpgsql set search_path to 'public', 'pg_temp' as $f$
begin
  if new.price is not null and new.price < 1000 then new.price := null; end if;
  return new;
end $f$;
drop trigger if exists trg_listings_0a_price_floor on public.listings;
create trigger trg_listings_0a_price_floor
  before insert or update of price on public.listings
  for each row execute function public.listings_price_floor();
