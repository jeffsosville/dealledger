-- W14 (applied 2026-11-25). The scraper code (price_disclosure.py) shipped
-- before this migration; every run since logged "price_not_disclosed/
-- broker_price_text not written ... column missing" and dropped the fields.
alter table public.listings_direct
  add column if not exists price_not_disclosed boolean not null default false,
  add column if not exists broker_price_text text;
comment on column public.listings_direct.price_not_disclosed is 'W14: broker explicitly withholds the price (e.g. "Contact for price"); see scrapers/price_disclosure.py';
comment on column public.listings_direct.broker_price_text is 'W14: the broker''s verbatim price phrase (<=120 chars) when price_not_disclosed';
notify pgrst, 'reload schema';
