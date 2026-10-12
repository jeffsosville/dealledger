-- Results table for agents/marketplace_scan.py (brokers whose own site
-- links buyers to a marketplace). Safe to re-run.
create table if not exists broker_marketplace_links (
  domain text primary key,
  checked_at timestamptz not null default now(),
  homepage_status int,
  final_url text,
  marketplaces text[],
  marketplace_links jsonb,
  listings_page text,
  page_listing_count int,
  company_name text,
  phones text[],
  emails text[],
  note text);
alter table broker_marketplace_links enable row level security;
drop policy if exists bml_read on broker_marketplace_links;
create policy bml_read on broker_marketplace_links for select to anon using (true);

-- Outdated-site signals (added 2026-10-12)
alter table broker_marketplace_links add column if not exists site_signals jsonb;
alter table broker_marketplace_links add column if not exists dated_score int;
