// lib/seo.ts
//
// Data and labels for the browse pages under /businesses-for-sale:
//   /businesses-for-sale                       all states and business types
//   /businesses-for-sale/{state}               e.g. /businesses-for-sale/florida
//   /businesses-for-sale/{state}/{type}        e.g. /businesses-for-sale/florida/restaurants
//   /businesses-for-sale/type/{type}           e.g. /businesses-for-sale/type/cleaning
//
// Rows come from mv_seo_listings / mv_seo_page_stats (sql/2026-10-08_seo_listings.sql):
// the public record with a cleaned two-letter state and a business type.
// A page exists only when it has enough listings to be useful (thresholds
// below); thinner pages 404 instead of publishing near-empty lists.

import { getSupabase } from './supabase';

export const STATE_MIN = 50;   // listings needed for a state page
export const COMBO_MIN = 25;   // listings needed for a type-in-state page
export const TYPE_MIN = 10;    // listings needed for a national type page
export const LIST_LIMIT = 200; // listings shown per page

export const STATES: Record<string, string> = {
  AL: 'Alabama', AK: 'Alaska', AZ: 'Arizona', AR: 'Arkansas', CA: 'California', CO: 'Colorado',
  CT: 'Connecticut', DE: 'Delaware', DC: 'Washington, DC', FL: 'Florida', GA: 'Georgia', HI: 'Hawaii',
  ID: 'Idaho', IL: 'Illinois', IN: 'Indiana', IA: 'Iowa', KS: 'Kansas', KY: 'Kentucky',
  LA: 'Louisiana', ME: 'Maine', MD: 'Maryland', MA: 'Massachusetts', MI: 'Michigan', MN: 'Minnesota',
  MS: 'Mississippi', MO: 'Missouri', MT: 'Montana', NE: 'Nebraska', NV: 'Nevada', NH: 'New Hampshire',
  NJ: 'New Jersey', NM: 'New Mexico', NY: 'New York', NC: 'North Carolina', ND: 'North Dakota',
  OH: 'Ohio', OK: 'Oklahoma', OR: 'Oregon', PA: 'Pennsylvania', RI: 'Rhode Island',
  SC: 'South Carolina', SD: 'South Dakota', TN: 'Tennessee', TX: 'Texas', UT: 'Utah',
  VT: 'Vermont', VA: 'Virginia', WA: 'Washington', WV: 'West Virginia', WI: 'Wisconsin', WY: 'Wyoming',
};

export const stateSlug = (code: string) =>
  code === 'DC' ? 'washington-dc' : STATES[code].toLowerCase().replace(/[^a-z]+/g, '-');

export const stateFromSlug = (slug: string): string | null =>
  Object.keys(STATES).find((c) => stateSlug(c) === slug) ?? null;

// vertical (database value) -> url slug, plural label, singular-ish noun
export const TYPES: Record<string, { slug: string; label: string; noun: string }> = {
  restaurant:       { slug: 'restaurants',      label: 'Restaurants',                    noun: 'restaurants' },
  retail:           { slug: 'retail',           label: 'Retail Businesses',              noun: 'retail businesses' },
  healthcare:       { slug: 'healthcare',       label: 'Healthcare Businesses',          noun: 'healthcare businesses' },
  technology:       { slug: 'technology',       label: 'Technology & Online Businesses', noun: 'technology and online businesses' },
  automotive:       { slug: 'automotive',       label: 'Automotive Businesses',          noun: 'automotive businesses' },
  construction:     { slug: 'construction',     label: 'Construction Companies',         noun: 'construction companies' },
  manufacturing:    { slug: 'manufacturing',    label: 'Manufacturing Companies',        noun: 'manufacturing companies' },
  delivery_route:   { slug: 'delivery-routes',  label: 'Delivery Routes',                noun: 'delivery routes' },
  cleaning:         { slug: 'cleaning',         label: 'Cleaning Businesses',            noun: 'cleaning businesses' },
  amusement:        { slug: 'entertainment',    label: 'Amusement & Entertainment Businesses', noun: 'amusement and entertainment businesses' },
  hvac:             { slug: 'hvac',             label: 'HVAC Companies',                 noun: 'HVAC companies' },
  landscaping:      { slug: 'landscaping',      label: 'Landscaping Businesses',         noun: 'landscaping businesses' },
  laundromat:       { slug: 'laundromats',      label: 'Laundromats',                    noun: 'laundromats' },
  plumbing:         { slug: 'plumbing',         label: 'Plumbing Companies',             noun: 'plumbing companies' },
  electrical:       { slug: 'electrical',       label: 'Electrical Contractors',         noun: 'electrical contractors' },
  roofing:          { slug: 'roofing',          label: 'Roofing Companies',              noun: 'roofing companies' },
  dry_cleaner:      { slug: 'dry-cleaners',     label: 'Dry Cleaners',                   noun: 'dry cleaners' },
  vending:          { slug: 'vending',          label: 'Vending Routes',                 noun: 'vending routes and businesses' },
  pool_service:     { slug: 'pool-service',     label: 'Pool Service Businesses',        noun: 'pool service businesses' },
  junk_removal:     { slug: 'junk-removal',     label: 'Junk Removal Businesses',        noun: 'junk removal businesses' },
  pest_control:     { slug: 'pest-control',     label: 'Pest Control Companies',         noun: 'pest control companies' },
  atm:              { slug: 'atm',              label: 'ATM Routes',                     noun: 'ATM routes' },
  pressure_washing: { slug: 'pressure-washing', label: 'Pressure Washing Businesses',    noun: 'pressure washing businesses' },
};

export const typeFromSlug = (slug: string): string | null =>
  Object.keys(TYPES).find((v) => TYPES[v].slug === slug) ?? null;

export type Stats = {
  scope: 'state' | 'vertical' | 'vertical_state';
  page_vertical: string | null;
  page_state: string | null;
  listings: number;
  brokers: number;
  median_price: number | null;
  median_cash_flow: number | null;
  median_multiple: number | null;
  new_30d: number;
  relisted: number;
  with_cash_flow: number;
};

export type Row = {
  listing_number: number;
  title: string | null;
  city: string | null;
  state_code: string | null;
  vertical: string;
  price: number | null;
  cash_flow: number | null;
  dom_display: string | null;
  dom_basis: string | null;
  estimated_listed_date: string | null;
  broker_domain: string | null;
};

const STAT_COLS =
  'scope, page_vertical, page_state, listings, brokers, median_price, median_cash_flow, median_multiple, new_30d, relisted, with_cash_flow';
const ROW_COLS =
  'listing_number, title, city, state_code, vertical, price, cash_flow, dom_display, dom_basis, estimated_listed_date, broker_domain';

export async function allStats(): Promise<Stats[]> {
  const { data } = await getSupabase().from('mv_seo_page_stats').select(STAT_COLS).range(0, 1999);
  return (data || []) as Stats[];
}

export async function listingsFor(filter: { state?: string; vertical?: string }): Promise<Row[]> {
  let q = getSupabase().from('mv_seo_listings').select(ROW_COLS);
  if (filter.state) q = q.eq('state_code', filter.state);
  if (filter.vertical) q = q.eq('vertical', filter.vertical);
  const { data } = await q
    .order('estimated_listed_date', { ascending: false, nullsFirst: false })
    .range(0, LIST_LIMIT - 1);
  return (data || []) as Row[];
}

export const pageUrl = (path: string) => `https://dealledger.org${path}`;
export const statePath = (code: string) => `/businesses-for-sale/${stateSlug(code)}`;
export const typePath = (vertical: string) => `/businesses-for-sale/type/${TYPES[vertical].slug}`;
export const comboPath = (code: string, vertical: string) =>
  `/businesses-for-sale/${stateSlug(code)}/${TYPES[vertical].slug}`;

export const hasStatePage = (s: Stats) => s.scope === 'state' && !!s.page_state && !!STATES[s.page_state] && s.listings >= STATE_MIN;
export const hasTypePage = (s: Stats) => s.scope === 'vertical' && !!s.page_vertical && !!TYPES[s.page_vertical] && s.listings >= TYPE_MIN;
export const hasComboPage = (s: Stats) =>
  s.scope === 'vertical_state' && !!s.page_state && !!STATES[s.page_state] &&
  !!s.page_vertical && !!TYPES[s.page_vertical] && s.listings >= COMBO_MIN;

export const fmtMoney = (n: number | null) => {
  if (n == null || n <= 0) return '—';
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(n >= 10_000_000 ? 0 : 1)}M`;
  if (n >= 1_000) return `$${Math.round(n / 1_000)}K`;
  return `$${Math.round(n)}`;
};
export const fmtNum = (n: number | null) => (n == null ? '—' : n.toLocaleString('en-US'));
export const fmtMultiple = (n: number | null) => (n == null ? '—' : `${n.toFixed(1)}×`);
