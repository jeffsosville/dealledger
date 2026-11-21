// lib/intelligence — DealLedger's query layer. The REST API (/api/v1/*) and the
// MCP server (/api/mcp) both call these functions, so they can never disagree.
//
// Evidence rules (same as the listing page; do not loosen here):
//   - Listing age comes only from mv_listings_page_v2's evidence hierarchy and is
//     an observed minimum, never verified days on market.
//   - Price changes come only from public.price_history (reviewed, broker-direct),
//     reported as observation windows; the exact day is unknown.
//   - The relisted flag is a heuristic match: an unverified signal, never a fact.
//   - Search results come from mv_seo_listings_v2, the same public set the site shows.
import { getSupabase } from '../supabase';
import { STATES, TYPES, displayTitle, stateFromSlug, typeFromSlug } from '../seo';

export const API_VERSION = '1.0';
export const SITE = 'https://dealledger.org';
export const SOURCE =
  'DealLedger (dealledger.org) — CC0 public record of businesses for sale, observed directly on broker websites';
export const LICENSE = 'CC0-1.0';

export class ApiError extends Error {
  constructor(public status: number, public code: string, message: string) {
    super(message);
  }
}

export const listingUrl = (n: number) => `${SITE}/listing/${n}`;
const day = (v: unknown) => (v ? String(v).slice(0, 10) : null);
const num = (v: unknown) => (v == null || v === '' ? null : Number(v));
const pos = (v: unknown) => {
  const n = num(v);
  return n != null && n > 0 ? n : null;
};
export const hostOf = (url?: string | null) =>
  url ? url.replace(/^https?:\/\/(www\.)?/i, '').split(/[/?#:]/)[0].toLowerCase() || null : null;

// ── Input normalising ──────────────────────────────────────────────────────────
export function toStateCode(s?: string | null): string | null {
  if (!s) return null;
  const t = s.trim();
  if (STATES[t.toUpperCase()]) return t.toUpperCase();
  return stateFromSlug(t.toLowerCase().replace(/[^a-z]+/g, '-'));
}
export function toVertical(s?: string | null): string | null {
  if (!s) return null;
  const t = s.trim().toLowerCase();
  if (TYPES[t]) return t;
  return typeFromSlug(t.replace(/[\s_]+/g, '-'));
}
// Accept 1166992202, "1166992202" or "https://dealledger.org/listing/1166992202".
export function toListingNumber(v: unknown): number | null {
  const m = String(v ?? '').match(/(\d{5,})/);
  return m ? Number(m[1]) : null;
}
function state(s?: string | null) {
  const c = toStateCode(s);
  if (s && !c) throw new ApiError(400, 'unknown_state', `Unknown state "${s}". Use a code ("TX") or name ("Texas").`);
  return c;
}
function vertical(s?: string | null) {
  const v = toVertical(s);
  if (s && !v) throw new ApiError(400, 'unknown_type', `Unknown business type "${s}". Use one of: ${Object.keys(TYPES).join(', ')}.`);
  return v;
}
function isoDate(s?: string | null, name = 'date') {
  if (!s) return null;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) throw new ApiError(400, 'bad_date', `${name} must be YYYY-MM-DD.`);
  return s;
}
function page(limit: unknown, offset: unknown, max: number, dflt: number) {
  const l = limit == null || limit === '' ? dflt : Number(limit);
  const o = offset == null || offset === '' ? 0 : Number(offset);
  if (!Number.isInteger(l) || l < 1 || l > max) throw new ApiError(400, 'bad_limit', `limit must be 1–${max}.`);
  if (!Number.isInteger(o) || o < 0) throw new ApiError(400, 'bad_offset', 'offset must be 0 or more.');
  return { limit: l, offset: o };
}
function money(v: unknown, name: string) {
  if (v == null || v === '') return null;
  const n = Number(v);
  if (!Number.isFinite(n) || n < 0) throw new ApiError(400, 'bad_number', `${name} must be a non-negative number.`);
  return n;
}

// ── Shared shapes ──────────────────────────────────────────────────────────────
const AGE_CONFIDENCE: Record<string, string> = {
  window: 'high — appeared between two complete crawls of the broker site',
  observed: 'medium — first dated sighting; the listing may be older',
};
export function observedAge(rec: any) {
  if (!rec?.dom_basis) return { status: 'unknown', note: 'No dated evidence for this listing.' };
  if (rec.dom_basis === 'in_db') {
    return {
      status: 'unknown',
      first_recorded_in_dealledger: day(rec.estimated_listed_date),
      note: 'No dated sighting. The date above is when the record was created, not when the listing was observed or first published.',
    };
  }
  return {
    status: 'observed',
    label: rec.dom_display ?? null,
    listed_on_or_before: day(rec.estimated_listed_date),
    listed_after: day(rec.listed_after),
    observed_days: rec.dom_days_eff ?? null,
    observed_days_max: rec.dom_days_max ?? null,
    basis: rec.dom_basis,
    evidence: rec.age_source ?? null,
    confidence: AGE_CONFIDENCE[rec.dom_basis] ?? 'unknown',
    note: 'Observed age is a minimum from DealLedger crawls, not verified days on market.',
  };
}

const PRICE_CONFIDENCE =
  'reviewed — both prices observed on the broker’s own listing page; exact day of change unknown';
function priceChange(c: any) {
  return {
    from_price: Number(c.prev_price),
    to_price: Number(c.next_price),
    pct_change: Number(c.pct_change),
    changed_after: day(c.prev_last_seen ?? c.changed_after),
    changed_on_or_before: day(c.next_first_seen ?? c.changed_on_or_before),
    window_days: c.window_days ?? null,
    confidence: PRICE_CONFIDENCE,
  };
}

export const RELIST_BASIS =
  'Heuristic match to an earlier listing in DealLedger’s historical archive with the same state, the same asking price ' +
  '(over $50K) and the same opening words of the title, under a different listing number. Not confirmed.';
export const relistSignal = (relisted: boolean) =>
  relisted
    ? { status: 'possible_relist', confidence: 'low — unverified', basis: RELIST_BASIS }
    : {
        status: 'no_relist_evidence',
        confidence: 'n/a',
        basis: 'No match found. This does not prove the business was never listed before; DealLedger only sees what it has observed.',
      };

// One public listing row (mv_seo_listings_v2) as the API shows it.
const LISTING_COLS =
  'listing_number, title, city, state_code, state_source, vertical, vertical_source, price, cash_flow, revenue, ' +
  'dom_days_eff, dom_days_max, dom_basis, dom_display, listed_after, estimated_listed_date, age_source, last_seen, relisted, ' +
  'broker_domain, source_url';
function listingSummary(r: any) {
  const price = pos(r.price), cf = pos(r.cash_flow);
  return {
    listing_number: Number(r.listing_number),
    dealledger_url: listingUrl(Number(r.listing_number)),
    title: displayTitle(r),
    city: r.city ?? null,
    state: r.state_code ?? null,
    state_source: r.state_source ?? null,
    business_type: r.vertical ?? null,
    business_type_label: TYPES[r.vertical]?.label ?? r.vertical ?? null,
    business_type_source: r.vertical_source ?? null,
    asking_price: price,
    cash_flow: cf,
    revenue: pos(r.revenue),
    price_to_cash_flow: price && cf ? Number((price / cf).toFixed(2)) : null,
    observed_age: observedAge(r),
    last_seen: day(r.last_seen),
    relist_signal: r.relisted ? 'possible_relist' : 'no_relist_evidence',
    broker_site: r.broker_domain ?? hostOf(r.source_url),
    broker_page: r.source_url ?? null,
  };
}

export type Page<T> = {
  results: T[];
  total: number;
  limit: number;
  offset: number;
  next_offset: number | null;
};
const pageOf = <T,>(results: T[], total: number | null, limit: number, offset: number): Page<T> => {
  const t = total ?? results.length;
  return { results, total: t, limit, offset, next_offset: offset + results.length < t ? offset + results.length : null };
};

// PostgREST returns at most 1,000 rows per request, so larger pages (CSV) are
// fetched in chunks. build() must return a fresh, fully filtered query each time.
const CHUNK = 1000;
async function fetchPage(build: (withCount: boolean) => any, offset: number, limit: number) {
  const rows: any[] = [];
  let total: number | null = null;
  for (let from = offset; rows.length < limit; ) {
    const n = Math.min(CHUNK, limit - rows.length);
    const { data, error, count } = await build(total == null).range(from, from + n - 1);
    if (error) throw new ApiError(500, 'query_failed', error.message);
    if (total == null) total = count ?? null;
    rows.push(...(data || []));
    if (!data || data.length < n) break;
    from += data.length;
  }
  return { rows, total };
}

// ── searchListings ─────────────────────────────────────────────────────────────
export type ListingQuery = {
  state?: string; type?: string; q?: string; broker?: string;
  min_price?: unknown; max_price?: unknown; min_cash_flow?: unknown;
  min_age_days?: unknown; relisted?: unknown;
  sort?: string; limit?: unknown; offset?: unknown;
};
const SORTS: Record<string, [string, boolean]> = {
  newest: ['estimated_listed_date', false],
  oldest: ['estimated_listed_date', true],
  price_desc: ['price', false],
  price_asc: ['price', true],
  cash_flow_desc: ['cash_flow', false],
  age_desc: ['dom_days_eff', false],
};
export async function searchListings(a: ListingQuery, maxLimit = 100) {
  const st = state(a.state), vt = vertical(a.type);
  const { limit, offset } = page(a.limit, a.offset, maxLimit, 25);
  const sort = a.sort || 'newest';
  if (!SORTS[sort]) throw new ApiError(400, 'bad_sort', `sort must be one of: ${Object.keys(SORTS).join(', ')}.`);
  const minP = money(a.min_price, 'min_price'), maxP = money(a.max_price, 'max_price');
  const minCf = money(a.min_cash_flow, 'min_cash_flow'), minAge = money(a.min_age_days, 'min_age_days');

  const [col, asc] = SORTS[sort];
  const build = (withCount: boolean) => {
    let q = getSupabase().from('mv_seo_listings_v2').select(LISTING_COLS, withCount ? { count: 'exact' } : undefined);
    if (st) q = q.eq('state_code', st);
    if (vt) q = q.eq('vertical', vt);
    if (minP != null) q = q.gte('price', minP);
    if (maxP != null) q = q.lte('price', maxP);
    if (minCf != null) q = q.gte('cash_flow', minCf);
    if (minAge != null) q = q.gte('dom_days_eff', minAge).in('dom_basis', ['window', 'observed']);
    if (a.relisted === 'true' || a.relisted === true) q = q.eq('relisted', true);
    if (a.broker) q = q.eq('broker_domain', hostOf(`https://${String(a.broker)}`) ?? String(a.broker).toLowerCase());
    if (a.q) {
      const term = String(a.q).replace(/[%_*,()\\]/g, ' ').trim().slice(0, 80);
      if (term) q = q.ilike('title', `%${term}%`);
    }
    return q.order(col, { ascending: asc, nullsFirst: false }).order('listing_number', { ascending: true });
  };
  const { rows: data, total: count } = await fetchPage(build, offset, limit);
  return pageOf(data.map(listingSummary), count, limit, offset);
}

// ── getListing (full history) ──────────────────────────────────────────────────
export async function getListing(id: unknown) {
  const n = toListingNumber(id);
  if (!n) throw new ApiError(400, 'bad_listing', 'Give a DealLedger listing number or a dealledger.org/listing/… URL.');
  const sb = getSupabase();
  // Basics from the legacy table, exactly as the listing page reads them, so the
  // API and /listing/{n} always describe the same record.
  const { data: l, error } = await sb
    .from('listings')
    .select('listing_number, header, price, cash_flow, state, city, category, url, last_seen, is_active, relisted')
    .eq('listing_number', n)
    .maybeSingle();
  if (error) throw new ApiError(500, 'query_failed', error.message);
  if (!l) throw new ApiError(404, 'not_found', `DealLedger has no listing ${n}.`);
  const [{ data: rec }, { data: seo }, { data: ph, error: phErr }] = await Promise.all([
    sb.from('mv_listings_page_v2')
      .select('estimated_listed_date, listed_after, dom_days_eff, dom_days_max, dom_basis, dom_display, age_source')
      .eq('listing_number', n).maybeSingle(),
    sb.from('mv_seo_listings_v2')
      .select('title, city, state_code, state_source, vertical, vertical_source, revenue, broker_domain')
      .eq('listing_number', n).maybeSingle(),
    sb.from('price_history')
      .select('prev_price, prev_last_seen, next_price, next_first_seen, pct_change, window_days')
      .eq('listing_number', n).order('next_first_seen', { ascending: false }),
  ]);
  if (phErr) throw new ApiError(500, 'query_failed', phErr.message);
  const L: any = l, S: any = seo;
  // Location and type: the public (SEO) record where it exists — it fills in states the
  // legacy row lacks — else the legacy row.
  const st = S?.state_code ?? L.state ?? null;
  const city = S?.city ?? L.city ?? null;
  const price = pos(L.price), cf = pos(L.cash_flow);
  return {
    listing_number: n,
    dealledger_url: listingUrl(n),
    title: displayTitle({ title: S?.title ?? L.header, city, state_code: st, price: L.price }),
    in_public_index: !!S,
    city,
    state: st,
    state_source: S?.state_source ?? (L.state ? 'listing' : null),
    business_type: S?.vertical ?? null,
    business_type_label: S ? TYPES[S.vertical]?.label ?? S.vertical : null,
    category: L.category ?? null,
    asking_price: price,
    cash_flow: cf,
    revenue: pos(S?.revenue),
    price_to_cash_flow: price && cf ? Number((price / cf).toFixed(2)) : null,
    status: L.is_active ? 'listed' : 'no longer listed on the broker site',
    last_seen: day(L.last_seen),
    observed_age: observedAge(rec),
    price_changes: (ph || [])
      .filter((c: any) => Number(c.prev_price) > 0 && Number(c.next_price) > 0 && String(c.next_first_seen) > String(c.prev_last_seen))
      .map(priceChange),
    relist_signal: relistSignal(!!L.relisted),
    broker_site: S?.broker_domain ?? hostOf(L.url),
    broker_page: L.url ?? null,
  };
}

// ── searchPriceChanges ─────────────────────────────────────────────────────────
// Filtering, ordering and counting all run in the database (public.api_price_changes),
// so every matching change is reachable by paging.
export type PriceChangeQuery = {
  state?: string; type?: string; direction?: string; min_pct?: unknown; since?: string;
  limit?: unknown; offset?: unknown;
};
export async function searchPriceChanges(a: PriceChangeQuery, maxLimit = 100) {
  const st = state(a.state), vt = vertical(a.type);
  const { limit, offset } = page(a.limit, a.offset, maxLimit, 20);
  const dir = a.direction || 'down';
  if (!['down', 'up', 'any'].includes(dir)) throw new ApiError(400, 'bad_direction', 'direction must be down, up or any.');
  const minPct = money(a.min_pct, 'min_pct');
  const since = isoDate(a.since, 'since');

  const build = (withCount: boolean) => {
    let q = getSupabase().from('api_price_changes').select('*', withCount ? { count: 'exact' } : undefined);
    if (st) q = q.eq('state_code', st);
    if (vt) q = q.eq('vertical', vt);
    if (since) q = q.gte('changed_on_or_before', since);
    if (dir === 'down') q = q.lt('pct_change', minPct ? -minPct : 0);
    if (dir === 'up') q = q.gt('pct_change', minPct ?? 0);
    if (dir === 'any' && minPct) q = q.or(`pct_change.lte.${-minPct},pct_change.gte.${minPct}`);
    return q.order('changed_on_or_before', { ascending: false }).order('listing_number', { ascending: true });
  };
  const { rows: data, total: count } = await fetchPage(build, offset, limit);
  const results = (data || []).map((r: any) => ({
    listing_number: Number(r.listing_number),
    dealledger_url: listingUrl(Number(r.listing_number)),
    title: displayTitle({ title: r.title, city: r.city, state_code: r.state_code, price: r.current_price }),
    city: r.city ?? null,
    state: r.state_code ?? null,
    business_type: r.vertical ?? null,
    business_type_label: TYPES[r.vertical]?.label ?? r.vertical ?? null,
    broker_site: r.broker_domain ?? hostOf(r.source_url),
    ...priceChange(r),
  }));
  return pageOf(results, count, limit, offset);
}

// ── Reference lists ────────────────────────────────────────────────────────────
export const referenceData = () => ({
  states: Object.entries(STATES).map(([code, name]) => ({ code, name })),
  business_types: Object.entries(TYPES).map(([key, t]) => ({ key, label: t.label, slug: t.slug })),
});
