// lib/brokerRegistry.ts
//
// Shared types and formatting for the broker registry (/brokers) and the
// broker pages (/broker/[slug]). Data comes from the `broker_directory`
// materialized view: one row per broker website whose published listings
// DealLedger has observed directly. See sql/2026-10-08_broker_registry.sql.

export type RegistryRow = {
  slug: string;
  domain: string;
  firm_name: string;
  name_verified: boolean;
  hq_city: string | null;
  hq_state: string | null;
  is_network: boolean;
  homepage_url: string | null;
  listings_page_url: string | null;
  ibba_member: boolean;
  about: string | null;
  about_source: string | null;
  tracking_since: string | null;
  active_count: number;
  removed_180d: number;
  lifetime_count: number;
  first_observed: string | null;
  last_observed: string | null;
  median_asking: number | null;
  states_listed: string | null;
  refreshed_at: string | null;
};

export const REGISTRY_COLUMNS =
  'slug, domain, firm_name, name_verified, hq_city, hq_state, is_network, homepage_url, ' +
  'listings_page_url, ibba_member, about, about_source, tracking_since, active_count, ' +
  'removed_180d, lifetime_count, first_observed, last_observed, median_asking, ' +
  'states_listed, refreshed_at';

export const REGISTRY_VIEW = 'broker_directory';

// Broker-direct collection began on this date.
export const OBSERVATION_START_LABEL = 'March 24, 2026';

// A listing found during the first two weeks of reading a broker's site was
// usually already listed before we arrived. Its first-seen date is a floor
// ("listed by"), not the date it was listed.
export const FLOOR_GRACE_DAYS = 14;

const US_STATES = new Set(
  ('AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV ' +
    'NH NJ NM NY NC ND OH OK OR PA PR RI SC SD TN TX UT VT VA WA WV WI WY').split(' ')
);

export const cleanStates = (s: string | null): string[] =>
  (s || '')
    .split(',')
    .map((x) => x.trim().toUpperCase())
    .filter((x) => US_STATES.has(x));

// Names in the source registry are often all lower case ("we sell restaurants").
// Title-case those; leave anything that already has capitals alone.
const UPPER_TOKENS = new Set(['llc', 'inc', 'ltd', 'llp', 'pc', 'pa', 'lp', 'usa', 'us', 'cbi', 'cbb', 'm&a', 'm&ami', 'ibba', 're/max', 'kw']);
const LOWER_TOKENS = new Set(['of', 'and', 'the', 'for', 'in', 'at', 'by', 'to']);

export const displayName = (name: string | null, domain?: string): string => {
  const n = (name || '').trim();
  if (!n) return domain || 'Unnamed broker';
  if (n === domain || /\.[a-z]{2,}$/.test(n)) return n; // a bare domain stays as-is
  if (n !== n.toLowerCase()) return n;
  return n
    .split(/(\s+)/)
    .map((w, i) => {
      if (/^\s+$/.test(w)) return w;
      const bare = w.replace(/[.,]+$/, '');
      const trail = w.slice(bare.length);
      if (UPPER_TOKENS.has(bare)) return bare.toUpperCase() + trail;
      if (i > 0 && LOWER_TOKENS.has(bare)) return w;
      return w
        .split(/([-/&])/)
        .map((p) => (p.length ? p[0].toUpperCase() + p.slice(1) : p))
        .join('');
    })
    .join('');
};

const titleCity = (c: string | null) =>
  c && c === c.toLowerCase()
    ? c.replace(/\b([a-z])/g, (m) => m.toUpperCase())
    : c;

export const displayLocation = (r: Pick<RegistryRow, 'hq_city' | 'hq_state' | 'is_network'>) => {
  if (r.is_network) return 'Network';
  const parts = [titleCity(r.hq_city), r.hq_state && US_STATES.has(r.hq_state) ? r.hq_state : null].filter(Boolean);
  return parts.length ? parts.join(', ') : '—';
};

export const fmtNum = (n: number | null | undefined) =>
  n == null ? '—' : new Intl.NumberFormat('en-US').format(n);

export const fmtPrice = (n: number | null | undefined) => {
  if (n == null || n <= 0) return '—';
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(n >= 10_000_000 ? 0 : 1)}M`;
  if (n >= 1_000) return `$${Math.round(n / 1_000)}K`;
  return `$${Math.round(n)}`;
};

export const fmtDate = (s: string | null | undefined) => {
  if (!s) return '—';
  const d = new Date(s);
  if (isNaN(d.getTime())) return '—';
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
};

export const isFloored = (firstSeen: string | null | undefined, trackingSince: string | null | undefined) => {
  if (!firstSeen || !trackingSince) return true;
  const cutoff = new Date(trackingSince).getTime() + FLOOR_GRACE_DAYS * 86400 * 1000;
  return new Date(firstSeen).getTime() <= cutoff;
};

export const bareHost = (url: string | null | undefined) =>
  url ? url.replace(/^https?:\/\/(www\.)?/i, '').replace(/\/$/, '') : null;

// Marketplaces and listing platforms are not brokers. Listings whose URL points
// at one of these can leak into the broker-direct table when a broker links to
// its marketplace page instead of hosting its own; never give them a broker page.
export const MARKETPLACE_DOMAINS = [
  'bizbuysell.com',
  'bizquest.com',
  'businessesforsale.com',
  'businessbroker.net',
  'bizben.com',
  'loopnet.com',
  'dealstream.com',
  'flippa.com',
];

export const isMarketplaceDomain = (domain: string | null | undefined) =>
  !!domain && MARKETPLACE_DOMAINS.some((m) => domain === m || domain.endsWith('.' + m));
