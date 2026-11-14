// pages/listing/[id].tsx
//
// DealLedger listing detail page — /listing/{listing_number}

import type { GetStaticPaths, GetStaticProps, InferGetStaticPropsType } from 'next';
import Head from 'next/head';
import { getSupabase } from '../../lib/supabase';
import { goListing } from '../../lib/clickout';
import { displayName, isMarketplaceDomain } from '../../lib/brokerRegistry';
import AlertSignup from '../../components/AlertSignup';
import { firstNiche } from '../../lib/niches';
import { STATES, TYPES } from '../../lib/seo';


// ─── TYPES ─────────────────────────────────────────────────────────────────
type Listing = {
  id: number;
  listing_number: number;
  source: string;
  header: string | null;
  price: number | null;
  cash_flow: number | null;
  state: string | null;
  city: string | null;
  category: string | null;
  url: string | null;
  first_seen: string | null;
  last_seen: string | null;
  estimated_listed_date: string | null;
  days_on_market: number | null;
  listing_views: number | null;
  is_active: boolean | null;
  price_reduced: boolean | null;
  quality_tier: string | null;
  quality_score: number | null;
  relisted: boolean | null;
  firm_key: string | null;
  dom_basis?: string | null; // window | observed | in_db | null
  listed_after?: string | null;
  days_max?: number | null;
  age_label?: string | null;
  age_source?: string | null;
};

// One broker-direct price change, as an observation window. The exact day of the
// change is unknown: it happened after prev_last_seen and on or before next_first_seen.
type PriceChange = {
  prev_price: number;
  prev_last_seen: string;
  next_price: number;
  next_first_seen: string;
  pct_change: number;
  window_days: number;
};

type PageProps = {
  listing: Listing | null;
  priceChanges: PriceChange[];
  registryBroker: { slug: string; name: string } | null;
  domPercentile: number | null;
  alert: { state: string | null; vertical: string | null; label: string } | null;
};

// ─── DATA FETCHING ─────────────────────────────────────────────────────────
export const getStaticPaths: GetStaticPaths = async () => {
  return { paths: [], fallback: 'blocking' };
};

export const getStaticProps: GetStaticProps<PageProps> = async ({ params }) => {
  const idParam = params?.id;
  const listingNumber = typeof idParam === 'string' ? parseInt(idParam, 10) : NaN;

  console.log('[listing] === REQUEST ===');
  console.log('[listing] listingNumber:', listingNumber);

  if (!listingNumber || Number.isNaN(listingNumber)) {
    return { notFound: true };
  }

  const sb = getSupabase();

  const { data: listingRow, error: listingErr } = await sb
    .from('listings')
    .select(
      'id, listing_number, source, header, price, cash_flow, state, city, category, url, first_seen, last_seen, estimated_listed_date, days_on_market, listing_views, is_active, price_reduced, quality_tier, quality_score, relisted, firm_key'
    )
    .eq('listing_number', listingNumber)
    .maybeSingle();

  console.log('[listing] Query 1:', listingErr ? 'ERR' : 'OK', listingRow ? `id=${listingRow.id}` : 'null');

  if (listingErr || !listingRow) return { notFound: true };
  const listing = listingRow as Listing;

  // Listing age comes only from the evidence hierarchy in mv_listings_page_v2 (2026-11-07):
  // window (complete crawls) > observed (first dated sighting) > in_db (first database record).
  // The legacy listings.first_seen / estimated_listed_date / days_on_market fields can be seeded
  // or backdated, so they are never shown: no evidence row means no age.
  const { data: rec } = await sb
    .from('mv_listings_page_v2')
    .select('estimated_listed_date, listed_after, dom_days_eff, dom_days_max, dom_basis, dom_display, age_source')
    .eq('listing_number', listingNumber)
    .maybeSingle();
  listing.estimated_listed_date = rec?.dom_basis ? rec.estimated_listed_date : null;
  listing.days_on_market = rec?.dom_basis ? rec.dom_days_eff : null;
  listing.dom_basis = rec?.dom_basis ?? null;
  listing.listed_after = rec?.listed_after ?? null;
  listing.days_max = rec?.dom_days_max ?? null;
  listing.age_label =
    rec?.dom_basis === 'window' && rec?.dom_display
      ? rec.dom_display.replace(/^Listed between .+ and /, 'Listed ')
      : rec?.dom_display ?? null;
  listing.age_source = rec?.age_source ?? null;
  listing.first_seen = null;

  // Broker-direct listings: link to the broker's page in the registry, matched by website.
  let registryBroker: { slug: string; name: string } | null = null;
  if (listing.url) {
    const host = listing.url.replace(/^https?:\/\/(www\.)?/i, '').split(/[/?#:]/)[0].toLowerCase();
    if (host) {
      const { data: rb } = await sb
        .from('broker_directory')
        .select('slug, firm_name, domain')
        .eq('slug', host.replace(/[^a-z0-9]+/g, '-'))
        .maybeSingle();
      if (rb && !isMarketplaceDomain(rb.domain)) registryBroker = { slug: rb.slug, name: displayName(rb.firm_name, rb.domain) };
    }
  }

  // "More like this" alert: the listing's most specific niche (or broad type) in its state.
  let alert: PageProps['alert'] = null;
  {
    const { data: seo } = await sb
      .from('mv_seo_listings_v2')
      .select('title, vertical, state_code')
      .eq('listing_number', listingNumber)
      .maybeSingle();
    if (seo) {
      const niche = firstNiche({ title: seo.title, vertical: seo.vertical });
      const key = niche ? niche.slug : (TYPES[seo.vertical] ? seo.vertical : null);
      const what = niche ? niche.label : key ? TYPES[key].label : 'Businesses';
      const state = seo.state_code && STATES[seo.state_code] ? seo.state_code : null;
      if (key || state) alert = { state, vertical: key, label: `${what}${state ? ` in ${STATES[state]}` : ''}` };
    }
  }

  // No percentile or 'slowest' comparison: observed age is a minimum, not days on market.
  const domPercentile: number | null = null;

  // Reviewed historical price changes (public.price_history: approved, broker-direct only).
  // Absent or failing -> no price-change section; the page never infers one.
  let priceChanges: PriceChange[] = [];
  {
    const { data: ph, error: phErr } = await sb
      .from('price_history')
      .select('prev_price, prev_last_seen, next_price, next_first_seen, pct_change, window_days')
      .eq('listing_number', listingNumber)
      .order('next_first_seen', { ascending: false });
    if (!phErr && Array.isArray(ph)) {
      priceChanges = ph
        .map((c: any) => ({
          prev_price: Number(c.prev_price),
          prev_last_seen: String(c.prev_last_seen).slice(0, 10),
          next_price: Number(c.next_price),
          next_first_seen: String(c.next_first_seen).slice(0, 10),
          pct_change: Number(c.pct_change),
          window_days: Number(c.window_days),
        }))
        .filter((c) => c.prev_price > 0 && c.next_price > 0 && c.next_first_seen > c.prev_last_seen);
    }
  }

  console.log('[listing] === SUCCESS ===');

  return {
    props: { listing, priceChanges, registryBroker, domPercentile, alert },
    revalidate: 60 * 60 * 6,
  };
};

// ─── FORMATTERS ────────────────────────────────────────────────────────────
const fmtPrice = (n: number | null | undefined) => {
  if (n == null) return '—';
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(n >= 10_000_000 ? 0 : 1)}M`;
  if (n >= 1_000) return `$${Math.round(n / 1_000)}K`;
  return `$${n}`;
};

const fmtPriceFull = (n: number | null | undefined) => {
  if (n == null) return '—';
  return `$${new Intl.NumberFormat('en-US').format(n)}`;
};

const fmtMonthYear = (s: string | null | undefined) => {
  if (!s) return '—';
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
};

const padDLId = (id: number) => `DL-${String(id).padStart(8, '0')}`;

// 'YYYY-MM-DD' -> 'Jun 14, 2026', read as a calendar date (no time-zone shift).
const fmtDay = (iso: string, withYear = true) =>
  new Date(`${iso}T00:00:00Z`).toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', ...(withYear ? { year: 'numeric' } : {}), timeZone: 'UTC',
  });

// The observation window of a change: 'Jun 10 – Jun 14, 2026'.
const fmtWindow = (from: string, to: string) =>
  from.slice(0, 4) === to.slice(0, 4)
    ? `${fmtDay(from, false)} – ${fmtDay(to)}`
    : `${fmtDay(from)} – ${fmtDay(to)}`;

const fmtPct = (p: number) => `${p < 0 ? '−' : '+'}${Math.abs(p).toFixed(1)}%`;

// ─── PAGE ──────────────────────────────────────────────────────────────────
export default function ListingPage({
  listing,
  priceChanges,
  registryBroker,
  domPercentile,
  alert,
}: InferGetStaticPropsType<typeof getStaticProps>) {
  if (!listing) return null;

  const currentPrice = listing.price;

  void domPercentile;
  const ageValue =
    listing.days_on_market == null
      ? '—'
      : listing.dom_basis === 'window'
      ? `${listing.days_on_market} days`
      : `${listing.days_on_market}+ days`;
  const ageSub = listing.age_label || 'No dated evidence yet';

  type ChangeLogEntry = { date: string; description: string; sortKey: string };
  const changeLog: ChangeLogEntry[] = [];

  const changes = priceChanges || [];
  const latestChange = changes.length > 0 ? changes[0] : null;
  changes.forEach((c) => {
    changeLog.push({
      date: fmtWindow(c.prev_last_seen, c.next_first_seen),
      description: `Asking price ${c.next_price < c.prev_price ? 'reduced' : 'raised'} from ${fmtPriceFull(c.prev_price)} to ${fmtPriceFull(c.next_price)} (${fmtPct(c.pct_change)})`,
      sortKey: c.next_first_seen,
    });
  });

  if (listing.estimated_listed_date && listing.dom_basis) {
    changeLog.push({
      sortKey: listing.estimated_listed_date.slice(0, 10),
      date: listing.estimated_listed_date.slice(0, 10),
      description: listing.dom_basis === 'window'
        ? 'Appeared on the broker’s site (complete crawls before and after)'
        : listing.dom_basis === 'observed'
        ? 'First observed by DealLedger on the broker’s site'
        : 'First recorded in the DealLedger database',
    });
  }
  changeLog.sort((a, b) => b.sortKey.localeCompare(a.sortKey));

  const today = new Date().toLocaleDateString('en-US', {
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  });

  const locationLine = [listing.city, listing.state].filter(Boolean).join(', ');
  const subtitleParts: string[] = [];
  if (locationLine) subtitleParts.push(locationLine);
  if (registryBroker) subtitleParts.push(`Listed by ${registryBroker.name}`);

  const pageTitle = `${listing.header || 'Listing'} — ${padDLId(listing.id)} · DealLedger`;
  const canonical = `https://dealledger.org/listing/${listing.listing_number}`;

  // Every listing page carried the same sentence with one number changed:
  // "Public record of listing 1168432925". Across ~32,000 URLs that reads as a
  // templated corpus, which is what Google's "Discovered - currently not
  // indexed" means (34,589 pages as of 2026-10-08). Build it from the fields
  // that actually differ between listings.
  const metaDescription = (() => {
    const where = [listing.city, listing.state].filter(Boolean).join(', ');
    const bits: string[] = [];
    bits.push(listing.header || 'Business for sale');
    if (where) bits.push(`in ${where}`);
    const facts: string[] = [];
    if (listing.price != null) facts.push(`asking ${fmtPriceFull(listing.price)}`);
    if (listing.cash_flow != null) facts.push(`cash flow ${fmtPriceFull(listing.cash_flow)}`);
    if (listing.days_on_market != null) {
      if (listing.age_label) facts.push(listing.age_label.charAt(0).toLowerCase() + listing.age_label.slice(1));
    }
    const head = bits.join(' ');
    return facts.length
      ? `${head} — ${facts.join(', ')}. Observed from the broker's own site. ${padDLId(listing.id)}.`
      : `${head}. Public record observed from the broker's own site. ${padDLId(listing.id)}.`;
  })();

  // schema.org. Two graphs on purpose:
  //   Product/Offer   — the listing as a thing for sale
  //   Dataset         — the observation record behind it. This is the half
  //                     competitors cannot publish, because their data is not
  //                     open, and it is what Google Dataset Search indexes.
  const jsonLd = {
    '@context': 'https://schema.org',
    '@graph': [
      {
        '@type': 'Product',
        '@id': `${canonical}#listing`,
        name: listing.header || `Listing ${padDLId(listing.id)}`,
        description: metaDescription,
        sku: padDLId(listing.id),
        category: listing.category || 'Business for sale',
        url: canonical,
        ...(listing.price != null
          ? {
              offers: {
                '@type': 'Offer',
                price: listing.price,
                priceCurrency: 'USD',
                // Only the active state is asserted. A listing that has left
                // observation is not known to have sold, so no availability is
                // claimed for it.
                ...(listing.is_active
                  ? { availability: 'https://schema.org/InStock' }
                  : {}),
                ...(listing.dom_basis === 'window' && listing.estimated_listed_date
                  ? { availabilityStarts: listing.estimated_listed_date }
                  : {}),
                ...(listing.city || listing.state
                  ? {
                      areaServed: {
                        '@type': 'Place',
                        address: {
                          '@type': 'PostalAddress',
                          ...(listing.city ? { addressLocality: listing.city } : {}),
                          ...(listing.state ? { addressRegion: listing.state } : {}),
                          addressCountry: 'US',
                        },
                      },
                    }
                  : {}),
              },
            }
          : {}),
      },
      {
        '@type': 'Dataset',
        '@id': `${canonical}#record`,
        name: `DealLedger observation record ${padDLId(listing.id)}`,
        description:
          'Dated observation record for a single US business-for-sale listing: ' +
          'latest recorded asking price, cash flow, location, and listing age with its evidence basis. ' +
          'Part of the DealLedger open index.',
        url: canonical,
        license: 'https://creativecommons.org/publicdomain/zero/1.0/',
        isAccessibleForFree: true,
        creator: { '@type': 'Organization', name: 'DealLedger', url: 'https://dealledger.org' },
        isPartOf: { '@type': 'Dataset', '@id': 'https://dealledger.org/#dataset' },
        ...(listing.estimated_listed_date ? { temporalCoverage: `${listing.estimated_listed_date.slice(0, 10)}/..` } : {}),
        // No `dateModified` field: nothing we store records when the listing itself
        // last changed (last_seen bumps on every crawl; first_seen and the
        // estimated listed date are not modification dates).
        ...(listing.state ? { spatialCoverage: { '@type': 'Place', name: listing.state } } : {}),
        variableMeasured: [
          'asking price', 'cash flow', 'listing age (evidence basis)',
        ],
      },
    ],
  };

  return (
    <>
      <Head>
        <title>{pageTitle}</title>
        <meta name="description" content={metaDescription} />
        <link rel="canonical" href={canonical} />
        <meta property="og:type" content="website" />
        <meta property="og:title" content={pageTitle} />
        <meta property="og:description" content={metaDescription} />
        <meta property="og:url" content={canonical} />
        <meta property="og:site_name" content="DealLedger" />
        <meta name="twitter:card" content="summary" />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
        />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="" />
        <link
          rel="stylesheet"
          href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap"
        />
      </Head>

      <style jsx global>{`
        :root {
          --bg: #fafaf9;
          --bg-card: #f5f5f4;
          --ink: #1c1917;
          --ink-soft: #57534e;
          --ink-mute: #78716c;
          --rule: #d4d4d4;
          --accent: #c2410c;
          --link: #0c4a6e;
          /* Named --serif for historical reasons; the site is IBM Plex Sans. */
          --serif: 'IBM Plex Sans', -apple-system, system-ui, sans-serif;
          --mono: 'IBM Plex Mono', ui-monospace, monospace;
        }
        * { box-sizing: border-box; }
        body {
          margin: 0;
          background: var(--bg);
          color: var(--ink);
          font-family: var(--serif);
          font-size: 16px;
          line-height: 1.55;
          -webkit-font-smoothing: antialiased;
        }
        a {
          color: var(--link);
          text-decoration: underline;
          text-underline-offset: 2px;
        }
        a:hover { color: var(--accent); }
      `}</style>

      <div className="page">
        <header className="masthead">
          <div className="masthead-inner">
            <a href="/" className="brand">DealLedger</a>
            <div className="masthead-meta">
              PUBLIC RECORD · {today.toUpperCase()} · <a href="/alerts" className="masthead-alerts">GET ALERTS</a>
            </div>
          </div>
          <div className="masthead-rule" />
        </header>

        <main className="content">
          <div className="eyebrow">{padDLId(listing.id)}</div>
          <h1 className="headline">{listing.header}</h1>
          <p className="subtitle">{subtitleParts.join(' · ')}</p>

          <div className="stats">
            <Stat
              label="Asking Price"
              value={fmtPrice(currentPrice)}
              sub={
                currentPrice == null
                  ? 'Not disclosed'
                  : latestChange
                  ? `${latestChange.next_price < latestChange.prev_price ? 'Reduced' : 'Raised'} ${Math.abs(latestChange.pct_change).toFixed(1)}% · ${fmtWindow(latestChange.prev_last_seen, latestChange.next_first_seen)}`
                  : 'Latest recorded'
              }
            />
            <Stat
              label="Listing age"
              value={ageValue}
              sub={ageSub}
            />
            <Stat
              label="Cash Flow"
              value={fmtPrice(listing.cash_flow)}
              sub={
                listing.cash_flow == null
                  ? 'Not disclosed'
                  : currentPrice && listing.cash_flow
                  ? `${(currentPrice / listing.cash_flow).toFixed(1)}× multiple`
                  : ''
              }
            />
          </div>

          {listing.estimated_listed_date && listing.dom_basis && (
            <Section title="TIMELINE" subtitle="Observed events">
              <Timeline
                listedDate={listing.estimated_listed_date}
                label={listing.dom_basis === 'window' ? 'Appeared by' : listing.dom_basis === 'observed' ? 'Observed since' : 'In DealLedger since'}
              />
            </Section>
          )}

          {changes.length > 0 && (
            <Section title="PRICE CHANGES" subtitle="From the broker&rsquo;s own listing">
              <div className="changelog">
                {changes.map((c, i) => (
                  <div key={i} className="changelog-row">
                    <div className="changelog-date">{fmtWindow(c.prev_last_seen, c.next_first_seen)}</div>
                    <div className="changelog-desc">
                      {fmtPriceFull(c.prev_price)} → {fmtPriceFull(c.next_price)}{' '}
                      <strong>{fmtPct(c.pct_change)}</strong>
                      <br />
                      <span className="pc-note">
                        Last seen at {fmtPriceFull(c.prev_price)} on {fmtDay(c.prev_last_seen)}; first seen at{' '}
                        {fmtPriceFull(c.next_price)} on {fmtDay(c.next_first_seen)}.
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </Section>
          )}

          {changeLog.length > 0 && (
            <Section title="CHANGE LOG" subtitle="What's changed">
              <div className="changelog">
                {changeLog.map((entry, i) => (
                  <div key={i} className="changelog-row">
                    <div className="changelog-date">{entry.date}</div>
                    <div className="changelog-desc">{entry.description}</div>
                  </div>
                ))}
              </div>
            </Section>
          )}

          {registryBroker && (
            <Section title="LISTED BY" subtitle={registryBroker.name}>
              <div className="broker-card">
                <div className="broker-name">{registryBroker.name}</div>
                <div className="broker-link">
                  <a href={`/broker/${registryBroker.slug}`}>
                    All listings we observe from this broker →
                  </a>
                </div>
              </div>
            </Section>
          )}

          <Section title="ACQUISITION FINANCING" subtitle="">
            <div style={{ background: 'var(--bg-card)', border: '1px solid var(--rule)', padding: '22px 24px' }}>
              <p style={{ margin: 0, color: 'var(--ink-soft)', maxWidth: '64ch' }}>
                Most SBA-eligible businesses on this ledger qualify for 7(a) acquisition
                financing at approximately 10% buyer equity. DealLedger maintains relationships
                with SBA lender partners. <a href="/sba">See SBA options →</a>
              </p>
            </div>
          </Section>

          {alert && (
            <AlertSignup
              {...alert}
              path={`/listing/${listing.listing_number}`}
              heading={`Get more like this: new listings for ${alert.label}, by email`}
            />
          )}

          <Section title="METHODOLOGY" subtitle="What we observe">
            <div className="prose">
              {changes.length > 0 ? (
                <p>
                  DealLedger records the asking price each time it observes the
                  broker&rsquo;s listing. A price change is shown as a window:
                  after the last observation at the old price and on or before the
                  first observation at the new one. The exact day isn&rsquo;t known.
                </p>
              ) : (
                <p>
                  DealLedger displays the latest recorded asking price from the
                  broker&rsquo;s listing. Historical price changes are not
                  currently tracked.
                </p>
              )}
              {listing.quality_tier && listing.quality_tier !== 'Verified' && (
                <p>
                  Listing quality:{' '}
                  <span className="quality-tier">{listing.quality_tier}</span>{' '}
                  ({listing.quality_score}/100). This reflects metadata
                  completeness, not whether the listing exists.
                </p>
              )}
              <p>
                <a href={listing.url ? goListing(listing.listing_number, 'listing') : '#'} target="_blank" rel="nofollow noopener">
                  See listing →
                </a>
              </p>
            </div>
          </Section>

          <Section title="RIGHT TO RESPOND" subtitle="">
            <div className="respond">
              <p>
                If you represent this listing or {registryBroker?.name || 'this broker'} and would like
                to add context to this record, contact{' '}
                <a href="mailto:corrections@dealledger.org">
                  corrections@dealledger.org
                </a>
                . Corrections are published openly alongside the original
                observation.
              </p>
            </div>
          </Section>

          <footer className="footer">
            <div className="footer-rule" />
            <div className="footer-text">
              DealLedger · Public record · CC0
              <br />
              An open public registry of business brokerage activity in the
              U.S. lower middle market.
            </div>
          </footer>
        </main>
      </div>

      <style jsx>{`
        .page { min-height: 100vh; }

        .masthead { padding: 24px 0 0 0; }
        .masthead-inner {
          max-width: 880px;
          margin: 0 auto;
          padding: 0 32px 18px 32px;
          display: flex;
          justify-content: space-between;
          align-items: baseline;
        }
        .brand {
          font-family: var(--serif);
          font-weight: 700;
          font-size: 22px;
          text-decoration: none;
          letter-spacing: -0.01em;
        }
        .masthead-meta {
          font-family: var(--mono);
          font-size: 11px;
          letter-spacing: 0.08em;
          color: var(--ink-mute);
        }
        .masthead-alerts { color: var(--accent); text-decoration: none; }
        .masthead-rule {
          max-width: 880px;
          margin: 0 auto;
          border-top: 1px solid var(--rule);
        }

        .content {
          max-width: 880px;
          margin: 0 auto;
          padding: 56px 32px 96px 32px;
        }

        .eyebrow {
          font-family: var(--mono);
          font-size: 12px;
          letter-spacing: 0.1em;
          color: var(--accent);
          margin-bottom: 14px;
        }
        .headline {
          font-family: var(--serif);
          font-weight: 500;
          font-size: 44px;
          line-height: 1.1;
          letter-spacing: -0.015em;
          margin: 0 0 14px 0;
        }
        .subtitle {
          font-size: 17px;
          color: var(--ink-soft);
          margin: 0 0 40px 0;
        }

        .stats {
          display: grid;
          grid-template-columns: repeat(4, 1fr);
          gap: 0;
          background: var(--bg-card);
          border: 1px solid var(--rule);
          margin-bottom: 56px;
        }

        .changelog {
          background: var(--bg-card);
          border: 1px solid var(--rule);
        }
        .changelog-row {
          display: grid;
          grid-template-columns: 110px 1fr;
          gap: 18px;
          padding: 12px 20px;
          border-bottom: 1px solid var(--rule);
          font-size: 14px;
        }
        .changelog-row:last-child { border-bottom: none; }
        .changelog-date {
          font-family: var(--mono);
          font-size: 12px;
          color: var(--ink-mute);
          padding-top: 2px;
        }
        .changelog-desc { color: var(--ink-soft); }

        .pc-note {
          font-size: 12px;
          color: var(--ink-mute);
        }
        .broker-card {
          background: var(--bg-card);
          border: 1px solid var(--rule);
          padding: 24px;
        }
        .broker-name {
          font-family: var(--serif);
          font-size: 22px;
          font-weight: 500;
          letter-spacing: -0.01em;
        }
        .broker-loc {
          font-size: 14px;
          color: var(--ink-soft);
          margin-top: 4px;
          margin-bottom: 20px;
        }
        .broker-stats {
          display: grid;
          grid-template-columns: repeat(3, 1fr);
          gap: 0;
          border-top: 1px solid var(--rule);
        }
        .broker-stat {
          padding: 16px 16px 14px 0;
          border-right: 1px solid var(--rule);
        }
        .broker-stat:last-child { border-right: none; padding-right: 0; }
        .broker-stat-label {
          font-family: var(--mono);
          font-size: 10px;
          letter-spacing: 0.1em;
          text-transform: uppercase;
          color: var(--ink-mute);
          margin-bottom: 6px;
        }
        .broker-stat-value {
          font-family: var(--serif);
          font-size: 24px;
          font-weight: 500;
          line-height: 1;
        }
        .broker-link {
          margin-top: 20px;
          padding-top: 16px;
          border-top: 1px solid var(--rule);
          font-size: 14px;
        }

        .prose p {
          margin: 0 0 14px 0;
          color: var(--ink-soft);
          max-width: 64ch;
        }
        .prose p:last-child { margin-bottom: 0; }
        .quality-tier {
          font-family: var(--mono);
          font-size: 13px;
          color: var(--accent);
        }

        .respond {
          background: var(--bg-card);
          border: 1px solid var(--rule);
          padding: 22px 24px;
        }
        .respond p {
          margin: 0;
          color: var(--ink-soft);
          max-width: 64ch;
        }

        .footer { margin-top: 80px; }
        .footer-rule {
          border-top: 1px solid var(--rule);
          margin-bottom: 20px;
        }
        .footer-text {
          font-family: var(--mono);
          font-size: 11px;
          letter-spacing: 0.04em;
          color: var(--ink-mute);
          line-height: 1.7;
        }

        @media (max-width: 720px) {
          .headline { font-size: 32px; }
          .stats { grid-template-columns: 1fr 1fr; }
          .content { padding: 40px 22px 64px 22px; }
          .broker-stats { grid-template-columns: 1fr; }
          .broker-stat {
            border-right: none;
            border-bottom: 1px solid var(--rule);
            padding: 14px 0;
          }
          .broker-stat:last-child { border-bottom: none; }
        }
      `}</style>
    </>
  );
}

// ─── SUB-COMPONENTS ────────────────────────────────────────────────────────

function Stat({
  label,
  value,
  sub,
  accent = false,
}: {
  label: string;
  value: string;
  sub: string;
  accent?: boolean;
}) {
  return (
    <div className="stat">
      <div className="stat-label">{label}</div>
      <div className={`stat-value ${accent ? 'stat-value-accent' : ''}`}>{value}</div>
      <div className="stat-sub">{sub}</div>
      <style jsx>{`
        .stat {
          padding: 22px 24px 20px 24px;
          border-right: 1px solid var(--rule);
        }
        .stat:last-child { border-right: none; }
        .stat-label {
          font-family: var(--mono);
          font-size: 10px;
          letter-spacing: 0.1em;
          text-transform: uppercase;
          color: var(--ink-mute);
          margin-bottom: 8px;
        }
        .stat-value {
          font-family: var(--serif);
          font-size: 36px;
          font-weight: 500;
          line-height: 1;
          letter-spacing: -0.02em;
          margin-bottom: 6px;
        }
        .stat-value-accent { color: var(--accent); }
        .stat-sub {
          font-size: 13px;
          color: var(--ink-soft);
        }
      `}</style>
    </div>
  );
}

function Section({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle: string;
  children: React.ReactNode;
}) {
  return (
    <section className="section">
      <div className="section-eyebrow">— {title}</div>
      {subtitle ? <h2 className="section-title">{subtitle}</h2> : null}
      <div className="section-body">{children}</div>
      <style jsx>{`
        .section { margin-bottom: 56px; }
        .section-eyebrow {
          font-family: var(--mono);
          font-size: 11px;
          letter-spacing: 0.14em;
          color: var(--accent);
          margin-bottom: 8px;
        }
        .section-title {
          font-family: var(--serif);
          font-weight: 500;
          font-size: 26px;
          line-height: 1.2;
          letter-spacing: -0.01em;
          margin: 0 0 20px 0;
        }
      `}</style>
    </section>
  );
}

function Timeline({
  listedDate,
  label,
}: {
  listedDate: string | null;
  label: string;
}) {
  const events: { label: string; date: string }[] = [];

  if (listedDate) {
    events.push({ label, date: fmtMonthYear(listedDate) });
  }

  if (events.length === 0) return null;

  return (
    <div className="tl-wrap">
      <div className="tl-track">
        {events.map((e, i) => (
          <div key={i} className="tl-event">
            <div className="tl-label">{e.label}</div>
            <div className={`tl-dot ${i === 0 ? 'tl-dot-first' : ''} ${i === events.length - 1 ? 'tl-dot-last' : ''}`} />
            <div className="tl-date">{e.date}</div>
          </div>
        ))}
      </div>
      <style jsx>{`
        .tl-wrap {
          background: var(--bg-card);
          border: 1px solid var(--rule);
          padding: 32px 24px;
        }
        .tl-track {
          display: grid;
          grid-template-columns: repeat(${events.length}, 1fr);
          position: relative;
        }
        .tl-track::before {
          content: '';
          position: absolute;
          top: 50%;
          left: 8%;
          right: 8%;
          height: 1px;
          background: var(--rule);
          z-index: 0;
        }
        .tl-event {
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 12px;
          position: relative;
          z-index: 1;
        }
        .tl-label {
          font-size: 12px;
          color: var(--ink-soft);
          text-align: center;
          min-height: 32px;
          display: flex;
          align-items: flex-end;
        }
        .tl-dot {
          width: 12px;
          height: 12px;
          border-radius: 50%;
          background: var(--ink-soft);
          border: 2px solid var(--bg-card);
          box-shadow: 0 0 0 1px var(--rule);
        }
        .tl-dot-first { background: #4a7a4a; }
        .tl-dot-last { background: var(--accent); }
        .tl-date {
          font-family: var(--mono);
          font-size: 11px;
          color: var(--ink-mute);
          text-align: center;
        }
      `}</style>
    </div>
  );
}

