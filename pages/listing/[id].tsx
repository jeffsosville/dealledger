// pages/listing/[id].tsx
//
// DealLedger listing detail page — /listing/{listing_number}

import type { GetStaticPaths, GetStaticProps, InferGetStaticPropsType } from 'next';
import Head from 'next/head';
import { getSupabase } from '../../lib/supabase';
import { displayName, isMarketplaceDomain } from '../../lib/brokerRegistry';

const INDUSTRY_MEDIAN_DOM = 180;

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
  bbs_account_id: number | null;
  quality_tier: string | null;
  quality_score: number | null;
  relisted: boolean | null;
  firm_key: string | null;
  dom_basis?: string | null;
};

type HistoryRow = {
  price: number | null;
  source_file: string | null;
};

type Broker = {
  account: number;
  companyname: string | null;
  companyurl: string | null;
  regionccode: string | null;
  firm_key: string | null;
  active_listings_apr2026: number | null;
  sold_listings_apr2026: number | null;
  soldListingsCount: number | null;
};

type PageProps = {
  listing: Listing | null;
  history: HistoryRow[];
  broker: Broker | null;
  registryBroker: { slug: string; name: string } | null;
  domPercentile: number | null;
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
      'id, listing_number, source, header, price, cash_flow, state, city, category, url, first_seen, last_seen, estimated_listed_date, days_on_market, listing_views, is_active, price_reduced, bbs_account_id, quality_tier, quality_score, relisted, firm_key'
    )
    .eq('listing_number', listingNumber)
    .maybeSingle();

  console.log('[listing] Query 1:', listingErr ? 'ERR' : 'OK', listingRow ? `id=${listingRow.id} bbs_account=${listingRow.bbs_account_id}` : 'null');

  if (listingErr || !listingRow) return { notFound: true };
  const listing = listingRow as Listing;

  // Dates and DOM come from the public record (mv_listings_page), the same source as the homepage.
  // Raw listings.estimated_listed_date / days_on_market are stale for broker-direct listings.
  const { data: rec } = await sb
    .from('mv_listings_page')
    .select('estimated_listed_date, dom_days_eff, dom_basis')
    .eq('listing_number', listingNumber)
    .maybeSingle();
  if (rec) {
    listing.estimated_listed_date = rec.estimated_listed_date ?? listing.estimated_listed_date;
    listing.days_on_market = rec.dom_days_eff ?? listing.days_on_market;
    listing.dom_basis = rec.dom_basis ?? null;
  } else {
    listing.dom_basis = null;
  }

  const { data: histRows, error: histErr } = await sb
    .from('listing_history')
    .select('price, source_file')
    .eq('listnumber', listingNumber)
    .order('source_file', { ascending: true });

  console.log('[listing] Query 2: rows=', histRows?.length ?? 0, 'err=', histErr);

  const history: HistoryRow[] = (histRows || []).map((r: any) => ({
    price: r.price != null ? Number(r.price) : null,
    source_file: r.source_file ?? null,
  }));

  let broker: Broker | null = null;
  if (listing.bbs_account_id != null) {
    const accountId = typeof listing.bbs_account_id === 'string'
      ? parseInt(listing.bbs_account_id, 10)
      : listing.bbs_account_id;

    const { data: brokerRow, error: brokerErr } = await sb
      .from('broker_master')
      .select(
        'account, companyname, companyurl, regionccode, firm_key, active_listings_apr2026, sold_listings_apr2026, "soldListingsCount"'
      )
      .eq('account', accountId)
      .maybeSingle();

    console.log('[listing] Query 3:', brokerRow ? `FOUND ${brokerRow.companyname}` : 'null', 'err=', brokerErr);

    if (brokerRow) broker = brokerRow as Broker;
  }

  // Broker-direct listings: link to the broker's page in the registry, matched by website.
  let registryBroker: { slug: string; name: string } | null = null;
  if (!broker && listing.url) {
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

  let domPercentile: number | null = null;
  if (typeof listing.days_on_market === 'number') {
    const { count: shorter } = await sb
      .from('mv_listings_page')
      .select('listing_number', { count: 'exact', head: true })
      .lt('dom_days_eff', listing.days_on_market);
    const { count: total } = await sb
      .from('mv_listings_page')
      .select('listing_number', { count: 'exact', head: true })
      .not('dom_days_eff', 'is', null);

    console.log('[listing] Query 5: shorter=', shorter, 'total=', total);

    const shorterCount = shorter ?? 0;
    if (total != null && total > 0) {
      domPercentile = Math.round((shorterCount / total) * 100);
    }
  }

  console.log('[listing] === SUCCESS ===');

  return {
    props: { listing, history, broker, registryBroker, domPercentile },
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

const fmtNum = (n: number | null | undefined) =>
  n == null ? '—' : new Intl.NumberFormat('en-US').format(n);

const fmtMonthYear = (s: string | null | undefined) => {
  if (!s) return '—';
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
};

const padDLId = (id: number) => `DL-${String(id).padStart(8, '0')}`;

const buildObservations = (history: HistoryRow[], currentPrice: number | null) => {
  const obs: { label: string; date: string; price: number | null }[] = [];

  history.forEach((h) => {
    if (h.source_file === 'nov_2025') {
      obs.push({ label: 'Nov 2025', date: '2025-11-15', price: h.price });
    } else if (h.source_file?.startsWith('bizbuysell_listings_')) {
      obs.push({ label: 'Apr 2026', date: '2026-04-15', price: h.price });
    }
  });

  // Dedupe by label
  const seen = new Set<string>();
  const dedup = obs.filter((o) => {
    if (seen.has(o.label)) return false;
    seen.add(o.label);
    return true;
  });

  // Sort chronologically by ISO date — oldest first
  dedup.sort((a, b) => a.date.localeCompare(b.date));

  // Push "Today" if current price differs from the most recent observation
  const lastObsPrice = dedup[dedup.length - 1]?.price ?? null;
  if (
    currentPrice != null &&
    lastObsPrice != null &&
    currentPrice !== lastObsPrice
  ) {
    dedup.push({ label: 'Today', date: new Date().toISOString(), price: currentPrice });
  }

  return dedup;
};

// ─── PAGE ──────────────────────────────────────────────────────────────────
export default function ListingPage({
  listing,
  history,
  broker,
  registryBroker,
  domPercentile,
}: InferGetStaticPropsType<typeof getStaticProps>) {
  if (!listing) return null;

  const observations = buildObservations(history, listing.price);
  const earliestObsPrice =
    observations.length > 0 ? observations[0].price : listing.price;
  const currentPrice = listing.price;

  let reductionPct: number | null = null;
  if (
    earliestObsPrice != null &&
    currentPrice != null &&
    earliestObsPrice > currentPrice &&
    earliestObsPrice > 0
  ) {
    reductionPct = Math.round(
      ((earliestObsPrice - currentPrice) / earliestObsPrice) * 100
    );
  }

  let domContext = 'No DOM data';
  if (domPercentile != null) {
    if (domPercentile >= 50) {
      domContext = `Top ${100 - domPercentile}% slowest`;
    } else {
      domContext = `Faster than ${100 - domPercentile}% of listings`;
    }
  }

  const domFlag =
    listing.days_on_market != null &&
    listing.days_on_market > INDUSTRY_MEDIAN_DOM * 1.5;

  type ChangeLogEntry = { date: string; description: string };
  const changeLog: ChangeLogEntry[] = [];

  // Today's price change (if observed)
  if (
    observations.length > 0 &&
    currentPrice != null &&
    observations[observations.length - 1].label === 'Today'
  ) {
    const prev = observations[observations.length - 2]?.price;
    if (prev != null && prev !== currentPrice) {
      changeLog.push({
        date: new Date().toISOString().slice(0, 10),
        description: `Asking price ${
          prev > currentPrice ? 'reduced' : 'increased'
        } from ${fmtPriceFull(prev)} to ${fmtPriceFull(currentPrice)}`,
      });
    }
  }

  // Snapshot observations — sorted newest first
  observations
    .filter((o) => o.label !== 'Today')
    .slice()
    .sort((a, b) => b.date.localeCompare(a.date))
    .forEach((o) => {
      changeLog.push({
        date: o.date.slice(0, 10),
        description: `Observed at ${fmtPriceFull(o.price)}`,
      });
    });

  const isFloor = listing.dom_basis === 'floor';
  if (listing.estimated_listed_date) {
    changeLog.push({
      date: listing.estimated_listed_date.slice(0, 10),
      description: isFloor
        ? 'Already listed when we started tracking this broker — listed on or before this date'
        : listing.dom_basis === 'observed'
        ? 'First seen on the broker’s site'
        : 'Estimated original listing date',
    });
  }

  const today = new Date().toLocaleDateString('en-US', {
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  });

  const locationLine = [listing.city, listing.state].filter(Boolean).join(', ');
  const subtitleParts: string[] = [];
  if (locationLine) subtitleParts.push(locationLine);
  if (broker?.companyname) subtitleParts.push(`Listed by ${broker.companyname}`);
  else if (registryBroker) subtitleParts.push(`Listed by ${registryBroker.name}`);

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
      facts.push(listing.dom_basis === 'floor'
        ? `listed at least ${listing.days_on_market} days`
        : `${listing.days_on_market} days on market`);
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
                availability: listing.is_active
                  ? 'https://schema.org/InStock'
                  : 'https://schema.org/SoldOut',
                ...(listing.estimated_listed_date
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
                ...(broker?.companyname
                  ? { seller: { '@type': 'Organization', name: broker.companyname,
                                ...(broker.companyurl ? { url: broker.companyurl } : {}) } }
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
          'asking price, cash flow, location, first-seen date, days on market, ' +
          'and every change observed since. Part of the DealLedger open index.',
        url: canonical,
        license: 'https://creativecommons.org/publicdomain/zero/1.0/',
        isAccessibleForFree: true,
        creator: { '@type': 'Organization', name: 'DealLedger', url: 'https://dealledger.org' },
        isPartOf: { '@type': 'Dataset', '@id': 'https://dealledger.org/#dataset' },
        ...(listing.first_seen ? { temporalCoverage: `${listing.first_seen.slice(0, 10)}/..` } : {}),
        ...(listing.last_seen ? { dateModified: listing.last_seen.slice(0, 10) } : {}),
        ...(listing.state ? { spatialCoverage: { '@type': 'Place', name: listing.state } } : {}),
        variableMeasured: [
          'asking price', 'cash flow', 'days on market', 'first seen date', 'price changes',
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
              PUBLIC RECORD · {today.toUpperCase()}
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
                reductionPct != null
                  ? `Reduced ${reductionPct}%`
                  : currentPrice == null
                  ? 'Not disclosed'
                  : 'No price change observed'
              }
            />
            <Stat
              label="Days on Market"
              value={listing.days_on_market != null ? `${listing.days_on_market}${isFloor ? '+' : ''}` : '—'}
              sub={domContext}
              accent={domFlag}
            />
            <Stat
              label="Times Listed"
              value="1"
              sub="1 listing observed"
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

          {(observations.length > 0 || listing.estimated_listed_date) && (
            <Section title="TIMELINE" subtitle="Observed events">
              <Timeline
                listedDate={listing.estimated_listed_date}
                floor={isFloor}
                observations={observations}
              />
            </Section>
          )}

          {observations.length >= 2 && (
            <Section title="PRICE HISTORY" subtitle="Asking price over time">
              <PriceChart observations={observations} />
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

          {broker && (
            <Section title="LISTED BY" subtitle="Broker fingerprint">
              <div className="broker-card">
                <div className="broker-name">
                  {broker.companyname || `Account ${broker.account}`}
                </div>
                {broker.regionccode && (
                  <div className="broker-loc">{broker.regionccode}</div>
                )}
                <div className="broker-stats">
                  <div className="broker-stat">
                    <div className="broker-stat-label">Active Listings</div>
                    <div className="broker-stat-value">
                      {fmtNum(broker.active_listings_apr2026)}
                    </div>
                  </div>
                  <div className="broker-stat">
                    <div className="broker-stat-label">Closes (12mo)</div>
                    <div className="broker-stat-value">
                      {broker.sold_listings_apr2026 && broker.sold_listings_apr2026 > 0
                        ? fmtNum(broker.sold_listings_apr2026)
                        : '—'}
                    </div>
                  </div>
                  <div className="broker-stat">
                    <div className="broker-stat-label">Lifetime Sold</div>
                    <div className="broker-stat-value">
                      {fmtNum(broker.soldListingsCount)}
                    </div>
                  </div>
                </div>
                {broker.firm_key && (
                  <div className="broker-link">
                    <a href={`/broker/${slugFromFirmKey(broker.firm_key)}`}>
                      View full broker record →
                    </a>
                  </div>
                )}
              </div>
            </Section>
          )}

          {!broker && registryBroker && (
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

          <Section title="METHODOLOGY" subtitle="What we observe">
            <div className="prose">
              <p>
                DealLedger publishes what is publicly displayed. Price
                observations are recorded each time the listing is scraped.
              </p>
              {listing.quality_tier && listing.quality_tier !== 'Verified' && (
                <p>
                  Listing quality:{' '}
                  <span className="quality-tier">{listing.quality_tier}</span>{' '}
                  ({listing.quality_score}/100). This reflects metadata
                  completeness, not whether the listing exists.
                </p>
              )}
              <p>
                <a href={listing.url || '#'} target="_blank" rel="noopener noreferrer">
                  See listing →
                </a>
              </p>
            </div>
          </Section>

          <Section title="RIGHT TO RESPOND" subtitle="">
            <div className="respond">
              <p>
                If you represent this listing or {broker?.companyname || 'this broker'} and would like
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
  floor = false,
  observations,
}: {
  listedDate: string | null;
  floor?: boolean;
  observations: { label: string; date: string; price: number | null }[];
}) {
  const events: { label: string; date: string }[] = [];

  if (listedDate) {
    events.push({ label: floor ? 'Listed by' : 'First seen', date: fmtMonthYear(listedDate) });
  }

  observations.forEach((o, i) => {
    const prev = i > 0 ? observations[i - 1].price : null;
    let label = o.price != null ? fmtPrice(o.price) : 'Observed';
    if (prev != null && o.price != null && prev !== o.price) {
      label = `${fmtPrice(prev)} → ${fmtPrice(o.price)}`;
    }
    events.push({ label, date: o.label });
  });

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

function PriceChart({
  observations,
}: {
  observations: { label: string; date: string; price: number | null }[];
}) {
  const valid = observations.filter((o) => o.price != null) as {
    label: string;
    date: string;
    price: number;
  }[];

  if (valid.length < 2) return null;

  const prices = valid.map((o) => o.price);
  const max = Math.max(...prices);
  const min = Math.min(...prices);
  const range = max - min || 1;
  const yMax = max + range * 0.1;
  const yMin = Math.max(0, min - range * 0.1);
  const yRange = yMax - yMin || 1;

  const W = 720;
  const H = 240;
  const padL = 60;
  const padR = 30;
  const padT = 30;
  const padB = 40;
  const innerW = W - padL - padR;
  const innerH = H - padT - padB;

  const xs = valid.map((_, i) => padL + (innerW * i) / (valid.length - 1));
  const ys = valid.map((o) => padT + innerH - ((o.price - yMin) / yRange) * innerH);

  let pathD = `M ${xs[0]} ${ys[0]}`;
  for (let i = 1; i < valid.length; i++) {
    pathD += ` L ${xs[i]} ${ys[i - 1]}`;
    pathD += ` L ${xs[i]} ${ys[i]}`;
  }

  const ticks = [yMin, (yMin + yMax) / 2, yMax];

  return (
    <div className="chart-wrap">
      <svg viewBox={`0 0 ${W} ${H}`} className="chart-svg">
        {ticks.map((t, i) => {
          const y = padT + innerH - ((t - yMin) / yRange) * innerH;
          return (
            <g key={i}>
              <line
                x1={padL}
                x2={W - padR}
                y1={y}
                y2={y}
                stroke="var(--rule)"
                strokeWidth="1"
                strokeDasharray={i === 0 ? '' : '2 4'}
              />
              <text
                x={padL - 8}
                y={y + 4}
                textAnchor="end"
                fontSize="11"
                fontFamily="var(--mono)"
                fill="var(--ink-mute)"
              >
                {fmtPrice(t)}
              </text>
            </g>
          );
        })}

        <path d={pathD} fill="none" stroke="var(--ink-soft)" strokeWidth="2" />

        {valid.map((o, i) => (
          <g key={i}>
            <circle cx={xs[i]} cy={ys[i]} r="5" fill="var(--ink-soft)" />
            <text
              x={xs[i]}
              y={ys[i] - 12}
              textAnchor="middle"
              fontSize="12"
              fontFamily="var(--mono)"
              fontWeight="500"
              fill="var(--ink)"
            >
              {fmtPrice(o.price)}
            </text>
          </g>
        ))}

        {valid.map((o, i) => (
          <text
            key={i}
            x={xs[i]}
            y={H - 12}
            textAnchor="middle"
            fontSize="11"
            fontFamily="var(--mono)"
            fill="var(--ink-mute)"
          >
            {o.label}
          </text>
        ))}
      </svg>
      <style jsx>{`
        .chart-wrap {
          background: var(--bg-card);
          border: 1px solid var(--rule);
          padding: 16px;
        }
        .chart-svg {
          width: 100%;
          height: auto;
          display: block;
        }
      `}</style>
    </div>
  );
}

function slugFromFirmKey(firmKey: string): string {
  const value = firmKey.replace(/^(url:|name:)/, '');
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}
