// pages/broker/[slug].tsx
//
// DealLedger broker page — /broker/{slug}
//
// One page per broker website, built only from listings DealLedger has
// observed on that broker's own site. Data: `broker_directory` (one row per
// site) plus the matching rows in `listings`. Slugs are the site's domain
// with punctuation turned into hyphens (vestedbb.com → vestedbb-com).
//
// Older firm slugs from the retired registry are redirected here when the
// firm's website matches a broker we observe; otherwise they return 404.

import type { GetStaticPaths, GetStaticProps, InferGetStaticPropsType } from 'next';
import Head from 'next/head';
import { getSupabase } from '../../lib/supabase';
import {
  REGISTRY_COLUMNS,
  REGISTRY_VIEW,
  OBSERVATION_START_LABEL,
  RegistryRow,
  bareHost,
  cleanStates,
  displayLocation,
  displayName,
  fmtDate,
  fmtNum,
  fmtPrice,
  isFloored,
  isMarketplaceDomain,
} from '../../lib/brokerRegistry';

const ACTIVE_LIMIT = 300;
const REMOVED_LIMIT = 50;

type ListingRow = {
  listing_number: number;
  header: string | null;
  price: number | null;
  cash_flow: number | null;
  city: string | null;
  state: string | null;
  url: string | null;
  first_seen: string | null;
  last_seen: string | null;
};

type PageProps = {
  firm: RegistryRow;
  active: ListingRow[];
  removed: ListingRow[];
  removedShown: number;
};

const LISTING_COLUMNS = 'listing_number, header, price, cash_flow, city, state, url, first_seen, last_seen';

// Listing URLs on this exact host, with or without www.
const hostFilter = (domain: string) =>
  [`url.ilike.*://${domain}/*`, `url.ilike.*://www.${domain}/*`, `url.eq.https://${domain}`, `url.eq.https://www.${domain}`].join(',');

const slugFromHost = (url: string | null) => {
  const h = bareHost(url)?.split('/')[0]?.toLowerCase();
  return h ? h.replace(/[^a-z0-9]+/g, '-') : null;
};

export const getStaticPaths: GetStaticPaths = async () => ({ paths: [], fallback: 'blocking' });

export const getStaticProps: GetStaticProps<PageProps> = async ({ params }) => {
  const slug = typeof params?.slug === 'string' ? params.slug.toLowerCase() : null;
  if (!slug) return { notFound: true };

  const sb = getSupabase();

  const { data: firmRow } = await sb
    .from(REGISTRY_VIEW)
    .select(REGISTRY_COLUMNS)
    .eq('slug', slug)
    .maybeSingle();

  if (!firmRow) {
    // Retired registry slug? Send it to the broker's current page if we can match its website.
    const { data: legacy } = await sb
      .from('broker_firms')
      .select('companyurl')
      .eq('slug', slug)
      .maybeSingle();
    const target = slugFromHost(legacy?.companyurl ?? null);
    if (target && target !== slug) {
      const { data: hit } = await sb.from(REGISTRY_VIEW).select('slug').eq('slug', target).maybeSingle();
      if (hit) return { redirect: { destination: `/broker/${hit.slug}`, permanent: true } };
    }
    return { notFound: true, revalidate: 60 * 60 * 24 };
  }

  const firm = firmRow as unknown as RegistryRow;
  if (isMarketplaceDomain(firm.domain)) return { notFound: true, revalidate: 60 * 60 * 24 };

  const { data: activeRows } = await sb
    .from('listings')
    .select(LISTING_COLUMNS)
    .eq('source', 'broker_direct')
    .eq('is_active', true)
    .or(hostFilter(firm.domain))
    .order('first_seen', { ascending: false, nullsFirst: false })
    .range(0, ACTIVE_LIMIT - 1);

  const since = new Date(Date.now() - 180 * 86400 * 1000).toISOString();
  const { data: removedRows } = await sb
    .from('listings')
    .select(LISTING_COLUMNS)
    .eq('source', 'broker_direct')
    .eq('is_active', false)
    .gte('last_seen', since)
    .or(hostFilter(firm.domain))
    .order('last_seen', { ascending: false, nullsFirst: false })
    .range(0, REMOVED_LIMIT - 1);

  return {
    props: {
      firm,
      active: (activeRows || []) as ListingRow[],
      removed: (removedRows || []) as ListingRow[],
      removedShown: (removedRows || []).length,
    },
    revalidate: 60 * 60 * 6,
  };
};

const place = (r: { city: string | null; state: string | null }) => {
  // Broker sites often put labels into the city field ("Location Brooklyn", "of Boston").
  const raw = (r.city || '').replace(/^\s*(location|city|of)[:\s]+/i, '').trim() || null;
  const c = raw && raw === raw.toLowerCase() ? raw.replace(/\b([a-z])/g, (m) => m.toUpperCase()) : raw;
  return [c, r.state?.toUpperCase()].filter(Boolean).join(', ') || '—';
};

export default function BrokerPage({ firm, active, removed, removedShown }: InferGetStaticPropsType<typeof getStaticProps>) {
  const name = displayName(firm.firm_name, firm.domain);
  const pageUrl = `https://dealledger.org/broker/${firm.slug}`;
  const site = firm.homepage_url || `https://${firm.domain}`;
  const states = cleanStates(firm.states_listed);
  const location = displayLocation(firm);

  const descBits = [
    `${fmtNum(firm.active_count)} business${firm.active_count === 1 ? '' : 'es'} for sale currently listed on ${firm.domain}`,
    firm.median_asking ? `median asking price ${fmtPrice(firm.median_asking)}` : null,
    `${fmtNum(firm.lifetime_count)} listings observed since ${fmtDate(firm.first_observed)}`,
  ].filter(Boolean);
  const metaDescription = `${name}${firm.is_network ? '' : location !== '—' ? ` (${location})` : ''}: ${descBits.join('; ')}. Public record from DealLedger.`;
  const pageTitle = `${name} — businesses for sale & listing history | DealLedger`;
  const thin = firm.active_count === 0 && firm.lifetime_count < 3;

  const activeHidden = Math.max(0, firm.active_count - active.length);
  const removedHidden = Math.max(0, firm.removed_180d - removedShown);

  return (
    <>
      <Head>
        <title>{pageTitle}</title>
        <meta name="description" content={metaDescription} />
        <link rel="canonical" href={pageUrl} />
        {thin && <meta name="robots" content="noindex, follow" />}
        <meta property="og:type" content="profile" />
        <meta property="og:title" content={`${name} — DealLedger`} />
        <meta property="og:description" content={metaDescription} />
        <meta property="og:url" content={pageUrl} />
        <meta property="og:site_name" content="DealLedger" />
        <meta name="twitter:card" content="summary" />
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{
            __html: JSON.stringify({
              '@context': 'https://schema.org',
              '@type': 'Organization',
              '@id': `${pageUrl}#firm`,
              name,
              url: site,
              sameAs: [site],
              ...(firm.hq_city && firm.hq_state && !firm.is_network
                ? { address: { '@type': 'PostalAddress', addressLocality: displayLocation(firm).split(',')[0], addressRegion: firm.hq_state, addressCountry: 'US' } }
                : {}),
              subjectOf: {
                '@type': 'Dataset',
                name: `DealLedger observations: ${name}`,
                url: pageUrl,
                description: metaDescription,
                license: 'https://creativecommons.org/publicdomain/zero/1.0/',
                isAccessibleForFree: true,
                isPartOf: { '@type': 'Dataset', '@id': 'https://dealledger.org/#dataset' },
              },
            }),
          }}
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
        a { color: var(--link); text-decoration: underline; text-underline-offset: 2px; }
        a:hover { color: var(--accent); }
      `}</style>

      <div className="page">
        <header className="masthead">
          <div className="masthead-inner">
            <a href="/" className="brand">DealLedger</a>
            <a href="/brokers" className="masthead-meta">← BROKER REGISTRY</a>
          </div>
          <div className="masthead-rule" />
        </header>

        <main className="content">
          <div className="eyebrow">— {firm.is_network ? 'BROKERAGE NETWORK' : 'BUSINESS BROKER'}</div>
          <h1 className="headline">{name}</h1>
          <p className="subtitle">
            {[
              firm.is_network ? 'National network, shown under its main website' : location !== '—' ? location : null,
              firm.ibba_member ? 'IBBA member' : null,
            ]
              .filter(Boolean)
              .join(' · ')}
          </p>
          <p className="firm-url">
            <a href={site} target="_blank" rel="noopener">{firm.domain} →</a>
            {firm.listings_page_url && firm.listings_page_url !== site && (
              <>
                {' '}·{' '}
                <a href={firm.listings_page_url} target="_blank" rel="noopener">their listings page →</a>
              </>
            )}
          </p>

          {firm.about && (
            <div className="about">
              <p>{firm.about}</p>
              {firm.about_source && (
                <p className="about-source">
                  From <a href={firm.about_source} target="_blank" rel="noopener">{bareHost(firm.about_source)}</a>
                </p>
              )}
            </div>
          )}

          <div className="stats">
            <div className="stat">
              <div className="stat-label">Active listings</div>
              <div className="stat-value">{fmtNum(firm.active_count)}</div>
              <div className="stat-sub">On their site in our latest collection</div>
            </div>
            <div className="stat">
              <div className="stat-label">Median asking</div>
              <div className="stat-value">{fmtPrice(firm.median_asking)}</div>
              <div className="stat-sub">Active listings with a stated price</div>
            </div>
            <div className="stat">
              <div className="stat-label">Removed, 180 days</div>
              <div className="stat-value">{fmtNum(firm.removed_180d)}</div>
              <div className="stat-sub">Sold, under contract or withdrawn</div>
            </div>
            <div className="stat">
              <div className="stat-label">Lifetime observed</div>
              <div className="stat-value">{fmtNum(firm.lifetime_count)}</div>
              <div className="stat-sub">Reading their site since {fmtDate(firm.tracking_since || firm.first_observed)}</div>
            </div>
          </div>

          {states.length > 1 && (
            <p className="note">Active listings in {states.length} states: {states.join(', ')}.</p>
          )}

          <section className="section">
            <div className="section-eyebrow">— FOR SALE NOW</div>
            <h2 className="section-title">
              Businesses currently listed by {name}
            </h2>
            <p className="note">
              “First seen” is the first day the listing appeared on {firm.domain} in our collection.
              Dates marked “by” are for listings that were already up when we started reading
              the site, so they were listed on or before that date.
            </p>
            {active.length === 0 ? (
              <p className="empty">No listings visible on their site in our latest collection.</p>
            ) : (
              <div className="table">
                <div className="t-head">
                  <div>Business</div>
                  <div>Location</div>
                  <div className="num">Asking</div>
                  <div className="num">Cash flow</div>
                  <div className="num">First seen</div>
                </div>
                {active.map((l) => (
                  <div className="t-row" key={l.listing_number}>
                    <div className="t-title">
                      <a href={`/listing/${l.listing_number}`}>{l.header || 'Untitled listing'}</a>
                      {l.url && (
                        <a className="src" href={l.url} target="_blank" rel="noopener">source ↗</a>
                      )}
                    </div>
                    <div className="t-loc">{place(l)}</div>
                    <div className="num">{fmtPrice(l.price)}</div>
                    <div className="num">{fmtPrice(l.cash_flow)}</div>
                    <div className="num t-date">
                      {isFloored(l.first_seen, firm.tracking_since) ? `by ${fmtDate(l.first_seen)}` : fmtDate(l.first_seen)}
                    </div>
                  </div>
                ))}
              </div>
            )}
            {activeHidden > 0 && (
              <p className="more">
                Showing the {fmtNum(active.length)} most recently listed. See all {fmtNum(firm.active_count)} on{' '}
                <a href={firm.listings_page_url || site} target="_blank" rel="noopener">{firm.domain}</a>.
              </p>
            )}
          </section>

          {removed.length > 0 && (
            <section className="section">
              <div className="section-eyebrow">— RECENTLY REMOVED</div>
              <h2 className="section-title">No longer on their site (last 180 days)</h2>
              <p className="note">
                Removal is not proof of a sale. A listing can come down because it sold, went under
                contract, or was withdrawn.
              </p>
              <div className="table">
                <div className="t-head">
                  <div>Business</div>
                  <div>Location</div>
                  <div className="num">Last asking</div>
                  <div className="num">First seen</div>
                  <div className="num">Last seen</div>
                </div>
                {removed.map((l) => (
                  <div className="t-row" key={l.listing_number}>
                    <div className="t-title">
                      <a href={`/listing/${l.listing_number}`}>{l.header || 'Untitled listing'}</a>
                    </div>
                    <div className="t-loc">{place(l)}</div>
                    <div className="num">{fmtPrice(l.price)}</div>
                    <div className="num t-date">
                      {isFloored(l.first_seen, firm.tracking_since) ? `by ${fmtDate(l.first_seen)}` : fmtDate(l.first_seen)}
                    </div>
                    <div className="num t-date">{fmtDate(l.last_seen)}</div>
                  </div>
                ))}
              </div>
              {removedHidden > 0 && (
                <p className="more">Plus {fmtNum(removedHidden)} more removed in the same period.</p>
              )}
            </section>
          )}

          <section className="section claim">
            <div className="section-eyebrow">— IS THIS YOUR FIRM?</div>
            <p>
              This page is built from the listings {name} publishes on its own website. It’s free,
              there’s nothing to sign up for, and buyers who find a listing here go straight to your
              site. If something is wrong, a listings page is missing, or you’d like a short
              description of your firm shown here, email{' '}
              <a href={`mailto:info@dealledger.org?subject=${encodeURIComponent(`Broker page: ${firm.domain}`)}`}>
                info@dealledger.org
              </a>
              .
            </p>
            <p className="link-hint">
              Want to point clients to your public record? Link to <code>{pageUrl}</code>
            </p>
          </section>

          <section className="section">
            <div className="section-eyebrow">— METHODOLOGY</div>
            <div className="prose">
              <p>
                DealLedger reads the public listings pages of business brokers and records when each
                listing first appears, when its details change, and when it comes down. Nothing here
                comes from behind a login. Collection from broker websites began on{' '}
                {OBSERVATION_START_LABEL}; a listing first seen that day may have been listed earlier.
                Data last updated {fmtDate(firm.refreshed_at)}. All data is public domain (CC0).{' '}
                <a href="/methodology.html">Full methodology</a>.
              </p>
            </div>
          </section>

          <footer className="footer">
            <div className="footer-rule" />
            <div className="footer-text">
              DealLedger · Public record · CC0 ·{' '}
              <a href="/brokers">All brokers</a>
            </div>
          </footer>
        </main>
      </div>

      <style jsx>{`
        .page { min-height: 100vh; }
        .masthead { padding: 24px 0 0 0; }
        .masthead-inner {
          max-width: 1100px; margin: 0 auto; padding: 0 32px 18px 32px;
          display: flex; justify-content: space-between; align-items: baseline;
        }
        .brand { font-weight: 700; font-size: 22px; text-decoration: none; letter-spacing: -0.01em; }
        .masthead-meta {
          font-family: var(--mono); font-size: 11px; letter-spacing: 0.08em;
          color: var(--ink-mute); text-decoration: none;
        }
        .masthead-rule { max-width: 1100px; margin: 0 auto; border-top: 1px solid var(--rule); }
        .content { max-width: 1100px; margin: 0 auto; padding: 56px 32px 96px 32px; }
        .eyebrow {
          font-family: var(--mono); font-size: 12px; letter-spacing: 0.1em;
          color: var(--accent); margin-bottom: 14px;
        }
        .headline {
          font-weight: 500; font-size: 44px; line-height: 1.1;
          letter-spacing: -0.015em; margin: 0 0 10px 0;
        }
        .subtitle { font-size: 17px; color: var(--ink-soft); margin: 0 0 6px 0; }
        .firm-url { font-family: var(--mono); font-size: 14px; margin: 0 0 32px 0; overflow-wrap: anywhere; }
        .about {
          border-left: 2px solid var(--rule); padding: 2px 0 2px 16px;
          margin: 0 0 32px 0; max-width: 70ch; color: var(--ink-soft);
        }
        .about p { margin: 0 0 6px 0; }
        .about-source { font-size: 13px; color: var(--ink-mute); }
        .stats {
          display: grid; grid-template-columns: repeat(4, 1fr);
          border: 1px solid var(--rule); background: var(--bg-card); margin-bottom: 20px;
        }
        .stat { padding: 18px 20px; border-right: 1px solid var(--rule); }
        .stat:last-child { border-right: none; }
        .stat-label {
          font-family: var(--mono); font-size: 10px; letter-spacing: 0.1em;
          text-transform: uppercase; color: var(--ink-mute);
        }
        .stat-value { font-family: var(--mono); font-size: 28px; margin: 6px 0 2px 0; }
        .stat-sub { font-size: 12px; color: var(--ink-mute); }
        .note { font-size: 14px; color: var(--ink-soft); max-width: 75ch; margin: 0 0 12px 0; }
        .note strong { color: var(--ink); }
        .section { margin-top: 56px; }
        .section-eyebrow {
          font-family: var(--mono); font-size: 11px; letter-spacing: 0.14em;
          color: var(--accent); margin-bottom: 8px;
        }
        .section-title {
          font-weight: 500; font-size: 24px; line-height: 1.2;
          letter-spacing: -0.01em; margin: 0 0 16px 0;
        }
        .table { border: 1px solid var(--rule); background: var(--bg-card); font-size: 14px; }
        .t-head, .t-row {
          display: grid; grid-template-columns: 1fr 160px 90px 90px 110px;
          gap: 14px; padding: 10px 18px; border-bottom: 1px solid var(--rule); align-items: baseline;
        }
        .t-row:last-child { border-bottom: none; }
        .t-head {
          font-family: var(--mono); font-size: 10px; letter-spacing: 0.08em;
          text-transform: uppercase; color: var(--ink-mute);
        }
        .t-title a { color: var(--ink); text-decoration: none; }
        .t-title a:hover { color: var(--accent); text-decoration: underline; }
        .t-title .src {
          font-family: var(--mono); font-size: 11px; color: var(--link);
          margin-left: 8px; white-space: nowrap;
        }
        .t-loc { font-size: 13px; color: var(--ink-soft); }
        .num { font-family: var(--mono); text-align: right; }
        .t-date { font-size: 12px; color: var(--ink-soft); }
        .empty, .more { font-size: 14px; color: var(--ink-mute); margin: 12px 0 0 0; }
        .claim {
          border: 1px solid var(--rule); padding: 20px 24px; background: var(--bg-card); max-width: 80ch;
        }
        .claim p { margin: 0 0 10px 0; color: var(--ink-soft); }
        .link-hint { font-size: 13px; }
        .link-hint code { font-family: var(--mono); font-size: 12px; overflow-wrap: anywhere; }
        .prose p { margin: 0; color: var(--ink-soft); max-width: 75ch; font-size: 14px; }
        .footer { margin-top: 80px; }
        .footer-rule { border-top: 1px solid var(--rule); margin-bottom: 20px; }
        .footer-text { font-family: var(--mono); font-size: 11px; color: var(--ink-mute); }
        @media (max-width: 900px) {
          .stats { grid-template-columns: repeat(2, 1fr); }
          .stat:nth-child(2) { border-right: none; }
          .stat:nth-child(-n + 2) { border-bottom: 1px solid var(--rule); }
          .t-head, .t-row { grid-template-columns: 1fr 80px 96px; padding: 10px 14px; }
          .t-head > :nth-child(2), .t-row > :nth-child(2),
          .t-head > :nth-child(4), .t-row > :nth-child(4) { display: none; }
        }
        @media (max-width: 720px) {
          .headline { font-size: 32px; }
          .content { padding: 40px 16px 64px 16px; }
          .masthead-inner { padding: 0 16px 18px 16px; }
        }
      `}</style>
    </>
  );
}
