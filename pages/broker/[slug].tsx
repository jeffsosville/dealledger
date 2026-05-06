// pages/broker/[slug].tsx
//
// DealLedger broker firm page — /broker/{slug}

import type { GetStaticPaths, GetStaticProps, InferGetStaticPropsType } from 'next';
import Head from 'next/head';
import { getSupabase } from '../../lib/supabase';

// ─── TYPES ─────────────────────────────────────────────────────────────────
type Firm = {
  firm_key: string;
  slug: string;
  companyname: string | null;
  companyurl: string | null;
  regions: string | null;
  state_count: number | null;
  total_people: number | null;
  principal_count: number | null;
  agent_count: number | null;
  sum_active_per_agent: number | null;
  sum_sold_per_agent: number | null;
  sum_sold_6mo_per_agent: number | null;
  company_overview: string | null;
  introduction: string | null;
  affiliations: string | null;
  services: string | null;
  telephone: string | null;
  email: string | null;
};

type ListingRow = {
  id: number;
  listing_number: number;
  header: string | null;
  price: number | null;
  state: string | null;
  city: string | null;
  category: string | null;
  days_on_market: number | null;
  quality_tier: string | null;
};

type SoldRow = {
  id: number;
  listing_number: number;
  header: string | null;
  price: number | null;
  state: string | null;
  city: string | null;
  category: string | null;
  last_seen: string | null;
};

type Person = {
  account: number;
  parentaccount: number | null;
  firstname: string | null;
  lastname: string | null;
  regionccode: string | null;
  city: string | null;
  activeListingsCount: number | null;
  soldListingsCount: number | null;
  soldlistingslastsixmonths: number | null;
};

type PageProps = {
  firm: Firm | null;
  activeListings: ListingRow[];
  soldListings: SoldRow[];
  roster: Person[];
  realActiveCount: number;
  realSoldCount: number;
  recentSoldCount: number;
};

// ─── DATA FETCHING ─────────────────────────────────────────────────────────
export const getStaticPaths: GetStaticPaths = async () => {
  // Pre-build nothing. fallback: 'blocking' resolves every URL on demand.
  // Faster deploys, no thin-page worry — every firm gets a real page.
  return { paths: [], fallback: 'blocking' };
};

export const getStaticProps: GetStaticProps<PageProps> = async ({ params }) => {
  const slug = params?.slug;
  if (typeof slug !== 'string') return { notFound: true };

  console.log('[broker] === REQUEST ===');
  console.log('[broker] slug:', slug);

  const sb = getSupabase();

  // Resolve slug → firm
  const { data: firmRow, error: firmErr } = await sb
    .from('broker_firms')
    .select('*')
    .eq('slug', slug)
    .maybeSingle();

  console.log('[broker] firm query:', firmErr ? 'ERR' : 'OK', firmRow ? firmRow.companyname : 'null');

  if (firmErr || !firmRow) return { notFound: true };
  const firm = firmRow as Firm;

  // Pull ALL active listings linked to this firm (no truncation)
  // .range(0, 9999) overrides Supabase's default 1000-row cap
  const { data: activeRows } = await sb
    .from('listings')
    .select(
      'id, listing_number, header, price, state, city, category, days_on_market, quality_tier'
    )
    .eq('firm_key', firm.firm_key)
    .eq('is_active', true)
    .order('days_on_market', { ascending: false, nullsFirst: false })
    .range(0, 9999);

  // Pull ALL inactive/no-longer-observed listings (no truncation)
  const { data: soldRows } = await sb
    .from('listings')
    .select('id, listing_number, header, price, state, city, category, last_seen')
    .eq('firm_key', firm.firm_key)
    .eq('is_active', false)
    .order('last_seen', { ascending: false, nullsFirst: false })
    .range(0, 9999);

  // ALL roster — principals first, then by sold count
  const { data: rosterRows } = await sb
    .from('broker_master')
    .select(
      'account, parentaccount, firstname, lastname, regionccode, city, "activeListingsCount", "soldListingsCount", soldlistingslastsixmonths'
    )
    .eq('firm_key', firm.firm_key)
    .order('soldListingsCount', { ascending: false, nullsFirst: false })
    .range(0, 9999);

  // Real counts (ground truth from listings table, not broker_master sums)
  const { count: realActive } = await sb
    .from('listings')
    .select('listing_number', { count: 'exact', head: true })
    .eq('firm_key', firm.firm_key)
    .eq('is_active', true);

  const { count: realSold } = await sb
    .from('listings')
    .select('listing_number', { count: 'exact', head: true })
    .eq('firm_key', firm.firm_key)
    .eq('is_active', false);

  // "Recent sold" — listings that went inactive in the last 180 days
  const sixMonthsAgo = new Date();
  sixMonthsAgo.setDate(sixMonthsAgo.getDate() - 180);
  const { count: recentSold } = await sb
    .from('listings')
    .select('listing_number', { count: 'exact', head: true })
    .eq('firm_key', firm.firm_key)
    .eq('is_active', false)
    .gte('last_seen', sixMonthsAgo.toISOString());

  console.log('[broker] counts:', { realActive, realSold, recentSold });
  console.log('[broker] === SUCCESS ===');

  return {
    props: {
      firm,
      activeListings: (activeRows || []) as ListingRow[],
      soldListings: (soldRows || []) as SoldRow[],
      roster: (rosterRows || []) as Person[],
      realActiveCount: realActive ?? 0,
      realSoldCount: realSold ?? 0,
      recentSoldCount: recentSold ?? 0,
    },
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

const fmtNum = (n: number | null | undefined) =>
  n == null ? '—' : new Intl.NumberFormat('en-US').format(n);

const fmtDateShort = (s: string | null | undefined) => {
  if (!s) return '—';
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
};

const padDLId = (id: number) => `DL-${String(id).padStart(8, '0')}`;

const stripUrlPrefix = (url: string | null) => {
  if (!url) return null;
  return url.replace(/^https?:\/\/(www\.)?/, '').replace(/\/$/, '');
};

const personName = (p: Person) => {
  const f = (p.firstname || '').trim();
  const l = (p.lastname || '').trim();
  // Filter out junk patterns we saw in the data
  if (l === '#NAME?' || l.match(/^Lic\s/i) || l.match(/^[A-Z]{2}#/)) return f;
  return [f, l].filter(Boolean).join(' ') || '—';
};

// ─── PAGE ──────────────────────────────────────────────────────────────────
export default function BrokerPage({
  firm,
  activeListings,
  soldListings,
  roster,
  realActiveCount,
  realSoldCount,
  recentSoldCount,
}: InferGetStaticPropsType<typeof getStaticProps>) {
  if (!firm) return null;

  const isSolo = (firm.total_people ?? 0) === 1;
  const isMultiState = (firm.state_count ?? 0) >= 2;

  const today = new Date().toLocaleDateString('en-US', {
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  });

  const cleanUrl = stripUrlPrefix(firm.companyurl);
  const subtitleParts: string[] = [];
  if (firm.regions) subtitleParts.push(firm.regions);
  if (isSolo && roster[0]) {
    const nm = personName(roster[0]);
    if (nm !== '—' && nm !== firm.companyname) subtitleParts.push(nm);
  } else if (firm.total_people && firm.total_people > 1) {
    subtitleParts.push(
      `${firm.total_people} brokers${isMultiState ? `, ${firm.state_count} states` : ''}`
    );
  }

  const eyebrow = isSolo ? 'BROKER' : isMultiState ? 'BROKERAGE FIRM' : 'BROKERAGE';

  const pageTitle = `${firm.companyname || 'Broker'} — DealLedger`;
  const metaDescription = `Public record of ${firm.companyname || 'broker'}: ${realActiveCount} active listings, ${realSoldCount} previously observed listings.`;

  return (
    <>
      <Head>
        <title>{pageTitle}</title>
        <meta name="description" content={metaDescription} />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="" />
        <link
          rel="stylesheet"
          href="https://fonts.googleapis.com/css2?family=Lora:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap"
        />
      </Head>

      <style jsx global>{`
        :root {
          --bg: #faf8f4;
          --bg-card: #f4f0e8;
          --ink: #1a1612;
          --ink-soft: #4a4036;
          --ink-mute: #8a7e6e;
          --rule: #d8d0c0;
          --accent: #b7361a;
          --link: #1a1612;
          --serif: 'Lora', Georgia, serif;
          --mono: 'JetBrains Mono', ui-monospace, monospace;
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
            <div className="masthead-meta">PUBLIC RECORD · {today.toUpperCase()}</div>
          </div>
          <div className="masthead-rule" />
        </header>

        <main className="content">
          <div className="eyebrow">— {eyebrow}</div>
          <h1 className="headline">{firm.companyname || firm.firm_key}</h1>
          {subtitleParts.length > 0 && (
            <p className="subtitle">{subtitleParts.join(' · ')}</p>
          )}

          {cleanUrl && (
            <p className="firm-url">
              <a href={firm.companyurl || '#'} target="_blank" rel="noopener noreferrer nofollow">
                {cleanUrl} →
              </a>
            </p>
          )}

          {/* ─── STATS ROW ─────────────────────────────────────────── */}
          <div className="stats">
            <Stat
              label="Active Listings"
              value={fmtNum(realActiveCount)}
              sub={
                realActiveCount > 0
                  ? `Currently observed`
                  : 'No active listings'
              }
            />
            <Stat
              label="Last 6mo Activity"
              value={fmtNum(recentSoldCount)}
              sub={
                recentSoldCount > 0
                  ? 'No longer observed'
                  : 'No recent change'
              }
            />
            <Stat
              label="Lifetime Observed"
              value={fmtNum(realSoldCount)}
              sub="Inactive listings tracked"
            />
            <Stat
              label={isSolo ? 'Type' : 'Brokers'}
              value={isSolo ? 'Solo' : fmtNum(firm.total_people)}
              sub={
                isSolo
                  ? 'Single broker'
                  : isMultiState
                  ? `${firm.state_count} states`
                  : firm.regions || ''
              }
            />
          </div>

          {/* ─── ABOUT ─────────────────────────────────────────────── */}
          {(firm.company_overview || firm.introduction) && (
            <Section title="ABOUT" subtitle={`About ${firm.companyname || 'this firm'}`}>
              <div className="prose">
                {firm.company_overview && (
                  <div className="bio">
                    {firm.company_overview.split(/\n\n+/).map((p, i) => (
                      <p key={i}>{p}</p>
                    ))}
                  </div>
                )}
                {firm.introduction && firm.introduction !== firm.company_overview && (
                  <div className="bio">
                    {firm.introduction.split(/\n\n+/).map((p, i) => (
                      <p key={i}>{p}</p>
                    ))}
                  </div>
                )}
              </div>
            </Section>
          )}

          {/* ─── AFFILIATIONS / SERVICES ───────────────────────────── */}
          {(firm.affiliations || firm.services) && (
            <Section title="CREDENTIALS" subtitle="Affiliations & services">
              <div className="creds">
                {firm.affiliations && (
                  <div className="cred-block">
                    <div className="cred-label">Affiliations</div>
                    <div className="cred-text">{firm.affiliations}</div>
                  </div>
                )}
                {firm.services && (
                  <div className="cred-block">
                    <div className="cred-label">Services</div>
                    <div className="cred-text">{firm.services}</div>
                  </div>
                )}
              </div>
            </Section>
          )}

          {/* ─── ACTIVE LISTINGS ───────────────────────────────────── */}
          {activeListings.length > 0 && (
            <Section
              title="ACTIVE LISTINGS"
              subtitle={`${fmtNum(activeListings.length)} listing${activeListings.length === 1 ? '' : 's'}`}
            >
              <ListingTable rows={activeListings} kind="active" />
            </Section>
          )}

          {/* ─── ROSTER (skip for solos) ───────────────────────────── */}
          {!isSolo && roster.length > 0 && (
            <Section
              title="BROKERS AT THIS FIRM"
              subtitle={`${roster.length} ${roster.length === 1 ? 'broker' : 'brokers'} on record`}
            >
              <RosterTable rows={roster} />
            </Section>
          )}

          {/* ─── PREVIOUSLY OBSERVED ───────────────────────────────── */}
          {soldListings.length > 0 && (
            <Section
              title="PREVIOUSLY OBSERVED"
              subtitle={`${fmtNum(soldListings.length)} listing${soldListings.length === 1 ? '' : 's'} no longer in latest scrape`}
            >
              <SoldTable rows={soldListings} />
              <p className="caveat">
                These listings appeared in past scrapes and were not present in
                our most recent refresh. They may still be active elsewhere;
                "no longer observed" is not the same as "sold."
              </p>
            </Section>
          )}

          {/* ─── METHODOLOGY ───────────────────────────────────────── */}
          <Section title="METHODOLOGY" subtitle="What we observe">
            <div className="prose">
              <p>
                DealLedger publishes what is publicly displayed. Broker records
                aggregate every observed listing tied to this firm across our
                sources. Counts above reflect distinct listings observed, not
                closed transactions confirmed by the broker.
              </p>
              <p>
                We do not have direct visibility into which listings sold. When
                a listing stops appearing in our scrapes, we mark it as no
                longer observed — but we do not claim it sold. Some close;
                some withdraw; some are re-listed under a new ID; some are
                simply missed by our scraper. We surface the observation; you
                draw the conclusion.
              </p>
            </div>
          </Section>

          {/* ─── RIGHT TO RESPOND ──────────────────────────────────── */}
          <Section title="RIGHT TO RESPOND" subtitle="">
            <div className="respond">
              <p>
                If you represent {firm.companyname || 'this firm'} and would
                like to add context to this record — corrections, updated bio,
                additional credentials — contact{' '}
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
              An open public registry of business brokerage activity in the U.S.
              lower middle market.
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
          margin: 0 0 8px 0;
        }
        .firm-url {
          font-family: var(--mono);
          font-size: 13px;
          margin: 0 0 40px 0;
        }
        .firm-url a {
          color: var(--ink-soft);
          text-decoration: none;
        }
        .firm-url a:hover { color: var(--accent); text-decoration: underline; }

        .stats {
          display: grid;
          grid-template-columns: repeat(4, 1fr);
          gap: 0;
          background: var(--bg-card);
          border: 1px solid var(--rule);
          margin-bottom: 56px;
        }

        .prose p {
          margin: 0 0 14px 0;
          color: var(--ink-soft);
          max-width: 64ch;
        }
        .prose p:last-child { margin-bottom: 0; }
        .bio { margin-bottom: 16px; }
        .bio:last-child { margin-bottom: 0; }

        .creds {
          background: var(--bg-card);
          border: 1px solid var(--rule);
          padding: 22px 24px;
        }
        .cred-block { margin-bottom: 16px; }
        .cred-block:last-child { margin-bottom: 0; }
        .cred-label {
          font-family: var(--mono);
          font-size: 10px;
          letter-spacing: 0.1em;
          text-transform: uppercase;
          color: var(--ink-mute);
          margin-bottom: 6px;
        }
        .cred-text {
          color: var(--ink-soft);
          white-space: pre-wrap;
        }

        .caveat {
          font-size: 13px;
          color: var(--ink-mute);
          margin-top: 16px;
          margin-bottom: 0;
          max-width: 64ch;
          line-height: 1.6;
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
        .footer-rule { border-top: 1px solid var(--rule); margin-bottom: 20px; }
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
}: {
  label: string;
  value: string;
  sub: string;
}) {
  return (
    <div className="stat">
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
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
        .stat-sub {
          font-size: 13px;
          color: var(--ink-soft);
        }
        @media (max-width: 720px) {
          .stat:nth-child(2) { border-right: none; }
          .stat:nth-child(1), .stat:nth-child(2) {
            border-bottom: 1px solid var(--rule);
          }
          .stat-value { font-size: 28px; }
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

function ListingTable({
  rows,
  kind,
}: {
  rows: ListingRow[];
  kind: 'active';
}) {
  return (
    <div className="lt-wrap">
      <div className="lt-head">
        <div className="lt-cell lt-cell-title">Listing</div>
        <div className="lt-cell lt-cell-loc">Location</div>
        <div className="lt-cell lt-cell-price">Price</div>
        <div className="lt-cell lt-cell-dom">DOM</div>
      </div>
      {rows.map((r) => (
        <a key={r.listing_number} className="lt-row" href={`/listing/${r.listing_number}`}>
          <div className="lt-cell lt-cell-title">
            <div className="lt-title">{r.header || padDLId(r.id)}</div>
            {r.category && <div className="lt-cat">{r.category}</div>}
          </div>
          <div className="lt-cell lt-cell-loc">
            {[r.city, r.state].filter(Boolean).join(', ') || '—'}
          </div>
          <div className="lt-cell lt-cell-price">{fmtPrice(r.price)}</div>
          <div className="lt-cell lt-cell-dom">
            {r.days_on_market != null ? r.days_on_market : '—'}
          </div>
        </a>
      ))}
      <style jsx>{`
        .lt-wrap {
          background: var(--bg-card);
          border: 1px solid var(--rule);
          font-size: 14px;
        }
        .lt-head, .lt-row {
          display: grid;
          grid-template-columns: 1fr 160px 100px 60px;
          gap: 16px;
          padding: 12px 20px;
          border-bottom: 1px solid var(--rule);
          align-items: center;
        }
        .lt-row:last-child { border-bottom: none; }
        .lt-row {
          text-decoration: none;
          color: var(--ink);
          transition: background 0.1s;
        }
        .lt-row:hover { background: rgba(183, 54, 26, 0.04); }
        .lt-head {
          font-family: var(--mono);
          font-size: 10px;
          letter-spacing: 0.08em;
          text-transform: uppercase;
          color: var(--ink-mute);
        }
        .lt-cell-price, .lt-cell-dom {
          font-family: var(--mono);
          text-align: right;
        }
        .lt-title { line-height: 1.3; }
        .lt-cat {
          font-family: var(--mono);
          font-size: 10px;
          letter-spacing: 0.06em;
          color: var(--ink-mute);
          text-transform: uppercase;
          margin-top: 3px;
        }
        @media (max-width: 720px) {
          .lt-head, .lt-row {
            grid-template-columns: 1fr 80px;
            gap: 8px;
            padding: 10px 14px;
          }
          .lt-cell-loc, .lt-cell-dom { display: none; }
        }
      `}</style>
    </div>
  );
}

function SoldTable({ rows }: { rows: SoldRow[] }) {
  return (
    <div className="st-wrap">
      <div className="st-head">
        <div className="st-cell st-cell-title">Listing</div>
        <div className="st-cell st-cell-loc">Location</div>
        <div className="st-cell st-cell-price">Last Asking</div>
        <div className="st-cell st-cell-date">Last Seen</div>
      </div>
      {rows.map((r) => (
        <a key={r.listing_number} className="st-row" href={`/listing/${r.listing_number}`}>
          <div className="st-cell st-cell-title">
            <div className="st-title">{r.header || padDLId(r.id)}</div>
            {r.category && <div className="st-cat">{r.category}</div>}
          </div>
          <div className="st-cell st-cell-loc">
            {[r.city, r.state].filter(Boolean).join(', ') || '—'}
          </div>
          <div className="st-cell st-cell-price">{fmtPrice(r.price)}</div>
          <div className="st-cell st-cell-date">{fmtDateShort(r.last_seen)}</div>
        </a>
      ))}
      <style jsx>{`
        .st-wrap {
          background: var(--bg-card);
          border: 1px solid var(--rule);
          font-size: 14px;
        }
        .st-head, .st-row {
          display: grid;
          grid-template-columns: 1fr 160px 110px 90px;
          gap: 16px;
          padding: 12px 20px;
          border-bottom: 1px solid var(--rule);
          align-items: center;
        }
        .st-row:last-child { border-bottom: none; }
        .st-row {
          text-decoration: none;
          color: var(--ink);
          transition: background 0.1s;
        }
        .st-row:hover { background: rgba(183, 54, 26, 0.04); }
        .st-head {
          font-family: var(--mono);
          font-size: 10px;
          letter-spacing: 0.08em;
          text-transform: uppercase;
          color: var(--ink-mute);
        }
        .st-cell-price, .st-cell-date {
          font-family: var(--mono);
          text-align: right;
        }
        .st-title { line-height: 1.3; }
        .st-cat {
          font-family: var(--mono);
          font-size: 10px;
          letter-spacing: 0.06em;
          color: var(--ink-mute);
          text-transform: uppercase;
          margin-top: 3px;
        }
        @media (max-width: 720px) {
          .st-head, .st-row {
            grid-template-columns: 1fr 90px;
            gap: 8px;
            padding: 10px 14px;
          }
          .st-cell-loc, .st-cell-date { display: none; }
        }
      `}</style>
    </div>
  );
}

function RosterTable({ rows }: { rows: Person[] }) {
  return (
    <div className="rt-wrap">
      <div className="rt-head">
        <div className="rt-cell rt-cell-name">Broker</div>
        <div className="rt-cell rt-cell-loc">Location</div>
        <div className="rt-cell rt-cell-active">Active</div>
        <div className="rt-cell rt-cell-sold">Sold (Lifetime)</div>
      </div>
      {rows.map((p) => (
        <div key={p.account} className="rt-row">
          <div className="rt-cell rt-cell-name">
            <div className="rt-name">{personName(p)}</div>
            {p.parentaccount === 0 && <div className="rt-tag">Principal</div>}
          </div>
          <div className="rt-cell rt-cell-loc">
            {[p.city, p.regionccode].filter(Boolean).join(', ') || '—'}
          </div>
          <div className="rt-cell rt-cell-active">{fmtNum(p.activeListingsCount)}</div>
          <div className="rt-cell rt-cell-sold">{fmtNum(p.soldListingsCount)}</div>
        </div>
      ))}
      <style jsx>{`
        .rt-wrap {
          background: var(--bg-card);
          border: 1px solid var(--rule);
          font-size: 14px;
        }
        .rt-head, .rt-row {
          display: grid;
          grid-template-columns: 1fr 180px 80px 110px;
          gap: 16px;
          padding: 12px 20px;
          border-bottom: 1px solid var(--rule);
          align-items: center;
        }
        .rt-row:last-child { border-bottom: none; }
        .rt-head {
          font-family: var(--mono);
          font-size: 10px;
          letter-spacing: 0.08em;
          text-transform: uppercase;
          color: var(--ink-mute);
        }
        .rt-cell-active, .rt-cell-sold {
          font-family: var(--mono);
          text-align: right;
        }
        .rt-name { line-height: 1.3; }
        .rt-tag {
          display: inline-block;
          font-family: var(--mono);
          font-size: 9px;
          letter-spacing: 0.08em;
          text-transform: uppercase;
          color: var(--accent);
          margin-top: 3px;
        }
        @media (max-width: 720px) {
          .rt-head, .rt-row {
            grid-template-columns: 1fr 70px 90px;
            gap: 8px;
            padding: 10px 14px;
          }
          .rt-cell-loc { display: none; }
        }
      `}</style>
    </div>
  );
}
