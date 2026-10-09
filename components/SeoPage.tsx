// components/SeoPage.tsx — shared layout for the /businesses-for-sale browse pages.
import Head from 'next/head';
import { goListing } from '../lib/clickout';
import { Row, Stats, STATES, fmtMoney, fmtMultiple, fmtNum, pageUrl } from '../lib/seo';

export type LinkGroup = { title: string; links: { href: string; label: string; count?: number }[] };
export type Crumb = { href: string; label: string };

type Props = {
  path: string;
  title: string;            // <title>
  h1: string;
  eyebrow: string;
  description: string;      // meta description
  intro: string;            // first paragraph, plain sentence with the numbers
  stats: Stats | null;      // null on the hub page
  rows: Row[];
  crumbs: Crumb[];
  groups: LinkGroup[];
  showState?: boolean;      // show the state next to the city (national pages)
  refreshedNote?: string;
};

const place = (r: Row, showState: boolean) => {
  const city = (r.city || '').replace(/^\s*(location|city|of)[:\s]+/i, '').trim();
  const c = city && city === city.toLowerCase() ? city.replace(/\b([a-z])/g, (m) => m.toUpperCase()) : city;
  const parts = [c || null, showState || !c ? r.state_code : null].filter(Boolean);
  return parts.join(', ') || '—';
};

const domCell = (r: Row) => {
  if (!r.dom_display) return '—';
  if (r.dom_basis === 'floor') return r.dom_display.replace('listed on or before ', 'by ');
  return r.dom_display;
};

export default function SeoPage(p: Props) {
  const url = pageUrl(p.path);
  const shown = p.rows.length;
  const total = p.stats?.listings ?? shown;

  const ld = [
    {
      '@context': 'https://schema.org',
      '@type': 'CollectionPage',
      name: p.h1,
      url,
      description: p.description,
      isPartOf: { '@type': 'WebSite', name: 'DealLedger', url: 'https://dealledger.org' },
      ...(shown
        ? {
            mainEntity: {
              '@type': 'ItemList',
              numberOfItems: total,
              itemListElement: p.rows.slice(0, 30).map((r, i) => ({
                '@type': 'ListItem',
                position: i + 1,
                url: pageUrl(`/listing/${r.listing_number}`),
                name: r.title || 'Business for sale',
              })),
            },
          }
        : {}),
    },
    {
      '@context': 'https://schema.org',
      '@type': 'BreadcrumbList',
      itemListElement: p.crumbs.map((c, i) => ({ '@type': 'ListItem', position: i + 1, name: c.label, item: pageUrl(c.href) })),
    },
  ];

  return (
    <>
      <Head>
        <title>{p.title}</title>
        <meta name="description" content={p.description} />
        <link rel="canonical" href={url} />
        <meta property="og:type" content="website" />
        <meta property="og:title" content={p.title} />
        <meta property="og:description" content={p.description} />
        <meta property="og:url" content={url} />
        <meta property="og:site_name" content="DealLedger" />
        <meta name="twitter:card" content="summary" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(ld) }} />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="" />
        <link
          rel="stylesheet"
          href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap"
        />
      </Head>

      <style jsx global>{`
        :root {
          --bg: #fafaf9; --bg-card: #f5f5f4; --ink: #1c1917; --ink-soft: #57534e; --ink-mute: #78716c;
          --rule: #d4d4d4; --accent: #c2410c; --link: #0c4a6e;
          --serif: 'IBM Plex Sans', -apple-system, system-ui, sans-serif;
          --mono: 'IBM Plex Mono', ui-monospace, monospace;
        }
        * { box-sizing: border-box; }
        body {
          margin: 0; background: var(--bg); color: var(--ink); font-family: var(--serif);
          font-size: 16px; line-height: 1.55; -webkit-font-smoothing: antialiased;
        }
        a { color: var(--link); text-decoration: underline; text-underline-offset: 2px; }
        a:hover { color: var(--accent); }
      `}</style>

      <div className="page">
        <header className="masthead">
          <div className="masthead-inner">
            <a href="/" className="brand">DealLedger</a>
            <a href="/businesses-for-sale" className="masthead-meta">BROWSE BY STATE &amp; TYPE</a>
          </div>
          <div className="masthead-rule" />
        </header>

        <main className="content">
          <nav className="crumbs" aria-label="Breadcrumb">
            {p.crumbs.map((c, i) => (
              <span key={c.href}>
                {i > 0 && <span className="sep"> / </span>}
                {i === p.crumbs.length - 1 ? <span>{c.label}</span> : <a href={c.href}>{c.label}</a>}
              </span>
            ))}
          </nav>
          <div className="eyebrow">— {p.eyebrow}</div>
          <h1 className="headline">{p.h1}</h1>
          <p className="intro">{p.intro}</p>

          {p.stats && (
            <div className="stats">
              <div className="stat">
                <div className="stat-label">Listings</div>
                <div className="stat-value">{fmtNum(p.stats.listings)}</div>
                <div className="stat-sub">from {fmtNum(p.stats.brokers)} broker sites</div>
              </div>
              <div className="stat">
                <div className="stat-label">New, last 30 days</div>
                <div className="stat-value">{fmtNum(p.stats.new_30d)}</div>
                <div className="stat-sub">first seen on a broker site</div>
              </div>
              <div className="stat">
                <div className="stat-label">Median asking</div>
                <div className="stat-value">{fmtMoney(p.stats.median_price)}</div>
                <div className="stat-sub">median cash flow {fmtMoney(p.stats.median_cash_flow)}</div>
              </div>
              <div className="stat">
                <div className="stat-label">Price ÷ cash flow</div>
                <div className="stat-value">{fmtMultiple(p.stats.median_multiple)}</div>
                <div className="stat-sub">median, {fmtNum(p.stats.with_cash_flow)} listings report cash flow</div>
              </div>
            </div>
          )}

          {shown > 0 && (
            <section className="section">
              <div className="section-eyebrow">— LISTINGS</div>
              <h2 className="section-title">
                {total > shown ? `${fmtNum(shown)} most recently listed of ${fmtNum(total)}` : `All ${fmtNum(total)} listings`}
              </h2>
              <div className="table">
                <div className="t-head">
                  <div>Business</div>
                  <div>Location</div>
                  <div className="num">Asking</div>
                  <div className="num">Cash flow</div>
                  <div className="num">On market</div>
                </div>
                {p.rows.map((r) => (
                  <div className="t-row" key={r.listing_number}>
                    <div className="t-title">
                      <a href={`/listing/${r.listing_number}`}>{r.title || 'Untitled listing'}</a>
                      <a className="src" href={goListing(r.listing_number, 'seo')} target="_blank" rel="nofollow noopener">
                        broker ↗
                      </a>
                    </div>
                    <div className="t-loc">{place(r, !!p.showState)}</div>
                    <div className="num">{fmtMoney(r.price)}</div>
                    <div className="num">{fmtMoney(r.cash_flow)}</div>
                    <div className="num t-date">{domCell(r)}</div>
                  </div>
                ))}
              </div>
              <p className="more">
                “broker ↗” opens the listing on the broker’s own website. DealLedger never stands
                between you and the seller’s broker.
              </p>
            </section>
          )}

          {p.groups.filter((g) => g.links.length).map((g) => (
            <section className="section" key={g.title}>
              <div className="section-eyebrow">— BROWSE</div>
              <h2 className="section-title">{g.title}</h2>
              <ul className="links">
                {g.links.map((l) => (
                  <li key={l.href}>
                    <a href={l.href}>{l.label}</a>
                    {l.count != null && <span className="count"> {fmtNum(l.count)}</span>}
                  </li>
                ))}
              </ul>
            </section>
          ))}

          <section className="section">
            <div className="section-eyebrow">— HOW THIS PAGE IS BUILT</div>
            <div className="prose">
              <p>
                DealLedger reads the public listings pages of business brokers directly and records when
                each listing first appears and when it comes down. Days on market count from the first
                day we saw the listing; “by” means the listing was already up when we started reading
                that broker’s site, so it has been listed at least that long. Price ÷ cash flow is the
                asking price divided by the seller’s stated cash flow (SDE), where both are published.
                All data is public domain (CC0). <a href="/methodology.html">Full methodology</a>.
                {p.refreshedNote ? ` ${p.refreshedNote}` : ''}
              </p>
            </div>
          </section>

          <footer className="footer">
            <div className="footer-rule" />
            <div className="footer-text">
              DealLedger · Public record · CC0 · <a href="/businesses-for-sale">All states</a> ·{' '}
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
          display: flex; justify-content: space-between; align-items: baseline; gap: 16px;
        }
        .brand { font-weight: 700; font-size: 22px; text-decoration: none; letter-spacing: -0.01em; }
        .masthead-meta { font-family: var(--mono); font-size: 11px; letter-spacing: 0.08em; color: var(--ink-mute); text-decoration: none; }
        .masthead-rule { max-width: 1100px; margin: 0 auto; border-top: 1px solid var(--rule); }
        .content { max-width: 1100px; margin: 0 auto; padding: 40px 32px 96px 32px; }
        .crumbs { font-family: var(--mono); font-size: 12px; color: var(--ink-mute); margin-bottom: 28px; }
        .crumbs a { color: var(--ink-mute); }
        .eyebrow { font-family: var(--mono); font-size: 12px; letter-spacing: 0.1em; color: var(--accent); margin-bottom: 14px; }
        .headline { font-weight: 500; font-size: 44px; line-height: 1.1; letter-spacing: -0.015em; margin: 0 0 14px 0; }
        .intro { font-size: 17px; color: var(--ink-soft); margin: 0 0 32px 0; max-width: 72ch; }
        .stats { display: grid; grid-template-columns: repeat(4, 1fr); border: 1px solid var(--rule); background: var(--bg-card); }
        .stat { padding: 18px 20px; border-right: 1px solid var(--rule); }
        .stat:last-child { border-right: none; }
        .stat-label { font-family: var(--mono); font-size: 10px; letter-spacing: 0.1em; text-transform: uppercase; color: var(--ink-mute); }
        .stat-value { font-family: var(--mono); font-size: 28px; margin: 6px 0 2px 0; }
        .stat-sub { font-size: 12px; color: var(--ink-mute); }
        .section { margin-top: 56px; }
        .section-eyebrow { font-family: var(--mono); font-size: 11px; letter-spacing: 0.14em; color: var(--accent); margin-bottom: 8px; }
        .section-title { font-weight: 500; font-size: 24px; line-height: 1.2; letter-spacing: -0.01em; margin: 0 0 16px 0; }
        .table { border: 1px solid var(--rule); background: var(--bg-card); font-size: 14px; }
        .t-head, .t-row {
          display: grid; grid-template-columns: 1fr 170px 90px 90px 140px;
          gap: 14px; padding: 10px 18px; border-bottom: 1px solid var(--rule); align-items: baseline;
        }
        .t-row:last-child { border-bottom: none; }
        .t-head { font-family: var(--mono); font-size: 10px; letter-spacing: 0.08em; text-transform: uppercase; color: var(--ink-mute); }
        .t-title a { color: var(--ink); text-decoration: none; }
        .t-title a:hover { color: var(--accent); text-decoration: underline; }
        .t-title .src { font-family: var(--mono); font-size: 11px; color: var(--link); margin-left: 8px; white-space: nowrap; }
        .t-loc { font-size: 13px; color: var(--ink-soft); }
        .num { font-family: var(--mono); text-align: right; }
        .t-date { font-size: 12px; color: var(--ink-soft); }
        .more { font-size: 13px; color: var(--ink-mute); margin: 12px 0 0 0; }
        .links { list-style: none; padding: 0; margin: 0; columns: 3; column-gap: 32px; font-size: 15px; }
        .links li { break-inside: avoid; padding: 4px 0; }
        .count { font-family: var(--mono); font-size: 12px; color: var(--ink-mute); }
        .prose p { margin: 0; color: var(--ink-soft); max-width: 75ch; font-size: 14px; }
        .footer { margin-top: 80px; }
        .footer-rule { border-top: 1px solid var(--rule); margin-bottom: 20px; }
        .footer-text { font-family: var(--mono); font-size: 11px; color: var(--ink-mute); }
        @media (max-width: 900px) {
          .stats { grid-template-columns: repeat(2, 1fr); }
          .stat:nth-child(2) { border-right: none; }
          .stat:nth-child(-n + 2) { border-bottom: 1px solid var(--rule); }
          .t-head, .t-row { grid-template-columns: 1fr 76px 90px; padding: 10px 14px; }
          .t-head > :nth-child(2), .t-row > :nth-child(2), .t-head > :nth-child(4), .t-row > :nth-child(4) { display: none; }
          .links { columns: 2; }
        }
        @media (max-width: 720px) {
          .headline { font-size: 30px; }
          .content { padding: 28px 16px 64px 16px; }
          .masthead-inner { padding: 0 16px 18px 16px; }
          .links { columns: 1; }
        }
      `}</style>
    </>
  );
}

export { STATES };
