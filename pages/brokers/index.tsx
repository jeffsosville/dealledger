// pages/brokers/index.tsx
//
// DealLedger broker registry — /brokers
// One row per broker website whose published listings DealLedger has
// observed directly. Data: `broker_directory` materialized view
// (sql/2026-10-08_broker_registry.sql), refreshed nightly after the bridge.

import type { GetServerSideProps, InferGetServerSidePropsType } from 'next';
import Head from 'next/head';
import { getSupabase } from '../../lib/supabase';
import {
  REGISTRY_COLUMNS,
  REGISTRY_VIEW,
  MARKETPLACE_DOMAINS,
  isMarketplaceDomain,
  OBSERVATION_START_LABEL,
  RegistryRow,
  cleanStates,
  displayLocation,
  displayName,
  fmtDate,
  fmtNum,
} from '../../lib/brokerRegistry';

const PAGE_SIZE = 100;

type SortKey = 'active_count' | 'removed_180d' | 'lifetime_count' | 'last_observed' | 'firm_name';

const SORTS: Record<SortKey, { label: string; ascending: boolean }> = {
  active_count: { label: 'Active listings', ascending: false },
  removed_180d: { label: 'Removed, last 180 days', ascending: false },
  lifetime_count: { label: 'Lifetime observed', ascending: false },
  last_observed: { label: 'Most recently seen', ascending: false },
  firm_name: { label: 'Alphabetical', ascending: true },
};

const STATES = (
  'AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV ' +
  'NH NJ NM NY NC ND OH OK OR PA PR RI SC SD TN TX UT VT VA WA WV WI WY'
).split(' ');

type PageProps = {
  firms: RegistryRow[];
  totalFirms: number;
  totals: { firms: number; activeFirms: number; activeListings: number; refreshedAt: string | null };
  page: number;
  pageCount: number;
  sort: SortKey;
  stateFilter: string | null;
  search: string | null;
  showInactive: boolean;
};

export const getServerSideProps: GetServerSideProps<PageProps> = async ({ query, res }) => {
  const sb = getSupabase();

  const sort = (Object.keys(SORTS) as SortKey[]).includes(query.sort as SortKey)
    ? (query.sort as SortKey)
    : 'active_count';
  const page = Math.max(1, parseInt((query.page as string) || '1', 10) || 1);
  const stateRaw = typeof query.state === 'string' ? query.state.toUpperCase() : '';
  const stateFilter = STATES.includes(stateRaw) ? stateRaw : null;
  const search =
    typeof query.q === 'string' && query.q.trim().length > 0
      ? query.q.trim().replace(/[%,()]/g, ' ').slice(0, 80)
      : null;
  // By default the registry lists firms with listings we can see today.
  // ?all=1 includes firms whose listings we have observed before but not now.
  const showInactive = query.all === '1';

  let q = sb.from(REGISTRY_VIEW).select(REGISTRY_COLUMNS, { count: 'exact' });
  q = q.not('domain', 'in', `(${MARKETPLACE_DOMAINS.join(',')})`);
  for (const m of MARKETPLACE_DOMAINS) q = q.not('domain', 'ilike', `%.${m}`);
  if (!showInactive) q = q.gt('active_count', 0);
  if (stateFilter) q = q.or(`hq_state.eq.${stateFilter},states_listed.ilike.%${stateFilter}%`);
  if (search) q = q.or(`firm_name.ilike.%${search}%,domain.ilike.%${search}%`);
  q = q.order(sort, { ascending: SORTS[sort].ascending, nullsFirst: false }).order('slug');

  const from = (page - 1) * PAGE_SIZE;
  const { data, count, error } = await q.range(from, from + PAGE_SIZE - 1);
  if (error) console.error('[brokers] query error', error.message);

  // Headline totals: always for the whole registry, independent of filters.
  const { data: totalRows } = await sb
    .from(REGISTRY_VIEW)
    .select('domain, active_count, refreshed_at')
    .range(0, 4999);
  const all = ((totalRows || []) as { domain: string; active_count: number; refreshed_at: string | null }[]).filter(
    (r) => !isMarketplaceDomain(r.domain)
  );
  const totals = {
    firms: all.length,
    activeFirms: all.filter((r) => r.active_count > 0).length,
    activeListings: all.reduce((s, r) => s + (r.active_count || 0), 0),
    refreshedAt: all[0]?.refreshed_at ?? null,
  };

  res.setHeader('Cache-Control', 'public, s-maxage=3600, stale-while-revalidate=86400');

  const totalFirms = count ?? 0;
  return {
    props: {
      firms: (data || []) as unknown as RegistryRow[],
      totalFirms,
      totals,
      page,
      pageCount: Math.max(1, Math.ceil(totalFirms / PAGE_SIZE)),
      sort,
      stateFilter,
      search,
      showInactive,
    },
  };
};

export default function BrokersIndex({
  firms,
  totalFirms,
  totals,
  page,
  pageCount,
  sort,
  stateFilter,
  search,
  showInactive,
}: InferGetServerSidePropsType<typeof getServerSideProps>) {
  const today = new Date().toLocaleDateString('en-US', {
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  });

  const buildHref = (overrides: Record<string, string | number | null>) => {
    const params = new URLSearchParams();
    const merged: Record<string, string | number | null> = {
      sort,
      page,
      state: stateFilter,
      q: search,
      all: showInactive ? '1' : null,
      ...overrides,
    };
    Object.entries(merged).forEach(([k, v]) => {
      if (v == null || v === '') return;
      if (k === 'page' && v === 1) return;
      if (k === 'sort' && v === 'active_count') return;
      params.set(k, String(v));
    });
    const qs = params.toString();
    return `/brokers${qs ? '?' + qs : ''}`;
  };

  const filtered = Boolean(stateFilter || search);
  const metaDescription = `Public registry of ${fmtNum(totals.activeFirms)} U.S. business brokers with ${fmtNum(
    totals.activeListings
  )} listings currently observed on their own websites. Free, open data (CC0).`;

  return (
    <>
      <Head>
        <title>U.S. Business Broker Registry — DealLedger</title>
        <meta name="description" content={metaDescription} />
        <link rel="canonical" href="https://dealledger.org/brokers" />
        {(filtered || page > 1 || showInactive || sort !== 'active_count') && (
          <meta name="robots" content="noindex, follow" />
        )}
        <meta property="og:type" content="website" />
        <meta property="og:title" content="U.S. Business Broker Registry — DealLedger" />
        <meta property="og:description" content={metaDescription} />
        <meta property="og:url" content="https://dealledger.org/brokers" />
        <meta property="og:site_name" content="DealLedger" />
        <meta name="twitter:card" content="summary" />
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
            <div className="masthead-meta">PUBLIC RECORD · {today.toUpperCase()}</div>
          </div>
          <div className="masthead-rule" />
        </header>

        <main className="content">
          <div className="eyebrow">— BROKER REGISTRY</div>
          <h1 className="headline">U.S. Business Broker Registry</h1>
          <p className="subtitle">
            {fmtNum(totals.activeFirms)} brokers with {fmtNum(totals.activeListings)} listings
            currently observed on their own websites
            {totals.refreshedAt ? ` · updated ${fmtDate(totals.refreshedAt)}` : ''}.
          </p>
          <p className="coverage-note">
            This is our coverage, not the whole market. It lists brokers whose sites we
            currently read; many firms are not yet indexed.{' '}
            <a href="mailto:info@dealledger.org?subject=Add%20my%20brokerage">Add your firm →</a>
          </p>

          <form className="filters" method="GET" action="/brokers">
            {sort !== 'active_count' && <input type="hidden" name="sort" value={sort} />}
            {showInactive && <input type="hidden" name="all" value="1" />}
            <div className="filter-group">
              <label className="filter-label" htmlFor="q">Search</label>
              <input
                id="q"
                type="text"
                name="q"
                defaultValue={search || ''}
                placeholder="Firm name or website…"
                className="filter-input"
              />
            </div>
            <div className="filter-group">
              <label className="filter-label" htmlFor="state">State</label>
              <select id="state" name="state" defaultValue={stateFilter || ''} className="filter-select">
                <option value="">All states</option>
                {STATES.map((s) => (
                  <option key={s} value={s}>{s}</option>
                ))}
              </select>
            </div>
            <button type="submit" className="filter-btn">Apply</button>
            {(filtered || showInactive) && (
              <a href="/brokers" className="filter-clear">Clear</a>
            )}
          </form>

          <div className="sort-tabs">
            <span className="sort-tabs-label">Sort by:</span>
            {(Object.keys(SORTS) as SortKey[]).map((k) => (
              <a
                key={k}
                href={buildHref({ sort: k, page: 1 })}
                className={`sort-tab ${sort === k ? 'sort-tab-active' : ''}`}
              >
                {SORTS[k].label}
              </a>
            ))}
          </div>

          <p className="result-meta">
            {fmtNum(totalFirms)} {totalFirms === 1 ? 'firm' : 'firms'}
            {stateFilter ? ` based in or listing in ${stateFilter}` : ''}
            {search ? ` matching “${search}”` : ''}
            {showInactive ? ', including firms with no listings observed today' : ''}.{' '}
            <a href={buildHref({ all: showInactive ? null : '1', page: 1 })}>
              {showInactive ? 'Show only firms with active listings' : 'Include firms with no active listings'}
            </a>
          </p>

          <div className="table-wrap">
            <div className="table-head">
              <div className="cell cell-rank">#</div>
              <div className="cell cell-firm">Firm</div>
              <div className="cell cell-loc">Based in</div>
              <div className="cell cell-num">Active</div>
              <div className="cell cell-num">Removed 180d</div>
              <div className="cell cell-num">Lifetime</div>
              <div className="cell cell-num">Last seen</div>
            </div>
            {firms.map((f, i) => {
              const rank = (page - 1) * PAGE_SIZE + i + 1;
              const states = cleanStates(f.states_listed);
              return (
                <a key={f.slug} className="table-row" href={`/broker/${f.slug}`}>
                  <div className="cell cell-rank">{rank}</div>
                  <div className="cell cell-firm">
                    <div className="firm-name">{displayName(f.firm_name, f.domain)}</div>
                    <div className="firm-domain">
                      {f.domain}
                      {states.length > 1 ? ` · listings in ${states.length} states` : ''}
                    </div>
                  </div>
                  <div className="cell cell-loc">{displayLocation(f)}</div>
                  <div className="cell cell-num">{fmtNum(f.active_count)}</div>
                  <div className="cell cell-num">{fmtNum(f.removed_180d)}</div>
                  <div className="cell cell-num">{fmtNum(f.lifetime_count)}</div>
                  <div className="cell cell-num cell-date">{fmtDate(f.last_observed)}</div>
                </a>
              );
            })}
            {firms.length === 0 && (
              <div className="empty-state">No firms match these filters.</div>
            )}
          </div>

          {pageCount > 1 && (
            <div className="pagination">
              {page > 1 ? (
                <a href={buildHref({ page: page - 1 })} className="page-link">← Previous</a>
              ) : <span />}
              <span className="page-meta">Page {page} of {pageCount}</span>
              {page < pageCount ? (
                <a href={buildHref({ page: page + 1 })} className="page-link">Next →</a>
              ) : <span />}
            </div>
          )}

          <section className="methodology">
            <div className="section-eyebrow">— METHODOLOGY</div>
            <h2 className="section-title">What we observe</h2>
            <div className="prose">
              <p>
                Each row is a business brokerage website. DealLedger reads the listings each
                broker publishes on its own site, records when each listing first and last
                appears, and keeps that history. We collect public pages only, never anything
                behind a login.
              </p>
              <p>
                <strong>Active</strong> is the number of listings visible on the broker’s site in
                our latest collection. <strong>Removed 180d</strong> counts listings that were
                visible and then disappeared in the last 180 days. A removed listing may have
                sold, gone under contract, or been withdrawn; we can’t tell which.{' '}
                <strong>Lifetime</strong> is every distinct listing we have observed from that
                site. Broker-direct collection began on {OBSERVATION_START_LABEL}.
              </p>
              <p>
                Firms that operate as national networks are shown once, under the network’s
                website, not under any single office. Rankings are observational, not editorial.
              </p>
              <p>
                See something wrong about your firm? Email{' '}
                <a href="mailto:info@dealledger.org">info@dealledger.org</a> and we’ll fix it.
                Full methodology: <a href="/methodology.html">dealledger.org/methodology</a>.
              </p>
            </div>
          </section>

          <footer className="footer">
            <div className="footer-rule" />
            <div className="footer-text">
              DealLedger · Public record · CC0
              <br />
              An open registry of U.S. business brokers and the listings they publish.
            </div>
          </footer>
        </main>
      </div>

      <style jsx>{`
        .page { min-height: 100vh; }

        .masthead { padding: 24px 0 0 0; }
        .masthead-inner {
          max-width: 1100px;
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
          max-width: 1100px;
          margin: 0 auto;
          border-top: 1px solid var(--rule);
        }

        .content {
          max-width: 1100px;
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

        .filters {
          display: flex;
          gap: 16px;
          align-items: flex-end;
          margin-bottom: 24px;
          flex-wrap: wrap;
        }
        .filter-group { display: flex; flex-direction: column; gap: 6px; }
        .filter-label {
          font-family: var(--mono);
          font-size: 10px;
          letter-spacing: 0.1em;
          text-transform: uppercase;
          color: var(--ink-mute);
        }
        .filter-input, .filter-select {
          font-family: var(--serif);
          font-size: 14px;
          padding: 8px 12px;
          background: var(--bg);
          border: 1px solid var(--rule);
          color: var(--ink);
          min-width: 180px;
        }
        .filter-input:focus, .filter-select:focus {
          outline: 1px solid var(--accent);
          outline-offset: -1px;
        }
        .filter-btn {
          font-family: var(--mono);
          font-size: 11px;
          letter-spacing: 0.1em;
          text-transform: uppercase;
          padding: 9px 16px;
          background: var(--ink);
          color: var(--bg);
          border: 1px solid var(--ink);
          cursor: pointer;
        }
        .filter-btn:hover { background: var(--accent); border-color: var(--accent); }
        .filter-clear {
          font-family: var(--mono);
          font-size: 11px;
          letter-spacing: 0.08em;
          text-transform: uppercase;
          color: var(--ink-mute);
          align-self: center;
          padding-bottom: 4px;
        }

        .sort-tabs {
          display: flex;
          gap: 0;
          align-items: center;
          flex-wrap: wrap;
          margin-bottom: 24px;
          padding-bottom: 16px;
          border-bottom: 1px solid var(--rule);
        }
        .sort-tabs-label {
          font-family: var(--mono);
          font-size: 11px;
          letter-spacing: 0.1em;
          text-transform: uppercase;
          color: var(--ink-mute);
          margin-right: 16px;
        }
        .sort-tab {
          font-family: var(--mono);
          font-size: 12px;
          letter-spacing: 0.04em;
          text-decoration: none;
          color: var(--ink-soft);
          padding: 8px 14px;
          margin-right: 4px;
          border: 1px solid transparent;
        }
        .sort-tab:hover { color: var(--accent); }
        .sort-tab-active {
          color: var(--ink);
          background: var(--bg-card);
          border-color: var(--rule);
        }

        .table-wrap {
          background: var(--bg-card);
          border: 1px solid var(--rule);
          font-size: 14px;
        }
        .table-head, .table-row {
          display: grid;
          grid-template-columns: 44px 1fr 150px 70px 100px 80px 110px;
          gap: 16px;
          padding: 12px 20px;
          border-bottom: 1px solid var(--rule);
          align-items: center;
        }
        .table-row:last-child { border-bottom: none; }
        .table-row {
          text-decoration: none;
          color: var(--ink);
          transition: background 0.1s;
        }
        .table-row:hover { background: rgba(183, 54, 26, 0.04); }
        .table-head {
          font-family: var(--mono);
          font-size: 10px;
          letter-spacing: 0.08em;
          text-transform: uppercase;
          color: var(--ink-mute);
        }
        .cell-rank {
          font-family: var(--mono);
          color: var(--ink-mute);
          font-size: 12px;
        }
        .firm-name {
          line-height: 1.3;
          font-weight: 500;
        }
        .firm-domain {
          font-family: var(--mono);
          font-size: 11px;
          color: var(--ink-mute);
          margin-top: 2px;
          overflow-wrap: anywhere;
        }
        .cell-date { font-size: 12px; color: var(--ink-soft); }
        .coverage-note {
          font-size: 14px;
          color: var(--ink-mute);
          margin: -28px 0 36px 0;
          max-width: 70ch;
        }
        .result-meta {
          font-size: 13px;
          color: var(--ink-mute);
          margin: 0 0 12px 0;
        }
        .cell-loc {
          font-size: 13px;
          color: var(--ink-soft);
        }
        .cell-num {
          font-family: var(--mono);
          text-align: right;
        }

        .empty-state {
          padding: 48px 20px;
          text-align: center;
          color: var(--ink-mute);
          font-style: italic;
        }

        .pagination {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-top: 32px;
          padding: 16px 0;
        }
        .page-link {
          font-family: var(--mono);
          font-size: 12px;
          letter-spacing: 0.04em;
          text-decoration: none;
          color: var(--ink);
          padding: 8px 14px;
          border: 1px solid var(--rule);
        }
        .page-link:hover { color: var(--accent); border-color: var(--accent); }
        .page-meta {
          font-family: var(--mono);
          font-size: 12px;
          color: var(--ink-mute);
        }

        .methodology { margin-top: 64px; }
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
        .prose p {
          margin: 0 0 14px 0;
          color: var(--ink-soft);
          max-width: 64ch;
        }
        .prose p:last-child { margin-bottom: 0; }
        .prose strong {
          color: var(--ink);
          font-weight: 600;
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

        @media (max-width: 900px) {
          .table-head, .table-row {
            grid-template-columns: 30px 1fr 60px 90px;
            gap: 10px;
            padding: 10px 14px;
          }
          .cell-loc, .table-head .cell-loc,
          .cell:nth-child(5), .cell:nth-child(6),
          .table-head .cell:nth-child(5), .table-head .cell:nth-child(6) {
            display: none;
          }
        }
        @media (max-width: 720px) {
          .headline { font-size: 32px; }
          .content { padding: 40px 22px 64px 22px; }
          .filter-input, .filter-select { min-width: 140px; }
        }
      `}</style>
    </>
  );
}
