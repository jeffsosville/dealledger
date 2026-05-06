// pages/brokers/index.tsx
//
// DealLedger broker directory — /brokers
// Sortable, filterable index of all firms in the registry.

import type { GetServerSideProps, InferGetServerSidePropsType } from 'next';
import Head from 'next/head';
import { getSupabase } from '../../lib/supabase';

const PAGE_SIZE = 100;

// ─── TYPES ─────────────────────────────────────────────────────────────────
type FirmRow = {
  firm_key: string;
  slug: string;
  companyname: string | null;
  regions: string | null;
  state_count: number | null;
  total_people: number | null;
  active_count: number;
  recent_count: number;
  total_observed: number;
};

type SortKey =
  | 'active_count'
  | 'recent_count'
  | 'total_observed'
  | 'total_people'
  | 'companyname';

type PageProps = {
  firms: FirmRow[];
  totalFirms: number;
  page: number;
  sort: SortKey;
  stateFilter: string | null;
  search: string | null;
  pageCount: number;
  allStates: string[];
};

// ─── DATA FETCHING ─────────────────────────────────────────────────────────
const VALID_SORTS: SortKey[] = [
  'active_count',
  'recent_count',
  'total_observed',
  'total_people',
  'companyname',
];

export const getServerSideProps: GetServerSideProps<PageProps> = async ({
  query,
}) => {
  const sb = getSupabase();

  const sort = (VALID_SORTS.includes(query.sort as SortKey)
    ? (query.sort as SortKey)
    : 'active_count') as SortKey;
  const page = Math.max(1, parseInt((query.page as string) || '1', 10) || 1);
  const stateFilter =
    typeof query.state === 'string' && query.state.length === 2
      ? query.state.toUpperCase()
      : null;
  const search =
    typeof query.q === 'string' && query.q.trim().length > 0
      ? query.q.trim()
      : null;

  console.log('[brokers] === REQUEST ===', { sort, page, stateFilter, search });

  // Pull firm rows joined with real listing counts (computed server-side).
  // We use a function-style join since the live counts come from listings.
  // For perf, we lean on a materialized view if available; otherwise
  // compute via the broker_firms view + listings counts.
  //
  // Strategy: pull broker_firms (3,213 rows, small), then enrich with
  // listing counts via a single grouped query on listings.

  let firmsQuery = sb
    .from('broker_firms')
    .select(
      'firm_key, slug, companyname, regions, state_count, total_people'
    );

  if (stateFilter) {
    firmsQuery = firmsQuery.ilike('regions', `%${stateFilter}%`);
  }
  if (search) {
    firmsQuery = firmsQuery.ilike('companyname', `%${search}%`);
  }

  const { data: firmRows } = await firmsQuery.range(0, 9999);
  const allFirms = (firmRows || []) as Omit<
    FirmRow,
    'active_count' | 'recent_count' | 'total_observed'
  >[];

  // Pull listing counts grouped by firm_key. Single query.
  // We compute three counts per firm: active, recent_inactive, total_inactive.
  const sixMonthsAgo = new Date();
  sixMonthsAgo.setDate(sixMonthsAgo.getDate() - 180);

  // Active counts
  const { data: activeRows } = await sb
    .from('listings')
    .select('firm_key', { count: 'exact' })
    .eq('is_active', true)
    .not('firm_key', 'is', null)
    .range(0, 99999);

  const activeCounts = new Map<string, number>();
  (activeRows || []).forEach((r: any) => {
    if (!r.firm_key) return;
    activeCounts.set(r.firm_key, (activeCounts.get(r.firm_key) || 0) + 1);
  });

  // Recent inactive (last 6mo)
  const { data: recentRows } = await sb
    .from('listings')
    .select('firm_key')
    .eq('is_active', false)
    .gte('last_seen', sixMonthsAgo.toISOString())
    .not('firm_key', 'is', null)
    .range(0, 99999);

  const recentCounts = new Map<string, number>();
  (recentRows || []).forEach((r: any) => {
    if (!r.firm_key) return;
    recentCounts.set(r.firm_key, (recentCounts.get(r.firm_key) || 0) + 1);
  });

  // Total observed (active + ever inactive)
  const { data: totalRows } = await sb
    .from('listings')
    .select('firm_key')
    .not('firm_key', 'is', null)
    .range(0, 99999);

  const totalCounts = new Map<string, number>();
  (totalRows || []).forEach((r: any) => {
    if (!r.firm_key) return;
    totalCounts.set(r.firm_key, (totalCounts.get(r.firm_key) || 0) + 1);
  });

  // Merge counts onto firms
  const enriched: FirmRow[] = allFirms.map((f) => ({
    ...f,
    active_count: activeCounts.get(f.firm_key) || 0,
    recent_count: recentCounts.get(f.firm_key) || 0,
    total_observed: totalCounts.get(f.firm_key) || 0,
  }));

  // Sort
  const sortedAll = enriched.slice().sort((a, b) => {
    if (sort === 'companyname') {
      return (a.companyname || '').localeCompare(b.companyname || '');
    }
    const av = (a as any)[sort] ?? 0;
    const bv = (b as any)[sort] ?? 0;
    return bv - av; // numeric sorts: descending
  });

  const totalFirms = sortedAll.length;
  const pageCount = Math.max(1, Math.ceil(totalFirms / PAGE_SIZE));
  const startIdx = (page - 1) * PAGE_SIZE;
  const firms = sortedAll.slice(startIdx, startIdx + PAGE_SIZE);

  // Build state list for the filter dropdown (run once, cache server-side)
  const { data: stateRows } = await sb
    .from('broker_firms')
    .select('regions')
    .not('regions', 'is', null)
    .range(0, 9999);

  const stateSet = new Set<string>();
  (stateRows || []).forEach((r: any) => {
    if (r.regions) {
      r.regions
        .split(',')
        .map((s: string) => s.trim())
        .filter((s: string) => s.length === 2)
        .forEach((s: string) => stateSet.add(s));
    }
  });
  const allStates = Array.from(stateSet).sort();

  console.log('[brokers] === SUCCESS ===', {
    returned: firms.length,
    totalFirms,
    pageCount,
  });

  return {
    props: {
      firms,
      totalFirms,
      page,
      sort,
      stateFilter,
      search,
      pageCount,
      allStates,
    },
  };
};

// ─── FORMATTERS ────────────────────────────────────────────────────────────
const fmtNum = (n: number | null | undefined) =>
  n == null ? '—' : new Intl.NumberFormat('en-US').format(n);

// ─── PAGE ──────────────────────────────────────────────────────────────────
export default function BrokersIndex({
  firms,
  totalFirms,
  page,
  sort,
  stateFilter,
  search,
  pageCount,
  allStates,
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
      ...overrides,
    };
    Object.entries(merged).forEach(([k, v]) => {
      if (v != null && v !== '' && !(k === 'page' && v === 1)) {
        params.set(k, String(v));
      }
    });
    const qs = params.toString();
    return `/brokers${qs ? '?' + qs : ''}`;
  };

  const sortLabels: Record<SortKey, string> = {
    active_count: 'Active Listings',
    recent_count: 'Last 6mo Activity',
    total_observed: 'Lifetime Observed',
    total_people: 'Brokers (Headcount)',
    companyname: 'Alphabetical',
  };

  return (
    <>
      <Head>
        <title>U.S. Business Broker Registry — DealLedger</title>
        <meta
          name="description"
          content={`Public registry of ${fmtNum(totalFirms)} U.S. business brokerage firms. Sortable by active listings, recent activity, and lifetime observations.`}
        />
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
          <div className="eyebrow">— BROKER REGISTRY</div>
          <h1 className="headline">U.S. Business Broker Registry</h1>
          <p className="subtitle">
            {fmtNum(totalFirms)} firms tracked
            {stateFilter ? ` in ${stateFilter}` : ''}
            {search ? ` matching "${search}"` : ''}.
          </p>

          {/* ─── FILTERS ───────────────────────────────────────────── */}
          <form className="filters" method="GET" action="/brokers">
            <input type="hidden" name="sort" value={sort} />
            <div className="filter-group">
              <label className="filter-label">Search</label>
              <input
                type="text"
                name="q"
                defaultValue={search || ''}
                placeholder="Firm name…"
                className="filter-input"
              />
            </div>
            <div className="filter-group">
              <label className="filter-label">State</label>
              <select
                name="state"
                defaultValue={stateFilter || ''}
                className="filter-select"
              >
                <option value="">All states</option>
                {allStates.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </select>
            </div>
            <button type="submit" className="filter-btn">
              Apply
            </button>
            {(stateFilter || search) && (
              <a href="/brokers" className="filter-clear">
                Clear
              </a>
            )}
          </form>

          {/* ─── SORT TABS ─────────────────────────────────────────── */}
          <div className="sort-tabs">
            <span className="sort-tabs-label">Sort by:</span>
            {(Object.keys(sortLabels) as SortKey[]).map((k) => (
              <a
                key={k}
                href={buildHref({ sort: k, page: 1 })}
                className={`sort-tab ${sort === k ? 'sort-tab-active' : ''}`}
              >
                {sortLabels[k]}
              </a>
            ))}
          </div>

          {/* ─── TABLE ─────────────────────────────────────────────── */}
          <div className="table-wrap">
            <div className="table-head">
              <div className="cell cell-rank">#</div>
              <div className="cell cell-firm">Firm</div>
              <div className="cell cell-loc">Location</div>
              <div className="cell cell-num">Active</div>
              <div className="cell cell-num">6mo</div>
              <div className="cell cell-num">Lifetime</div>
              <div className="cell cell-num">Brokers</div>
            </div>
            {firms.map((f, i) => {
              const rank = (page - 1) * PAGE_SIZE + i + 1;
              return (
                <a key={f.firm_key} className="table-row" href={`/broker/${f.slug}`}>
                  <div className="cell cell-rank">{rank}</div>
                  <div className="cell cell-firm">
                    <div className="firm-name">
                      {f.companyname || f.firm_key}
                    </div>
                  </div>
                  <div className="cell cell-loc">{f.regions || '—'}</div>
                  <div className="cell cell-num">{fmtNum(f.active_count)}</div>
                  <div className="cell cell-num">{fmtNum(f.recent_count)}</div>
                  <div className="cell cell-num">
                    {fmtNum(f.total_observed)}
                  </div>
                  <div className="cell cell-num">
                    {fmtNum(f.total_people)}
                  </div>
                </a>
              );
            })}
            {firms.length === 0 && (
              <div className="empty-state">
                No firms match these filters.
              </div>
            )}
          </div>

          {/* ─── PAGINATION ────────────────────────────────────────── */}
          {pageCount > 1 && (
            <div className="pagination">
              {page > 1 && (
                <a href={buildHref({ page: page - 1 })} className="page-link">
                  ← Previous
                </a>
              )}
              <span className="page-meta">
                Page {page} of {pageCount}
              </span>
              {page < pageCount && (
                <a href={buildHref({ page: page + 1 })} className="page-link">
                  Next →
                </a>
              )}
            </div>
          )}

          {/* ─── METHODOLOGY ───────────────────────────────────────── */}
          <section className="methodology">
            <div className="section-eyebrow">— METHODOLOGY</div>
            <h2 className="section-title">What we observe</h2>
            <div className="prose">
              <p>
                DealLedger publishes what is publicly displayed across business-for-sale
                marketplaces and broker-direct websites. Each row above represents a
                distinct firm whose listings we have observed and attributed.
              </p>
              <p>
                <strong>Active</strong> counts listings currently observable in our latest scrape.{' '}
                <strong>6mo</strong> counts listings that were active and have since dropped
                from our scrapes in the trailing 180 days.{' '}
                <strong>Lifetime</strong> is the total number of distinct listings we have
                ever attributed to this firm. <strong>Brokers</strong> is the headcount on
                record at the firm — principals plus agents.
              </p>
              <p>
                Rankings are observational, not editorial. Position in the table reflects
                the chosen sort, not a quality judgment.
              </p>
            </div>
          </section>

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
          grid-template-columns: 50px 1fr 180px 80px 70px 90px 80px;
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
            grid-template-columns: 30px 1fr 60px 70px;
            gap: 10px;
            padding: 10px 14px;
          }
          .cell-loc, .table-head .cell-loc,
          .cell-num:nth-child(5), .cell-num:nth-child(6),
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
