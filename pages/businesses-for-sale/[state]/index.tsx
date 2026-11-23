// /businesses-for-sale/{state} — every public listing in one state.
import type { GetStaticPaths, GetStaticProps, InferGetStaticPropsType } from 'next';
import SeoPage from '../../../components/SeoPage';
import { NICHE_COMBO_MIN, nicheCounts } from '../../../lib/niches';
import {
  Row, Stats, STATES, TYPES, allStats, comboPath, fmtMoney, fmtNum, hasComboPage, hasStatePage,
  listingsFor, stateFromSlug, statePath,
} from '../../../lib/seo';

type Props = { code: string; stats: Stats | null; thin: boolean; rows: Row[]; types: { vertical: string; count: number }[]; others: { code: string; count: number }[]; niches: { slug: string; label: string; count: number }[] };

export const getStaticPaths: GetStaticPaths = async () => ({ paths: [], fallback: 'blocking' });

export const getStaticProps: GetStaticProps<Props> = async ({ params }) => {
  const code = stateFromSlug(String(params?.state || ''));
  if (!code) return { notFound: true, revalidate: 3600 };
  const all = await allStats();
  const stats = all.find((s) => s.scope === 'state' && s.page_state === code);
  // Below STATE_MIN: a thin noindex page rather than a 404, so the type + state
  // finder never lands on a dead end (sitemap still uses hasStatePage).
  const thin = !stats || !hasStatePage(stats);
  const rows = await listingsFor({ state: code });
  const types = all
    .filter((s) => hasComboPage(s) && s.page_state === code)
    .sort((a, b) => b.listings - a.listings)
    .map((s) => ({ vertical: s.page_vertical as string, count: s.listings }));
  const others = all
    .filter((s) => hasStatePage(s) && s.page_state !== code)
    .sort((a, b) => STATES[a.page_state!].localeCompare(STATES[b.page_state!]))
    .map((s) => ({ code: s.page_state as string, count: s.listings }));
  const niches = (await nicheCounts())
    .map((c) => ({ slug: c.slug, label: c.label, count: c.states[code] || 0 }))
    .filter((c) => c.count >= NICHE_COMBO_MIN)
    .sort((a, b) => b.count - a.count);
  return { props: { code, stats: thin ? null : stats!, thin, rows, types, others, niches }, revalidate: 60 * 60 * 24 };
};

export default function StatePage({ code, stats, thin, rows, types, others, niches }: InferGetStaticPropsType<typeof getStaticProps>) {
  const name = STATES[code];
  const path = statePath(code);
  const n = stats ? stats.listings : rows.length;
  return (
    <SeoPage
      path={path}
      noindex={thin}
      alert={{ state: code, label: `Businesses in ${name}` }}
      title={`Businesses for Sale in ${name}${n ? ` (${fmtNum(n)} listings)` : ''} — DealLedger`}
      h1={`Businesses for Sale in ${name}`}
      eyebrow={`${code} · LISTED DIRECTLY BY BROKERS`}
      description={stats
        ? `${fmtNum(stats.listings)} businesses for sale in ${name} from ${fmtNum(stats.brokers)} broker websites. Median asking ${fmtMoney(stats.median_price)}. Evidence-based listing age, price-to-cash-flow, and a direct link to each broker.`
        : `Businesses for sale in ${name}, tracked from broker websites by DealLedger.`}
      intro={stats
        ? `${fmtNum(stats.listings)} businesses are listed for sale in ${name} on ${fmtNum(stats.brokers)} business broker websites that DealLedger reads directly, and ${fmtNum(stats.new_30d)} of them first appeared in the last 30 days. Each listing shows how long it has really been on the market and links straight to the broker.`
        : n === 0
          ? `DealLedger isn't tracking any businesses for sale in ${name} right now. We check broker websites daily; see other states below.`
          : `DealLedger is tracking ${fmtNum(n)} ${n === 1 ? 'business' : 'businesses'} for sale in ${name} right now. Every listing below links to the broker who has it.`}
      stats={stats}
      rows={rows}
      finder={{ state: code }}
      query={{ state: code }}
      crumbs={[{ href: '/', label: 'DealLedger' }, { href: '/businesses-for-sale', label: 'Businesses for sale' }, { href: path, label: name }]}
      groups={[
        { title: `By type in ${name}`, links: types.map((t) => ({ href: comboPath(code, t.vertical), label: TYPES[t.vertical].label, count: t.count })) },
        { title: `Specific types in ${name}`, links: niches.map((n) => ({ href: `${path}/${n.slug}`, label: n.label, count: n.count })) },
        { title: 'Other states', links: others.map((o) => ({ href: statePath(o.code), label: STATES[o.code], count: o.count })) },
      ]}
    />
  );
}
