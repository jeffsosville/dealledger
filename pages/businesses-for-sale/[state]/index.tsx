// /businesses-for-sale/{state} — every public listing in one state.
import type { GetStaticPaths, GetStaticProps, InferGetStaticPropsType } from 'next';
import SeoPage from '../../../components/SeoPage';
import {
  Row, Stats, STATES, TYPES, allStats, comboPath, fmtMoney, fmtNum, hasComboPage, hasStatePage,
  listingsFor, stateFromSlug, statePath,
} from '../../../lib/seo';

type Props = { code: string; stats: Stats; rows: Row[]; types: { vertical: string; count: number }[]; others: { code: string; count: number }[] };

export const getStaticPaths: GetStaticPaths = async () => ({ paths: [], fallback: 'blocking' });

export const getStaticProps: GetStaticProps<Props> = async ({ params }) => {
  const code = stateFromSlug(String(params?.state || ''));
  if (!code) return { notFound: true, revalidate: 86400 };
  const all = await allStats();
  const stats = all.find((s) => s.scope === 'state' && s.page_state === code);
  if (!stats || !hasStatePage(stats)) return { notFound: true, revalidate: 86400 };
  const rows = await listingsFor({ state: code });
  const types = all
    .filter((s) => hasComboPage(s) && s.page_state === code)
    .sort((a, b) => b.listings - a.listings)
    .map((s) => ({ vertical: s.page_vertical as string, count: s.listings }));
  const others = all
    .filter((s) => hasStatePage(s) && s.page_state !== code)
    .sort((a, b) => STATES[a.page_state!].localeCompare(STATES[b.page_state!]))
    .map((s) => ({ code: s.page_state as string, count: s.listings }));
  return { props: { code, stats, rows, types, others }, revalidate: 60 * 60 * 6 };
};

export default function StatePage({ code, stats, rows, types, others }: InferGetStaticPropsType<typeof getStaticProps>) {
  const name = STATES[code];
  const path = statePath(code);
  return (
    <SeoPage
      path={path}
      title={`Businesses for Sale in ${name} (${fmtNum(stats.listings)} listings) — DealLedger`}
      h1={`Businesses for Sale in ${name}`}
      eyebrow={`${code} · LISTED DIRECTLY BY BROKERS`}
      description={`${fmtNum(stats.listings)} businesses for sale in ${name} from ${fmtNum(stats.brokers)} broker websites. Median asking ${fmtMoney(stats.median_price)}. Real days on market, price-to-cash-flow, and a direct link to each broker.`}
      intro={`${fmtNum(stats.listings)} businesses are listed for sale in ${name} on ${fmtNum(stats.brokers)} business broker websites that DealLedger reads directly, and ${fmtNum(stats.new_30d)} of them first appeared in the last 30 days. Each listing shows how long it has really been on the market and links straight to the broker.`}
      stats={stats}
      rows={rows}
      crumbs={[{ href: '/', label: 'DealLedger' }, { href: '/businesses-for-sale', label: 'Businesses for sale' }, { href: path, label: name }]}
      groups={[
        { title: `By type in ${name}`, links: types.map((t) => ({ href: comboPath(code, t.vertical), label: TYPES[t.vertical].label, count: t.count })) },
        { title: 'Other states', links: others.map((o) => ({ href: statePath(o.code), label: STATES[o.code], count: o.count })) },
      ]}
    />
  );
}
