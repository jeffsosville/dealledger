// /businesses-for-sale/type/{type} — one business type nationwide.
import type { GetStaticPaths, GetStaticProps, InferGetStaticPropsType } from 'next';
import SeoPage from '../../../components/SeoPage';
import {
  Row, Stats, STATES, TYPES, allStats, comboPath, fmtMoney, fmtMultiple, fmtNum, hasComboPage,
  hasTypePage, listingsFor, typeFromSlug, typePath,
} from '../../../lib/seo';

type Props = { vertical: string; stats: Stats; rows: Row[]; byState: { code: string; count: number }[]; otherTypes: { vertical: string; count: number }[] };

export const getStaticPaths: GetStaticPaths = async () => ({ paths: [], fallback: 'blocking' });

export const getStaticProps: GetStaticProps<Props> = async ({ params }) => {
  const vertical = typeFromSlug(String(params?.type || ''));
  if (!vertical) return { notFound: true, revalidate: 86400 };
  const all = await allStats();
  const stats = all.find((s) => s.scope === 'vertical' && s.page_vertical === vertical);
  if (!stats || !hasTypePage(stats)) return { notFound: true, revalidate: 86400 };
  const rows = await listingsFor({ vertical });
  const byState = all
    .filter((s) => hasComboPage(s) && s.page_vertical === vertical)
    .sort((a, b) => b.listings - a.listings)
    .map((s) => ({ code: s.page_state as string, count: s.listings }));
  const otherTypes = all
    .filter((s) => hasTypePage(s) && s.page_vertical !== vertical)
    .sort((a, b) => b.listings - a.listings)
    .map((s) => ({ vertical: s.page_vertical as string, count: s.listings }));
  return { props: { vertical, stats, rows, byState, otherTypes }, revalidate: 60 * 60 * 6 };
};

export default function TypePage({ vertical, stats: s, rows, byState, otherTypes }: InferGetStaticPropsType<typeof getStaticProps>) {
  const t = TYPES[vertical];
  const path = typePath(vertical);
  const multiple = s.median_multiple != null ? ` and a median of ${fmtMultiple(s.median_multiple)} cash flow` : '';
  return (
    <SeoPage
      path={path}
      title={`${t.label} for Sale (${fmtNum(s.listings)} listings) — DealLedger`}
      h1={`${t.label} for Sale`}
      eyebrow={`NATIONWIDE · ${t.label.toUpperCase()}`}
      description={`${fmtNum(s.listings)} ${t.noun} for sale across the US from ${fmtNum(s.brokers)} broker websites. Median asking ${fmtMoney(s.median_price)}. True days on market and a direct link to each broker.`}
      intro={`${fmtNum(s.listings)} ${t.noun} are for sale on ${fmtNum(s.brokers)} broker websites that DealLedger reads directly, with ${fmtNum(s.new_30d)} new in the last 30 days, a median asking price of ${fmtMoney(s.median_price)}${multiple}.`}
      stats={s}
      rows={rows}
      showState
      crumbs={[{ href: '/', label: 'DealLedger' }, { href: '/businesses-for-sale', label: 'Businesses for sale' }, { href: path, label: t.label }]}
      groups={[
        { title: `${t.label} by state`, links: byState.map((o) => ({ href: comboPath(o.code, vertical), label: STATES[o.code], count: o.count })) },
        { title: 'Other business types', links: otherTypes.map((o) => ({ href: typePath(o.vertical), label: TYPES[o.vertical].label, count: o.count })) },
      ]}
    />
  );
}
