// /businesses-for-sale/type/{type} — one business type nationwide.
import type { GetStaticPaths, GetStaticProps, InferGetStaticPropsType } from 'next';
import SeoPage from '../../../components/SeoPage';
import {
  Row, Stats, STATES, TYPES, allStats, comboPath, fmtMoney, fmtMultiple, fmtNum, hasComboPage,
  hasTypePage, listingsFor, typeFromSlug, typePath,
} from '../../../lib/seo';
import { TYPE_GUIDES } from '../../../lib/typeGuides';

type Props = { vertical: string; stats: Stats | null; thin: boolean; rows: Row[]; byState: { code: string; count: number }[]; otherTypes: { vertical: string; count: number }[] };

export const getStaticPaths: GetStaticPaths = async () => ({ paths: [], fallback: 'blocking' });

export const getStaticProps: GetStaticProps<Props> = async ({ params }) => {
  const vertical = typeFromSlug(String(params?.type || ''));
  if (!vertical) return { notFound: true, revalidate: 86400 };
  const all = await allStats();
  const found = all.find((s) => s.scope === 'vertical' && s.page_vertical === vertical);
  // Below TYPE_MIN: render a thin noindex page instead of a 404 (sitemap still uses hasTypePage).
  const thin = !found || !hasTypePage(found);
  const stats = thin ? null : found!;
  const rows = await listingsFor({ vertical });
  const byState = all
    .filter((s) => hasComboPage(s) && s.page_vertical === vertical)
    .sort((a, b) => b.listings - a.listings)
    .map((s) => ({ code: s.page_state as string, count: s.listings }));
  const otherTypes = all
    .filter((s) => hasTypePage(s) && s.page_vertical !== vertical)
    .sort((a, b) => b.listings - a.listings)
    .map((s) => ({ vertical: s.page_vertical as string, count: s.listings }));
  return { props: { vertical, stats, thin, rows, byState, otherTypes }, revalidate: thin ? 60 * 60 * 24 : 60 * 60 * 6 };
};

export default function TypePage({ vertical, stats: s, thin, rows, byState, otherTypes }: InferGetStaticPropsType<typeof getStaticProps>) {
  const t = TYPES[vertical];
  const path = typePath(vertical);
  const guide = TYPE_GUIDES[vertical];
  const n = s ? s.listings : rows.length;
  const multiple = s && s.median_multiple != null ? ` and a median of ${fmtMultiple(s.median_multiple)} cash flow` : '';
  return (
    <SeoPage
      path={path}
      noindex={thin}
      title={guide && s ? guide.title.replace('{n}', fmtNum(s.listings)) : `${t.label} for Sale${n ? ` (${fmtNum(n)} listings)` : ''} — DealLedger`}
      h1={guide?.h1 || `${t.label} for Sale`}
      guide={guide}
      eyebrow={`NATIONWIDE · ${t.label.toUpperCase()}`}
      description={s
        ? `${fmtNum(s.listings)} ${t.noun} for sale across the US from ${fmtNum(s.brokers)} broker websites. Median asking ${fmtMoney(s.median_price)}. Evidence-based listing age and a direct link to each broker.`
        : `${t.label} for sale across the US, tracked from broker websites by DealLedger.`}
      intro={s
        ? `${fmtNum(s.listings)} ${t.noun} are for sale on ${fmtNum(s.brokers)} broker websites that DealLedger reads directly, with ${fmtNum(s.new_30d)} new in the last 30 days, a median asking price of ${fmtMoney(s.median_price)}${multiple}.`
        : n === 0
          ? `DealLedger isn't tracking any ${t.noun} for sale right now. We check broker websites daily.`
          : `DealLedger is tracking ${fmtNum(n)} ${t.noun} for sale right now. Every listing below links to the broker who has it.`}
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
