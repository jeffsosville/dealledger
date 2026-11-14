// /businesses-for-sale/type/{type} — one business type nationwide.
import type { GetStaticPaths, GetStaticProps, InferGetStaticPropsType } from 'next';
import SeoPage from '../../../components/SeoPage';
import {
  Row, Stats, STATES, TYPES, allStats, comboPath, fmtMoney, fmtMultiple, fmtNum, hasComboPage,
  hasTypePage, listingsFor, typeFromSlug, typePath,
} from '../../../lib/seo';
import { TYPE_GUIDES } from '../../../lib/typeGuides';
import NichePage from '../../../components/NichePage';
import { NICHE_MIN, VERTICAL_NICHE_GROUPS, nicheCounts, nichePageProps, nichePath, type NichePageProps } from '../../../lib/niches';

type TypeProps = { kind: 'type'; vertical: string; stats: Stats | null; thin: boolean; rows: Row[]; byState: { code: string; count: number }[]; otherTypes: { vertical: string; count: number }[]; niches: { slug: string; label: string; count: number }[] };

type Props = TypeProps | ({ kind: 'niche' } & NichePageProps);

export const getStaticPaths: GetStaticPaths = async () => ({ paths: [], fallback: 'blocking' });

export const getStaticProps: GetStaticProps<Props> = async ({ params }) => {
  const slug = String(params?.type || '');
  const vertical = typeFromSlug(slug);
  if (!vertical) {
    const niche = await nichePageProps(slug, null);
    if (!niche) return { notFound: true, revalidate: 86400 };
    return { props: { kind: 'niche', ...niche }, revalidate: niche.thin ? 60 * 60 * 24 : 60 * 60 * 6 };
  }
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
  const groups = VERTICAL_NICHE_GROUPS[vertical] || [];
  const niches = groups.length
    ? (await nicheCounts()).filter((c) => groups.includes(c.group) && c.count >= NICHE_MIN)
        .sort((a, b) => b.count - a.count).map(({ slug, label, count }) => ({ slug, label, count }))
    : [];
  return { props: { kind: 'type', vertical, stats, thin, rows, byState, otherTypes, niches }, revalidate: thin ? 60 * 60 * 24 : 60 * 60 * 6 };
};

export default function TypeOrNichePage(p: InferGetStaticPropsType<typeof getStaticProps>) {
  return p.kind === 'niche' ? <NichePage {...p} /> : <TypePage {...p} />;
}

function TypePage({ vertical, stats: s, thin, rows, byState, otherTypes, niches }: TypeProps) {
  const t = TYPES[vertical];
  const path = typePath(vertical);
  const guide = TYPE_GUIDES[vertical];
  const n = s ? s.listings : rows.length;
  const multiple = s && s.median_multiple != null ? ` and a median of ${fmtMultiple(s.median_multiple)} cash flow` : '';
  return (
    <SeoPage
      path={path}
      noindex={thin}
      alert={{ vertical, label: t.label }}
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
        { title: `${t.label} by specialty`, links: niches.map((o) => ({ href: nichePath(o.slug), label: o.label, count: o.count })) },
        { title: `${t.label} by state`, links: byState.map((o) => ({ href: comboPath(o.code, vertical), label: STATES[o.code], count: o.count })) },
        { title: 'Other business types', links: otherTypes.map((o) => ({ href: typePath(o.vertical), label: TYPES[o.vertical].label, count: o.count })) },
      ]}
    />
  );
}
