// /businesses-for-sale/{state}/{type} — one business type in one state.
import type { GetStaticPaths, GetStaticProps, InferGetStaticPropsType } from 'next';
import SeoPage from '../../../components/SeoPage';
import {
  Row, Stats, STATES, TYPES, allStats, comboPath, fmtMoney, fmtMultiple, fmtNum, hasComboPage,
  hasStatePage, hasTypePage, listingsFor, stateFromSlug, statePath, typeFromSlug, typePath,
} from '../../../lib/seo';

type Props = {
  code: string; vertical: string; stats: Stats | null; thin: boolean; rows: Row[]; hasState: boolean; hasType: boolean;
  sameTypeElsewhere: { code: string; count: number }[]; otherTypesHere: { vertical: string; count: number }[];
};

export const getStaticPaths: GetStaticPaths = async () => ({ paths: [], fallback: 'blocking' });

export const getStaticProps: GetStaticProps<Props> = async ({ params }) => {
  const code = stateFromSlug(String(params?.state || ''));
  const vertical = typeFromSlug(String(params?.type || ''));
  if (!code || !vertical) return { notFound: true, revalidate: 86400 };
  const all = await allStats();
  const found = all.find((s) => s.scope === 'vertical_state' && s.page_state === code && s.page_vertical === vertical);
  // Below COMBO_MIN we still render (people land here from links and AI answers),
  // but as a thin noindex page without the stats block. Sitemap keeps using hasComboPage.
  const thin = !found || !hasComboPage(found);
  const stats = thin ? null : found!;
  const rows = await listingsFor({ state: code, vertical });
  const sameTypeElsewhere = all
    .filter((s) => hasComboPage(s) && s.page_vertical === vertical && s.page_state !== code)
    .sort((a, b) => b.listings - a.listings)
    .map((s) => ({ code: s.page_state as string, count: s.listings }));
  const otherTypesHere = all
    .filter((s) => hasComboPage(s) && s.page_state === code && s.page_vertical !== vertical)
    .sort((a, b) => b.listings - a.listings)
    .map((s) => ({ vertical: s.page_vertical as string, count: s.listings }));
  return {
    props: {
      code, vertical, stats, thin, rows, sameTypeElsewhere, otherTypesHere,
      hasState: all.some((s) => hasStatePage(s) && s.page_state === code),
      hasType: all.some((s) => hasTypePage(s) && s.page_vertical === vertical),
    },
    revalidate: thin ? 60 * 60 * 24 : 60 * 60 * 6,
  };
};

export default function ComboPage(p: InferGetStaticPropsType<typeof getStaticProps>) {
  const name = STATES[p.code];
  const t = TYPES[p.vertical];
  const path = comboPath(p.code, p.vertical);
  const s = p.stats;
  const n = s ? s.listings : p.rows.length;
  const multiple = s && s.median_multiple != null ? `, a median of ${fmtMultiple(s.median_multiple)} cash flow` : '';
  const thinIntro = n === 0
    ? `DealLedger isn't tracking any ${t.noun} for sale in ${name} right now. We check broker websites daily; see nearby states and related business types below.`
    : `DealLedger is tracking ${n === 1 ? 'one' : fmtNum(n)} ${n === 1 ? t.noun.replace(/s$/, '') : t.noun} for sale in ${name} right now. Every listing below links to the broker who has it.`;
  return (
    <SeoPage
      path={path}
      noindex={p.thin}
      title={`${t.label} for Sale in ${name}${n ? ` (${fmtNum(n)})` : ''} — DealLedger`}
      h1={`${t.label} for Sale in ${name}`}
      eyebrow={`${p.code} · ${t.label.toUpperCase()}`}
      description={s
        ? `${fmtNum(s.listings)} ${t.noun} for sale in ${name}, listed by ${fmtNum(s.brokers)} brokers. Median asking ${fmtMoney(s.median_price)}${multiple}. Evidence-based listing age and a direct link to each broker.`
        : `${t.label} for sale in ${name}, tracked from broker websites by DealLedger.`}
      intro={s
        ? `${fmtNum(s.listings)} ${t.noun} are for sale in ${name} across ${fmtNum(s.brokers)} broker websites, with ${fmtNum(s.new_30d)} new in the last 30 days. Median asking price is ${fmtMoney(s.median_price)}${multiple}. Every listing below links to the broker who has it.`
        : thinIntro}
      stats={s}
      rows={p.rows}
      crumbs={[
        { href: '/', label: 'DealLedger' },
        { href: '/businesses-for-sale', label: 'Businesses for sale' },
        ...(p.hasState ? [{ href: statePath(p.code), label: name }] : []),
        { href: path, label: t.label },
      ]}
      groups={[
        { title: `Other businesses for sale in ${name}`, links: p.otherTypesHere.map((o) => ({ href: comboPath(p.code, o.vertical), label: TYPES[o.vertical].label, count: o.count })) },
        {
          title: `${t.label} in other states`,
          links: [
            ...(p.hasType ? [{ href: typePath(p.vertical), label: `All ${t.label.toLowerCase()} nationwide` }] : []),
            ...p.sameTypeElsewhere.map((o) => ({ href: comboPath(o.code, p.vertical), label: STATES[o.code], count: o.count })),
          ],
        },
      ]}
    />
  );
}
