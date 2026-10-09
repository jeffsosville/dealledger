// /businesses-for-sale/{state}/{type} — one business type in one state.
import type { GetStaticPaths, GetStaticProps, InferGetStaticPropsType } from 'next';
import SeoPage from '../../../components/SeoPage';
import {
  Row, Stats, STATES, TYPES, allStats, comboPath, fmtMoney, fmtMultiple, fmtNum, hasComboPage,
  hasStatePage, hasTypePage, listingsFor, stateFromSlug, statePath, typeFromSlug, typePath,
} from '../../../lib/seo';

type Props = {
  code: string; vertical: string; stats: Stats; rows: Row[]; hasState: boolean; hasType: boolean;
  sameTypeElsewhere: { code: string; count: number }[]; otherTypesHere: { vertical: string; count: number }[];
};

export const getStaticPaths: GetStaticPaths = async () => ({ paths: [], fallback: 'blocking' });

export const getStaticProps: GetStaticProps<Props> = async ({ params }) => {
  const code = stateFromSlug(String(params?.state || ''));
  const vertical = typeFromSlug(String(params?.type || ''));
  if (!code || !vertical) return { notFound: true, revalidate: 86400 };
  const all = await allStats();
  const stats = all.find((s) => s.scope === 'vertical_state' && s.page_state === code && s.page_vertical === vertical);
  if (!stats || !hasComboPage(stats)) return { notFound: true, revalidate: 86400 };
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
      code, vertical, stats, rows, sameTypeElsewhere, otherTypesHere,
      hasState: all.some((s) => hasStatePage(s) && s.page_state === code),
      hasType: all.some((s) => hasTypePage(s) && s.page_vertical === vertical),
    },
    revalidate: 60 * 60 * 6,
  };
};

export default function ComboPage(p: InferGetStaticPropsType<typeof getStaticProps>) {
  const name = STATES[p.code];
  const t = TYPES[p.vertical];
  const path = comboPath(p.code, p.vertical);
  const s = p.stats;
  const multiple = s.median_multiple != null ? `, a median of ${fmtMultiple(s.median_multiple)} cash flow` : '';
  return (
    <SeoPage
      path={path}
      title={`${t.label} for Sale in ${name} (${fmtNum(s.listings)}) — DealLedger`}
      h1={`${t.label} for Sale in ${name}`}
      eyebrow={`${p.code} · ${t.label.toUpperCase()}`}
      description={`${fmtNum(s.listings)} ${t.noun} for sale in ${name}, listed by ${fmtNum(s.brokers)} brokers. Median asking ${fmtMoney(s.median_price)}${multiple}. True days on market and a direct link to each broker.`}
      intro={`${fmtNum(s.listings)} ${t.noun} are for sale in ${name} across ${fmtNum(s.brokers)} broker websites, with ${fmtNum(s.new_30d)} new in the last 30 days. Median asking price is ${fmtMoney(s.median_price)}${multiple}. Every listing below links to the broker who has it.`}
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
