// /businesses-for-sale — hub: every state and business type with a page.
import type { GetStaticProps, InferGetStaticPropsType } from 'next';
import SeoPage from '../../components/SeoPage';
import { STATES, TYPES, allStats, fmtNum, hasStatePage, hasTypePage, statePath, typePath } from '../../lib/seo';

type Props = { states: { code: string; count: number }[]; types: { vertical: string; count: number }[]; total: number };

export const getStaticProps: GetStaticProps<Props> = async () => {
  const all = await allStats();
  const states = all
    .filter(hasStatePage)
    .sort((a, b) => STATES[a.page_state!].localeCompare(STATES[b.page_state!]))
    .map((s) => ({ code: s.page_state as string, count: s.listings }));
  const types = all
    .filter(hasTypePage)
    .sort((a, b) => b.listings - a.listings)
    .map((s) => ({ vertical: s.page_vertical as string, count: s.listings }));
  const total = all.filter((s) => s.scope === 'state').reduce((n, s) => n + s.listings, 0);
  return { props: { states, types, total }, revalidate: 60 * 60 * 6 };
};

export default function Hub({ states, types, total }: InferGetStaticPropsType<typeof getStaticProps>) {
  return (
    <SeoPage
      path="/businesses-for-sale"
      title="Businesses for Sale by State and Type — DealLedger"
      h1="Businesses for Sale"
      eyebrow="BROWSE BY STATE & TYPE"
      description={`Browse ${fmtNum(total)} businesses for sale by state and business type, collected directly from business broker websites. True days on market and a direct link to every broker.`}
      intro={`${fmtNum(total)} businesses for sale, collected directly from business broker websites and sorted by state and type. Every listing shows how long it has really been listed and links straight to the broker. No accounts, no gated contact forms.`}
      stats={null}
      rows={[]}
      crumbs={[{ href: '/', label: 'DealLedger' }, { href: '/businesses-for-sale', label: 'Businesses for sale' }]}
      groups={[
        { title: 'By state', links: states.map((s) => ({ href: statePath(s.code), label: STATES[s.code], count: s.count })) },
        { title: 'By type, nationwide', links: types.map((t) => ({ href: typePath(t.vertical), label: TYPES[t.vertical].label, count: t.count })) },
      ]}
    />
  );
}
