// components/NichePage.tsx — a specific business type (lib/niches.ts), nationwide or in one state.
import SeoPage from './SeoPage';
import { STATES, fmtMoney, fmtMultiple, fmtNum, stateSlug, statePath } from '../lib/seo';
import { nichePath, type NichePageProps } from '../lib/niches';
import { NICHE_GUIDES } from '../lib/nicheGuides';

const comboNichePath = (code: string, slug: string) => `/businesses-for-sale/${stateSlug(code)}/${slug}`;

export default function NichePage(p: NichePageProps) {
  const s = p.stats;
  const where = p.state ? ` in ${STATES[p.state]}` : '';
  const path = p.state ? comboNichePath(p.state, p.slug) : nichePath(p.slug);
  const n = s.listings;
  const guide = p.state ? undefined : NICHE_GUIDES[p.slug];
  const multiple = s.median_multiple != null ? `, a median of ${fmtMultiple(s.median_multiple)} cash flow` : '';
  const full = !p.thin;
  return (
    <SeoPage
      path={path}
      noindex={p.thin}
      noCsv
      alert={{ state: p.state, vertical: p.slug, label: `${p.label}${where}` }}
      title={guide && full ? guide.title.replace('{n}', fmtNum(n))
        : `${p.label} for Sale${where}${n ? ` (${fmtNum(n)})` : ''} — DealLedger`}
      h1={guide?.h1 || `${p.label} for Sale${where}`}
      guide={guide}
      eyebrow={`${p.state || 'NATIONWIDE'} · ${p.label.toUpperCase()}`}
      description={full
        ? `${fmtNum(n)} ${p.noun} for sale${where}, listed by ${fmtNum(s.brokers)} brokers. Median asking ${fmtMoney(s.median_price)}${multiple}. Evidence-based listing age and a direct link to each broker.`
        : `${p.label} for sale${where}, tracked from broker websites by DealLedger.`}
      intro={full
        ? `${fmtNum(n)} ${p.noun} are for sale${where} on ${fmtNum(s.brokers)} broker websites that DealLedger reads directly, with ${fmtNum(s.new_30d)} new in the last 30 days. Median asking price is ${fmtMoney(s.median_price)}${multiple}. Every listing below links to the broker who has it.`
        : n === 0
          ? `DealLedger isn't tracking any ${p.noun} for sale${where} right now. We check broker websites daily; see related businesses below.`
          : `DealLedger is tracking ${fmtNum(n)} of these for sale${where} right now. Every listing below links to the broker who has it.`}
      stats={full ? s : null}
      rows={p.rows}
      showState={!p.state}
      crumbs={[
        { href: '/', label: 'DealLedger' },
        { href: '/businesses-for-sale', label: 'Businesses for sale' },
        ...(p.state ? [{ href: statePath(p.state), label: STATES[p.state] }] : []),
        { href: path, label: p.label },
      ]}
      groups={[
        {
          title: p.state ? `${p.label} in other states` : `${p.label} by state`,
          links: [
            ...(p.state && p.nationalCount >= 10 ? [{ href: nichePath(p.slug), label: `All ${p.label.toLowerCase()} nationwide`, count: p.nationalCount }] : []),
            ...p.states.map((o) => ({ href: comboNichePath(o.code, p.slug), label: STATES[o.code], count: o.count })),
          ],
        },
        {
          title: `Related businesses${where}`,
          links: p.related.map((r) => ({ href: p.state ? comboNichePath(p.state, r.slug) : nichePath(r.slug), label: r.label, count: r.count })),
        },
      ]}
    />
  );
}
