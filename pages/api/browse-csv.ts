// /businesses-for-sale/{...}.csv (rewritten here in next.config.js) — every
// listing behind a browse page, as CSV. Same source as the page
// (mv_seo_listings) and the same existence rules, but not capped at the
// page's LIST_LIMIT: the download is the complete set.
//
//   /businesses-for-sale/new-york/laundromats.csv   ?state=new-york&type=laundromats
//   /businesses-for-sale/new-york.csv               ?state=new-york
//   /businesses-for-sale/type/laundromats.csv       ?type=laundromats
//
// Public domain (CC0), like everything DealLedger publishes.
import type { NextApiRequest, NextApiResponse } from 'next';
import { getSupabase } from '../../lib/supabase';
import {
  TYPES, allStats, displayTitle, hasComboPage, hasStatePage, hasTypePage,
  pageUrl, stateFromSlug, typeFromSlug,
} from '../../lib/seo';

const COLS =
  'listing_number, title, city, state_code, state_source, vertical, vertical_source, price, cash_flow, revenue, ' +
  'dom_days_eff, dom_days_max, dom_basis, listed_after, estimated_listed_date, last_seen, relisted, broker_domain, source_url';

const HEADER = [
  'dealledger_url', 'title', 'display_title', 'city', 'state', 'state_source', 'business_type', 'type_source',
  'asking_price', 'cash_flow', 'price_to_cash_flow', 'revenue',
  'age_days_min', 'age_days_max', 'age_basis', 'listed_after', 'age_date', 'last_seen', 'relisted',
  'broker_domain', 'listing_url',
];

const cell = (v: unknown) => {
  if (v == null) return '';
  const s = String(v);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const stateSlug = typeof req.query.state === 'string' ? req.query.state : '';
  const typeSlug = typeof req.query.type === 'string' ? req.query.type : '';
  const code = stateSlug ? stateFromSlug(stateSlug) : null;
  const vertical = typeSlug ? typeFromSlug(typeSlug) : null;
  if ((stateSlug && !code) || (typeSlug && !vertical) || (!code && !vertical)) {
    return res.status(404).send('Not found');
  }

  // Only pages that exist get a download.
  const all = await allStats();
  const exists = all.some((s) =>
    code && vertical ? hasComboPage(s) && s.page_state === code && s.page_vertical === vertical
    : code ? hasStatePage(s) && s.page_state === code
    : hasTypePage(s) && s.page_vertical === vertical);
  if (!exists) return res.status(404).send('Not found');

  const lines: string[] = [HEADER.join(',')];
  const PAGE = 1000;
  for (let from = 0; ; from += PAGE) {
    let q = getSupabase().from('mv_seo_listings_v2').select(COLS);
    if (code) q = q.eq('state_code', code);
    if (vertical) q = q.eq('vertical', vertical);
    const { data, error } = await q
      .order('estimated_listed_date', { ascending: false, nullsFirst: false })
      .order('listing_number', { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) return res.status(500).send('Query failed');
    for (const r of (data || []) as any[]) {
      const multiple = r.price > 0 && r.cash_flow > 0 ? (r.price / r.cash_flow).toFixed(2) : null;
      lines.push([
        pageUrl(`/listing/${r.listing_number}`), r.title, displayTitle(r), r.city, r.state_code, r.state_source,
        TYPES[r.vertical]?.label ?? r.vertical, r.vertical_source,
        r.price > 0 ? r.price : null, r.cash_flow > 0 ? r.cash_flow : null, multiple, r.revenue > 0 ? r.revenue : null,
        r.dom_days_eff, r.dom_days_max, r.dom_basis, r.listed_after, r.estimated_listed_date, r.last_seen, r.relisted ? 'yes' : 'no',
        r.broker_domain, r.source_url,
      ].map(cell).join(','));
    }
    if (!data || data.length < PAGE) break;
  }

  const name = ['dealledger', stateSlug || 'us', typeSlug || 'all-types'].join('-');
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `inline; filename="${name}.csv"`);
  res.setHeader('Link', '<https://creativecommons.org/publicdomain/zero/1.0/>; rel="license"');
  res.setHeader('X-License', 'CC0-1.0 public domain. Source: DealLedger (dealledger.org). See /methodology.html');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'public, s-maxage=21600, stale-while-revalidate=86400');
  res.status(200).send(lines.join('\r\n') + '\r\n');
}

// Large national types (restaurants) can exceed the default 4MB response limit check.
export const config = { api: { responseLimit: false } };
