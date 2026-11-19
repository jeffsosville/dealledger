// lib/homeRender.ts — server-side fill for the homepage (see pages/index.tsx).
// Server-only: imports fs.

import fs from 'fs';
import path from 'path';
import { getSupabase } from './supabase';
import { TYPES, typePath } from './seo';

const PER_PAGE = 50;
const COLUMNS =
  'listing_number,title,state,price,source_url,estimated_listed_date,listed_after,dom_days_eff,dom_days_max,dom_basis';

type Row = {
  listing_number: string | number;
  title: string | null;
  state: string | null;
  price: number | null;
  source_url: string | null;
  estimated_listed_date: string | null;
  listed_after: string | null;
  dom_days_eff: number | null;
  dom_days_max: number | null;
  dom_basis: string | null; // window | observed | in_db | null
  last_cut_pct?: number | null; // latest approved asking-price change, when it was a cut
  last_cut_on?: string | null;
};

let template: string | null = null;
function getTemplate(): string {
  if (template === null) {
    template = fs.readFileSync(path.join(process.cwd(), 'lib', 'home.html'), 'utf8');
  }
  return template;
}

// --- rendering: mirrors paint() in lib/home.html ---------------------------

const esc = (s: unknown) =>
  String(s).replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' } as Record<string, string>)[c],
  );

function fmtPrice(p: number | null): string {
  if (!p) return '—';
  if (p >= 1000000) return '$' + (p / 1000000).toFixed(1) + 'M';
  if (p >= 1000) return '$' + Math.round(p / 1000) + 'K';
  return '$' + p.toLocaleString('en-US');
}

function cut(t: string | null, n: number): string {
  if (!t) return 'Untitled';
  return t.length <= n ? t : t.slice(0, n) + '…';
}

function dayClass(d: number): string {
  return d <= 30 ? 'd0' : d <= 90 ? 'd1' : d <= 365 ? 'd2' : 'd3';
}

function daysCell(r: Row): string {
  const d = r.dom_days_eff;
  if (d === null || d === undefined || !r.dom_basis) return '<span class="days">—</span>';
  const rnd = (x: number) => (x < 60 ? x : x <= 365 ? Math.round(x / 5) * 5 : Math.round(x / 10) * 10);
  if (r.dom_basis === 'window' && r.dom_days_max != null) {
    return `<span class="days ${dayClass(d)}" title="Days since it appeared, from complete crawls of the broker’s site">${d}</span>`;
  }
  const title = r.dom_basis === 'in_db'
    ? 'In DealLedger at least this long (date the record was first written to our database)'
    : 'Observed by DealLedger at least this long (first dated sighting in our crawl)';
  return `<span class="days ${dayClass(d)}" title="${title}">${rnd(d)}+</span>`;
}

function fmtD(s: string): string {
  const d = new Date(s + 'T00:00:00Z');
  return isNaN(d.getTime())
    ? esc(s)
    : d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
}

function seenCell(r: Row): string {
  if (!r.estimated_listed_date || !r.dom_basis) return '—';
  if (r.dom_basis === 'window') return `Listed ${fmtD(r.estimated_listed_date)}`;
  if (r.dom_basis === 'in_db') return `In DealLedger since ${fmtD(r.estimated_listed_date)}`;
  return `Observed since ${fmtD(r.estimated_listed_date)}`;
}

// Mirrors stateLabel() in lib/home.html.
const STATE_CODE: Record<string, string> = {
  alabama: 'AL', alaska: 'AK', arizona: 'AZ', arkansas: 'AR', california: 'CA', colorado: 'CO', connecticut: 'CT',
  delaware: 'DE', 'district of columbia': 'DC', florida: 'FL', georgia: 'GA', hawaii: 'HI', idaho: 'ID', illinois: 'IL',
  indiana: 'IN', iowa: 'IA', kansas: 'KS', kentucky: 'KY', louisiana: 'LA', maine: 'ME', maryland: 'MD',
  massachusetts: 'MA', michigan: 'MI', minnesota: 'MN', mississippi: 'MS', missouri: 'MO', montana: 'MT',
  nebraska: 'NE', nevada: 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
  'north carolina': 'NC', 'north dakota': 'ND', ohio: 'OH', oklahoma: 'OK', oregon: 'OR', pennsylvania: 'PA',
  'rhode island': 'RI', 'south carolina': 'SC', 'south dakota': 'SD', tennessee: 'TN', texas: 'TX', utah: 'UT',
  vermont: 'VT', virginia: 'VA', washington: 'WA', 'west virginia': 'WV', wisconsin: 'WI', wyoming: 'WY',
};
function stateLabel(s: string | null): string {
  if (!s) return '';
  const t = s.trim();
  return STATE_CODE[t.toLowerCase()] || t;
}

function brokerLink(url: string | null): string {
  const m = /^https?:\/\/(?:www\.)?([^\/:?#]+)/i.exec(url || '');
  if (!m) return '';
  const host = m[1].toLowerCase();
  return `<a class="brk" href="/broker/${host.replace(/[^a-z0-9]+/g, '-')}">${esc(host)}</a>`;
}

// Mirrors the de-duplication in paint(): the same business posted on two
// sites of one brokerage is shown once per page.
function dedupe(rows: Row[]): Row[] {
  const seen = new Set<string>();
  return rows.filter((r) => {
    const k = (r.title || '').trim().toLowerCase() + '|' + (r.price || '');
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

// Latest approved asking-price cut (public.price_history via v_listing_last_cut).
function cutTag(r: Row): string {
  if (r.last_cut_pct == null || !r.last_cut_on) return '';
  const d = new Date(r.last_cut_on + 'T00:00:00Z');
  const when = isNaN(d.getTime()) ? '' : ' · ' + d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
  return `<span class="cut" title="Asking price reduced, first seen at the lower price on ${esc(r.last_cut_on)}">↓${Math.abs(Math.round(Number(r.last_cut_pct)))}%${when}</span>`;
}

function rowsHtml(all: Row[]): string {
  const rows = dedupe(all);
  if (!rows.length) return '<tr><td colspan="5" class="empty">No listings match.</td></tr>';
  return rows
    .map((r) => {
      const n = encodeURIComponent(String(r.listing_number));
      return (
        '<tr>' +
        `<td class="seen" data-state="${r.state ? ' · ' + esc(stateLabel(r.state)) : ''}">${seenCell(r)}</td>` +
        `<td class="title"><a href="/listing/${n}">${esc(cut(r.title, 70))}</a>` +
        (r.source_url
          ? `<a class="src" href="/go/${n}?s=home" target="_blank" rel="nofollow noopener" title="Open the listing on the broker’s website" aria-label="Open the listing on the broker’s website">listing ↗&#xFE0E;</a>`
          : '') +
        brokerLink(r.source_url) +
        '</td>' +
        `<td class="state">${esc(stateLabel(r.state) || '—')}</td>` +
        `<td class="num price">${fmtPrice(r.price)}${cutTag(r)}</td>` +
        `<td class="num daysc">${daysCell(r)}</td>` +
        '</tr>'
      );
    })
    .join('');
}

// JSON inside a <script> tag: escape "<" so no value can close the tag.
const scriptJson = (v: unknown) => JSON.stringify(v).replace(/</g, '\\u003c');

export type HomeData = {
  // The ledger's default view: listings with an asking price, newest first.
  shown: number;
  rows: Row[];
  // Headline figures, over every listing on the site.
  total: number;
  brokers: number | null;
  producing: number | null;
};

const fmt = (n: number) => n.toLocaleString('en-US');

function ogDescription(d: HomeData | null): string {
  const base = "An open, daily record of US businesses listed for sale on brokers' own websites. Free, CC0.";
  if (!d) return base;
  return `${fmt(d.total)} listings on the record. ` + base;
}

// Server-rendered links to every business-type page, so crawlers reach them from the homepage.
const typeLinksHtml = (): string =>
  Object.keys(TYPES)
    .sort((a, b) => TYPES[a].label.localeCompare(TYPES[b].label))
    .map((v) => `<a href="${typePath(v)}">${esc(TYPES[v].label)} for sale</a>`)
    .join('');

export function renderHome(d: HomeData | null): string {
  const html = getTemplate().replace('<!--DL:TYPES-->', typeLinksHtml());
  const og = esc(ogDescription(d));
  if (!d) {
    return html
      .split('<!--DL:OGDESC-->').join(og)
      .replace('<!--DL:TOTAL-->', '—')
      .replace('<!--DL:BROKERS-->', '—')
      .replace('<!--DL:PRODUCING-->', '—')
      .replace('<!--DL:SHOWN-->', '—')
      .replace('<!--DL:ROWS-->', '')
      .replace('<!--DL:PAGEINFO-->', '')
      .replace('<!--DL:INITIAL-->', '');
  }
  const pages = Math.max(1, Math.ceil(d.shown / PER_PAGE));
  return html
    .split('<!--DL:OGDESC-->').join(og)
    .replace('<!--DL:TOTAL-->', fmt(d.total))
    .replace('<!--DL:BROKERS-->', d.brokers === null ? '—' : fmt(d.brokers))
    .replace('<!--DL:PRODUCING-->', d.producing === null ? '—' : fmt(d.producing))
    .replace('<!--DL:SHOWN-->', fmt(d.shown))
    .replace('<!--DL:ROWS-->', rowsHtml(d.rows))
    .replace('<!--DL:PAGEINFO-->', `Page 1 of ${fmt(pages)}`)
    .replace(
      '<!--DL:INITIAL-->',
      `<script>window.__DL_INITIAL__=${scriptJson({ total: d.shown, rows: d.rows })};</script>`,
    );
}

// --- data -------------------------------------------------------------------

export async function loadHome(): Promise<HomeData | null> {
  try {
    const sb = getSupabase();
    const [page, stats] = await Promise.all([
      // Same query the page script runs for its default view.
      sb
        .from('mv_listings_page_v2')
        .select(COLUMNS, { count: 'exact' })
        .eq('source', 'broker_direct')
        .not('price', 'is', null)
        .order('dom_days_eff', { ascending: true, nullsFirst: false })
        .range(0, PER_PAGE - 1),
      // Headline figures come only from public_stats.
      sb.from('public_stats').select('listings_on_site, broker_sites_on_site, broker_sites_producing').limit(1).maybeSingle(),
    ]);
    const ps = stats.data as { listings_on_site?: number; broker_sites_on_site?: number; broker_sites_producing?: number } | null;
    if (page.error || !page.data || stats.error || typeof ps?.listings_on_site !== 'number') {
      return null;
    }
    const rows = page.data as unknown as Row[];
    // Mark rows whose latest approved price change was a reduction.
    if (rows.length) {
      const { data: cuts } = await sb
        .from('v_listing_last_cut')
        .select('listing_number,last_cut_pct,last_cut_on')
        .in('listing_number', rows.map((r) => r.listing_number));
      const byNum = new Map((cuts || []).map((c: { listing_number: number }) => [String(c.listing_number), c]));
      for (const r of rows) Object.assign(r, byNum.get(String(r.listing_number)) || {});
    }
    const brokers = ps.broker_sites_on_site;
    const producing = ps.broker_sites_producing;
    return {
      shown: page.count ?? rows.length,
      rows,
      total: ps.listings_on_site,
      brokers: typeof brokers === 'number' ? brokers : null,
      producing: typeof producing === 'number' ? producing : null,
    };
  } catch {
    return null;
  }
}
