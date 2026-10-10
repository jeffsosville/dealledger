// lib/homeRender.ts — server-side fill for the homepage (see pages/index.tsx).
// Server-only: imports fs.

import fs from 'fs';
import path from 'path';
import { getSupabase } from './supabase';

const PER_PAGE = 50;
const COLUMNS =
  'listing_number,title,state,price,source_url,estimated_listed_date,dom_days_eff,dom_basis';

type Row = {
  listing_number: string | number;
  title: string | null;
  state: string | null;
  price: number | null;
  source_url: string | null;
  estimated_listed_date: string | null;
  dom_days_eff: number | null;
  dom_basis: string | null;
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
  if (d === null || d === undefined) return '<span class="days">—</span>';
  const shown = d < 60 ? d : d <= 365 ? Math.round(d / 5) * 5 : Math.round(d / 10) * 10;
  const floor = r.dom_basis === 'floor';
  const title = floor
    ? 'Already listed when we started tracking this broker — at least this long'
    : 'Days since we first saw this listing on the broker’s site';
  return `<span class="days ${dayClass(d)}" title="${title}">${shown}${floor ? '+' : ''}</span>`;
}

function seenCell(r: Row): string {
  if (!r.estimated_listed_date) return '—';
  const d = new Date(r.estimated_listed_date + 'T00:00:00Z');
  const s = isNaN(d.getTime())
    ? esc(r.estimated_listed_date)
    : d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
  return (r.dom_basis === 'floor' ? '≤ ' : '') + s;
}

function brokerLink(url: string | null): string {
  const m = /^https?:\/\/(?:www\.)?([^\/:?#]+)/i.exec(url || '');
  if (!m) return '';
  const host = m[1].toLowerCase();
  return `<a class="brk" href="/broker/${host.replace(/[^a-z0-9]+/g, '-')}">${esc(host)}</a>`;
}

function rowsHtml(rows: Row[]): string {
  if (!rows.length) return '<tr><td colspan="5" class="empty">No listings match.</td></tr>';
  return rows
    .map((r) => {
      const n = encodeURIComponent(String(r.listing_number));
      return (
        '<tr>' +
        `<td class="seen" data-state="${r.state ? ' · ' + esc(r.state) : ''}">${seenCell(r)}</td>` +
        `<td class="title"><a href="/listing/${n}">${esc(cut(r.title, 70))}</a>` +
        (r.source_url
          ? `<a class="src" href="/go/${n}?s=home" target="_blank" rel="nofollow noopener" title="Open the broker’s listing" aria-label="Open the broker’s listing">↗&#xFE0E;</a>`
          : '') +
        brokerLink(r.source_url) +
        '</td>' +
        `<td class="state">${esc(r.state || '—')}</td>` +
        `<td class="num price">${fmtPrice(r.price)}</td>` +
        `<td class="num daysc">${daysCell(r)}</td>` +
        '</tr>'
      );
    })
    .join('');
}

// JSON inside a <script> tag: escape "<" so no value can close the tag.
const scriptJson = (v: unknown) => JSON.stringify(v).replace(/</g, '\\u003c');

export function renderHome(data: { total: number; rows: Row[] } | null): string {
  const html = getTemplate();
  if (!data) {
    return html
      .split('<!--DL:TOTAL-->').join('—')
      .replace('<!--DL:ROWS-->', '')
      .replace('<!--DL:PAGEINFO-->', '')
      .replace('<!--DL:INITIAL-->', '');
  }
  const pages = Math.max(1, Math.ceil(data.total / PER_PAGE));
  return html
    .split('<!--DL:TOTAL-->').join(data.total.toLocaleString('en-US'))
    .replace('<!--DL:ROWS-->', rowsHtml(data.rows))
    .replace('<!--DL:PAGEINFO-->', `Page 1 of ${pages.toLocaleString('en-US')}`)
    .replace('<!--DL:INITIAL-->', `<script>window.__DL_INITIAL__=${scriptJson(data)};</script>`);
}

// --- data: same query the page script runs on first load -------------------

export async function loadFirstPage(): Promise<{ total: number; rows: Row[] } | null> {
  try {
    const sb = getSupabase();
    const { data, count, error } = await sb
      .from('mv_listings_page')
      .select(COLUMNS, { count: 'exact' })
      .eq('source', 'broker_direct')
      .order('dom_days_eff', { ascending: true, nullsFirst: false })
      .range(0, PER_PAGE - 1);
    if (error || !data) return null;
    const rows = data as unknown as Row[];
    return { total: count ?? rows.length, rows };
  } catch {
    return null;
  }
}

