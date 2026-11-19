// /api/mcp — DealLedger's public MCP server (read-only, no auth).
//
// Three tools, each returning evidence, dates, a confidence level and a
// permanent DealLedger URL:
//   get_listing_history   — what DealLedger has observed about one listing
//   search_price_changes  — reviewed asking-price changes, filterable
//   check_relisting       — whether a listing has relist evidence
//
// Rules this file must keep (same as the listing page):
//   - Listing age comes only from mv_listings_page_v2's evidence hierarchy and
//     is reported as observed age, never as verified days on market.
//   - Price changes come only from public.price_history (reviewed, broker-direct)
//     and are reported as observation windows; the exact day is unknown.
//   - The relisted flag is a heuristic match, so it is reported as an
//     unverified signal, never as a fact.
//
// Stateless Streamable HTTP: each POST gets a fresh server (Vercel serverless).
import type { NextApiRequest, NextApiResponse } from 'next';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { z } from 'zod';
import { getSupabase } from '../../lib/supabase';
import { STATES, TYPES, displayTitle, stateFromSlug, typeFromSlug } from '../../lib/seo';

const SITE = 'https://dealledger.org';
const SOURCE = 'DealLedger (dealledger.org) — CC0 public record of businesses for sale, observed directly on broker websites';
const listingUrl = (n: number) => `${SITE}/listing/${n}`;
const day = (v: unknown) => (v ? String(v).slice(0, 10) : null);
const hostOf = (url?: string | null) =>
  url ? url.replace(/^https?:\/\/(www\.)?/i, '').split(/[/?#:]/)[0].toLowerCase() || null : null;

function toStateCode(s?: string): string | null {
  if (!s) return null;
  const t = s.trim();
  if (STATES[t.toUpperCase()]) return t.toUpperCase();
  return stateFromSlug(t.toLowerCase().replace(/[^a-z]+/g, '-'));
}
function toVertical(s?: string): string | null {
  if (!s) return null;
  const t = s.trim().toLowerCase();
  if (TYPES[t]) return t;
  return typeFromSlug(t.replace(/[\s_]+/g, '-'));
}
// Accept 1166992202 or "https://dealledger.org/listing/1166992202".
function toListingNumber(v: string | number): number | null {
  const m = String(v).match(/(\d{5,})/);
  return m ? Number(m[1]) : null;
}

const json = (v: unknown) => ({ content: [{ type: 'text' as const, text: JSON.stringify(v, null, 2) }] });
const fail = (msg: string) => ({ content: [{ type: 'text' as const, text: msg }], isError: true });

// Observed age: the listing page's evidence hierarchy, with a confidence level.
const AGE_CONFIDENCE: Record<string, string> = {
  window: 'high — appeared between two complete crawls of the broker site',
  observed: 'medium — first dated sighting; the listing may be older',
};
function observedAge(rec: any) {
  if (!rec?.dom_basis) return { status: 'unknown', note: 'No dated evidence for this listing.' };
  // A database write date is not proof the listing was publicly observed that day:
  // no age, no day count — only the record date, labelled as such.
  if (rec.dom_basis === 'in_db') {
    return {
      status: 'unknown',
      first_recorded_in_dealledger: day(rec.estimated_listed_date),
      note: 'DealLedger has no dated sighting for this listing. The date above is when the record was created, not when the listing was observed or first published.',
    };
  }
  return {
    label: rec.dom_display,
    listed_on_or_before: day(rec.estimated_listed_date),
    listed_after: day(rec.listed_after),
    observed_days: rec.dom_days_eff,
    basis: rec.dom_basis,
    evidence: rec.age_source,
    confidence: AGE_CONFIDENCE[rec.dom_basis] ?? 'unknown',
    note: 'Observed age is a minimum from DealLedger crawls, not verified days on market.',
  };
}

function priceChange(c: any) {
  return {
    from_price: Number(c.prev_price),
    to_price: Number(c.next_price),
    pct_change: Number(c.pct_change),
    changed_after: day(c.prev_last_seen),
    changed_on_or_before: day(c.next_first_seen),
    window_days: c.window_days ?? null,
    confidence: 'reviewed — both prices observed on the broker’s own listing page; exact day of change unknown',
  };
}
const validChange = (c: any) =>
  Number(c.prev_price) > 0 && Number(c.next_price) > 0 && String(c.next_first_seen) > String(c.prev_last_seen);

const RELIST_BASIS =
  'Heuristic match to an earlier listing in DealLedger’s historical archive with the same state, the same asking price ' +
  '(over $50K) and the same opening words of the title, under a different listing number. Not confirmed.';

function buildServer() {
  const server = new McpServer({ name: 'dealledger', version: '1.1.0' });
  const sb = getSupabase();

  async function loadListing(n: number) {
    const { data, error } = await sb
      .from('listings')
      .select('listing_number, header, price, cash_flow, state, city, category, url, last_seen, is_active, relisted')
      .eq('listing_number', n)
      .maybeSingle();
    if (error) throw new Error(error.message);
    return data as any;
  }

  server.tool(
    'get_listing_history',
    'Everything DealLedger has observed about one business-for-sale listing: current asking price and cash flow, ' +
      'observed listing age with its evidence and confidence, reviewed asking-price changes with dates, relist signal, ' +
      'whether it is still listed, the broker’s original page and the permanent DealLedger URL.',
    { listing: z.union([z.number(), z.string()]).describe('DealLedger listing number or a dealledger.org/listing/… URL') },
    async ({ listing }) => {
      const n = toListingNumber(listing);
      if (!n) return fail('Give a DealLedger listing number or a dealledger.org/listing/… URL.');
      try {
        const l = await loadListing(n);
        if (!l) return fail(`DealLedger has no listing ${n}.`);
        const [{ data: rec }, { data: ph }] = await Promise.all([
          sb.from('mv_listings_page_v2')
            .select('estimated_listed_date, listed_after, dom_days_eff, dom_basis, dom_display, age_source')
            .eq('listing_number', n).maybeSingle(),
          sb.from('price_history')
            .select('prev_price, prev_last_seen, next_price, next_first_seen, pct_change, window_days')
            .eq('listing_number', n).order('next_first_seen', { ascending: false }),
        ]);
        return json({
          title: displayTitle({ title: l.header, city: l.city, state_code: l.state, price: l.price }),
          dealledger_url: listingUrl(n),
          broker_page: l.url,
          broker_site: hostOf(l.url),
          location: [l.city, l.state].filter(Boolean).join(', ') || null,
          category: l.category,
          asking_price: l.price,
          cash_flow: l.cash_flow,
          status: l.is_active ? 'listed' : 'no longer listed on the broker site',
          last_seen: day(l.last_seen),
          observed_age: observedAge(rec),
          price_changes: (ph || []).filter(validChange).map(priceChange),
          relist_signal: l.relisted
            ? { status: 'possible_relist', confidence: 'low — unverified', basis: RELIST_BASIS }
            : { status: 'no_relist_evidence' },
          source: SOURCE,
        });
      } catch (e: any) {
        return fail(`Lookup failed: ${e.message}`);
      }
    }
  );

  server.tool(
    'search_price_changes',
    'Search reviewed asking-price changes on US businesses for sale (cuts and increases observed on brokers’ own listing pages). ' +
      'Each result gives the old and new price, the date window in which the change happened, the listing and its permanent DealLedger URL. ' +
      `Business types: ${Object.keys(TYPES).join(', ')}.`,
    {
      state: z.string().optional().describe('US state, as a code ("FL") or name ("Florida")'),
      type: z.string().optional().describe('Business type, e.g. "laundromat", "restaurant"'),
      direction: z.enum(['down', 'up', 'any']).optional().describe('"down" for price cuts (default), "up" for increases, "any"'),
      min_pct: z.number().optional().describe('Minimum size of the change in percent, e.g. 10'),
      since: z.string().optional().describe('Only changes observed on or after this date (YYYY-MM-DD)'),
      limit: z.number().int().min(1).max(50).optional().describe('Number of results, default 20'),
    },
    async (a) => {
      const state = toStateCode(a.state);
      if (a.state && !state) return fail(`Unknown state "${a.state}".`);
      const vertical = toVertical(a.type);
      if (a.type && !vertical) return fail(`Unknown business type "${a.type}". Use one of: ${Object.keys(TYPES).join(', ')}.`);
      const dir = a.direction ?? 'down';

      let q = sb.from('price_history')
        .select('listing_number, prev_price, prev_last_seen, next_price, next_first_seen, pct_change, window_days')
        .order('next_first_seen', { ascending: false })
        .limit(1000);
      if (dir === 'down') q = q.lt('pct_change', 0);
      if (dir === 'up') q = q.gt('pct_change', 0);
      if (a.since) q = q.gte('next_first_seen', a.since);
      const { data: ph, error } = await q;
      if (error) return fail(`Search failed: ${error.message}`);
      let changes = (ph || []).filter(validChange);
      if (a.min_pct != null) changes = changes.filter((c: any) => Math.abs(Number(c.pct_change)) >= a.min_pct!);
      if (!changes.length) return json({ count: 0, results: [], source: SOURCE });

      // Titles and places from the same SEO view the browse pages use (it carries state code and type).
      const nums = Array.from(new Set(changes.map((c: any) => Number(c.listing_number))));
      const { data: meta, error: mErr } = await sb
        .from('mv_seo_listings_v2')
        .select('listing_number, title, city, state_code, vertical, price, broker_domain')
        .in('listing_number', nums);
      if (mErr) return fail(`Search failed: ${mErr.message}`);
      const byNum = new Map((meta || []).map((m: any) => [Number(m.listing_number), m]));

      const results = changes
        .filter((c: any) => {
          const m = byNum.get(Number(c.listing_number));
          if (!m) return false; // not currently public: skip rather than show a change the site would hide
          if (state && m.state_code !== state) return false;
          if (vertical && m.vertical !== vertical) return false;
          return true;
        })
        .slice(0, a.limit ?? 20)
        .map((c: any) => {
          const m: any = byNum.get(Number(c.listing_number));
          return {
            title: displayTitle(m),
            location: [m.city, m.state_code].filter(Boolean).join(', ') || null,
            type: TYPES[m.vertical]?.label ?? m.vertical,
            broker_site: m.broker_domain,
            dealledger_url: listingUrl(Number(c.listing_number)),
            ...priceChange(c),
          };
        });
      return json({ count: results.length, results, source: SOURCE });
    }
  );

  server.tool(
    'check_relisting',
    'Check whether a business-for-sale listing shows evidence of having been listed before under a different listing. ' +
      'Returns the signal, its confidence and how it was derived. A possible relist is never confirmed proof.',
    { listing: z.union([z.number(), z.string()]).describe('DealLedger listing number or a dealledger.org/listing/… URL') },
    async ({ listing }) => {
      const n = toListingNumber(listing);
      if (!n) return fail('Give a DealLedger listing number or a dealledger.org/listing/… URL.');
      try {
        const l = await loadListing(n);
        if (!l) return fail(`DealLedger has no listing ${n}.`);
        return json({
          dealledger_url: listingUrl(n),
          title: displayTitle({ title: l.header, city: l.city, state_code: l.state, price: l.price }),
          ...(l.relisted
            ? { status: 'possible_relist', confidence: 'low — unverified', basis: RELIST_BASIS }
            : {
                status: 'no_relist_evidence',
                confidence: 'n/a',
                basis: 'No match found. This does not prove the business was never listed before; DealLedger only sees what it has observed.',
              }),
          source: SOURCE,
        });
      } catch (e: any) {
        return fail(`Lookup failed: ${e.message}`);
      }
    }
  );

  return server;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    res.status(405).setHeader('Allow', 'POST').json({
      jsonrpc: '2.0',
      error: { code: -32000, message: 'DealLedger MCP: send MCP requests by POST to https://dealledger.org/api/mcp' },
      id: null,
    });
    return;
  }
  const server = buildServer();
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
  res.on('close', () => {
    transport.close();
    server.close();
  });
  try {
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (e: any) {
    console.error('[mcp]', e?.message);
    if (!res.headersSent) {
      res.status(500).json({ jsonrpc: '2.0', error: { code: -32603, message: 'Internal error' }, id: null });
    }
  }
}
