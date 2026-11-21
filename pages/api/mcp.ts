// /api/mcp — DealLedger's public MCP server (read-only, no auth).
//
// Four tools, each returning evidence, dates, a confidence level and a
// permanent DealLedger URL:
//   get_listing_history   — what DealLedger has observed about one listing
//   search_price_changes  — reviewed asking-price changes, filterable
//   search_listings       — public listings, filtered and paged
//   check_relisting       — whether a listing has relist evidence
//
// All queries live in lib/intelligence, shared with the REST API (/api/v1).
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
import { TYPES } from '../../lib/seo';
import {
  ApiError, SOURCE, getListing, searchListings, searchPriceChanges,
} from '../../lib/intelligence';

const json = (v: unknown) => ({ content: [{ type: 'text' as const, text: JSON.stringify(v, null, 2) }] });
const fail = (msg: string) => ({ content: [{ type: 'text' as const, text: msg }], isError: true });
const TYPE_LIST = Object.keys(TYPES).join(', ');

// Usage log (sql/2026-11-19_mcp_calls.sql): tool, arguments, result count,
// error, time taken and the client's self-reported name. No IP, no identity.
// Logging never blocks or breaks a tool call.
function resultCount(out: any): number | null {
  if (out?.isError) return 0;
  try {
    const v = JSON.parse(out?.content?.[0]?.text ?? 'null');
    return Array.isArray(v?.results) ? v.results.length : 1;
  } catch {
    return null;
  }
}

function buildServer(client: string | null) {
  const server = new McpServer({ name: 'dealledger', version: '1.3.0' });
  const sb = getSupabase();

  function logged<A>(tool: string, fn: (a: A) => Promise<any>) {
    return async (a: A) => {
      const t0 = Date.now();
      let out: any;
      try {
        out = await fn(a);
      } catch (e: any) {
        out = e instanceof ApiError && e.status < 500 ? fail(e.message) : fail(`Lookup failed: ${e?.message ?? e}`);
      }
      const ms = Date.now() - t0;
      try {
        await sb.from('mcp_calls').insert({
          tool,
          args: a ?? null,
          result_count: resultCount(out),
          is_error: !!out?.isError,
          error: out?.isError ? String(out?.content?.[0]?.text ?? '').slice(0, 300) : null,
          ms,
          client,
        });
      } catch {
        /* never let logging fail a call */
      }
      return out;
    };
  }

  const paging = {
    limit: z.number().int().min(1).max(50).optional().describe('Number of results, default 20'),
    offset: z.number().int().min(0).optional().describe('Skip this many results; use next_offset from the previous answer to page'),
  };

  server.tool(
    'get_listing_history',
    'Everything DealLedger has observed about one business-for-sale listing: current asking price and cash flow, ' +
      'observed listing age with its evidence and confidence, reviewed asking-price changes with dates, relist signal, ' +
      'whether it is still listed, the broker’s original page and the permanent DealLedger URL.',
    { listing: z.union([z.number(), z.string()]).describe('DealLedger listing number or a dealledger.org/listing/… URL') },
    logged('get_listing_history', async ({ listing }) => json({ ...(await getListing(listing)), source: SOURCE }))
  );

  server.tool(
    'search_listings',
    'Search US businesses for sale in DealLedger’s public record (observed on brokers’ own websites). ' +
      'Each result gives price, cash flow, observed listing age with evidence, relist signal, broker and the permanent DealLedger URL. ' +
      `total is the full number of matches; page with offset. Business types: ${TYPE_LIST}.`,
    {
      state: z.string().optional().describe('US state, as a code ("TX") or name ("Texas")'),
      type: z.string().optional().describe('Business type, e.g. "laundromat", "restaurant"'),
      q: z.string().optional().describe('Words in the listing title, e.g. "car wash"'),
      min_price: z.number().optional().describe('Minimum asking price in USD'),
      max_price: z.number().optional().describe('Maximum asking price in USD'),
      min_cash_flow: z.number().optional().describe('Minimum reported cash flow in USD'),
      min_age_days: z.number().optional().describe('Only listings observed on the market at least this many days'),
      sort: z.enum(['newest', 'oldest', 'price_desc', 'price_asc', 'cash_flow_desc', 'age_desc']).optional(),
      ...paging,
    },
    logged('search_listings', async (a: any) => json({ ...(await searchListings({ ...a, limit: a.limit ?? 20 }, 50)), source: SOURCE }))
  );

  server.tool(
    'search_price_changes',
    'Search reviewed asking-price changes on US businesses for sale (cuts and increases observed on brokers’ own listing pages). ' +
      'Each result gives the old and new price, the date window in which the change happened, the listing and its permanent DealLedger URL. ' +
      `total is the full number of matches; page with offset. Business types: ${TYPE_LIST}.`,
    {
      state: z.string().optional().describe('US state, as a code ("FL") or name ("Florida")'),
      type: z.string().optional().describe('Business type, e.g. "laundromat", "restaurant"'),
      direction: z.enum(['down', 'up', 'any']).optional().describe('"down" for price cuts (default), "up" for increases, "any"'),
      min_pct: z.number().optional().describe('Minimum size of the change in percent, e.g. 10'),
      since: z.string().optional().describe('Only changes observed on or after this date (YYYY-MM-DD)'),
      ...paging,
    },
    logged('search_price_changes', async (a: any) => json({ ...(await searchPriceChanges(a, 50)), source: SOURCE }))
  );

  server.tool(
    'check_relisting',
    'Check whether a business-for-sale listing shows evidence of having been listed before under a different listing. ' +
      'Returns the signal, its confidence and how it was derived. A possible relist is never confirmed proof.',
    { listing: z.union([z.number(), z.string()]).describe('DealLedger listing number or a dealledger.org/listing/… URL') },
    logged('check_relisting', async ({ listing }) => {
      const l = await getListing(listing);
      return json({ dealledger_url: l.dealledger_url, title: l.title, ...l.relist_signal, source: SOURCE });
    })
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
  const ua = req.headers['user-agent'];
  const server = buildServer(typeof ua === 'string' ? ua.slice(0, 80) : null);
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
