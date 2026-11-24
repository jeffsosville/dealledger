// Shared plumbing for /api/v1/* routes: CORS, caching, JSON/CSV output,
// error shape and usage logging.
import type { NextApiRequest, NextApiResponse } from 'next';
import { getSupabase } from '../supabase';
import { API_VERSION, ApiError, LICENSE, SOURCE } from './index';
import { resolveCaller, type Caller } from './apikeys';

type Handler = (req: NextApiRequest) => Promise<{ body: any; csv?: { name: string; header: string[]; rows: any[][] } }>;

const cell = (v: unknown) => {
  if (v == null) return '';
  const s = typeof v === 'object' ? JSON.stringify(v) : String(v);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};
export const str = (v: unknown) => (typeof v === 'string' ? v : Array.isArray(v) ? String(v[0]) : undefined);
export const wantsCsv = (req: NextApiRequest) => str(req.query.format) === 'csv';

// Usage log: same table as the MCP server (tool = "api:<route>"). Salted IP hash + key id, never a raw IP.
async function log(route: string, req: NextApiRequest, count: number | null, err: string | null, ms: number, caller?: Caller) {
  try {
    const { format, key: _key, ...args } = req.query;
    await getSupabase().from('mcp_calls').insert({
      tool: `api:${route}${format === 'csv' ? ':csv' : ''}`,
      args,
      result_count: count,
      is_error: !!err,
      error: err ? err.slice(0, 300) : null,
      ms,
      client: (str(req.headers['user-agent']) ?? '').slice(0, 80) || null,
      api_key_id: caller?.keyId ?? null,
      ip_hash: caller?.ipHash ?? null,
    });
  } catch {
    /* logging never fails a request */
  }
}

export function route(name: string, fn: Handler) {
  return async (req: NextApiRequest, res: NextApiResponse) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'X-API-Key, Authorization');
    res.setHeader('X-DealLedger-API-Version', API_VERSION);
    res.setHeader('Link', '<https://creativecommons.org/publicdomain/zero/1.0/>; rel="license"');
    if (req.method === 'OPTIONS') return res.status(204).end();
    if (req.method !== 'GET') {
      res.setHeader('Allow', 'GET, OPTIONS');
      return res.status(405).json({ error: { code: 'method_not_allowed', message: 'Use GET.' } });
    }
    const t0 = Date.now();
    let caller: Caller | undefined;
    try {
      caller = await resolveCaller(req);
      if (Number.isFinite(caller.limit)) {
        res.setHeader('X-RateLimit-Limit', String(caller.limit));
        res.setHeader('X-RateLimit-Remaining', String(Math.max(0, caller.limit - caller.used - 1)));
      }
      const out = await fn(req);
      const count = Array.isArray(out.body?.results) ? out.body.results.length : 1;
      // Data refreshes daily; cache at the edge for an hour.
      // Keyed calls are never edge-cached, so every call counts against (and is credited to) its key.
      res.setHeader('Cache-Control', caller.keyId ? 'private, no-store' : 'public, s-maxage=3600, stale-while-revalidate=86400');
      if (out.csv && wantsCsv(req)) {
        const lines = [out.csv.header.join(','), ...out.csv.rows.map((r) => r.map(cell).join(','))];
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `inline; filename="${out.csv.name}.csv"`);
        res.status(200).send(lines.join('\n') + '\n');
      } else {
        res.status(200).json({ api_version: API_VERSION, ...out.body, source: SOURCE, license: LICENSE });
      }
      await log(name, req, count, null, Date.now() - t0, caller);
    } catch (e: any) {
      const status = e instanceof ApiError ? e.status : 500;
      const code = e instanceof ApiError ? e.code : 'internal_error';
      const message = e instanceof ApiError && status < 500 ? e.message : 'Something went wrong on our side. Try again shortly.';
      if (status >= 500) console.error(`[api ${name}]`, e?.message);
      res.setHeader('Cache-Control', 'no-store');
      res.status(status).json({ api_version: API_VERSION, error: { code, message } });
      // Don't log refused calls against the limit they were refused by.
      if (status !== 429) await log(name, req, 0, `${code}: ${e?.message ?? ''}`, Date.now() - t0, caller);
    }
  };
}
