// Free API keys for /api/v1. Server-only (uses the service role key).
// Table: api_keys (sql/2026-11-24_api_keys.sql). Keys are shown once, stored as sha256.
import type { NextApiRequest } from 'next';
import { createHash, randomBytes } from 'crypto';
import { createClient } from '@supabase/supabase-js';
import { ApiError } from './index';

export const ANON_DAILY_LIMIT = Number(process.env.API_ANON_DAILY_LIMIT ?? 50);
export const KEY_SIGNUP_URL = 'https://dealledger.org/developers#keys';

export const serviceDb = () =>
  createClient(process.env.NEXT_PUBLIC_SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!, {
    auth: { persistSession: false },
  });

const sha256 = (s: string) => createHash('sha256').update(s).digest('hex');
export const hashKey = (key: string) => sha256(key);

const B62 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
export function newKey() {
  const bytes = randomBytes(32);
  let body = '';
  for (const b of bytes) body += B62[b % 62];
  const key = `dl_${body}`;
  return { key, hash: hashKey(key), prefix: key.slice(0, 11) };
}

export function clientIp(req: NextApiRequest) {
  const fwd = req.headers['x-forwarded-for'];
  const ip = (Array.isArray(fwd) ? fwd[0] : fwd?.split(',')[0])?.trim() || (req.headers['x-real-ip'] as string) || req.socket?.remoteAddress || '';
  return ip;
}
// Salted so the log can't be reversed to an IP.
export const ipHash = (req: NextApiRequest) => {
  const ip = clientIp(req);
  return ip ? sha256(`${process.env.API_IP_SALT ?? 'dealledger-api'}:${ip}`).slice(0, 32) : null;
};

export function keyFrom(req: NextApiRequest) {
  const h = req.headers['x-api-key'];
  const auth = req.headers.authorization;
  const q = req.query.key;
  const v =
    (Array.isArray(h) ? h[0] : h) ||
    (auth?.toLowerCase().startsWith('bearer ') ? auth.slice(7) : undefined) ||
    (Array.isArray(q) ? q[0] : q);
  return v?.trim() || null;
}

export type Caller = { keyId: number | null; ipHash: string | null; limit: number; used: number };

const dayAgo = () => new Date(Date.now() - 864e5).toISOString();

/** Identify the caller and enforce the daily limit. Throws ApiError 401/429.
 *  Any other failure (DB down, env missing) fails open: the call goes through unmetered. */
export async function resolveCaller(req: NextApiRequest): Promise<Caller> {
  try {
    return await resolveCallerStrict(req);
  } catch (e) {
    if (e instanceof ApiError) throw e;
    console.error('[api keys] resolve failed, failing open', (e as any)?.message);
    return { keyId: null, ipHash: null, limit: Infinity, used: 0 };
  }
}

async function resolveCallerStrict(req: NextApiRequest): Promise<Caller> {
  const db = serviceDb();
  const ih = ipHash(req);
  const key = keyFrom(req);

  if (key) {
    const { data: k, error } = await db.from('api_keys').select('id, daily_limit, revoked_at').eq('key_hash', hashKey(key)).maybeSingle();
    if (error) throw error;
    if (!k || k.revoked_at) throw new ApiError(401, 'invalid_key', `That API key isn't valid. Get a free one at ${KEY_SIGNUP_URL}`);
    const { count } = await db.from('mcp_calls').select('id', { count: 'exact', head: true })
      .eq('api_key_id', k.id).gte('called_at', dayAgo());
    const used = count ?? 0;
    if (used >= k.daily_limit)
      throw new ApiError(429, 'rate_limited', `This key has used its ${k.daily_limit} calls for the last 24 hours. Need more? Email info@dealledger.org.`);
    db.from('api_keys').update({ last_used_at: new Date().toISOString() }).eq('id', k.id).then(() => {}, () => {});
    return { keyId: k.id, ipHash: ih, limit: k.daily_limit, used };
  }

  if (!ih || !Number.isFinite(ANON_DAILY_LIMIT) || ANON_DAILY_LIMIT <= 0) return { keyId: null, ipHash: ih, limit: Infinity, used: 0 };
  const { count } = await db.from('mcp_calls').select('id', { count: 'exact', head: true })
    .eq('ip_hash', ih).is('api_key_id', null).like('tool', 'api:%').gte('called_at', dayAgo());
  const used = count ?? 0;
  if (used >= ANON_DAILY_LIMIT)
    throw new ApiError(429, 'key_required', `Anonymous use is limited to ${ANON_DAILY_LIMIT} calls a day. Get a free API key at ${KEY_SIGNUP_URL} and send it as the X-API-Key header.`);
  return { keyId: null, ipHash: ih, limit: ANON_DAILY_LIMIT, used };
}
