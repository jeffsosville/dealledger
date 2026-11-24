// POST /api/v1/keys { email, name?, use_case?, website? } — issue a free API key and email it.
// The key is shown once (in the email and in this response) and stored only as a hash.
// `website` is a honeypot, same as alerts.
import type { NextApiRequest, NextApiResponse } from 'next';
import { ipHash, newKey, serviceDb } from '../../../lib/intelligence/apikeys';
import { sendEmail } from '../../../lib/alerts';

const MAX_KEYS_PER_EMAIL = 3;
const MAX_SIGNUPS_PER_IP_PER_DAY = 5;

const esc = (s: string) => s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]!));

const keyEmail = (key: string) => `<!doctype html><html><body style="margin:0;background:#fafaf9;">
<div style="max-width:560px;margin:0 auto;padding:32px 24px;font-family:-apple-system,system-ui,sans-serif;font-size:15px;line-height:1.55;color:#1c1917;">
<div style="font-family:Menlo,monospace;font-size:13px;font-weight:600;letter-spacing:0.04em;margin-bottom:24px;">DealLedger</div>
<p>Here's your free DealLedger API key. Keep it somewhere safe; we only store a hash and can't show it again.</p>
<p style="font-family:Menlo,monospace;font-size:14px;background:#f0ede6;padding:12px 16px;word-break:break-all;">${esc(key)}</p>
<p>Send it with each request:</p>
<p style="font-family:Menlo,monospace;font-size:13px;background:#f0ede6;padding:12px 16px;word-break:break-all;">curl -H "X-API-Key: ${esc(key)}" "https://dealledger.org/api/v1/listings?state=TX&amp;limit=5"</p>
<p>It's good for 10,000 calls a day. The data is CC0, so use it however you like. Docs: <a href="https://dealledger.org/developers" style="color:#0c4a6e;">dealledger.org/developers</a>. Reply to info@dealledger.org with questions or if you need more.</p>
<p style="margin-top:32px;padding-top:16px;border-top:1px solid #d4cfc4;font-size:12px;color:#78716c;">DealLedger · 10 Academy St, Skaneateles, NY 13152 · the public record of businesses for sale</p>
</div></body></html>`;

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Cache-Control', 'no-store');
  if (req.method === 'OPTIONS') return res.status(204).end();
  if (req.method !== 'POST') return res.status(405).json({ error: { code: 'method_not_allowed', message: 'POST only.' } });

  const b = (req.body || {}) as Record<string, unknown>;
  const s = (v: unknown, n: number) => (typeof v === 'string' && v.trim() ? v.trim().slice(0, n) : null);
  if (s(b.website, 200)) return res.status(200).json({ ok: true }); // honeypot

  const email = s(b.email, 254)?.toLowerCase() ?? null;
  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))
    return res.status(400).json({ error: { code: 'invalid_email', message: 'Enter a valid email.' } });

  const db = serviceDb();
  const ih = ipHash(req);
  const since = new Date(Date.now() - 864e5).toISOString();

  const [{ count: byEmail }, { count: byIp }] = await Promise.all([
    db.from('api_keys').select('id', { count: 'exact', head: true }).ilike('email', email).is('revoked_at', null),
    ih ? db.from('api_keys').select('id', { count: 'exact', head: true }).eq('ip_hash', ih).gte('created_at', since)
       : Promise.resolve({ count: 0 } as { count: number }),
  ]);
  if ((byEmail ?? 0) >= MAX_KEYS_PER_EMAIL)
    return res.status(429).json({ error: { code: 'too_many_keys', message: 'This email already has the maximum number of keys. Email info@dealledger.org if you need another.' } });
  if ((byIp ?? 0) >= MAX_SIGNUPS_PER_IP_PER_DAY)
    return res.status(429).json({ error: { code: 'too_many_signups', message: 'Too many keys requested today. Try again tomorrow.' } });

  const { key, hash, prefix } = newKey();
  const { error } = await db.from('api_keys').insert({
    key_hash: hash, key_prefix: prefix, email,
    name: s(b.name, 120), use_case: s(b.use_case, 500),
    user_agent: (req.headers['user-agent'] || '').slice(0, 300), ip_hash: ih,
  });
  if (error) {
    console.error('api key insert', error);
    return res.status(500).json({ error: { code: 'internal_error', message: 'Something went wrong. Try again.' } });
  }

  try {
    await sendEmail(email, 'Your DealLedger API key', keyEmail(key));
  } catch (e) {
    console.error('api key email', e); // key still returned below
  }
  return res.status(201).json({ ok: true, key, prefix, daily_limit: 10000, note: 'Shown once. Also emailed to you. Send as the X-API-Key header.' });
}
