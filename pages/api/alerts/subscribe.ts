// POST /api/alerts/subscribe { email, state?, vertical?, path?, website? }
// Creates (or reuses) an unconfirmed alert and emails a confirm link.
// `website` is a honeypot: humans never see it, bots fill it in.
import type { NextApiRequest, NextApiResponse } from 'next';
import { confirmEmail, scopeLabel, sendEmail, serviceDb, validScope } from '../../../lib/alerts';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST only' });
  const b = (req.body || {}) as Record<string, unknown>;
  const str = (v: unknown) => (typeof v === 'string' && v.trim() ? v.trim() : null);

  if (str(b.website)) return res.status(200).json({ ok: true }); // honeypot
  const email = str(b.email)?.toLowerCase().slice(0, 254) ?? null;
  const state = str(b.state)?.toUpperCase() ?? null;
  const vertical = str(b.vertical);
  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: 'Enter a valid email.' });
  if (!validScope(state, vertical)) return res.status(400).json({ error: 'Unknown page.' });

  const db = serviceDb();
  let q = db.from('alert_subscriptions').select('token, confirmed_at, unsubscribed_at').eq('email', email);
  q = state ? q.eq('state_code', state) : q.is('state_code', null);
  q = vertical ? q.eq('vertical', vertical) : q.is('vertical', null);
  const { data: existing, error: selErr } = await q.maybeSingle();
  if (selErr) {
    console.error('alerts select', selErr);
    return res.status(500).json({ error: 'Something went wrong. Try again.' });
  }

  if (existing?.confirmed_at && !existing.unsubscribed_at) return res.status(200).json({ ok: true, already: true });

  let token = existing?.token as string | undefined;
  if (existing) {
    // Re-subscribing after an unsubscribe: needs a fresh confirm.
    await db.from('alert_subscriptions')
      .update({ unsubscribed_at: null, confirmed_at: null })
      .eq('token', token!);
  } else {
    const fwd = req.headers['x-forwarded-for'];
    const { data, error } = await db.from('alert_subscriptions').insert({
      email, state_code: state, vertical,
      source_path: str(b.path)?.slice(0, 200) ?? null,
      user_agent: (req.headers['user-agent'] || '').slice(0, 300),
      ip_address: (Array.isArray(fwd) ? fwd[0] : fwd?.split(',')[0]?.trim()) || null,
    }).select('token').single();
    if (error || !data) {
      console.error('alerts insert', error);
      return res.status(500).json({ error: 'Something went wrong. Try again.' });
    }
    token = data.token;
  }

  try {
    const label = scopeLabel(state, vertical);
    await sendEmail(email, `Confirm your DealLedger alert: ${label}`, confirmEmail(label, token!));
  } catch (e) {
    console.error('alerts confirm email', e);
    return res.status(502).json({ error: "Couldn't send the confirmation email. Try again." });
  }
  return res.status(200).json({ ok: true });
}
