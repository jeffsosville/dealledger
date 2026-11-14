// GET or POST /api/alerts/unsubscribe?t=<token>
// GET: link in every digest. POST: RFC 8058 one-click (List-Unsubscribe-Post).
import type { NextApiRequest, NextApiResponse } from 'next';
import { resultPage, scopeLabel, serviceDb } from '../../../lib/alerts';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const t = typeof req.query.t === 'string' ? req.query.t : '';
  res.setHeader('Cache-Control', 'no-store');
  if (!UUID.test(t)) {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.status(400).send(resultPage('Link not valid', 'This unsubscribe link is incomplete.', '/', 'Go to DealLedger'));
  }
  const db = serviceDb();
  const { data } = await db.from('alert_subscriptions')
    .update({ unsubscribed_at: new Date().toISOString() })
    .eq('token', t).is('unsubscribed_at', null)
    .select('state_code, vertical').maybeSingle();

  if (req.method === 'POST') return res.status(200).json({ ok: true });
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  const what = data ? `<strong>${scopeLabel(data.state_code, data.vertical)}</strong>` : 'this alert';
  return res.status(200).send(resultPage('Unsubscribed', `You won’t get any more emails for ${what}.`, '/', 'Go to DealLedger'));
}
