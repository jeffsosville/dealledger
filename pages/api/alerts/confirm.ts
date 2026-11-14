// GET /api/alerts/confirm?t=<token> — double opt-in click from the confirm email.
import type { NextApiRequest, NextApiResponse } from 'next';
import { resultPage, scopeLabel, scopePath, serviceDb } from '../../../lib/alerts';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const t = typeof req.query.t === 'string' ? req.query.t : '';
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  if (!UUID.test(t)) return res.status(400).send(resultPage('Link not valid', 'This confirmation link is incomplete.', '/', 'Go to DealLedger'));

  const db = serviceDb();
  const { data } = await db.from('alert_subscriptions').select('state_code, vertical, confirmed_at').eq('token', t).maybeSingle();
  if (!data) return res.status(404).send(resultPage('Link not valid', 'We couldn’t find that alert.', '/', 'Go to DealLedger'));

  // last_sent_at starts at confirm time so the first digest only has listings found after this.
  if (!data.confirmed_at) {
    const now = new Date().toISOString();
    await db.from('alert_subscriptions').update({ confirmed_at: now, last_sent_at: now, unsubscribed_at: null }).eq('token', t);
  }
  const label = scopeLabel(data.state_code, data.vertical);
  return res.status(200).send(resultPage(
    'Alert confirmed',
    `You’ll get a weekly email with new listings for <strong>${label}</strong> when there are any. Every email has a one-click unsubscribe.`,
    scopePath(data.state_code, data.vertical),
    `Back to ${label}`,
  ));
}
