// scripts/send_confirm_reminders.ts — one reminder for alerts still unconfirmed 24h after signup.
//
// Picks rows with confirmed_at null, unsubscribed_at null, confirm_reminded_at null,
// created 24h–7d ago. One email per address (all its pending alerts, one button each),
// then sets confirm_reminded_at so nobody is reminded twice.
//
// Env: NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, RESEND_API_KEY.
// Run: npx tsx scripts/send_confirm_reminders.ts [--dry-run]
import { SITE, scopeLabel, sendEmail, serviceDb } from '../lib/alerts';

const dry = process.argv.includes('--dry-run') || !process.env.RESEND_API_KEY;
const esc = (s: string) => s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]!));
const btn = (href: string, text: string) =>
  `<a href="${href}" style="display:inline-block;background:#1c1917;color:#fafaf9;padding:10px 18px;text-decoration:none;font-family:Menlo,monospace;font-size:13px;">${esc(text)}</a>`;

type Row = { id: string; email: string; state_code: string | null; vertical: string | null; token: string };

function render(rows: Row[]) {
  const labels = rows.map((r) => scopeLabel(r.state_code, r.vertical));
  const items = rows.map((r, i) =>
    `<p style="margin:20px 0;"><strong>${esc(labels[i])}</strong><br><span style="display:inline-block;margin-top:8px;">${btn(`${SITE}/api/alerts/confirm?t=${r.token}`, 'Confirm alert')}</span></p>`).join('');
  const subject = rows.length === 1 ? `Reminder: confirm your DealLedger alert for ${labels[0]}` : `Reminder: confirm your ${rows.length} DealLedger alerts`;
  const html = `<!doctype html><html><body style="margin:0;background:#fafaf9;">
<div style="max-width:560px;margin:0 auto;padding:32px 24px;font-family:-apple-system,system-ui,sans-serif;font-size:15px;line-height:1.55;color:#1c1917;">
<div style="font-family:Menlo,monospace;font-size:13px;font-weight:600;margin-bottom:24px;">DealLedger</div>
<p>You signed up for new-listing alerts on DealLedger but haven't confirmed yet, so we can't send them. One click and you're set:</p>
${items}
<p>Once a week you'll get the new listings we found on broker websites that match. Nothing else.</p>
<p style="font-size:13px;color:#78716c;">This is the only reminder. If you didn't ask for this, ignore it and you won't hear from us.</p>
<p style="margin-top:32px;padding-top:16px;border-top:1px solid #d4cfc4;font-size:12px;color:#78716c;">DealLedger · 10&#8203; Academy St, Skaneateles,&#8203; NY 13152</p>
</div></body></html>`;
  return { subject, html };
}

async function main() {
  const db = serviceDb();
  const now = Date.now();
  const { data, error } = await db
    .from('alert_subscriptions')
    .select('id, email, state_code, vertical, token')
    .is('confirmed_at', null)
    .is('unsubscribed_at', null)
    .is('confirm_reminded_at', null)
    .lte('created_at', new Date(now - 24 * 3600e3).toISOString())
    .gte('created_at', new Date(now - 7 * 24 * 3600e3).toISOString());
  if (error) throw error;

  const byEmail = new Map<string, Row[]>();
  for (const r of (data ?? []) as Row[]) {
    const k = r.email.toLowerCase();
    byEmail.set(k, [...(byEmail.get(k) ?? []), r]);
  }
  console.log(`${data?.length ?? 0} pending alerts, ${byEmail.size} addresses${dry ? ' (dry run)' : ''}`);

  let sent = 0;
  for (const [email, rows] of byEmail) {
    const { subject, html } = render(rows);
    if (dry) { console.log(`would send to ${email}: ${subject}`); continue; }
    try {
      await sendEmail(email, subject, html);
      const { error: upErr } = await db.from('alert_subscriptions')
        .update({ confirm_reminded_at: new Date().toISOString() })
        .in('id', rows.map((r) => r.id));
      if (upErr) throw upErr;
      sent++;
    } catch (e) {
      console.error(`reminder failed for ${email}`, e);
    }
  }
  console.log(`sent ${sent}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
