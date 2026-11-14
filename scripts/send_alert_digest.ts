// scripts/send_alert_digest.ts — weekly new-listing digest for alert_subscriptions.
//
// For each confirmed, not-unsubscribed alert: listings matching its state and
// type/niche (lib/niches alertMatches - the same matching the pages use) whose
// evidence-based listed date is after last_sent_at, emailed via Resend, then
// last_sent_at advances. Nothing new = no email, last_sent_at stays put.
//
// "New" = dom_basis 'observed' or 'window' only; 'floor' / 'in_db' are
// database-entry dates, not listing dates.
//
// Env: NEXT_PUBLIC_SUPABASE_URL, NEXT_PUBLIC_SUPABASE_ANON_KEY,
//      SUPABASE_SERVICE_ROLE_KEY, RESEND_API_KEY.
// Run: npx tsx scripts/send_alert_digest.ts [--dry-run]
import { SITE, scopeLabel, scopePath, sendEmail, serviceDb, validScope } from '../lib/alerts';
import { alertMatches } from '../lib/niches';
import { displayTitle, fmtMoney } from '../lib/seo';

const MAX_ROWS = 25;
const NEW_BASES = new Set(['observed', 'window']);
const dry = process.argv.includes('--dry-run') || !process.env.RESEND_API_KEY;

const esc = (s: string) => s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]!));

type Sub = { id: string; email: string; state_code: string | null; vertical: string | null; token: string; last_sent_at: string };

function render(sub: Sub, label: string, path: string, rows: Awaited<ReturnType<typeof alertMatches>>) {
  const more = rows.length > MAX_ROWS;
  const shown = rows.slice(0, MAX_ROWS);
  const unsub = `${SITE}/api/alerts/unsubscribe?t=${sub.token}`;
  const items = shown.map((r) => {
    const where = [r.city, r.state_code].filter(Boolean).join(', ') || '—';
    return `<tr><td style="padding:12px 0;border-top:1px solid #d4cfc4;">
<a href="${SITE}/listing/${r.listing_number}" style="color:#0c4a6e;">${esc(displayTitle(r).slice(0, 110))}</a><br>
<span style="font-size:13px;color:#57534e;">${esc(where)} · asking ${fmtMoney(r.price)} · cash flow ${fmtMoney(r.cash_flow)}</span></td></tr>`;
  }).join('');
  const n = `${shown.length}${more ? '+' : ''}`;
  const html = `<!doctype html><html><body style="margin:0;background:#fafaf9;">
<div style="max-width:560px;margin:0 auto;padding:32px 24px;font-family:-apple-system,system-ui,sans-serif;font-size:15px;line-height:1.55;color:#1c1917;">
<div style="font-family:Menlo,monospace;font-size:13px;font-weight:600;margin-bottom:24px;">DealLedger</div>
<p><strong>${n} new</strong> for <a href="${SITE}${path}" style="color:#0c4a6e;">${esc(label)}</a> since your last email, read directly from broker websites.</p>
<table style="width:100%;border-collapse:collapse;">${items}</table>
<p><a href="${SITE}${path}" style="color:#0c4a6e;">See all on DealLedger</a></p>
<p style="margin-top:32px;padding-top:16px;border-top:1px solid #d4cfc4;font-size:12px;color:#78716c;">
You asked for this alert on dealledger.org. <a href="${unsub}" style="color:#78716c;">Unsubscribe</a><br>
DealLedger · 10 Academy St, Skaneateles, NY 13152</p></div></body></html>`;
  return { subject: `${n} new: ${label}`, html, unsub };
}

async function main() {
  const db = serviceDb();
  const { data: subs, error } = await db
    .from('alert_subscriptions')
    .select('id, email, state_code, vertical, token, last_sent_at')
    .not('confirmed_at', 'is', null)
    .is('unsubscribed_at', null);
  if (error) throw error;

  let sent = 0, quiet = 0, failed = 0;
  for (const sub of (subs || []) as Sub[]) {
    if (!validScope(sub.state_code, sub.vertical)) { failed++; console.log(`bad scope ${sub.id}`); continue; }
    const since = (sub.last_sent_at || '').slice(0, 10);
    const rows = (await alertMatches(sub.state_code, sub.vertical))
      .filter((r) => r.dom_basis && NEW_BASES.has(r.dom_basis) && (r.estimated_listed_date || '') > since)
      .sort((a, b) => (b.estimated_listed_date || '').localeCompare(a.estimated_listed_date || ''));
    if (!rows.length) { quiet++; continue; }
    const label = scopeLabel(sub.state_code, sub.vertical);
    const { subject, html, unsub } = render(sub, label, scopePath(sub.state_code, sub.vertical), rows);
    if (dry) { console.log(`--- ${sub.email}: ${subject}`); continue; }
    try {
      await sendEmail(sub.email, subject, html, {
        'List-Unsubscribe': `<${unsub}>`,
        'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click',
      });
      await db.from('alert_subscriptions').update({ last_sent_at: new Date().toISOString() }).eq('id', sub.id);
      sent++;
    } catch (e) {
      failed++;
      console.log(`send failed ${sub.id}: ${(e as Error).message}`);
    }
  }
  console.log(`alerts: ${(subs || []).length} active, ${sent} sent, ${quiet} nothing new, ${failed} failed${dry ? ' (dry run)' : ''}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
