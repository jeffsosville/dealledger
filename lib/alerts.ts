// lib/alerts.ts — server-only helpers for new-listing email alerts.
// Table: alert_subscriptions (sql/2026-11-13_alert_subscriptions.sql).
// Same service-role + Resend setup as pages/api/sba-inquiry.ts.

import { createClient } from '@supabase/supabase-js';
import { STATES, TYPES, comboPath, statePath, stateSlug, typePath } from './seo';
import { nicheFromSlug, nichePath } from './niches';

export const SITE = 'https://dealledger.org';
export const FROM_EMAIL = 'DealLedger <pulse@dealledger.org>';

export const serviceDb = () =>
  createClient(process.env.NEXT_PUBLIC_SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!, {
    auth: { persistSession: false },
  });

// `key` (stored in alert_subscriptions.vertical) is a lib/seo TYPES key, a lib/niches slug,
// or null for every type. state null = every state. Both null = everything.
export const validScope = (state: string | null, key: string | null) =>
  (state === null || !!STATES[state]) && (key === null || !!TYPES[key] || !!nicheFromSlug(key));

const keyLabel = (key: string | null) =>
  key ? (TYPES[key]?.label ?? nicheFromSlug(key)?.label ?? 'Businesses') : 'Businesses';

export const scopeLabel = (state: string | null, key: string | null) =>
  `${keyLabel(key)} for sale ${state ? `in ${STATES[state]}` : 'nationwide'}`;

export const scopePath = (state: string | null, key: string | null) => {
  if (!key) return state ? statePath(state) : '/businesses-for-sale';
  if (TYPES[key]) return state ? comboPath(state, key) : typePath(key);
  return state ? `/businesses-for-sale/${stateSlug(state)}/${key}` : nichePath(key);
};

export async function sendEmail(to: string, subject: string, html: string, headers?: Record<string, string>) {
  const r = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${process.env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ from: FROM_EMAIL, to: [to], subject, html, ...(headers ? { headers } : {}) }),
  });
  if (!r.ok) throw new Error(`Resend ${r.status}: ${await r.text()}`);
}

const esc = (s: string) => s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]!));

const shell = (body: string) => `<!doctype html><html><body style="margin:0;background:#fafaf9;">
<div style="max-width:560px;margin:0 auto;padding:32px 24px;font-family:'IBM Plex Sans',-apple-system,system-ui,sans-serif;font-size:15px;line-height:1.55;color:#1c1917;">
<div style="font-family:'IBM Plex Mono',Menlo,monospace;font-size:13px;font-weight:600;letter-spacing:0.04em;margin-bottom:24px;">DealLedger</div>
${body}
<p style="margin-top:32px;padding-top:16px;border-top:1px solid #d4cfc4;font-size:12px;color:#78716c;">
DealLedger · 10&#8203; Academy St, Skaneateles,&#8203; NY 13152 · the public record of businesses for sale
</p></div></body></html>`;

export const confirmEmail = (label: string, token: string) =>
  shell(`<p>Confirm your alert for <strong>${esc(label)}</strong>.</p>
<p>Once a week, DealLedger will email you the new listings we found on broker websites that match. Nothing else.</p>
<p style="margin:28px 0;"><a href="${SITE}/api/alerts/confirm?t=${token}" style="background:#1c1917;color:#fafaf9;padding:12px 20px;text-decoration:none;font-family:'IBM Plex Mono',Menlo,monospace;font-size:13px;">Confirm alert</a></p>
<p style="font-size:13px;color:#78716c;">If you didn't ask for this, ignore this email and you won't hear from us.</p>`);

/** Tiny standalone HTML page for confirm / unsubscribe responses. */
export const resultPage = (title: string, body: string, href: string, linkText: string) =>
  `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex"><title>${esc(title)} — DealLedger</title></head>
<body style="margin:0;background:#fafaf9;color:#1c1917;font-family:-apple-system,system-ui,sans-serif;">
<div style="max-width:560px;margin:80px auto;padding:0 24px;line-height:1.55;">
<div style="font-family:Menlo,monospace;font-size:13px;font-weight:600;margin-bottom:24px;"><a href="/" style="color:#1c1917;text-decoration:none;">DealLedger</a></div>
<h1 style="font-weight:500;font-size:28px;margin:0 0 12px;">${esc(title)}</h1>
<p>${body}</p>
<p><a href="${href}" style="color:#0c4a6e;">${esc(linkText)}</a></p>
</div></body></html>`;

/** Dropdown options for picker-mode signup (/alerts, 404). Server-side only. */
export async function alertPickerOptions() {
  const { NICHE_MIN, nicheCounts } = await import('./niches');
  const states = Object.entries(STATES)
    .map(([value, label]) => ({ value, label }))
    .sort((a, b) => a.label.localeCompare(b.label));
  const niches = (await nicheCounts()).filter((c) => c.count >= NICHE_MIN).map((c) => ({ value: c.slug, label: c.label }));
  const types = [...Object.entries(TYPES).map(([value, t]) => ({ value, label: t.label })), ...niches]
    .sort((a, b) => a.label.localeCompare(b.label));
  return { states, types };
}
