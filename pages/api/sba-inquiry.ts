// pages/api/sba-inquiry.ts
//
// POST: capture SBA Get Pre-Qualified lead
//  - writes to sba_inquiries table
//  - emails info@dealledger.org via Resend
//  - sends confirmation email to the buyer

import type { NextApiRequest, NextApiResponse } from 'next';
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,  // server-side write
);

const RESEND_API_KEY = process.env.RESEND_API_KEY!;
const FROM_EMAIL    = 'DealLedger <pulse@dealledger.org>';   // already verified domain
const TO_EMAIL      = 'info@dealledger.org';
const CC_EMAIL      = process.env.SBA_CC_EMAIL || '';        // optional fallback (e.g. your gmail)

type InquiryBody = {
  name: string;
  email: string;
  phone?: string;
  listing_id?: string;
  listing_title?: string;
  listing_price?: number | null;
  listing_url?: string;
  source?: 'listing_sidebar' | 'sba_landing';
  purchase_price_range?: string;
  down_payment?: string;
  credit_score_range?: string;
  timeline?: string;
  notes?: string;
};

const money = (n?: number | null) =>
  n == null ? '—' : n.toLocaleString('en-US', {
    style: 'currency', currency: 'USD', maximumFractionDigits: 0,
  });

function escapeHtml(s: string) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const body = req.body as InquiryBody;

    // basic validation
    if (!body.name || !body.email) {
      return res.status(400).json({ error: 'Name and email required' });
    }
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(body.email)) {
      return res.status(400).json({ error: 'Invalid email' });
    }

    const userAgent = req.headers['user-agent'] || '';
    const fwd = req.headers['x-forwarded-for'];
    const ip = (Array.isArray(fwd) ? fwd[0] : fwd?.split(',')[0]?.trim()) || '';

    // 1. Save to Supabase
    const { data: inquiry, error: dbError } = await supabase
      .from('sba_inquiries')
      .insert({
        name:           body.name,
        email:          body.email,
        phone:          body.phone || null,
        listing_id:     body.listing_id || null,
        listing_title:  body.listing_title || null,
        listing_price:  body.listing_price ?? null,
        listing_url:    body.listing_url || null,
        source:         body.source || 'listing_sidebar',
        purchase_price_range: body.purchase_price_range || null,
        down_payment:         body.down_payment || null,
        credit_score_range:   body.credit_score_range || null,
        timeline:             body.timeline || null,
        notes:                body.notes || null,
        user_agent:     userAgent,
        ip_address:     ip,
      })
      .select()
      .single();

    if (dbError) {
      console.error('Supabase insert error:', dbError);
      return res.status(500).json({ error: 'Failed to save inquiry' });
    }

    // 2. Email info@dealledger.org — DealLedger editorial style
    const subject = body.listing_title
      ? `SBA Inquiry — ${body.name} → ${body.listing_title}`
      : `SBA Inquiry — ${body.name}`;

    const internalHtml = `
<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f2eb;font-family:Georgia,'Libre Baskerville',serif;color:#0f0f0e;">
  <div style="max-width:600px;margin:0 auto;padding:32px 24px;">
    <div style="border-bottom:3px double #0f0f0e;padding-bottom:12px;margin-bottom:24px;">
      <div style="font-family:'IBM Plex Mono',Menlo,monospace;font-size:11px;letter-spacing:0.12em;text-transform:uppercase;color:#0f0f0e;">
        DealLedger
      </div>
      <div style="font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#c2410c;margin-top:4px;">
        Filed · ${new Date().toLocaleDateString('en-US', { year:'numeric', month:'short', day:'numeric' })}
      </div>
    </div>

    <div style="font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#c2410c;margin-bottom:8px;">
      New SBA Pre-Qualification Inquiry
    </div>
    <h1 style="font-family:Georgia,'Libre Baskerville',serif;font-size:22px;font-weight:700;margin:0 0 24px;line-height:1.3;">
      ${escapeHtml(body.name)}${body.listing_title ? ` &mdash; ${escapeHtml(body.listing_title)}` : ''}
    </h1>

    <table style="width:100%;border-collapse:collapse;font-size:14px;">
      <tr><td style="padding:10px 0;border-top:1px solid #d4cfc4;font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#78716c;width:130px;vertical-align:top;">Email</td>
          <td style="padding:10px 0;border-top:1px solid #d4cfc4;"><a href="mailto:${escapeHtml(body.email)}" style="color:#c2410c;text-decoration:none;">${escapeHtml(body.email)}</a></td></tr>
      ${body.phone ? `
      <tr><td style="padding:10px 0;border-top:1px solid #d4cfc4;font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#78716c;vertical-align:top;">Phone</td>
          <td style="padding:10px 0;border-top:1px solid #d4cfc4;">${escapeHtml(body.phone)}</td></tr>` : ''}
      ${body.listing_title ? `
      <tr><td style="padding:10px 0;border-top:1px solid #d4cfc4;font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#78716c;vertical-align:top;">Listing</td>
          <td style="padding:10px 0;border-top:1px solid #d4cfc4;">
            <strong>${escapeHtml(body.listing_title)}</strong><br/>
            ${body.listing_price ? `<span style="font-family:'IBM Plex Mono',Menlo,monospace;font-size:13px;color:#c2410c;">${money(body.listing_price)}</span><br/>` : ''}
            ${body.listing_url ? `<a href="${escapeHtml(body.listing_url)}" style="color:#78716c;font-size:12px;font-family:'IBM Plex Mono',Menlo,monospace;text-decoration:none;">${escapeHtml(body.listing_url)}</a>` : ''}
          </td></tr>` : ''}
      ${body.purchase_price_range ? `
      <tr><td style="padding:10px 0;border-top:1px solid #d4cfc4;font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#78716c;vertical-align:top;">Purchase Range</td>
          <td style="padding:10px 0;border-top:1px solid #d4cfc4;">${escapeHtml(body.purchase_price_range)}</td></tr>` : ''}
      ${body.down_payment ? `
      <tr><td style="padding:10px 0;border-top:1px solid #d4cfc4;font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#78716c;vertical-align:top;">Down Payment</td>
          <td style="padding:10px 0;border-top:1px solid #d4cfc4;">${escapeHtml(body.down_payment)}</td></tr>` : ''}
      ${body.credit_score_range ? `
      <tr><td style="padding:10px 0;border-top:1px solid #d4cfc4;font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#78716c;vertical-align:top;">Credit</td>
          <td style="padding:10px 0;border-top:1px solid #d4cfc4;">${escapeHtml(body.credit_score_range)}</td></tr>` : ''}
      ${body.timeline ? `
      <tr><td style="padding:10px 0;border-top:1px solid #d4cfc4;font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#78716c;vertical-align:top;">Timeline</td>
          <td style="padding:10px 0;border-top:1px solid #d4cfc4;">${escapeHtml(body.timeline)}</td></tr>` : ''}
      <tr><td style="padding:10px 0;border-top:1px solid #d4cfc4;font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#78716c;vertical-align:top;">Source</td>
          <td style="padding:10px 0;border-top:1px solid #d4cfc4;font-family:'IBM Plex Mono',Menlo,monospace;font-size:11px;color:#78716c;">${escapeHtml(body.source || 'listing_sidebar')}</td></tr>
      <tr><td style="padding:10px 0;border-top:1px solid #d4cfc4;border-bottom:1px solid #d4cfc4;font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#78716c;vertical-align:top;">Inquiry ID</td>
          <td style="padding:10px 0;border-top:1px solid #d4cfc4;border-bottom:1px solid #d4cfc4;font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;color:#78716c;">${inquiry.id}</td></tr>
    </table>

    <div style="margin-top:32px;padding-top:16px;border-top:3px double #0f0f0e;font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#78716c;">
      Reply to this email to respond directly to the buyer.
    </div>
  </div>
</body></html>
    `;

    try {
      await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${RESEND_API_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          from:     FROM_EMAIL,
          to:       [TO_EMAIL],
          ...(CC_EMAIL ? { cc: [CC_EMAIL] } : {}),
          reply_to: body.email,
          subject,
          html: internalHtml,
        }),
      });
    } catch (e) {
      console.error('Resend internal email failed:', e);
    }

    // 3. Confirmation email to the buyer — same DealLedger voice
    const buyerHtml = `
<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f2eb;font-family:Georgia,'Libre Baskerville',serif;color:#0f0f0e;">
  <div style="max-width:560px;margin:0 auto;padding:32px 24px;">
    <div style="border-bottom:3px double #0f0f0e;padding-bottom:12px;margin-bottom:32px;">
      <div style="font-family:'IBM Plex Mono',Menlo,monospace;font-size:11px;letter-spacing:0.12em;text-transform:uppercase;color:#0f0f0e;">
        DealLedger
      </div>
    </div>

    <div style="font-family:'IBM Plex Mono',Menlo,monospace;font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:#c2410c;margin-bottom:8px;">
      Inquiry received
    </div>
    <h1 style="font-family:Georgia,'Libre Baskerville',serif;font-size:24px;font-weight:700;margin:0 0 20px;line-height:1.3;">
      Thanks, ${escapeHtml(body.name.split(' ')[0])}.
    </h1>

    <p style="font-size:15px;line-height:1.7;color:#0f0f0e;margin:0 0 16px;">
      We received your SBA pre-qualification request${body.listing_title ? ` for <em>${escapeHtml(body.listing_title)}</em>` : ''}. We'll connect you with an SBA lender partner who specializes in business acquisitions.
    </p>

    <p style="font-size:15px;line-height:1.7;color:#0f0f0e;margin:0 0 16px;">
      Most acquisition buyers finance 70&ndash;90% of the purchase price through SBA 7(a) loans. Our partners typically respond within one business day with a pre-qualification range.
    </p>

    <p style="font-size:15px;line-height:1.7;color:#0f0f0e;margin:0 0 32px;">
      If you have questions in the meantime, just reply to this email.
    </p>

    <div style="border-top:1px solid #d4cfc4;padding-top:16px;font-family:'IBM Plex Mono',Menlo,monospace;font-size:11px;letter-spacing:0.05em;color:#78716c;line-height:1.7;">
      DealLedger &middot; Public record of businesses for sale.<br/>
      <a href="https://dealledger.org" style="color:#c2410c;text-decoration:none;">dealledger.org</a>
    </div>
  </div>
</body></html>
    `;

    try {
      await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${RESEND_API_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          from:    FROM_EMAIL,
          to:      [body.email],
          subject: 'Your SBA pre-qualification request — DealLedger',
          html:    buyerHtml,
        }),
      });
    } catch (e) {
      console.error('Resend confirmation email failed:', e);
    }

    return res.status(200).json({ success: true, id: inquiry.id });
  } catch (err) {
    console.error('SBA inquiry error:', err);
    return res.status(500).json({ error: 'Internal error' });
  }
}
