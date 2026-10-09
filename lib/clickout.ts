// lib/clickout.ts
//
// Outbound click tracking. Every link from dealledger.org to a broker's site
// goes through /go/{listing_number} or /go/b/{slug}. Those routes log one row
// in listing_clicks (via the dl_click / dl_click_broker RPCs) and redirect.
// The destination always comes from the database, so /go is not an open
// redirect. See sql/2026-10-08_click_tracking.sql.

import type { GetServerSidePropsContext } from 'next';
import { getSupabase } from './supabase';

/** Link to a listing on the broker's own site, tracked. */
export const goListing = (listingNumber: number | string, source: string) =>
  `/go/${encodeURIComponent(String(listingNumber))}?s=${encodeURIComponent(source)}`;

/** Link to a broker's homepage or listings page, tracked. */
export const goBroker = (slug: string, kind: 'home' | 'listings', source: string) =>
  `/go/b/${encodeURIComponent(slug)}?to=${kind}&s=${encodeURIComponent(source)}`;

export const requestMeta = (ctx: GetServerSidePropsContext) => {
  const s = ctx.query.s;
  return {
    source: typeof s === 'string' ? s.slice(0, 32) : null,
    referrer: (ctx.req.headers.referer as string | undefined) ?? null,
    ua: (ctx.req.headers['user-agent'] as string | undefined) ?? null,
  };
};

export const redirectTo = (url: string | null, fallback: string) => ({
  redirect: { destination: url || fallback, permanent: false },
});

export { getSupabase };
