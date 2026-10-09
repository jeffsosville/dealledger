// pages/go/[n].tsx — tracked click-out to a listing on the broker's site.
import type { GetServerSideProps } from 'next';
import { getSupabase, redirectTo, requestMeta } from '../../lib/clickout';

export const getServerSideProps: GetServerSideProps = async (ctx) => {
  const raw = typeof ctx.params?.n === 'string' ? ctx.params.n : '';
  if (!/^\d{1,15}$/.test(raw)) return { notFound: true };

  ctx.res.setHeader('Cache-Control', 'no-store');
  ctx.res.setHeader('X-Robots-Tag', 'noindex, nofollow');

  const { source, referrer, ua } = requestMeta(ctx);
  try {
    const { data } = await getSupabase().rpc('dl_click', {
      p_listing_number: Number(raw),
      p_source: source,
      p_referrer: referrer,
      p_ua: ua,
    });
    return redirectTo(typeof data === 'string' ? data : null, `/listing/${raw}`);
  } catch {
    return redirectTo(null, `/listing/${raw}`);
  }
};

export default function Go() {
  return null;
}
