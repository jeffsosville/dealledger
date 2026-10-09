// pages/go/b/[slug].tsx — tracked click-out to a broker's homepage or listings page.
import type { GetServerSideProps } from 'next';
import { getSupabase, redirectTo, requestMeta } from '../../../lib/clickout';

export const getServerSideProps: GetServerSideProps = async (ctx) => {
  const slug = typeof ctx.params?.slug === 'string' ? ctx.params.slug.toLowerCase() : '';
  if (!/^[a-z0-9-]{1,120}$/.test(slug)) return { notFound: true };

  ctx.res.setHeader('Cache-Control', 'no-store');
  ctx.res.setHeader('X-Robots-Tag', 'noindex, nofollow');

  const kind = ctx.query.to === 'listings' ? 'listings' : 'home';
  const { source, referrer, ua } = requestMeta(ctx);
  try {
    const { data } = await getSupabase().rpc('dl_click_broker', {
      p_slug: slug,
      p_kind: kind,
      p_source: source,
      p_referrer: referrer,
      p_ua: ua,
    });
    return redirectTo(typeof data === 'string' ? data : null, `/broker/${slug}`);
  } catch {
    return redirectTo(null, `/broker/${slug}`);
  }
};

export default function GoBroker() {
  return null;
}
