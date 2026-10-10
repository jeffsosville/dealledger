// pages/index.tsx
//
// Homepage. The markup lives in lib/home.html; this route fills in the
// listing count and the first page of the ledger on the server, so search
// engines, link previews and visitors without JavaScript see real numbers
// instead of "—". The page's own script then takes over for search, filters
// and paging, starting from the rows sent here (window.__DL_INITIAL__).
//
// If Supabase is unreachable the template is served unfilled and the
// browser loads the data itself, as before.

import type { GetServerSideProps } from 'next';
import { loadHome, renderHome } from '../lib/homeRender';

export const getServerSideProps: GetServerSideProps = async ({ res }) => {
  const data = await loadHome();
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  // Cached at Vercel's edge; listings change daily.
  res.setHeader(
    'Cache-Control',
    data
      ? 'public, s-maxage=900, stale-while-revalidate=86400'
      : 'public, s-maxage=60, stale-while-revalidate=600',
  );
  res.end(renderHome(data));
  return { props: {} };
};

export default function Home() {
  return null;
}
