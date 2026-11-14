// /sitemap-browse.xml (rewritten here in next.config.js) — the /businesses-for-sale pages.
// Built live from mv_seo_page_stats so it always matches the pages that exist.
import type { NextApiRequest, NextApiResponse } from 'next';
import { allStats, comboPath, hasComboPage, hasStatePage, hasTypePage, pageUrl, statePath, stateSlug, typePath, STATES } from '../../lib/seo';
import { NICHE_COMBO_MIN, NICHE_MIN, nicheCounts, nichePath } from '../../lib/niches';

export default async function handler(_req: NextApiRequest, res: NextApiResponse) {
  const [all, nc] = await Promise.all([allStats(), nicheCounts()]);
  const paths = [
    '/businesses-for-sale',
    ...all.filter(hasStatePage).map((s) => statePath(s.page_state!)),
    ...all.filter(hasTypePage).map((s) => typePath(s.page_vertical!)),
    ...all.filter(hasComboPage).map((s) => comboPath(s.page_state!, s.page_vertical!)),
    ...nc.filter((c) => c.count >= NICHE_MIN).map((c) => nichePath(c.slug)),
    ...nc.flatMap((c) => Object.entries(c.states)
      .filter(([code, n]) => n >= NICHE_COMBO_MIN && STATES[code])
      .map(([code]) => `/businesses-for-sale/${stateSlug(code)}/${c.slug}`)),
  ];
  // Stats behind these pages refresh daily, so today is an honest lastmod.
  const today = new Date().toISOString().slice(0, 10);
  const body =
    '<?xml version="1.0" encoding="UTF-8"?>\n' +
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' +
    paths.map((p) => `  <url>\n    <loc>${pageUrl(p)}</loc>\n    <lastmod>${today}</lastmod>\n    <changefreq>daily</changefreq>\n    <priority>0.9</priority>\n  </url>`).join('\n') +
    '\n</urlset>\n';
  res.setHeader('Content-Type', 'application/xml; charset=utf-8');
  res.setHeader('Cache-Control', 'public, s-maxage=21600, stale-while-revalidate=86400');
  res.status(200).send(body);
}
