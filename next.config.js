/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  trailingSlash: false,

  // The homepage (pages/index.tsx) reads its markup from lib/home.html at
  // request time; make sure the file ships with the serverless function.
  experimental: {
    outputFileTracingIncludes: {
      '/': ['./lib/home.html'],
    },
  },

  async redirects() {
    return [{ source: '/index.html', destination: '/', permanent: true }];
  },

  async rewrites() {
    return {
      beforeFiles: [
        // CSV download of every listing behind a browse page (pages/api/browse-csv.ts).
        { source: '/businesses-for-sale/type/:type([a-z0-9-]+)\\.csv', destination: '/api/browse-csv?type=:type' },
        { source: '/businesses-for-sale/:state([a-z-]+)/:type([a-z0-9-]+)\\.csv', destination: '/api/browse-csv?state=:state&type=:type' },
        { source: '/businesses-for-sale/:state([a-z-]+)\\.csv', destination: '/api/browse-csv?state=:state' },
      ],
      afterFiles: [
        { source: '/sitemap-browse.xml', destination: '/api/sitemap-browse' },
      ],
      fallback: [],
    };
  },
};

module.exports = nextConfig;
