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
    return [
      { source: '/index.html', destination: '/', permanent: true },
      // Singular and alternate type slugs people search for; these 404'd.
      { source: '/businesses-for-sale/type/laundromat', destination: '/businesses-for-sale/type/laundromats', permanent: true },
      { source: '/businesses-for-sale/type/restaurant', destination: '/businesses-for-sale/type/restaurants', permanent: true },
      { source: '/businesses-for-sale/type/commercial-cleaning', destination: '/businesses-for-sale/type/cleaning', permanent: true },
    ];
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
