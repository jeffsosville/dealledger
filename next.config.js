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
      // Type slugs that 404'd in Nov 2026 traffic (mostly links from AI answers).
      { source: '/businesses-for-sale/type/:t(staffing|staffing-and-recruiting|recruiting)', destination: '/businesses-for-sale/type/staffing-agencies', permanent: true },
      { source: '/businesses-for-sale/type/:t(it-msp|msp|msps|managed-service-providers|managed-services)', destination: '/businesses-for-sale/type/it-and-managed-services', permanent: true },
      { source: '/businesses-for-sale/:state([a-z-]+)/property-management', destination: '/businesses-for-sale/:state/property-management-companies', permanent: true },
      { source: '/businesses-for-sale/:state([a-z-]+)/bars-and-restaurants', destination: '/businesses-for-sale/:state/restaurants', permanent: true },
      // /virginia/type/construction → /virginia/construction
      { source: '/businesses-for-sale/:state([a-z-]+)/type/:t([a-z0-9-]+)', destination: '/businesses-for-sale/:state/:t', permanent: true },
      { source: '/listings', destination: '/businesses-for-sale', permanent: true },
      { source: '/search', destination: '/businesses-for-sale', permanent: false },
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
