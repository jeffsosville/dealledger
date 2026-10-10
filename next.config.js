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
      beforeFiles: [],
      afterFiles: [
        { source: '/sitemap-browse.xml', destination: '/api/sitemap-browse' },
      ],
      fallback: [],
    };
  },
};

module.exports = nextConfig;
