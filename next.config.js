/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  trailingSlash: false,

  // Serve public/index.html at the root URL.
  // Use beforeFiles so the rewrite matches before static file resolution.
  async rewrites() {
    return {
      beforeFiles: [
        { source: '/', destination: '/index.html' },
      ],
      afterFiles: [
        { source: '/sitemap-browse.xml', destination: '/api/sitemap-browse' },
      ],
      fallback: [],
    };
  },
};

module.exports = nextConfig;
