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
      afterFiles: [],
      fallback: [],
    };
  },
};

module.exports = nextConfig;
