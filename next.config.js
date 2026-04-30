/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  trailingSlash: false,
  // Serve public/index.html at the root URL.
  // Next.js doesn't do this automatically — / requires either a Next.js page
  // or an explicit rewrite. We use a rewrite so the URL stays as /.
  async rewrites() {
    return [
      { source: '/', destination: '/index.html' },
    ];
  },
};

module.exports = nextConfig;
