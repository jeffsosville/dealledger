// pages/_app.tsx
//
// Google Analytics (GA4) for every React-rendered page: broker pages,
// listing pages, the businesses-for-sale browse pages, /brokers, /sba.
//
// The homepage is not covered here — pages/index.tsx writes lib/home.html
// straight to the response, and that template carries its own gtag snippet.
// The static pages in public/ carry their own snippet too.
//
// gtag's config call sends the first page_view; client-side navigations
// between Next pages send their own page_view on routeChangeComplete.

import type { AppProps } from 'next/app';
import Script from 'next/script';
import { useRouter } from 'next/router';
import { useEffect } from 'react';

const GA_ID = 'G-P8PBL3LPF2';

declare global {
  interface Window {
    gtag?: (...args: unknown[]) => void;
  }
}

export default function App({ Component, pageProps }: AppProps) {
  const router = useRouter();

  useEffect(() => {
    const onRouteChange = (url: string) => {
      window.gtag?.('event', 'page_view', {
        page_path: url,
        page_location: window.location.href,
        page_title: document.title,
      });
    };
    router.events.on('routeChangeComplete', onRouteChange);
    return () => router.events.off('routeChangeComplete', onRouteChange);
  }, [router.events]);

  return (
    <>
      <Script
        src={`https://www.googletagmanager.com/gtag/js?id=${GA_ID}`}
        strategy="afterInteractive"
      />
      <Script id="ga4" strategy="afterInteractive">
        {`window.dataLayer = window.dataLayer || [];
function gtag(){dataLayer.push(arguments);}
gtag('js', new Date());
gtag('config', '${GA_ID}');`}
      </Script>
      <Component {...pageProps} />
    </>
  );
}
