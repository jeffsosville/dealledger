// pages/price-changes.tsx
//
// DealLedger — /price-changes
//
// Every reviewed asking-price change on a broker's own listing, newest first.
// Source: public.price_history (approved, broker-direct only; the same view the
// listing page's PRICE CHANGES section reads). Titles and locations come from
// the same lookup the listing page uses, so a row here always opens a listing
// page that shows the same change.
//
// Each change is an observation window: the price moved after prev_last_seen
// and on or before next_first_seen. The exact day is not known and not shown.
// No counts are printed on this page; public figures come from public_stats.

import type { GetServerSideProps, InferGetServerSidePropsType } from 'next';
import Head from 'next/head';
import { useState } from 'react';
import { getSupabase } from '../lib/supabase';

type Change = {
  listing_number: number;
  title: string;
  location: string;
  broker: string;
  prev_price: number;
  prev_last_seen: string;
  next_price: number;
  next_first_seen: string;
  pct_change: number;
};

type PageProps = { changes: Change[]; asOf: string };

type Dir = 'all' | 'down' | 'up';

const hostOf = (url: string | null | undefined) =>
  url ? url.replace(/^https?:\/\/(www\.)?/i, '').split(/[/?#:]/)[0].toLowerCase() : '';

export const getServerSideProps: GetServerSideProps<PageProps> = async ({ res }) => {
  const sb = getSupabase();

  const { data: ph, error: phErr } = await sb
    .from('price_history')
    .select('listing_number, prev_price, prev_last_seen, next_price, next_first_seen, pct_change')
    .order('next_first_seen', { ascending: false })
    .limit(5000);
  if (phErr) throw new Error(`price_history: ${phErr.message}`);

  const rows = (ph ?? [])
    .map((c: any) => ({
      listing_number: Number(c.listing_number),
      prev_price: Number(c.prev_price),
      prev_last_seen: String(c.prev_last_seen).slice(0, 10),
      next_price: Number(c.next_price),
      next_first_seen: String(c.next_first_seen).slice(0, 10),
      pct_change: Number(c.pct_change),
    }))
    // Same guard as the listing page: never show a change it would hide.
    .filter((c) => c.prev_price > 0 && c.next_price > 0 && c.next_first_seen > c.prev_last_seen);

  const ids = Array.from(new Set(rows.map((r) => r.listing_number)));
  const meta = new Map<number, { title: string; location: string; broker: string }>();
  for (let i = 0; i < ids.length; i += 200) {
    const { data: ls, error: lErr } = await sb
      .from('listings')
      .select('listing_number, header, city, state, url')
      .in('listing_number', ids.slice(i, i + 200));
    if (lErr) throw new Error(`listings: ${lErr.message}`);
    for (const l of ls ?? []) {
      meta.set(Number(l.listing_number), {
        title: (l.header || '').trim(),
        location: [l.city, l.state].filter(Boolean).join(', '),
        broker: hostOf(l.url),
      });
    }
  }

  // Only rows whose listing page exists (that page 404s without a listings row).
  const changes: Change[] = rows
    .filter((r) => meta.has(r.listing_number))
    .map((r) => ({ ...r, ...meta.get(r.listing_number)!, title: meta.get(r.listing_number)!.title || 'Untitled listing' }));

  res.setHeader('Cache-Control', 'public, s-maxage=3600, stale-while-revalidate=86400');
  return { props: { changes, asOf: new Date().toISOString().slice(0, 10) } };
};

const fmtPrice = (n: number) => `$${new Intl.NumberFormat('en-US').format(Math.round(n))}`;

const fmtDay = (iso: string, withYear = true) =>
  new Date(`${iso}T00:00:00Z`).toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', ...(withYear ? { year: 'numeric' } : {}), timeZone: 'UTC',
  });

const fmtWindow = (from: string, to: string) =>
  from.slice(0, 4) === to.slice(0, 4)
    ? `${fmtDay(from, false)} – ${fmtDay(to)}`
    : `${fmtDay(from)} – ${fmtDay(to)}`;

const fmtPct = (p: number) => `${p > 0 ? '+' : ''}${(Math.round(p * 10) / 10).toFixed(1)}%`;


export default function PriceChanges({ changes, asOf }: InferGetServerSidePropsType<typeof getServerSideProps>) {
  const [dir, setDir] = useState<Dir>('all');
  const shown = changes.filter((c) =>
    dir === 'all' ? true : dir === 'down' ? c.next_price < c.prev_price : c.next_price > c.prev_price
  );

  const title = 'Asking-price changes on business listings | DealLedger';
  const desc =
    "Asking-price changes recorded on business brokers' own listings, with the dates we saw each price. Open data, CC0.";

  return (
    <>
      <Head>
        <title>{title}</title>
        <meta name="description" content={desc} />
        <link rel="canonical" href="https://dealledger.org/price-changes" />
        <meta property="og:title" content="Asking-price changes" />
        <meta property="og:description" content={desc} />
        <meta property="og:url" content="https://dealledger.org/price-changes" />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="" />
        <link
          rel="stylesheet"
          href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap"
        />
      </Head>

      <style jsx global>{`
        :root {
          --bg: #fafaf9;
          --bg-card: #f5f5f4;
          --ink: #1c1917;
          --ink-soft: #57534e;
          --ink-mute: #78716c;
          --rule: #d4d4d4;
          --accent: #c2410c;
          --link: #0c4a6e;
          --serif: 'IBM Plex Sans', -apple-system, system-ui, sans-serif;
          --mono: 'IBM Plex Mono', ui-monospace, monospace;
        }
        * { box-sizing: border-box; }
        body {
          margin: 0;
          background: var(--bg);
          color: var(--ink);
          font-family: var(--serif);
          font-size: 16px;
          line-height: 1.55;
          -webkit-font-smoothing: antialiased;
        }
        a { color: var(--link); text-decoration: underline; text-underline-offset: 2px; }
        a:hover { color: var(--accent); }
      `}</style>

      <div className="page">
        <header className="masthead">
          <div className="masthead-inner">
            <a href="/" className="brand">DealLedger</a>
            <nav className="nav">
              <a href="/businesses-for-sale">Browse</a>
              <a href="/brokers">Brokers</a>
              <a href="/price-changes" className="active">Price Changes</a>
              <a href="/writing">Pulse</a>
              <a href="/#about">About</a>
            </nav>
          </div>
          <div className="masthead-rule" />
        </header>

        <main className="content">
          <div className="eyebrow">— PRICE CHANGES</div>
          <h1 className="headline">Asking-price changes</h1>
          <p className="intro">
            Each row is a listing whose asking price changed on the broker&rsquo;s own website. We show the
            last date we saw the old price and the first date we saw the new one; the change happened
            somewhere in that window. Every change here was reviewed against our dated snapshots of the
            broker&rsquo;s page. These are asking prices, not sale prices.
          </p>

          <div className="filters" role="group" aria-label="Direction">
            {(['all', 'down', 'up'] as Dir[]).map((d) => (
              <button
                key={d}
                type="button"
                className={dir === d ? 'f on' : 'f'}
                aria-pressed={dir === d}
                onClick={() => setDir(d)}
              >
                {d === 'all' ? 'All' : d === 'down' ? 'Reductions' : 'Increases'}
              </button>
            ))}
          </div>

          {shown.length === 0 ? (
            <p className="empty">No changes to show.</p>
          ) : (
            <div className="table">
              <div className="t-head">
                <div>Listing</div>
                <div className="num">Was</div>
                <div className="num">Now</div>
                <div className="num">Change</div>
                <div>Changed between</div>
              </div>
              {shown.map((c) => (
                <div className="t-row" key={`${c.listing_number}-${c.next_first_seen}`}>
                  <div className="t-title">
                    <a href={`/listing/${c.listing_number}`}>{c.title}</a>
                    <div className="t-sub">
                      {[c.location, c.broker].filter(Boolean).join(' · ')}
                    </div>
                  </div>
                  <div className="num t-was">{fmtPrice(c.prev_price)}</div>
                  <div className="num">{fmtPrice(c.next_price)}</div>
                  <div className={`num ${c.next_price < c.prev_price ? 'down' : 'up'}`}>{fmtPct(c.pct_change)}</div>
                  <div className="t-date">{fmtWindow(c.prev_last_seen, c.next_first_seen)}</div>
                </div>
              ))}
            </div>
          )}

          <p className="note">
            Updated {fmtDay(asOf)}. A listing appears here only after a change has been reviewed, so recent
            changes can take a few days to show. <a href="/methodology.html">Methodology</a>.
          </p>

          <footer className="footer">
            <div className="footer-rule" />
            <div className="footer-text">
              DealLedger · Public record · CC0 · <a href="/businesses-for-sale">Browse</a> ·{' '}
              <a href="/brokers">Brokers</a> · <a href="/methodology.html">Methodology</a>
            </div>
          </footer>
        </main>
      </div>

      <style jsx>{`
        .page { min-height: 100vh; }
        .masthead { padding: 24px 0 0 0; }
        .masthead-inner {
          max-width: 1100px; margin: 0 auto; padding: 0 32px 18px 32px;
          display: flex; justify-content: space-between; align-items: baseline; gap: 16px; flex-wrap: wrap;
        }
        .brand { font-weight: 700; font-size: 22px; text-decoration: none; letter-spacing: -0.01em; color: var(--ink); }
        .nav { display: flex; gap: 20px; flex-wrap: wrap; }
        .nav a {
          font-family: var(--mono); font-size: 11px; letter-spacing: 0.08em; text-transform: uppercase;
          color: var(--ink-mute); text-decoration: none;
        }
        .nav a:hover, .nav a.active { color: var(--accent); }
        .masthead-rule { max-width: 1100px; margin: 0 auto; border-top: 1px solid var(--rule); }
        .content { max-width: 1100px; margin: 0 auto; padding: 56px 32px 96px 32px; }
        .eyebrow {
          font-family: var(--mono); font-size: 12px; letter-spacing: 0.1em;
          color: var(--accent); margin-bottom: 14px;
        }
        .headline {
          font-weight: 500; font-size: 44px; line-height: 1.1;
          letter-spacing: -0.015em; margin: 0 0 14px 0;
        }
        .intro { font-size: 16px; color: var(--ink-soft); max-width: 72ch; margin: 0 0 28px 0; }
        .filters { display: flex; gap: 8px; margin: 0 0 16px 0; }
        .f {
          font-family: var(--mono); font-size: 12px; letter-spacing: 0.04em;
          padding: 6px 12px; border: 1px solid var(--rule); background: transparent;
          color: var(--ink-soft); cursor: pointer;
        }
        .f.on { border-color: var(--ink); color: var(--ink); background: var(--bg-card); }
        .table { border: 1px solid var(--rule); background: var(--bg-card); font-size: 14px; }
        .t-head, .t-row {
          display: grid; grid-template-columns: 1fr 110px 110px 80px 170px;
          gap: 14px; padding: 10px 18px; border-bottom: 1px solid var(--rule); align-items: baseline;
        }
        .t-row:last-child { border-bottom: none; }
        .t-head {
          font-family: var(--mono); font-size: 10px; letter-spacing: 0.08em;
          text-transform: uppercase; color: var(--ink-mute);
        }
        .t-title a { color: var(--ink); text-decoration: none; }
        .t-title a:hover { color: var(--accent); text-decoration: underline; }
        .t-sub { font-size: 12px; color: var(--ink-mute); margin-top: 2px; overflow-wrap: anywhere; }
        .num { font-family: var(--mono); text-align: right; }
        .t-was { color: var(--ink-mute); text-decoration: line-through; }
        .down { color: var(--accent); }
        .up { color: var(--link); }
        .t-date { font-size: 12px; color: var(--ink-soft); }
        .empty { font-size: 14px; color: var(--ink-mute); }
        .note { font-size: 13px; color: var(--ink-mute); max-width: 75ch; margin: 16px 0 0 0; }
        .footer { margin-top: 80px; }
        .footer-rule { border-top: 1px solid var(--rule); margin-bottom: 20px; }
        .footer-text { font-family: var(--mono); font-size: 11px; color: var(--ink-mute); }
        @media (max-width: 900px) {
          .t-head, .t-row { grid-template-columns: 1fr 96px 70px; padding: 10px 14px; }
          .t-head > :nth-child(2), .t-row > :nth-child(2),
          .t-head > :nth-child(5), .t-row > :nth-child(5) { display: none; }
        }
        @media (max-width: 720px) {
          .headline { font-size: 32px; }
          .content { padding: 40px 16px 64px 16px; }
          .masthead-inner { padding: 0 16px 18px 16px; }
        }
      `}</style>
    </>
  );
}
