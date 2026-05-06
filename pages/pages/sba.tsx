// pages/sba.tsx
//
// /sba — DealLedger SBA acquisition financing reference + Get Pre-Qualified.
// Visual style matches pages/listing/[id].tsx exactly: Lora serif, JetBrains
// Mono labels, paper #faf8f4, accent #b7361a, single-column 880px layout,
// "PUBLIC RECORD" masthead voice.

import Head from 'next/head';
import Link from 'next/link';
import { useState } from 'react';

export default function SBAPage() {
  const [name,  setName]  = useState('');
  const [email, setEmail] = useState('');
  const [phone, setPhone] = useState('');
  const [purchaseRange, setPurchaseRange] = useState('');
  const [downPayment,   setDownPayment]   = useState('');
  const [creditScore,   setCreditScore]   = useState('');
  const [timeline,      setTimeline]      = useState('');
  const [status, setStatus] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');
  const [errorMsg, setErrorMsg] = useState('');

  const today = new Date().toLocaleDateString('en-US', {
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name || !email) return;

    setStatus('loading');
    setErrorMsg('');

    try {
      const res = await fetch('/api/sba-inquiry', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name, email, phone,
          source: 'sba_landing',
          purchase_price_range: purchaseRange,
          down_payment:         downPayment,
          credit_score_range:   creditScore,
          timeline,
        }),
      });

      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || 'Submission failed');
      }
      setStatus('success');
    } catch (err: any) {
      setStatus('error');
      setErrorMsg(err.message || 'Something went wrong');
    }
  };

  return (
    <>
      <Head>
        <title>SBA Acquisition Financing — DealLedger</title>
        <meta
          name="description"
          content="SBA 7(a) acquisition financing for buyers on DealLedger. Get pre-qualified through a vetted lender partner."
        />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="" />
        <link
          rel="stylesheet"
          href="https://fonts.googleapis.com/css2?family=Lora:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap"
        />
      </Head>

      <style jsx global>{`
        :root {
          --bg: #faf8f4;
          --bg-card: #f4f0e8;
          --ink: #1a1612;
          --ink-soft: #4a4036;
          --ink-mute: #8a7e6e;
          --rule: #d8d0c0;
          --accent: #b7361a;
          --link: #1a1612;
          --serif: 'Lora', Georgia, serif;
          --mono: 'JetBrains Mono', ui-monospace, monospace;
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
        a {
          color: var(--link);
          text-decoration: underline;
          text-underline-offset: 2px;
        }
        a:hover { color: var(--accent); }
      `}</style>

      <div className="page">
        <header className="masthead">
          <div className="masthead-inner">
            <a href="/" className="brand">DealLedger</a>
            <div className="masthead-meta">
              PUBLIC RECORD · {today.toUpperCase()}
            </div>
          </div>
          <div className="masthead-rule" />
        </header>

        <main className="content">
          <div className="eyebrow">SBA · ACQUISITION FINANCING</div>
          <h1 className="headline">SBA 7(a) financing for buyers on the ledger.</h1>
          <p className="subtitle">
            We connect serious buyers with SBA lender partners who specialize in business
            acquisitions. Pre-qualification in under one business day. No cost.
          </p>

          {/* STATS */}
          <div className="stats">
            <Stat label="Min Down" value="10%"   sub="Buyer equity" />
            <Stat label="Term"      value="10 yr" sub="Standard 7(a)" />
            <Stat label="Max Loan"  value="$5M"   sub="SBA 7(a) cap" />
            <Stat label="Response"  value="24 hr" sub="Pre-qual range" />
          </div>

          {/* HOW IT WORKS */}
          <Section title="HOW IT WORKS" subtitle="Four steps">
            <div className="steps">
              <Step n="01" title="Inquire"
                    body="Quick form below: budget, timeline, credit range. Sixty seconds." />
              <Step n="02" title="Lender intro"
                    body="We connect you with an SBA partner matched to your deal size and industry." />
              <Step n="03" title="Pre-qualification"
                    body="Lender issues a pre-qualification letter you can use to make offers with confidence." />
              <Step n="04" title="Close & fund"
                    body="Lender handles underwriting through closing. Most acquisitions fund in 60–90 days." />
            </div>
          </Section>

          {/* WHAT IT COVERS */}
          <Section title="COVERAGE" subtitle="What SBA 7(a) finances">
            <div className="rows">
              <Row term="Purchase price"
                   def="Up to 90% of acquisition cost financed, including goodwill." />
              <Row term="Working capital"
                   def="Operating capital can be rolled into the loan to fund the first months of ownership." />
              <Row term="Equipment & vehicles"
                   def="ATM routes, vending machines, cleaning vans, service trucks — all eligible." />
              <Row term="Real estate"
                   def="If real estate is part of the deal, longer 25-year terms may apply." />
              <Row term="Closing costs"
                   def="Most fees, including the SBA guaranty fee, can be financed into the loan." />
              <Row term="Seller note refinance"
                   def="Replace short-term seller financing with long-term SBA terms." />
            </div>
          </Section>

          {/* FORM */}
          <Section title="GET PRE-QUALIFIED" subtitle="Tell us about your deal">
            {status === 'success' ? (
              <div className="success">
                <div className="success-eyebrow">FILED · {new Date().toISOString().slice(0,10)}</div>
                <div className="success-title">Inquiry received.</div>
                <p>
                  We&rsquo;ll route your request to an SBA lender partner. Expect a response within
                  one business day. Confirmation in your inbox.
                </p>
                <p>
                  In the meantime, browse listings on{' '}
                  <Link href="/" legacyBehavior><a>DealLedger</a></Link>.
                </p>
              </div>
            ) : (
              <form onSubmit={handleSubmit} className="form">
                <div className="form-row two">
                  <Field label="Name" required>
                    <input type="text" required value={name} onChange={(e) => setName(e.target.value)} />
                  </Field>
                  <Field label="Email" required>
                    <input type="email" required value={email} onChange={(e) => setEmail(e.target.value)} />
                  </Field>
                </div>

                <Field label="Phone" optional>
                  <input type="tel" value={phone} onChange={(e) => setPhone(e.target.value)} />
                </Field>

                <div className="form-row two">
                  <Field label="Purchase range">
                    <select value={purchaseRange} onChange={(e) => setPurchaseRange(e.target.value)}>
                      <option value="">Select…</option>
                      <option>Under $250K</option>
                      <option>$250K – $500K</option>
                      <option>$500K – $1M</option>
                      <option>$1M – $2.5M</option>
                      <option>$2.5M – $5M</option>
                      <option>$5M+</option>
                    </select>
                  </Field>
                  <Field label="Down payment">
                    <select value={downPayment} onChange={(e) => setDownPayment(e.target.value)}>
                      <option value="">Select…</option>
                      <option>10–15%</option>
                      <option>15–25%</option>
                      <option>25%+</option>
                    </select>
                  </Field>
                </div>

                <div className="form-row two">
                  <Field label="Credit score">
                    <select value={creditScore} onChange={(e) => setCreditScore(e.target.value)}>
                      <option value="">Select…</option>
                      <option>720+</option>
                      <option>680–720</option>
                      <option>640–680</option>
                      <option>Under 640</option>
                    </select>
                  </Field>
                  <Field label="Timeline">
                    <select value={timeline} onChange={(e) => setTimeline(e.target.value)}>
                      <option value="">Select…</option>
                      <option>Ready now</option>
                      <option>1–3 months</option>
                      <option>3–6 months</option>
                      <option>Just exploring</option>
                    </select>
                  </Field>
                </div>

                <button type="submit" disabled={status === 'loading'} className="submit">
                  {status === 'loading' ? 'Filing…' : 'Get pre-qualified →'}
                </button>

                {status === 'error' && <p className="form-error">{errorMsg}</p>}

                <p className="fineprint">
                  By submitting, you agree to be contacted by DealLedger and our SBA lender
                  partners about your inquiry. We don&rsquo;t sell your information.
                </p>
              </form>
            )}
          </Section>

          {/* FAQ */}
          <Section title="QUESTIONS" subtitle="">
            <div className="faq">
              <Faq q="Does this cost anything?"
                   a="No. SBA lenders pay us a referral fee when a loan funds. The cost to you is identical to going to the lender directly." />
              <Faq q="What's the minimum down payment?"
                   a="SBA 7(a) acquisition loans typically require 10% buyer equity. Some lenders allow seller notes on standby to count toward this." />
              <Faq q="How long does pre-qualification take?"
                   a="Most lender partners respond within one business day with a pre-qualification range based on your profile." />
              <Faq q="Which businesses qualify?"
                   a="Most for-profit US businesses with two-plus years of operating history and verifiable cash flow. ATM routes, cleaning, vending, HVAC, and other service businesses are common." />
              <Faq q="Do I have to use a specific lender?"
                   a="No. Pre-qualification is non-binding. We introduce you to a lender we believe is a good fit, but you're free to shop the deal elsewhere." />
            </div>
          </Section>

          {/* METHODOLOGY-STYLE CLOSER */}
          <Section title="METHODOLOGY" subtitle="What this is">
            <div className="prose">
              <p>
                DealLedger maintains relationships with SBA lender partners who specialize in
                business acquisitions. When buyers submit pre-qualification inquiries through this
                page, we route them to the partner best matched to the deal profile.
              </p>
              <p>
                We are compensated by lenders only when loans fund. We do not charge buyers, and
                we do not capture financing inquiries on listing detail pages — those remain
                neutral records.
              </p>
            </div>
          </Section>

          <footer className="footer">
            <div className="footer-rule" />
            <div className="footer-text">
              DealLedger · Public record · CC0
              <br />
              An open public registry of business brokerage activity in the U.S. lower middle market.
            </div>
          </footer>
        </main>
      </div>

      <style jsx>{`
        .page { min-height: 100vh; }

        .masthead { padding: 24px 0 0 0; }
        .masthead-inner {
          max-width: 880px;
          margin: 0 auto;
          padding: 0 32px 18px 32px;
          display: flex;
          justify-content: space-between;
          align-items: baseline;
        }
        .brand {
          font-family: var(--serif);
          font-weight: 700;
          font-size: 22px;
          text-decoration: none;
          letter-spacing: -0.01em;
        }
        .masthead-meta {
          font-family: var(--mono);
          font-size: 11px;
          letter-spacing: 0.08em;
          color: var(--ink-mute);
        }
        .masthead-rule {
          max-width: 880px;
          margin: 0 auto;
          border-top: 1px solid var(--rule);
        }

        .content {
          max-width: 880px;
          margin: 0 auto;
          padding: 56px 32px 96px 32px;
        }

        .eyebrow {
          font-family: var(--mono);
          font-size: 12px;
          letter-spacing: 0.1em;
          color: var(--accent);
          margin-bottom: 14px;
        }
        .headline {
          font-family: var(--serif);
          font-weight: 500;
          font-size: 44px;
          line-height: 1.1;
          letter-spacing: -0.015em;
          margin: 0 0 14px 0;
        }
        .subtitle {
          font-size: 17px;
          color: var(--ink-soft);
          margin: 0 0 40px 0;
          max-width: 64ch;
        }

        .stats {
          display: grid;
          grid-template-columns: repeat(4, 1fr);
          gap: 0;
          background: var(--bg-card);
          border: 1px solid var(--rule);
          margin-bottom: 56px;
        }

        .steps {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 0;
          background: var(--bg-card);
          border: 1px solid var(--rule);
        }

        .rows {
          background: var(--bg-card);
          border: 1px solid var(--rule);
        }

        .form {
          background: var(--bg-card);
          border: 1px solid var(--rule);
          padding: 24px;
        }
        .form-row { margin-bottom: 18px; }
        .form-row.two {
          display: grid;
          grid-template-columns: 1fr;
          gap: 18px;
        }
        .submit {
          width: 100%;
          padding: 14px;
          background: var(--ink);
          color: var(--bg);
          border: none;
          font-family: var(--mono);
          font-size: 13px;
          letter-spacing: 0.08em;
          cursor: pointer;
          margin-top: 6px;
        }
        .submit:hover:not(:disabled) { background: var(--accent); }
        .submit:disabled { opacity: 0.5; cursor: not-allowed; }
        .form-error {
          font-family: var(--mono);
          font-size: 12px;
          color: var(--accent);
          margin-top: 12px;
        }
        .fineprint {
          font-family: var(--mono);
          font-size: 11px;
          line-height: 1.7;
          color: var(--ink-mute);
          margin-top: 16px;
        }

        .success {
          background: var(--bg-card);
          border: 1px solid var(--rule);
          padding: 28px 24px;
        }
        .success-eyebrow {
          font-family: var(--mono);
          font-size: 11px;
          letter-spacing: 0.14em;
          color: var(--accent);
          margin-bottom: 8px;
        }
        .success-title {
          font-family: var(--serif);
          font-weight: 500;
          font-size: 26px;
          letter-spacing: -0.01em;
          margin-bottom: 14px;
        }
        .success p {
          margin: 0 0 12px 0;
          color: var(--ink-soft);
          max-width: 64ch;
        }
        .success p:last-child { margin-bottom: 0; }

        .faq { background: var(--bg-card); border: 1px solid var(--rule); }

        .prose p {
          margin: 0 0 14px 0;
          color: var(--ink-soft);
          max-width: 64ch;
        }
        .prose p:last-child { margin-bottom: 0; }

        .footer { margin-top: 80px; }
        .footer-rule {
          border-top: 1px solid var(--rule);
          margin-bottom: 20px;
        }
        .footer-text {
          font-family: var(--mono);
          font-size: 11px;
          letter-spacing: 0.04em;
          color: var(--ink-mute);
          line-height: 1.7;
        }

        @media (min-width: 600px) {
          .form-row.two { grid-template-columns: 1fr 1fr; gap: 18px; }
        }
        @media (max-width: 720px) {
          .headline { font-size: 32px; }
          .stats { grid-template-columns: 1fr 1fr; }
          .steps { grid-template-columns: 1fr; }
          .content { padding: 40px 22px 64px 22px; }
        }
      `}</style>
    </>
  );
}

/* ────────────────────────────────────────────────────────── */
/*  Sub-components — match listing-page conventions           */
/* ────────────────────────────────────────────────────────── */

function Stat({ label, value, sub }: { label: string; value: string; sub: string }) {
  return (
    <div className="stat">
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      <div className="stat-sub">{sub}</div>
      <style jsx>{`
        .stat {
          padding: 22px 24px 20px 24px;
          border-right: 1px solid var(--rule);
        }
        .stat:last-child { border-right: none; }
        .stat-label {
          font-family: var(--mono);
          font-size: 10px;
          letter-spacing: 0.1em;
          text-transform: uppercase;
          color: var(--ink-mute);
          margin-bottom: 8px;
        }
        .stat-value {
          font-family: var(--serif);
          font-size: 36px;
          font-weight: 500;
          line-height: 1;
          letter-spacing: -0.02em;
          margin-bottom: 6px;
        }
        .stat-sub {
          font-size: 13px;
          color: var(--ink-soft);
        }
        @media (max-width: 720px) {
          .stat:nth-child(2) { border-right: none; }
          .stat:nth-child(1), .stat:nth-child(2) {
            border-bottom: 1px solid var(--rule);
          }
        }
      `}</style>
    </div>
  );
}

function Section({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle: string;
  children: React.ReactNode;
}) {
  return (
    <section className="section">
      <div className="section-eyebrow">— {title}</div>
      {subtitle ? <h2 className="section-title">{subtitle}</h2> : null}
      <div className="section-body">{children}</div>
      <style jsx>{`
        .section { margin-bottom: 56px; }
        .section-eyebrow {
          font-family: var(--mono);
          font-size: 11px;
          letter-spacing: 0.14em;
          color: var(--accent);
          margin-bottom: 8px;
        }
        .section-title {
          font-family: var(--serif);
          font-weight: 500;
          font-size: 26px;
          line-height: 1.2;
          letter-spacing: -0.01em;
          margin: 0 0 20px 0;
        }
      `}</style>
    </section>
  );
}

function Step({ n, title, body }: { n: string; title: string; body: string }) {
  return (
    <div className="step">
      <div className="step-n">{n}</div>
      <h3>{title}</h3>
      <p>{body}</p>
      <style jsx>{`
        .step {
          padding: 22px 24px;
          border-right: 1px solid var(--rule);
          border-bottom: 1px solid var(--rule);
        }
        .step:nth-child(2n) { border-right: none; }
        .step:nth-last-child(-n+2) { border-bottom: none; }
        .step-n {
          font-family: var(--mono);
          font-size: 11px;
          letter-spacing: 0.14em;
          color: var(--accent);
          margin-bottom: 10px;
        }
        h3 {
          font-family: var(--serif);
          font-weight: 500;
          font-size: 18px;
          letter-spacing: -0.01em;
          margin: 0 0 6px 0;
        }
        p {
          font-size: 14px;
          color: var(--ink-soft);
          margin: 0;
        }
      `}</style>
    </div>
  );
}

function Row({ term, def }: { term: string; def: string }) {
  return (
    <div className="row">
      <div className="term">{term}</div>
      <div className="def">{def}</div>
      <style jsx>{`
        .row {
          display: grid;
          grid-template-columns: 1fr;
          gap: 4px;
          padding: 14px 20px;
          border-bottom: 1px solid var(--rule);
          font-size: 14px;
        }
        .row:last-child { border-bottom: none; }
        @media (min-width: 600px) {
          .row {
            grid-template-columns: 200px 1fr;
            gap: 24px;
          }
        }
        .term {
          font-family: var(--mono);
          font-size: 12px;
          letter-spacing: 0.05em;
          color: var(--ink);
        }
        .def {
          color: var(--ink-soft);
          line-height: 1.6;
        }
      `}</style>
    </div>
  );
}

function Field({
  label, required, optional, children,
}: {
  label: string;
  required?: boolean;
  optional?: boolean;
  children: React.ReactNode;
}) {
  return (
    <label className="field">
      <span>
        {label}
        {required && <em> *</em>}
        {optional && <em className="opt"> (optional)</em>}
      </span>
      {children}
      <style jsx>{`
        .field { display: block; }
        .field > span {
          display: block;
          font-family: var(--mono);
          font-size: 10px;
          letter-spacing: 0.1em;
          text-transform: uppercase;
          color: var(--ink-mute);
          margin-bottom: 6px;
        }
        .field > span :global(em) {
          font-style: normal;
          color: var(--accent);
        }
        .field > span :global(em.opt) {
          color: var(--ink-mute);
          text-transform: none;
          letter-spacing: 0;
        }
        .field :global(input),
        .field :global(select) {
          width: 100%;
          padding: 9px 10px;
          background: var(--bg);
          border: 1px solid var(--rule);
          font-family: var(--mono);
          font-size: 13px;
          color: var(--ink);
          outline: none;
          transition: border-color 0.15s ease;
        }
        .field :global(input:focus),
        .field :global(select:focus) {
          border-color: var(--accent);
        }
      `}</style>
    </label>
  );
}

function Faq({ q, a }: { q: string; a: string }) {
  return (
    <div className="faq-item">
      <div className="q">{q}</div>
      <div className="a">{a}</div>
      <style jsx>{`
        .faq-item {
          padding: 18px 22px;
          border-bottom: 1px solid var(--rule);
        }
        .faq-item:last-child { border-bottom: none; }
        .q {
          font-family: var(--serif);
          font-size: 17px;
          font-weight: 500;
          letter-spacing: -0.005em;
          color: var(--ink);
          margin-bottom: 8px;
        }
        .a {
          font-size: 14px;
          line-height: 1.65;
          color: var(--ink-soft);
          max-width: 64ch;
        }
      `}</style>
    </div>
  );
}
