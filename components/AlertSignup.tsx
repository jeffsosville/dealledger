// components/AlertSignup.tsx — "Email me new listings" box for browse pages.
import { useState } from 'react';

type Props = { state?: string | null; vertical?: string | null; label: string; path: string; empty?: boolean };

export default function AlertSignup({ state = null, vertical = null, label, path, empty }: Props) {
  const [email, setEmail] = useState('');
  const [hp, setHp] = useState('');
  const [status, setStatus] = useState<'idle' | 'sending' | 'sent' | 'already' | 'error'>('idle');
  const [err, setErr] = useState('');

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setStatus('sending');
    try {
      const r = await fetch('/api/alerts/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, state, vertical, path, website: hp }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(j.error || 'Something went wrong. Try again.');
      setStatus(j.already ? 'already' : 'sent');
    } catch (e2) {
      setErr((e2 as Error).message);
      setStatus('error');
    }
  };

  return (
    <section className="alert">
      <div className="alert-eyebrow">— GET ALERTS</div>
      <h2 className="alert-title">
        {empty ? `Get an email when one is listed` : `Email me new listings`}
      </h2>
      <p className="alert-sub">
        A weekly email of new listings for {label}, sent only when DealLedger finds some on broker websites. One-click unsubscribe.
      </p>
      {status === 'sent' ? (
        <p className="alert-done">Check your inbox for a confirmation link from pulse@dealledger.org.</p>
      ) : status === 'already' ? (
        <p className="alert-done">You’re already subscribed to this alert.</p>
      ) : (
        <form onSubmit={submit} className="alert-form">
          <input
            type="email" required placeholder="you@example.com" value={email}
            onChange={(e) => setEmail(e.target.value)} aria-label="Email address"
          />
          <input
            type="text" name="website" tabIndex={-1} autoComplete="off" value={hp}
            onChange={(e) => setHp(e.target.value)} className="hp" aria-hidden="true"
          />
          <button type="submit" disabled={status === 'sending'}>
            {status === 'sending' ? 'Sending…' : 'Alert me'}
          </button>
        </form>
      )}
      {status === 'error' && <p className="alert-err">{err}</p>}
      <style jsx>{`
        .alert { margin-top: 48px; border: 1px solid var(--rule); background: var(--bg-card); padding: 24px; }
        .alert-eyebrow { font-family: var(--mono); font-size: 11px; letter-spacing: 0.14em; color: var(--accent); margin-bottom: 8px; }
        .alert-title { font-weight: 500; font-size: 22px; line-height: 1.2; margin: 0 0 8px 0; }
        .alert-sub { color: var(--ink-soft); margin: 0 0 16px 0; font-size: 15px; }
        .alert-form { display: flex; gap: 8px; flex-wrap: wrap; }
        .alert-form input[type='email'] {
          flex: 1 1 260px; padding: 10px 12px; font: inherit; font-size: 15px;
          border: 1px solid var(--rule); background: #fff; color: var(--ink);
        }
        .alert-form button {
          padding: 10px 18px; font-family: var(--mono); font-size: 13px; letter-spacing: 0.04em;
          background: var(--ink); color: var(--bg); border: 0; cursor: pointer;
        }
        .alert-form button:disabled { opacity: 0.6; cursor: default; }
        .hp { position: absolute; left: -9999px; width: 1px; height: 1px; opacity: 0; }
        .alert-done { margin: 0; font-size: 15px; }
        .alert-err { margin: 8px 0 0 0; color: var(--accent); font-size: 14px; }
      `}</style>
    </section>
  );
}
