// components/AlertSignup.tsx — new-listing email signup.
//
// Fixed scope (browse, niche and listing pages): pass `state` / `vertical`
// and the label for the heading. Picker mode (/alerts, 404): pass `picker`
// with the state and type options and the visitor chooses.
import { useEffect, useState } from 'react';

type Opt = { value: string; label: string };
type Props = {
  state?: string | null;
  vertical?: string | null;     // TYPES key or niche slug
  label: string;                // "Pizzerias in New Jersey" (title case)
  path: string;
  empty?: boolean;              // page has no listings right now
  heading?: string;             // override the heading
  picker?: { states: Opt[]; types: Opt[] };
};

const lc = (s: string) => s.charAt(0).toLowerCase() + s.slice(1);

export default function AlertSignup({ state = null, vertical = null, label, path, empty, heading, picker }: Props) {
  const [email, setEmail] = useState('');
  const [hp, setHp] = useState('');
  const [st, setSt] = useState(state ?? '');
  const [ty, setTy] = useState(vertical ?? '');
  const [status, setStatus] = useState<'idle' | 'sending' | 'sent' | 'already' | 'error'>('idle');
  const [err, setErr] = useState('');
  // Picker preselect can arrive after hydration (/alerts?type=...&state=...).
  useEffect(() => { setSt(state ?? ''); }, [state]);
  useEffect(() => { setTy(vertical ?? ''); }, [vertical]);

  const h = heading ?? (empty
    ? `Get an email when ${lc(label)} come up for sale`
    : `New ${lc(label)}, before they hit the big marketplaces`);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setStatus('sending');
    try {
      const r = await fetch('/api/alerts/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email,
          state: picker ? st || null : state,
          vertical: picker ? ty || null : vertical,
          path,
          website: hp,
        }),
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
      <div className="alert-eyebrow">— NEW LISTING ALERTS</div>
      <h2 className="alert-title">{h}</h2>
      <p className="alert-sub">
        DealLedger reads broker websites directly, every day. Once a week we&rsquo;ll email you what&rsquo;s new,
        with how long each one has really been listed.
      </p>
      {status === 'sent' ? (
        <p className="alert-done">Almost done: check your inbox for a confirmation link from pulse@dealledger.org.</p>
      ) : status === 'already' ? (
        <p className="alert-done">You&rsquo;re already getting this alert.</p>
      ) : (
        <form onSubmit={submit} className="alert-form">
          {picker && (
            <div className="alert-pick">
              <select value={ty} onChange={(e) => setTy(e.target.value)} aria-label="Business type">
                <option value="">Any type of business</option>
                {picker.types.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
              <select value={st} onChange={(e) => setSt(e.target.value)} aria-label="State">
                <option value="">Anywhere in the US</option>
                {picker.states.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
            </div>
          )}
          <div className="alert-row">
            <input
              type="email" required placeholder="you@example.com" value={email}
              onChange={(e) => setEmail(e.target.value)} aria-label="Email address"
            />
            <input
              type="text" name="website" tabIndex={-1} autoComplete="off" value={hp}
              onChange={(e) => setHp(e.target.value)} className="hp" aria-hidden="true"
            />
            <button type="submit" disabled={status === 'sending'}>
              {status === 'sending' ? 'Sending…' : 'Send me new listings'}
            </button>
          </div>
          <p className="alert-fine">No account. One-click unsubscribe.</p>
        </form>
      )}
      {status === 'error' && <p className="alert-err">{err}</p>}
      <style jsx>{`
        .alert { margin: 40px 0; border: 1px solid var(--rule); border-left: 3px solid var(--accent); background: var(--bg-card); padding: 24px; }
        .alert-eyebrow { font-family: var(--mono); font-size: 11px; letter-spacing: 0.14em; color: var(--accent); margin-bottom: 8px; }
        .alert-title { font-weight: 500; font-size: 22px; line-height: 1.25; margin: 0 0 8px 0; }
        .alert-sub { color: var(--ink-soft); margin: 0 0 16px 0; font-size: 15px; max-width: 640px; }
        .alert-pick { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; }
        .alert-pick select { flex: 1 1 220px; padding: 10px; font: inherit; font-size: 15px; border: 1px solid var(--rule); background: #fff; color: var(--ink); }
        .alert-row { display: flex; gap: 8px; flex-wrap: wrap; }
        .alert-row input[type='email'] {
          flex: 1 1 260px; padding: 10px 12px; font: inherit; font-size: 15px;
          border: 1px solid var(--rule); background: #fff; color: var(--ink);
        }
        .alert-row button {
          padding: 10px 18px; font-family: var(--mono); font-size: 13px; letter-spacing: 0.04em;
          background: var(--ink); color: var(--bg); border: 0; cursor: pointer;
        }
        .alert-row button:disabled { opacity: 0.6; cursor: default; }
        .alert-fine { margin: 8px 0 0 0; font-size: 12px; color: var(--ink-mute); font-family: var(--mono); }
        .hp { position: absolute; left: -9999px; width: 1px; height: 1px; opacity: 0; }
        .alert-done { margin: 0; font-size: 15px; }
        .alert-err { margin: 8px 0 0 0; color: var(--accent); font-size: 14px; }
      `}</style>
    </section>
  );
}
