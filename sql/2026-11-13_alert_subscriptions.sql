-- 2026-11-13: email alerts for new listings on a browse page (state, type, or state+type).
--
-- Flow: /api/alerts/subscribe inserts an unconfirmed row and emails a confirm
-- link (double opt-in). /api/alerts/confirm sets confirmed_at. The weekly
-- digest (scripts/send_alert_digest.ts) mails confirmed, active rows the
-- listings first seen since last_sent_at, then advances last_sent_at.
-- /api/alerts/unsubscribe sets unsubscribed_at. Rows are never deleted by code.
--
-- Applied 2026-11-14 (migration alert_subscriptions). Both scope columns null = every new listing.
--
-- Service role only: RLS on, no policies. Emails are never exposed to anon.

create table if not exists public.alert_subscriptions (
  id              uuid primary key default gen_random_uuid(),
  email           text not null check (email ~ '^[^\s@]+@[^\s@]+\.[^\s@]+$'),
  state_code      text,            -- two-letter, null = any state
  vertical        text,            -- lib/seo.ts TYPES key or lib/niches.ts slug, null = any type
  token           uuid not null unique default gen_random_uuid(),
  source_path     text,
  confirmed_at    timestamptz,
  unsubscribed_at timestamptz,
  last_sent_at    timestamptz,
  created_at      timestamptz not null default now(),
  user_agent      text,
  ip_address      text
);

create unique index if not exists alert_subscriptions_email_scope_uq
  on public.alert_subscriptions (lower(email), coalesce(state_code, ''), coalesce(vertical, ''));

create index if not exists alert_subscriptions_active_idx
  on public.alert_subscriptions (confirmed_at)
  where confirmed_at is not null and unsubscribed_at is null;

alter table public.alert_subscriptions enable row level security;
revoke all on public.alert_subscriptions from anon, authenticated;
