-- 2026-11-18: one reminder for alerts left unconfirmed 24h after signup.
-- scripts/send_confirm_reminders.ts sets confirm_reminded_at; a row is reminded at most once.
alter table public.alert_subscriptions add column if not exists confirm_reminded_at timestamptz;

-- Rollback:
-- alter table public.alert_subscriptions drop column if exists confirm_reminded_at;
