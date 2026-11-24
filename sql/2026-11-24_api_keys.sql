-- 2026-11-24 — Free API keys for /api/v1.
-- Keys are emailed once and stored only as a SHA-256 hash. Service role only (RLS on, no policies).
-- mcp_calls gains api_key_id + ip_hash so usage can be tied to a key, and anonymous
-- traffic can be rate-limited per (hashed) IP. Additive. Rollback at bottom.

create table if not exists public.api_keys (
  id           bigint generated always as identity primary key,
  key_hash     text not null unique,          -- sha256 hex of the full key
  key_prefix   text not null,                 -- first 11 chars ("dl_" + 8), safe to show/log
  email        text not null,
  name         text,                          -- person or org
  use_case     text,                          -- one line: what they're building
  daily_limit  integer not null default 10000,
  created_at   timestamptz not null default now(),
  last_used_at timestamptz,
  revoked_at   timestamptz,
  user_agent   text,
  ip_hash      text
);
create index if not exists api_keys_email_idx on public.api_keys (lower(email));
alter table public.api_keys enable row level security;
revoke all on public.api_keys from anon, authenticated;

alter table public.mcp_calls add column if not exists api_key_id bigint references public.api_keys(id);
alter table public.mcp_calls add column if not exists ip_hash text;
create index if not exists mcp_calls_anon_ip_idx on public.mcp_calls (ip_hash, called_at desc) where api_key_id is null;
create index if not exists mcp_calls_key_idx on public.mcp_calls (api_key_id, called_at desc) where api_key_id is not null;

-- Who's using the API, last 7 days.
create or replace view public.v_api_key_usage with (security_invoker = true) as
select k.id, k.key_prefix, k.email, k.name, k.use_case, k.created_at, k.last_used_at, k.revoked_at,
       count(c.id) filter (where c.called_at > now() - interval '1 day')  as calls_24h,
       count(c.id) filter (where c.called_at > now() - interval '7 days') as calls_7d,
       sum(c.result_count) filter (where c.called_at > now() - interval '7 days') as rows_7d
from public.api_keys k
left join public.mcp_calls c on c.api_key_id = k.id
group by k.id;
revoke all on public.v_api_key_usage from anon, authenticated;

-- Rollback:
-- drop view public.v_api_key_usage;
-- alter table public.mcp_calls drop column api_key_id, drop column ip_hash;
-- drop table public.api_keys;
