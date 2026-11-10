-- Rollback for 2026-11-09 mcp_listing. Nothing depends on this function.
drop function if exists public.mcp_listing(bigint);
