import { createClient } from '@supabase/supabase-js';

// Anon key — safe to expose to the browser. RLS policies enforce read access.
// Service role key (NEVER use in this file or any browser-reachable file)
// stays in GitHub Actions secrets only.
const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!;

export function getSupabase() {
  return createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
    auth: { persistSession: false },
  });
}
