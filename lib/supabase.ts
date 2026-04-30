import { createClient } from '@supabase/supabase-js';

// Anon key — safe to expose to the browser. RLS policies enforce read access.
// Service role key (NEVER use in this file or any browser-reachable file)
// stays in GitHub Actions secrets only.

export function getSupabase() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const anonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
  
  if (!url || !anonKey) {
    throw new Error(
      `Missing Supabase env vars. URL: ${url ? 'set' : 'MISSING'}, KEY: ${anonKey ? 'set' : 'MISSING'}`
    );
  }
  
  return createClient(url, anonKey, {
    auth: { persistSession: false },
  });
}
