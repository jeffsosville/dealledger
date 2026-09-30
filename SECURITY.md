# Security

Please report security problems privately rather than in a public issue:
use GitHub's **Report a vulnerability** button on the repository's
Security tab. We will reply there.

The Supabase key in the site source is the public, read-only `anon` key.
It is meant to be public; access is limited by row-level security. If you
find a way to write data or read anything that isn't already public, that
is a vulnerability — please report it.
