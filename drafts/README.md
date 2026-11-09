# drafts/

Pages taken off dealledger.org but kept for rewriting. Nothing here is served
(Vercel serves `public/` only) and nothing links here. Old URLs redirect
temporarily (302) — see `vercel.json`.

| File | Was | Unlinked | Why |
|---|---|---|---|
| `writing/the-market-has-no-memory.html` | `/why` | 2026-11-09 | Built on marketplace data and aggregate days-on-market figures; rewrite around the broker-direct methodology and the listing-age evidence hierarchy |
| `writing/no-reason-to-fix.html` | `/writing/no-reason-to-fix` | 2026-11-09 | Marketplace-centred essay; rewrite or retire |
| `pulse.html` | `/pulse` | 2026-11-09 | Sample figures (median DOM, relist rate, view counts, listing totals) were hard-coded and not from `public_stats`; Pulse returns as a weekly post built from verified data |

To restore a page: `git mv` it back under `public/`, drop its redirect from
`vercel.json`, restore its rewrite, and re-add it to `public/sitemap-static.xml`.
