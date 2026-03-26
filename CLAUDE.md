# CLAUDE.md — DealLedger

## What this project is
DealLedger is an open public ledger for Main Street M&A — "EDGAR for small business." It is NOT a marketplace. The core value prop is data quality and transparency. The strategic positioning is a "Carfax for business listings" via the Listing Quality Score (LQS) system.

- Public site: dealledger.org
- Manifesto: dealledger.org/why.html
- License: CC0 data, MIT code
- Repo location: ~/dealledger-repo

---

## Supabase Projects

| Project | ID | Purpose |
|---|---|---|
| DealLedger (primary) | `kqckuedsyyosmccushyd` | 65,824 BizBuySell listings, LQS scores, broker data |
| CleaningExits / business-listings | `ctvrauiiskucinibnfaj` | Vertical marketplace listings |
| ATM CRM | `wgrmxhxozoyvcmvbfuxv` | Separate — do not touch from this repo |

Always use `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` env vars. Never hardcode credentials.

---

## Key Tables (DealLedger Supabase)

- `listings` — main BizBuySell listings table (65,824 rows). Fields include `listing_id`, `title`, `asking_price`, `revenue`, `cash_flow`, `category`, `state`, `is_active`, `last_seen_date`, `lqs_score`, `lqs_tier`.
- `listings_direct` — direct broker listing schema for the broker coverage layer
- `brokers` — 769 brokers seeded from CSV; 1,519 in `broker_master`
- `broker_master` — master broker table with 2,859 brokers, 1,415 with emails

---

## Listing Quality Score (LQS)

Scores listings 0–100. Four tiers:

| Tier | Score | % of listings |
|---|---|---|
| Verified | High | ~46% |
| Likely Real | Mid-high | ~45% |
| Unverified | Mid-low | ~8% |
| Likely Junk | Low | ~0.2% |

**Positive signals:** direct broker URL, financials present, unique description, reasonable multiple  
**Negative signals:** duplicate titles, franchise territory keywords, spam broker patterns

Components: `QualityBadge.tsx`, `QualityFilter.tsx`, `QualityStatBanner.tsx`

---

## Scraper Pipeline

### Main scrapers
- `scrapers/bbs_allstates.py` — correct all-states BizBuySell scraper (use this, not older variants)
- `unified_broker_scraper_v2.py` — ML-based broker scraper with pattern detection
- `bbs_pipeline.py` — orchestration pipeline (has known bugs, see below)
- `bizquest_views_refresh.py` — BizQuest view count refresh (unresolved: needs `save_to_supabase()` method)

### GitHub Actions
- `bbs_daily.yml` — calls `bbs_allstates.py` with `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` secrets
- Cron: `0 14 * * *` (9am ET) — if you see timezone drift, this is why

### Known bugs (fix before extending)
1. **`bbs_pipeline.py` line 171** — NoneType crash during Supabase loader phase. Guard with null check before accessing listing fields.
2. **Cron timezone mismatch** — proposed fix already applied (`0 14 * * *`), verify if still drifting.
3. **`bizquest_views_refresh.py`** — missing `save_to_supabase()` method; original working file is `bizquest_withviews.py`.

### Auto-deactivation logic
Listings not seen in 14 days are flipped to `is_active = false`. Do not remove this logic.

### BBS DOM estimation anchor
Listing #2367857 = May 14, 2025 ≈ 373.8 new listings/day. Use this as the reference point for any DOM calculations.

---

## Broker Scraper — Auto-Accept Rules

This is the approved use case for auto-accept / self-correcting loops in this repo. Claude may iterate autonomously on scraper logic without manual approval on each step, subject to the rules below.

### Self-correction loop (approved)
1. Write or edit scraper logic
2. Run against a **single test broker** (see gate below)
3. Read output — classify result: pattern match, `HTTP_403`, `CAPTCHA`, or `NO_PATTERN`
4. Adjust pattern or logic based on failure classification
5. Rerun and verify output
6. Repeat until clean result
7. Only then proceed to bulk execution — and only with explicit approval

### Single-broker test gate (mandatory)
**Never run the scraper across all brokers without first verifying output on a single test broker.**
- Pick a known-good broker from the knowledge base as the test target
- Confirm pattern match and clean data before expanding scope
- This gate applies even in auto-accept mode — bulk runs always require a manual go-ahead

### Failure classification reference
| Code | Meaning | Self-correction action |
|---|---|---|
| `HTTP_403` | Blocked by server | Rotate proxy, adjust headers, back off |
| `CAPTCHA` | Bot detection triggered | Flag for manual review — do not retry in loop |
| `NO_PATTERN` | Page structure not recognized | Attempt pattern detection, add to knowledge base if found |

### Knowledge base
- Supabase knowledge base: 1,148+ patterns
- After any new pattern is detected and confirmed, add it to the knowledge base immediately
- Specialized scrapers for: Murphy, Transworld, Sunbelt, VR, FCBB, Hedgestone

### Proxy
- DataImpulse — credentials in env vars, do not hardcode
- If `HTTP_403` persists after proxy rotation, stop and flag — do not burn through proxy quota in a loop

---

## Broker Outreach Infrastructure
- `broker_master_march_2026.xlsx` — 2,859 brokers, 1,415 with emails (Hunter.io)
- `instantly_broker_outreach.csv` — 105 contacts loaded into Instantly
- 1,303-contact DealLedger outreach CSV built, pending `jeff@dealledger.org` Google Workspace setup
- **Bug fixed:** Michael Nuanes NaN account ID (pandas NaN key false-match — always cast keys before lookup)

---

## Data Conventions
- BizBuySell EDGE subscription: $24.95/month (not $150 — update any docs that say otherwise)
- `match_direct_listings.py` — fuzzy-matching engine for direct broker listings
- `broker_coverage_analysis.py` — broker coverage reporting
- `docs/OPERATIONS.md` — operations runbook, keep updated

---

## Common Mistakes to Avoid
- Do NOT use older scraper variants — `bbs_allstates.py` in `scrapers/` is canonical
- Do NOT touch ATM CRM Supabase project (`wgrmxhxozoyvcmvbfuxv`) from this repo
- Do NOT hardcode Supabase credentials — always use env vars
- Do NOT remove auto-deactivation logic (14-day rule)
- Do NOT assume pandas NaN values are falsy for dict key lookups — always cast first
- Do NOT change the CC0/MIT licensing without explicit approval

---

## Deployment
- Frontend deploys via Vercel (Next.js App Router)
- DealLedger repo: `~/dealledger-repo`
- CleaningExits repo: `~/cleaningexits` (separate — vertical marketplace layer)

---

## Strategic Context (for AI reasoning)
DealLedger's moat is data quality and openness, not listings volume. Every feature decision should push toward transparency, verifiability, and broker accountability. The LQS system is the core differentiator. When in doubt: does this make the data more trustworthy and useful to a buyer or researcher? If yes, build it.
