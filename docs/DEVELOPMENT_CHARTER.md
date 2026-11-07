# DealLedger Development Charter

Status: Foundational project governance
Established: 2026-11-07

## Mission
Build a comprehensive, trustworthy, historically useful open record of businesses for sale, prioritizing verified broker-direct sources, dated observations, and historical continuity.

## Roles and decision rights
- **Founder / Product Owner:** Sets vision, priorities, constraints, and final approvals.
- **ChatGPT / Architect and Independent Reviewer:** Researches architecture, writes specifications and acceptance criteria, checks actual code paths and data contracts, challenges assumptions, and reviews evidence from tests. Recommendations are provisional until verified against the live system.
- **Claude / Lead Builder and Implementation Agent:** Maintains implementation context, implements approved specifications, runs tests, documents changes, and performs authorized migrations and deployments.
- **GitHub:** Canonical, versioned record of code, specifications, decisions, and implementation documentation.
- **Supabase:** Operational data and historical observations, subject to source provenance and access controls.
No assistant conversation is a substitute for repository documentation or database evidence.

## Required workflow
1. Define the user problem, desired outcome, and measurable success.
2. Audit the *active* runtime path: workflow entrypoints, invoked modules, current schema, source tables, data writes, and deployment configuration. Do not mistake unused or legacy code for production code.
3. Produce a concise specification with scope, risks, invariants, tests, and acceptance criteria.
4. Obtain founder approval for consequential implementation, migrations, and deployments.
5. Claude implements in small reviewable changes, preserving existing production behavior.
6. Independently review changes against the specification and evidence; test representative failures and regressions.
7. Deploy only with authorization and a rollback plan; document what changed, measurements, and unresolved issues.

## Product principles
1. **Discovery before presentation:** Automatically find broker-owned listing pages and individual listing URLs without routine human intervention.
2. **History is the asset:** Preserve source URL, observation timestamp, listing identity, asking-price observations, changes, and evidence of persistence.
3. **No silent failure:** Log every discovery/extraction attempt, stage transition, reason, and last success. Zero results trigger diagnosis and appropriate rediscovery, not permanent rejection.
4. **Evidence over inference:** Distinguish first observed from verified initial listing date; asking-price changes from sale prices; unavailable pages from confirmed removals.
5. **Source integrity:** Keep broker-direct public observations separate from private marketplace research and restricted data. Respect access restrictions and reasonable crawl rates.
6. **Incremental architecture:** Extend active systems rather than replacing them reflexively. Verify actual call paths before recommending changes.
7. **Automated recovery:** Known listing URLs should move into extraction automatically after validation. Repeated failures should escalate to a different diagnosis or method.
8. **Open infrastructure and trust:** Reliability, provenance, coverage, continuity, and adoption matter more than raw record counts.

## Current priorities (2026-11-07)
1. Benchmark the existing finder against rendered-browser discovery and independently verified listing evidence.
2. Resolve known listing URLs stuck in stage 2; audit auto-promotion and recheck queues.
3. Improve automated discovery for rejected and JavaScript-rendered broker sites.
4. Repair stage-3 failure escalation and health/status writes.
5. Increase recurring broker-direct observations and verifiable price changes.
6. Improve search, listing timelines, Market Pulse, UX, and distribution alongside data coverage.

## Measurement
Report weekly: broker domains evaluated; verified listing pages; active producing broker sources; unique broker-direct listings; sources with successful repeat crawls; listings with 2+ dated observations; independently verified price changes; false-positive/false-negative benchmark rates; and crawl cost and latency.

Do not claim a historical date, price change, or DOM precision beyond the underlying evidence.

## Immediate benchmark gate
Use a representative, reproducible 100-broker sample across failure and success buckets. Compare existing finder and rendered discovery, validate that discovered pages contain broker-owned business listings, and record false positives, recall, cost, and runtime. Start with a small smoke test; keep benchmark runs read-only and local until reviewed. Do not deploy discovery changes solely on a successful anecdotal test.
