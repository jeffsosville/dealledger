# BizQuest `is_fsbo` / `broker_company` fix — proposed diff

Item 0(b) from tonight's queue. Not applied — `scrapers/bizquest_views_refresh.py`
in the working tree is unchanged; this diff was captured with `git diff` after a
scratch edit, then the file was reverted.

## Root cause (confirmed by reading the code)

`flatten_listing()` (line 115) builds `brokerCompany` from
`listing['contactInfo']['brokerCompany']`, where `listing` is a raw object from the
**search-results** API (`BqBfsSearchResults`). That field is populated on only 585 of
43,629 active rows (1.3%) — verified this session. `is_fsbo` is then set to
`not brokerCompany` (line 138), so the parser is turning "the search-results payload
didn't include a company name" into "for sale by owner," which is wrong on its face:
`account_id`, taken from a *different, reliably-populated* field on the same object
(`listing.get('account')`, 79% populated), independently proves broker involvement —
verified this session at 29,166/34,472 (85%) resolving to a real, named row in
`broker_master`.

So two separate fields on the same API object disagree about whether a broker
exists, and the parser trusted the sparse one.

## What I could and couldn't verify live

I confirmed the counts above against Supabase directly. I could **not** confirm
whether BizQuest's per-listing detail endpoint (`BqBfsListingDetail`, already called
for every listing during the `enrich_with_views` pass) returns a richer `contactInfo`
than the search-results payload does — that would need a live authenticated call to
bizquest.com, and:

- `curl_cffi` (required — plain `requests` gets an immediate 403, confirmed) is
  broken in this sandbox: `_cffi_backend` is built for x86_64 but the interpreter
  here is arm64. This is an environment problem, not a BizQuest problem.
- I did not have another way to obtain the Chrome-TLS-impersonated session the
  scraper needs, and per the ground rules this was read-only/single-broker-test
  scope, not a license to work around a blocked tool.

So the detail-endpoint part of the fix below (merging `detail.contactInfo` into the
listing before flattening) is a reasonable, low-risk addition based on the fact that
the detail call is already being made for every listing anyway — but it is
**unverified that `contactInfo.brokerCompany` is actually richer there**. It should
be spot-checked against one real listing detail response before this ships. The
`is_fsbo` logic fix and the `broker_master` backfill do not depend on that
assumption and are safe independent of whether the detail endpoint helps.

## The fix

Three independent changes, in order of confidence:

1. **`is_fsbo` should require the absence of *both* signals, not just `brokerCompany`.**
   High confidence — this only uses fields already being captured correctly.
2. **Backfill `broker_company`'s display name from `broker_master.companyname` via
   `account_id`** when BizQuest's own field is blank. High confidence — `broker_master`
   is already loaded elsewhere in this codebase and the join was verified this session
   (85% hit rate). This is a one-time bulk `select` per run (`broker_master` is 8,123
   rows — trivial), not a per-listing API call.
3. **Merge `contactInfo` from the detail-endpoint response into the listing during
   enrichment**, preferring it only where the search-result's own field is blank.
   Unverified per above — flagged in a code comment as such so it isn't mistaken for
   confirmed.

```diff
diff --git a/scrapers/bizquest_views_refresh.py b/scrapers/bizquest_views_refresh.py
index 20fc5ea..44aefb2 100644
--- a/scrapers/bizquest_views_refresh.py
+++ b/scrapers/bizquest_views_refresh.py
@@ -135,7 +135,12 @@ class BizQuestScraper:
             flattened['contactTpnPhoneExt'] = ''
 
         flattened['accountId'] = listing.get('account', '')
-        flattened['isFSBO'] = 1 if not flattened['brokerCompany'] else 0
+        # FSBO means no identifiable broker at all. brokerCompany alone is a
+        # weak signal — the search-results API leaves it blank on ~99% of
+        # rows regardless of listing type — so an accountId (which resolves
+        # to a named brokerage in broker_master 85% of the time) also counts
+        # as "has a broker." Only flag FSBO when neither is present.
+        flattened['isFSBO'] = 1 if not (flattened['brokerCompany'] or flattened['accountId']) else 0
 
         return flattened
 
@@ -363,6 +368,20 @@ class BizQuestScraper:
                     listing['grossIncome'] = detail.get('grossIncome', '')
                     listing['summary'] = detail.get('summary', '')
                     listing['bizType'] = detail.get('primaryBizTypeId', '')
+                    # The search-results contactInfo is sparse (brokerCompany
+                    # blank on ~99% of rows). The per-listing detail call we're
+                    # already making for views may carry a fuller contactInfo —
+                    # prefer it wherever the search result left a field blank.
+                    # NOT YET LIVE-VERIFIED: confirm detail.contactInfo actually
+                    # includes brokerCompany before relying on this in production.
+                    detail_contact = detail.get('contactInfo') or {}
+                    if detail_contact:
+                        existing_contact = listing.get('contactInfo') or {}
+                        merged = dict(existing_contact)
+                        for k, v in detail_contact.items():
+                            if v and not merged.get(k):
+                                merged[k] = v
+                        listing['contactInfo'] = merged
                 else:
                     listing['profileViews'] = None
             time.sleep(0.3)
@@ -500,6 +519,22 @@ class BizQuestScraper:
             print("[-] No view data to write to history")
 
         # ---- 2. full marketplace mirror ----
+        # account_id resolves to a named brokerage in broker_master for ~85%
+        # of the rows that have one (verified 2026-09-03: 29,166/34,472).
+        # brokerCompany from BizQuest's own API is blank on ~99% of rows, so
+        # backfill the display name from broker_master when we can.
+        broker_names = {}
+        try:
+            bm = sb.table("broker_master").select("account,companyname").execute()
+            broker_names = {
+                str(r["account"]): r["companyname"]
+                for r in (bm.data or [])
+                if r.get("account") is not None and r.get("companyname")
+            }
+            print(f"[*] Loaded {len(broker_names):,} broker names from broker_master for backfill")
+        except Exception as e:
+            print(f"[!] broker_master lookup failed, skipping backfill: {e}")
+
         rows, skipped = {}, 0
         for l in listings:
             try:
@@ -517,6 +552,11 @@ class BizQuestScraper:
             if stub and not stub.startswith("http"):
                 stub = "https://www.bizquest.com" + stub
 
+            account_id = str(flat.get("accountId") or "") or None
+            broker_company = (flat.get("brokerCompany") or "").strip() or None
+            if not broker_company and account_id:
+                broker_company = broker_names.get(account_id)
+
             rows[ln] = {
                 "listing_number":   ln,
                 "header":           (flat.get("header") or "").strip() or None,
@@ -526,10 +566,10 @@ class BizQuestScraper:
                 "cash_flow":        self._as_int(flat.get("cashFlow")),
                 "gross_income":     self._as_int(flat.get("grossIncome")),
                 "category":         str(flat.get("bizType") or "") or None,
-                "broker_company":   (flat.get("brokerCompany") or "").strip() or None,
+                "broker_company":   broker_company,
                 "broker_contact":   (flat.get("contactFullName") or "").strip() or None,
-                "account_id":       str(flat.get("accountId") or "") or None,
-                "is_fsbo":          bool(flat.get("isFSBO")),
+                "account_id":       account_id,
+                "is_fsbo":          bool(flat.get("isFSBO")) and not broker_company,
                 "url":              stub or None,
                 "year_established": str(flat.get("yearEstablished") or "") or None,
                 "employees":        str(flat.get("employees") or "") or None,
```

## Expected effect

Applying just the `is_fsbo` logic change (independent of the detail-merge, which is
unverified) plus the `broker_master` backfill would, on the *next* scrape run,
reclassify any row with a populated `account_id` as non-FSBO regardless of whether
BizQuest's own `brokerCompany` field ever fills in — that's up to 34,472 of the
43,629 active rows (79%), bringing the FSBO rate down from 98.7% to at most ~21%,
with a real broker name attached wherever `broker_master` resolves (85% of those,
~29,166 rows).

This does **not** retroactively fix the 43,044 rows already marked `is_fsbo=true` in
the table today — that requires either a one-time backfill UPDATE (out of scope
tonight, no writes) or waiting for the next scrape run to re-upsert every row (the
script upserts on `listing_number` for the full 40k+ mirror every run, so this
should self-heal within one scheduled run once the fix ships).

## Not done tonight

- No live confirmation that `BqBfsListingDetail`'s `contactInfo` is richer than
  `BqBfsSearchResults`'s (environment couldn't make the authenticated call — see
  above). Spot-check before relying on that part of the diff.
- No retroactive backfill UPDATE against the 43,044 already-mismarked rows.
- Not applied, not committed, not run.
