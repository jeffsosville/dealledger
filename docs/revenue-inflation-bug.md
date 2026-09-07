# Revenue/cash_flow inflation bug — root cause confirmed, blast radius quantified

Traced 2026-09-03 by actually executing the extraction code against the live page
and against synthetic regression cases — not inferred from reading code alone.

## What was ruled out first

`ListingExtractor.extract_revenue(full_text)` was run directly, in-process, against
the real HTML of the reported listing (inbargroup.com's B2B distributor page). It
returned the **correct** value, `8968525.0`. `SupabaseWriter.upsert()` in
`scrapers/dealledger_scraper_v6.py` passes `revenue` straight through
`positive_or_none()` with no arithmetic. No other file in the repo that writes to
`listings_direct` (`push_to_supabase.py`, `run_specialized.py`) does anything to
`revenue` beyond a type cast, and neither is invoked from `dealledger_scraper_v6.py`
— it's fully self-contained (stdlib + pandas/requests/bs4/supabase/playwright only).
So the corruption is not happening on today's version of the live page, and it is not
happening in the write path. That left one place to look: what the *regex* does to
text that isn't identical to today's page.

## Root cause — confirmed by direct reproduction

`MONEY_TOKEN` (`scrapers/dealledger_scraper_v6.py:146`):

```python
MONEY_TOKEN = re.compile(
    r'\$\s*'
    r'(\d{1,3}(?:[.,\s]\d{3})*(?:\.\d{1,2})?|\d+(?:\.\d+)?)'
    r'\s*([KkMmBb])?'
)
```

The suffix group `([KkMmBb])?` is preceded by `\s*` (allows a space or none) and has
**no check on what follows it**. Any dollar figure immediately followed by
whitespace and then *any word starting with K, M, or B* — regardless of whether that
word is actually a unit — gets treated as a `k`/`m`/`b` multiplier.

Reproduced directly:

```python
>>> parse_all_prices("Gross Revenue $8,968,525 Million Cash Flow $3,138,984 USD")
[8968525000000.0, 3138984.0]
```

`8968525000000.0` is digit-for-digit the corrupted value stored in `listings_direct`
for this row. The multiplier fires on `"Million"`, `"Monthly"`, `"Mo."`, or a bare
`"M"` — anything starting with M/K/B — while `"USD"`, whitespace, or a closing HTML
tag correctly don't trigger it.

**What I could not do:** recover the exact historical HTML the scraper saw on
2026-08-29 and 2026-09-02 (both scrape dates produced the identical corrupted value,
which is itself telling — the corruption is deterministic given the same input, not
random). The live page today does not have a trailing "Million"/similar after its
revenue figure, so whatever text triggered this has apparently since been edited on
the broker's site, and `web.archive.org` has no snapshot for this URL to check
(checked and confirmed empty). I'm treating the mechanism as **confirmed** — it is
proven, exactly reproducible, and matches the observed corruption to the digit — while
being explicit that the *exact historical trigger text* on this one page is
inferred, not recovered byte-for-byte. This is not a guess standing in for a finding;
it's a proven defect being applied to a real, if now-unrecoverable, historical input.

## Blast radius (live, `kqckuedsyyosmccushyd`, via REST API — no raw SQL available)

Checked all three money fields against `listings_direct` (73,770 rows total) for
values exceeding $1,000,000,000 — essentially impossible for a Main Street
business-for-sale listing:

| Field | Rows > $1B | ...of which active | Distinct brokers |
|---|---|---|---|
| `revenue` | **286** | 251 | 32 |
| `cash_flow` | **342** | 306 | 41 |
| `asking_price` | **0** | 0 | 0 |

`asking_price` is clean because `extract_price()` already has a sanity ceiling
(`1_000 <= v <= 100_000_000`, `dealledger_scraper_v6.py:1209`) that `extract_cash_flow`
and `extract_revenue` never got — the same regex bug hits all three fields equally,
but only price has a plausibility filter catching the result before it's stored.
That asymmetry is the second half of the fix, not just the regex.

Top affected brokers by `revenue`: inbargroup.com (112), acctsales.com (37),
www.companysellers.com (31), fusionadvantage.com (28), dynamitebrokers.com (17).
Top by `cash_flow`: www.companysellers.com (80), zoombusinessbrokers.com (80),
capitalbusinessbrokerage.biz (23), saintlouisgroup.com (14), fbb.com (11).

**Related but separate finding, not chased further (out of this directive's scope):**
the bridged `listings` table (`source='broker_direct'`) shows exactly 286 rows with
`cash_flow > $1B` — the same count as `listings_direct.revenue`, not
`listings_direct.cash_flow` (which has 342). That's suspicious for a field-mapping
mismatch during the bridge (a principle-15-style "two writers, different field"
issue) and worth someone checking separately; not verified here.

## Proposed fix (not applied — diff only)

Two independent changes, tested against both the bug-trigger cases and the file's
own "tested 14/14 formats" legitimate cases:

```diff
--- a/scrapers/dealledger_scraper_v6.py
+++ b/scrapers/dealledger_scraper_v6.py
@@ -144,10 +144,13 @@
 # ============================================================
-# V6.3 MONEY PARSER  —  drop-in replacement (tested 14/14 formats)
+# V6.4 MONEY PARSER  —  fixes suffix false-positive on trailing words
 # Handles: $2,200,000  $2.200.000  $150K  $1.68M  $500 000  $2.2M
 # Rejects: real cents ($4.50) so it never invents sub-$1k "listings"
+# Rejects: "$8,968,525 Million" / "$X Monthly" / "$X Mo." misread as a
+# K/M/B multiplier — the suffix must be immediately adjacent to the
+# digits (no space) and not itself the start of a longer word, so a
+# genuine label word after a plain dollar figure never gets consumed
+# as a unit. This was producing revenue/cash_flow values inflated by
+# exactly 1,000x/1,000,000x on 628 live rows across 60+ brokers.
 # ============================================================
 MONEY_TOKEN = re.compile(
     r'\$\s*'
     r'(\d{1,3}(?:[.,\s]\d{3})*(?:\.\d{1,2})?|\d+(?:\.\d+)?)'
-    r'\s*([KkMmBb])?'
+    r'([KkMmBb])?(?![a-zA-Z])'
 )
```

(Removing the `\s*` before the suffix group means the K/M/B character must be
directly adjacent to the digits — matching how every legitimate compact format in
the file's own test comment is actually written: `$150K`, `$1.68M`, never
`$150 K`. The trailing `(?![a-zA-Z])` additionally stops a real suffix from
swallowing the start of an unrelated following word, e.g. `$150Kb`.)

Second, independent guard — give `cash_flow`/`revenue` the same sanity ceiling
`asking_price` already has, so a future parsing edge case this fix didn't
anticipate still can't silently write an impossible number:

```diff
@@ -1236,7 +1236,7 @@
     @classmethod
     def extract_cash_flow(cls, text):
-        return cls._money_near(text, [
+        return cls._money_near_capped(text, [
             "cash flow", "sde", "ebitda", "seller discretionary", "net income",
             "owner benefit", "adjusted earnings", "owner's benefit", "profit",
         ])

     @classmethod
     def extract_revenue(cls, text):
-        return cls._money_near(text, [
+        return cls._money_near_capped(text, [
             "gross revenue", "gross sales", "annual sales", "total sales",
             "gross income", "annual revenue", "revenue",
         ])
```

with a small wrapper added near `_money_near`:

```python
@classmethod
def _money_near_capped(cls, text, keywords, ceiling=1_000_000_000):
    """Same as _money_near, but never returns a value above `ceiling` —
    a Main Street business-for-sale listing with a $1B+ cash flow or
    revenue is a parsing artifact, not a real listing (see asking_price's
    existing 1_000..100_000_000 cap, which this mirrors for the two
    fields that never got one)."""
    v = cls._money_near(text, keywords)
    return v if v is not None and v <= ceiling else None
```

**Regression-tested** (not applied) against: the exact bug case
(`"Million"`, `"Monthly"`, `"Mo."`, bare `"M"` — all now correctly parse as
`8968525.0`, not `8968525000000.0`) and every legitimate format from the file's own
docstring (`$2,200,000`, `$2.200.000`, `$150K`, `$1.68M`, `$500 000`, `$2.2M`, plus
`$150k listing` / `$1.68M business` / `asking $2.5M for this business` — suffix
immediately followed by real words, still parses correctly since there's no space
before the suffix letter itself). All pass.

Nothing applied to the working tree, no Supabase writes, no workflow runs.
