# DealLedger Operations Runbook

Monthly data refresh and maintenance procedures.

---

## Monthly Refresh Schedule

Run on the **first Monday of each month**. Total time: ~2-3 hours.

---

## Step 1 — BizBuySell Full Market Scrape

**Script:** `bbs_allstates.py` (iCloud Desktop)

**What it does:** Scrapes all 51 states/regions from BizBuySell, bypassing the 10k per-query cap by iterating state by state. Writes directly to DealLedger Supabase.

**Run:**
```bash
cd ~/Library/Mobile\ Documents/com~apple~CloudDocs/Desktop/
export SUPABASE_SERVICE_KEY='...'
python3 bbs_allstates.py
```

**Modes:**
```bash
python3 bbs_allstates.py          # resume (skips completed states)
python3 bbs_allstates.py fresh    # fresh start, clears state files
python3 bbs_allstates.py states TX,CA,NY  # specific states only
```

**Expected output:** ~50,000-65,000 listings. Runtime: 45-60 minutes.

**State files saved to:** `~/Library/Mobile Documents/.../Desktop/state_files/bbs_XX.json`

---

## Step 2 — DOM Recalculation

After the scrape completes, recalculate days on market for all records.

**Run in Supabase SQL editor:**
```sql
UPDATE listings
SET days_on_market = CURRENT_DATE - estimated_listed_date
WHERE estimated_listed_date IS NOT NULL;
```

---

## Step 3 — BizQuest Views Scrape

**Script:** `bizquest_withviews.py` (iCloud Desktop/bizquest_states/)

**What it does:** Scrapes all listings from BizQuest state by state, then enriches each listing with `profileViews` from the detail API. Views = buyer demand signal.

**Run:**
```bash
cd ~/Library/Mobile\ Documents/com~apple~CloudDocs/Desktop/bizquest_states/
python3 bizquest_withviews.py          # scrape all states
python3 bizquest_withviews.py combine  # merge state files
python3 bizquest_withviews.py enrich   # add views → bizquest_enriched.csv
```

**Expected output:** ~40,000 listings with view counts. Runtime: 60-90 minutes.

**Output file:** `bizquest_enriched.csv` (~66MB)

---

## Step 4 — Views Sync to Supabase

Push BizQuest view counts into the DealLedger listings table.

**Run:**
```bash
python3 -c "
import csv
from supabase import create_client
sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
path = '/Users/jeffsosville/Library/Mobile Documents/com~apple~CloudDocs/Desktop/bizquest_states/bizquest_enriched.csv'
with open(path) as f:
    rows = list(csv.DictReader(f))
updates = []
for r in rows:
    try:
        ln = int(r['listNumber'])
        views = int(float(r.get('profileViews') or 0))
        if ln and views > 0:
            updates.append({'listing_number': ln, 'listing_views': views})
    except:
        pass
ok = err = 0
for i in range(0, len(updates), 100):
    batch = updates[i:i+100]
    try:
        sb.table('listings').upsert(batch, on_conflict='listing_number').execute()
        ok += len(batch)
    except Exception as e:
        err += len(batch)
print(f'Done: {ok:,} updated, {err} errors')
"
```

---

## Step 5 — Verify

Run in Supabase SQL editor to confirm numbers:

```sql
SELECT 
  COUNT(*) as total,
  COUNT(*) FILTER (WHERE days_on_market > 365) as zombies,
  ROUND(COUNT(*) FILTER (WHERE days_on_market > 365) * 100.0 / COUNT(*), 1) as zombie_pct,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_on_market) as median_dom,
  COUNT(*) FILTER (WHERE listing_views > 0) as with_views
FROM listings
WHERE estimated_listed_date IS NOT NULL;
```

---

## Daily Automation (GitHub Actions)

The daily BizBuySell scrape runs automatically via GitHub Actions:

- **Daily scrape:** 6am ET every day (`bbs_daily.yml`)
- **Weekly full scrape:** 7am ET every Sunday (`bbs_weekly_full.yml`)

These append new listings and update existing ones. The monthly manual run is the full 51-state refresh.

---

## Key Stats (March 2026 baseline)

| Metric | Value |
|--------|-------|
| Total listings | 62,132 |
| Zombie rate | 22.0% |
| Median DOM | 202 days |
| Listings with view data | 41,176 |
| States covered | 50 + DC |

---

## Hidden Gems Query

Low DOM + high views = hidden gems. Run monthly and tweet:

```sql
SELECT header, state, price, days_on_market, listing_views
FROM listings
WHERE days_on_market <= 90
AND listing_views > 2000
AND is_active = true
ORDER BY listing_views DESC
LIMIT 10;
```

---

*Last updated: March 2026*
