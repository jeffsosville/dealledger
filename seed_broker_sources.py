#!/usr/bin/env python3
"""
seed_broker_sources.py  —  V7 control plane bootstrap
Seeds broker_sources from broker_master_march_2026.csv

Usage:
  export SUPABASE_URL="https://kqckuedsyyosmccushyd.supabase.co"
  export SUPABASE_SERVICE_ROLE_KEY="..."
  python3 seed_broker_sources.py --csv broker_master_march_2026.csv [--dry-run] [--min-listings 0]
"""

import os, sys, argparse, time, re, json
import requests
import pandas as pd
from urllib.parse import urlparse

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

# Hard-block domains — definitely not listing pages
BLOCK_DOMAINS = [
    "linkedin.com", "reddit.com", "bizbuysell.com",
    "bizmls.com", "flexmls.com", "dealforce.com",
    "facebook.com", "twitter.com", "instagram.com",
]

# Soft-flag phrases — skip these (they become garbage URLs after normalization)
SOFT_FLAGS = [
    "can't find", "cant find", "not a business", "not a listing",
    "no listing", "duplicate", "unreachable", "needs location",
    "wants a sign", "limited list", "page unreachable", "login required",
    "this site can", "site can't be", "can't be reached", "needs sign",
    "needs login", "needs password", "private listing", "password required",
]


# Query params that are structural — keep these
STRUCTURAL_PARAMS = {
    "page", "offset", "start", "type", "category", "status",
    "sort", "sortby", "fwp_status", "ct_ct_status", "searchtype",
    "search-listings", "region", "state", "location",
}

def normalize_url(url: str) -> str:
    """Lowercase domain, strip www., strip trailing slash, force https.
    Preserves structural query params (pagination, filters) but drops tracking params."""
    url = str(url).strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    parsed = urlparse(url)
    netloc = parsed.netloc.lower().replace("www.", "")
    path = re.sub(r"/+$", "", parsed.path)

    # Keep only structural query params
    if parsed.query:
        from urllib.parse import parse_qs, urlencode
        params = parse_qs(parsed.query, keep_blank_values=False)
        kept = {k: v for k, v in params.items() if k.lower() in STRUCTURAL_PARAMS}
        qs = f"?{urlencode(kept, doseq=True)}" if kept else ""
    else:
        qs = ""

    return f"https://{netloc}{path}{qs}"


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc.lower().replace("www.", "")


def is_blocked(url: str) -> tuple[bool, str]:
    """Returns (blocked, reason)."""
    if pd.isna(url) or str(url).strip() == "":
        return True, "empty"
    url_lower = str(url).lower()
    for d in BLOCK_DOMAINS:
        if d in url_lower:
            return True, f"blocked_domain:{d}"
    return False, ""


def is_soft_flagged(url: str) -> tuple[bool, str]:
    url_lower = str(url).lower()
    for f in SOFT_FLAGS:
        if f in url_lower:
            return True, f"soft_flag:{f}"
    return False, ""


def detect_strategy(url: str) -> dict:
    """Heuristic strategy detection from URL patterns."""
    u = str(url).lower()
    if ".aspx" in u or "advancesearch" in u:
        return {"strategy_type": "asp_table",          "render_mode": "playwright", "strategy_confidence": 0.9}
    if "fwp_" in u:
        return {"strategy_type": "facetwp_wordpress",  "render_mode": "requests",   "strategy_confidence": 0.85}
    if "/wp-json/" in u:
        return {"strategy_type": "wordpress_rest",     "render_mode": "requests",   "strategy_confidence": 0.95}
    if "graphql" in u:
        return {"strategy_type": "graphql",            "render_mode": "requests",   "strategy_confidence": 0.9}
    if "__next" in u or "/_next" in u:
        return {"strategy_type": "nextjs",             "render_mode": "requests",   "strategy_confidence": 0.85}
    if any(p in u for p in ["/listings/", "/businesses-for-sale", "/all-listings",
                              "/business-listing", "/current-listings", "/route-listings",
                              "/businesses-for-sale/", "/buy-a-business"]):
        return {"strategy_type": "html",               "render_mode": "requests",   "strategy_confidence": 0.7}
    if any(p in u for p in ["search-result", "find-a-business", "search-listings",
                              "search_results", "/search/"]):
        return {"strategy_type": "html",               "render_mode": "requests",   "strategy_confidence": 0.65}
    if "currentlistings.aspx" in u or "listings.aspx" in u:
        return {"strategy_type": "asp_table",          "render_mode": "playwright", "strategy_confidence": 0.85}
    return {"strategy_type": None, "render_mode": None, "strategy_confidence": None}


def detect_vertical(company: str, url: str) -> str | None:
    text = (str(company) + " " + str(url)).lower()
    if any(w in text for w in ["restaurant", "food", "pizza", "cafe", "bar ", "dining", "eatery", "sushi", "kitchen"]):
        return "restaurant"
    if any(w in text for w in ["clean", "janitorial", "maid", "laundry", "laundromat", "wash"]):
        return "cleaning"
    if any(w in text for w in ["pool", "swim"]):
        return "pool_service"
    if any(w in text for w in ["vending", "amusement", "arcade"]):
        return "vending"
    if any(w in text for w in ["route", "fedex", "ups", "delivery", "distribution"]):
        return "route"
    if any(w in text for w in ["pest", "exterminator"]):
        return "pest_control"
    if any(w in text for w in ["hvac", "plumb", "electric", "contractor"]):
        return "hvac"
    if any(w in text for w in ["landscap", "lawn", "tree service"]):
        return "landscaping"
    if any(w in text for w in ["tech", "software", "saas", "website", "ecommerce", "digital", "app"]):
        return "tech"
    if any(w in text for w in ["franchise"]):
        return "franchise"
    if any(w in text for w in ["atm", "cash machine"]):
        return "atm"
    return None


def clean_int(val) -> int | None:
    if pd.isna(val): return None
    try: return int(float(str(val).replace(",", "")))
    except: return None


def clean_float(val) -> float | None:
    if pd.isna(val): return None
    try: return float(str(val).replace(",", ""))
    except: return None


def upsert_one(row: dict) -> bool:
    """Upsert a single row. Returns True on success."""
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/broker_sources",
        headers=HEADERS, json=[row], timeout=15,
    )
    return r.status_code in (200, 201)


def upsert_batch(rows: list[dict]) -> tuple[int, int]:
    if not rows: return 0, 0
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/broker_sources",
        headers=HEADERS, json=rows, timeout=30,
    )
    if r.status_code in (200, 201):
        return len(rows), 0

    # Batch failed — fall back to row-by-row so one bad row doesn't kill 100
    ok = err = 0
    for row in rows:
        if upsert_one(row):
            ok += 1
        else:
            err += 1
    if err:
        print(f"    ↳ batch fallback: {ok} ok, {err} failed individually")
    return ok, err


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--min-listings", type=int, default=0)
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
        sys.exit(1)

    seed_source = os.path.basename(args.csv).replace(".csv", "")

    print(f"Loading {args.csv}...")
    df = pd.read_csv(args.csv)
    df.columns = [c.strip() for c in df.columns]
    print(f"  {len(df)} rows loaded")

    # Find listing URL column
    listing_url_col = next(
        (c for c in ["ACTIVE LISTING", "active_listing_url", "listing_url"] if c in df.columns),
        None
    )
    if not listing_url_col:
        print(f"ERROR: Can't find listing URL column. Got: {list(df.columns)}")
        sys.exit(1)
    print(f"  Listing URL column: '{listing_url_col}'")

    rows = []
    rejected = []
    soft_flagged_count = 0

    for _, row in df.iterrows():
        raw_url = str(row.get(listing_url_col, "")).strip()

        # Skip NaN / empty before anything else
        if pd.isna(raw_url) or str(raw_url).strip().lower() in ("", "nan", "none"):
            rejected.append({"url": raw_url, "company": row.get("company_name",""), "reason": "empty_or_nan"})
            continue

        # Hard block
        blocked, block_reason = is_blocked(raw_url)
        if blocked:
            rejected.append({"url": raw_url, "company": row.get("company_name",""), "reason": block_reason})
            continue

        # Soft flag — log but proceed
        flagged, flag_reason = is_soft_flagged(raw_url)
        if flagged:
            soft_flagged_count += 1
            rejected.append({"url": raw_url, "company": row.get("company_name",""), "reason": flag_reason, "soft": True})
            continue  # still skip — these are genuinely unusable URLs

        # Normalize
        try:
            url = normalize_url(raw_url)
        except Exception as e:
            rejected.append({"url": raw_url, "company": row.get("company_name",""), "reason": f"normalize_error:{e}"})
            continue

        domain = extract_domain(url)

        active = clean_int(row.get("active_listings", 0)) or 0
        if active < args.min_listings:
            rejected.append({"url": url, "company": row.get("company_name",""), "reason": f"below_min:{active}"})
            continue

        # Priority tier
        if active >= 200:   priority = 10
        elif active >= 100: priority = 20
        elif active >= 50:  priority = 30
        elif active >= 20:  priority = 50
        elif active >= 5:   priority = 80
        else:               priority = 100

        company  = str(row.get("company_name", "")).strip()
        strategy = detect_strategy(url)
        vertical = detect_vertical(company, url)

        record = {
            # Identity
            "account_id":           str(row.get("account", "")).strip() or None,
            "broker_name":          str(row.get("broker_name", "")).strip() or None,
            "company_name":         company or None,
            "listing_url":          url,
            "homepage_url":         str(row.get("companyUrl_clean", "")).strip() or None,
            "domain":               domain,                          # FIX 1
            "city":                 str(row.get("city", "")).strip() or None,
            "state":                str(row.get("state", "")).strip() or None,
            "email":                str(row.get("email", "")).strip() or None,
            "phone":                str(row.get("phone", "")).strip() or None,

            # Estimates from CSV
            "active_listings_est":  active if active > 0 else None,
            "sold_listings_est":    clean_int(row.get("sold_listings")),
            "active_listings_source": "csv_estimate",                # FIX bonus A
            "leaderboard_score":    clean_float(row.get("leaderboard_score")),
            "is_featured":          bool(row.get("is_featured", False)) if not pd.isna(row.get("is_featured", "")) else False,

            # Routing
            "priority":             priority,
            "vertical_hint":        vertical,
            "seed_source":          seed_source,                     # FIX 4

            # Strategy hints
            "strategy_type":        strategy.get("strategy_type"),
            "render_mode":          strategy.get("render_mode"),
            "strategy_confidence":  strategy.get("strategy_confidence"),  # FIX bonus B
            "strategy_status":      "pending",                       # FIX 5
        }

        # Clean up None-like strings
        record = {k: (None if str(v) in ("", "nan", "None", "NaN") else v) for k, v in record.items()}
        rows.append(record)

    # Stats
    hard_blocked  = sum(1 for r in rejected if "soft" not in r and "below_min" not in r)
    soft_flagged  = sum(1 for r in rejected if r.get("soft"))
    below_min     = sum(1 for r in rejected if "below_min" in r.get("reason",""))

    print(f"\n  Hard blocked:   {hard_blocked}")
    print(f"  Soft flagged:   {soft_flagged}")
    print(f"  Below min:      {below_min}")
    print(f"  Ready to seed:  {len(rows)}")

    # Write rejected log
    rejected_path = "rejected_brokers.csv"
    pd.DataFrame(rejected).to_csv(rejected_path, index=False)
    print(f"  Rejected log:   {rejected_path}")

    # Strategy distribution
    strat_counts = {}
    for r in rows:
        s = r.get("strategy_type") or "unknown"
        strat_counts[s] = strat_counts.get(s, 0) + 1
    print(f"\n  Strategy distribution:")
    for s, c in sorted(strat_counts.items(), key=lambda x: -x[1]):
        print(f"    {s:30s}: {c}")

    if args.dry_run:
        print("\nDRY RUN — top 10 by priority:")
        for r in sorted(rows, key=lambda x: x["priority"])[:10]:
            print(f"  P{r['priority']:3d}  {str(r['active_listings_est']):>5}  "
                  f"{str(r['company_name'])[:40]:40s}  "
                  f"{str(r['strategy_type']):20s}  "
                  f"{str(r['listing_url'])[:55]}")
        return

    # Upsert
    total_ok = total_err = 0
    for i in range(0, len(rows), args.batch_size):
        batch = rows[i: i + args.batch_size]
        ok, err = upsert_batch(batch)
        total_ok += ok
        total_err += err
        print(f"  Batch {i // args.batch_size + 1:3d}: {ok:4d} ok  {err:3d} err  |  total {total_ok}/{len(rows)}")
        time.sleep(0.15)

    print(f"\n✓ Done — {total_ok} upserted, {total_err} errors")
    print(f"\n  Tier breakdown:")
    tier_map = {10:"200+", 20:"100-199", 30:"50-99", 50:"20-49", 80:"5-19", 100:"<5"}
    tier_counts = {}
    for r in rows:
        p = r["priority"]
        tier_counts[p] = tier_counts.get(p, 0) + 1
    for p in sorted(tier_counts):
        print(f"    Priority {p:3d} ({tier_map.get(p,'?'):8s}): {tier_counts[p]} brokers")


if __name__ == "__main__":
    main()