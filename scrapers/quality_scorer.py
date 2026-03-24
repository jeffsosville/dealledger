#!/usr/bin/env python3
"""
listing_quality_score.py

Computes a Listing Quality Score (LQS) for every record in
cleaning_listings_merge and writes results back to Supabase.

Score range: 0–100
Tier mapping:
  80–100 → Verified        (direct broker + full financials + unique)
  60–79  → Likely Real     (strong signals, minor gaps)
  40–59  → Unverified      (sparse data, no direct match)
  0–39   → Likely Junk     (spam signals, duplicates, templates)

Usage:
    python listing_quality_score.py
    python listing_quality_score.py --dry-run
    python listing_quality_score.py --sample 500
"""

import os, re, sys, time, json, argparse, logging
from collections import defaultdict
import requests
from difflib import SequenceMatcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CLEANING_URL = "https://ctvrauiiskucinibnfaj.supabase.co"
CLEANING_KEY = os.environ.get("CLEANINGEXITS_ANON_KEY", "")

# ── Scoring rules ─────────────────────────────────────────────────────────────
#
# POSITIVE signals (real listing indicators)
RULES_POSITIVE = [
    {
        "id":     "direct_broker_url",
        "label":  "Exists on broker site (direct URL)",
        "points": 25,
        "desc":   "Has a direct_broker_url that is not a BizBuySell/BizQuest domain",
    },
    {
        "id":     "has_price",
        "label":  "Has asking price",
        "points": 10,
        "desc":   "price field is populated and > 0",
    },
    {
        "id":     "has_cash_flow",
        "label":  "Has cash flow / SDE",
        "points": 10,
        "desc":   "cash_flow field is populated and > 0",
    },
    {
        "id":     "has_revenue",
        "label":  "Has revenue figure",
        "points": 5,
        "desc":   "revenue field is populated",
    },
    {
        "id":     "has_contact",
        "label":  "Has contact info",
        "points": 5,
        "desc":   "contact_name or contact_phone populated",
    },
    {
        "id":     "unique_description",
        "label":  "Unique description",
        "points": 8,
        "desc":   "notes/description not matching a known template pattern",
    },
    {
        "id":     "reasonable_multiple",
        "label":  "Reasonable valuation multiple (0.5x–5x)",
        "points": 7,
        "desc":   "price/cash_flow between 0.5 and 5.0",
    },
    {
        "id":     "has_location",
        "label":  "Has city + state",
        "points": 5,
        "desc":   "Both city and state populated",
    },
]

# NEGATIVE signals (junk indicators)
RULES_NEGATIVE = [
    {
        "id":     "duplicate_across_states",
        "label":  "Duplicate title across 3+ states",
        "points": -25,
        "desc":   "Same or near-identical header appearing in many states",
    },
    {
        "id":     "franchise_territory",
        "label":  "Franchise territory listing",
        "points": -20,
        "desc":   "Keywords: territory, franchise opportunity, area developer",
    },
    {
        "id":     "impossible_multiple",
        "label":  "Impossible multiple (CF > Price or <0.1x)",
        "points": -20,
        "desc":   "Cash flow exceeds price, or multiple under 0.1x — lead-gen trap signal",
    },
    {
        "id":     "template_description",
        "label":  "Template/boilerplate description",
        "points": -15,
        "desc":   "Description matches known AI-generated or copy-paste template patterns",
    },
    {
        "id":     "no_financials",
        "label":  "No price AND no cash flow",
        "points": -10,
        "desc":   "Both price and cash_flow are null/zero",
    },
    {
        "id":     "suspiciously_round_price",
        "label":  "Suspiciously round price with no financials",
        "points": -5,
        "desc":   "Price is exact round number (e.g. $50,000) with no supporting financials",
    },
    {
        "id":     "broker_spam_ring",
        "label":  "Known spam broker account",
        "points": -30,
        "desc":   "broker_account matches known spam rings (Carolina Crew, AI Listing Farm, etc.)",
    },
]

# ── Known spam broker patterns (from your fraud detection work) ───────────────
SPAM_BROKER_PATTERNS = [
    r"carolina.*clean",
    r"passive.*income.*clean",
    r"semi.?passive",
    r"healthy.*vend",
    r"nationwide.*opportunity",
    r"absentee.*owner.*clean",
]

# ── Template description fragments (boilerplate) ──────────────────────────────
TEMPLATE_FRAGMENTS = [
    "this is a great opportunity",
    "motivated seller",
    "turn-key operation",
    "turnkey business",
    "priced to sell",
    "owner is retiring",
    "serious inquiries only",
    "proof of funds required",
    "this business has been operating",
    "excellent reputation in the community",
    "room for growth",
    "be your own boss",
    "recession-proof",
    "multiple revenue streams",
]

MARKETPLACE_DOMAINS = ["bizbuysell.com", "bizquest.com", "businessbroker.net"]

def is_marketplace_url(url: str) -> bool:
    return any(d in (url or "") for d in MARKETPLACE_DOMAINS)


def normalize_title(t: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", (t or "").lower())).strip()


def score_listing(row: dict, duplicate_titles: dict) -> dict:
    """
    Returns:
        score       int  0–100
        tier        str
        fired_rules list of rule IDs that fired
        breakdown   dict rule_id -> points awarded
    """
    points = 0
    fired  = []
    breakdown = {}

    title     = row.get("header", "") or ""
    notes     = row.get("notes", "") or ""
    price     = row.get("price")
    cf        = row.get("cash_flow")
    revenue   = row.get("revenue")
    city      = row.get("city", "")
    state     = row.get("state", "")
    contact_n = row.get("contact_name", "")
    contact_p = row.get("contact_phone", "")
    broker_ac = row.get("broker_account", "") or ""
    d_url     = row.get("direct_broker_url", "") or ""
    url       = row.get("url", "") or ""
    norm_t    = normalize_title(title)

    # ── POSITIVE ──────────────────────────────────────────────────────────────

    # Direct broker URL (not a marketplace domain)
    if d_url and not is_marketplace_url(d_url):
        points += 25; fired.append("direct_broker_url"); breakdown["direct_broker_url"] = 25
    elif url and not is_marketplace_url(url):
        points += 15; fired.append("direct_broker_url_partial"); breakdown["direct_broker_url"] = 15

    # Has price
    try:
        p = float(price)
        if p > 0:
            points += 10; fired.append("has_price"); breakdown["has_price"] = 10
    except (TypeError, ValueError):
        pass

    # Has cash flow
    try:
        c = float(cf)
        if c > 0:
            points += 10; fired.append("has_cash_flow"); breakdown["has_cash_flow"] = 10
    except (TypeError, ValueError):
        pass

    # Has revenue
    if revenue and str(revenue).strip() not in ("", "0", "None"):
        points += 5; fired.append("has_revenue"); breakdown["has_revenue"] = 5

    # Has contact
    if contact_n or contact_p:
        points += 5; fired.append("has_contact"); breakdown["has_contact"] = 5

    # Unique description (not template)
    notes_lower = notes.lower()
    template_hits = sum(1 for f in TEMPLATE_FRAGMENTS if f in notes_lower)
    if len(notes) > 100 and template_hits == 0:
        points += 8; fired.append("unique_description"); breakdown["unique_description"] = 8

    # Reasonable multiple
    try:
        mult = float(price) / float(cf)
        if 0.5 <= mult <= 5.0:
            points += 7; fired.append("reasonable_multiple"); breakdown["reasonable_multiple"] = 7
    except (TypeError, ValueError, ZeroDivisionError):
        pass

    # Has location
    if city and state:
        points += 5; fired.append("has_location"); breakdown["has_location"] = 5

    # ── NEGATIVE ──────────────────────────────────────────────────────────────

    # Duplicate title across 3+ states
    dup_count = duplicate_titles.get(norm_t, 0)
    if dup_count >= 3:
        points -= 25; fired.append("duplicate_across_states"); breakdown["duplicate_across_states"] = -25
    elif dup_count == 2:
        points -= 10; fired.append("duplicate_across_states_mild"); breakdown["duplicate_across_states"] = -10

    # Franchise / territory keywords
    franchise_kw = ["territory", "franchise opportunity", "area developer",
                    "master franchise", "protected territory", "franchise resale"]
    if any(k in title.lower() for k in franchise_kw):
        points -= 20; fired.append("franchise_territory"); breakdown["franchise_territory"] = -20

    # Impossible multiple
    try:
        p, c = float(price), float(cf)
        if c > 0 and p > 0:
            mult = p / c
            if mult < 0.1 or c > p:
                points -= 20; fired.append("impossible_multiple"); breakdown["impossible_multiple"] = -20
    except (TypeError, ValueError):
        pass

    # Template description
    if template_hits >= 3:
        points -= 15; fired.append("template_description"); breakdown["template_description"] = -15

    # No financials at all
    no_price = not price or price == 0
    no_cf    = not cf or cf == 0
    if no_price and no_cf:
        points -= 10; fired.append("no_financials"); breakdown["no_financials"] = -10

    # Suspiciously round price, no financials
    try:
        p = float(price)
        if p > 0 and p % 5000 == 0 and no_cf:
            points -= 5; fired.append("suspiciously_round_price"); breakdown["suspiciously_round_price"] = -5
    except (TypeError, ValueError):
        pass

    # Known spam broker patterns
    for pat in SPAM_BROKER_PATTERNS:
        if re.search(pat, broker_ac.lower()) or re.search(pat, title.lower()):
            points -= 30; fired.append("broker_spam_ring"); breakdown["broker_spam_ring"] = -30
            break

    # Clamp 0–100
    score = max(0, min(100, points))

    if score >= 80:   tier = "Verified"
    elif score >= 60: tier = "Likely Real"
    elif score >= 40: tier = "Unverified"
    else:             tier = "Likely Junk"

    return {"score": score, "tier": tier, "fired_rules": fired, "breakdown": breakdown}


def build_duplicate_index(rows: list) -> dict:
    """Count how many times each normalized title appears."""
    counts = defaultdict(int)
    for r in rows:
        t = normalize_title(r.get("header", ""))
        if t:
            counts[t] += 1
    return dict(counts)


def sb_get_all(table, params):
    headers = {"apikey": CLEANING_KEY, "Authorization": f"Bearer {CLEANING_KEY}"}
    rows, offset = [], 0
    while True:
        p = {**params, "limit": 1000, "offset": offset}
        r = requests.get(f"{CLEANING_URL}/rest/v1/{table}", headers=headers, params=p, timeout=30)
        r.raise_for_status()
        batch = r.json()
        rows.extend(batch)
        if len(batch) < 1000: break
        offset += 1000
        time.sleep(0.1)
    return rows


def sb_upsert_scores(scored_rows):
    headers = {
        "apikey": CLEANING_KEY,
        "Authorization": f"Bearer {CLEANING_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    payload = [
        {
            "url": r["url"],
            "quality_score": r["quality_score"],
            "quality_tier": r["quality_tier"],
            "quality_rules_fired": json.dumps(r["quality_rules_fired"]),
            "quality_breakdown": json.dumps(r["quality_breakdown"]),
        }
        for r in scored_rows if r.get("url")
    ]
    for i in range(0, len(payload), 500):
        batch = payload[i:i+500]
        r = requests.post(
            f"{CLEANING_URL}/rest/v1/cleaning_listings_merge",
            headers=headers, json=batch, timeout=60
        )
        if r.status_code not in (200, 201):
            log.error(f"Upsert error: {r.status_code} {r.text[:200]}")
        time.sleep(0.1)


def print_report(results: list):
    from collections import Counter
    tiers = Counter(r["quality_tier"] for r in results)
    total = len(results)
    print("\n" + "="*60)
    print("LISTING QUALITY SCORE — DISTRIBUTION REPORT")
    print("="*60)
    for tier in ["Verified", "Likely Real", "Unverified", "Likely Junk"]:
        count = tiers.get(tier, 0)
        pct   = count / total * 100 if total else 0
        bar   = "█" * int(pct / 2)
        print(f"  {tier:<15} {count:>6}  {pct:>5.1f}%  {bar}")
    print("-"*60)
    print(f"  {'TOTAL':<15} {total:>6}")

    # Most common junk rules
    junk = [r for r in results if r["quality_tier"] == "Likely Junk"]
    rule_hits = defaultdict(int)
    for r in junk:
        for rule in r["quality_rules_fired"]:
            rule_hits[rule] += 1
    print(f"\nTop junk signals (in 'Likely Junk' tier):")
    for rule, count in sorted(rule_hits.items(), key=lambda x: -x[1])[:8]:
        print(f"  {rule:<35} {count:>5}")


def run(dry_run=False, sample=None):
    if not CLEANING_KEY:
        log.error("Set CLEANINGEXITS_ANON_KEY env var")
        sys.exit(1)

    log.info("Fetching listings...")
    rows = sb_get_all("cleaning_listings_merge", {
        "select": "id,url,header,price,cash_flow,revenue,city,state,"
                  "notes,contact_name,contact_phone,broker_account,"
                  "direct_broker_url,is_verified",
    })

    if sample:
        rows = rows[:sample]

    log.info(f"Scoring {len(rows)} listings...")
    dup_index = build_duplicate_index(rows)

    results = []
    for r in rows:
        scored = score_listing(r, dup_index)
        results.append({
            "url":                 r.get("url"),
            "quality_score":       scored["score"],
            "quality_tier":        scored["tier"],
            "quality_rules_fired": scored["fired_rules"],
            "quality_breakdown":   scored["breakdown"],
        })

    print_report(results)

    if dry_run:
        log.info("Dry run — not writing to Supabase")
        # Show a few examples per tier
        for tier in ["Verified", "Likely Real", "Unverified", "Likely Junk"]:
            examples = [r for r in results if r["quality_tier"] == tier][:2]
            for e in examples:
                print(f"\n[{tier}] score={e['quality_score']} rules={e['quality_rules_fired']}")
        return

    # Add quality score columns if not present (run once in Supabase SQL editor):
    # ALTER TABLE cleaning_listings_merge
    #   ADD COLUMN IF NOT EXISTS quality_score integer,
    #   ADD COLUMN IF NOT EXISTS quality_tier text,
    #   ADD COLUMN IF NOT EXISTS quality_rules_fired text,
    #   ADD COLUMN IF NOT EXISTS quality_breakdown text;
    log.info("Writing scores to Supabase...")
    sb_upsert_scores(results)
    log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sample", type=int, help="Only score first N rows")
    args = parser.parse_args()
    run(dry_run=args.dry_run, sample=args.sample)
