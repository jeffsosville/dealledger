"""
quality_scorer.py  —  DealLedger listing quality scoring
Scores all listings in the DealLedger `listings` table and writes
quality_score, quality_tier, quality_rules_fired, quality_breakdown back.

Tiers:
  80–100 → Verified
  60–79  → Likely Real
  40–59  → Unverified
  0–39   → Likely Junk

Usage:
  python3 quality_scorer.py
  python3 quality_scorer.py --dry-run
  python3 quality_scorer.py --sample 500
"""

import os, sys, re, json, logging, argparse
from collections import Counter
from datetime import datetime

import requests
from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

BATCH_SIZE = 500

TEMPLATE_FRAGMENTS = [
    "motivated seller", "turn-key", "turnkey", "absentee owner",
    "recession proof", "easy to operate", "serious inquiries only",
    "seller financing available", "asset sale", "priced to sell",
]

SPAM_BROKER_PATTERNS = [
    r"bizquest\s*network",
    r"vr\s*business",
    r"transworld.*franchise",
]

def normalize_title(t):
    return re.sub(r"\s+", " ", (t or "").lower().strip())

def is_marketplace_url(u):
    markets = ["bizbuysell.com", "bizquest.com", "businessesforsale.com",
               "loopnet.com", "crexi.com"]
    return any(m in (u or "").lower() for m in markets)

def score_listing(row: dict, duplicate_titles: dict) -> dict:
    points = 0
    fired  = []
    breakdown = {}

    title     = row.get("header", "") or ""
    notes     = row.get("notes", "") or ""
    price     = row.get("price")
    cf        = row.get("cash_flow")
    revenue   = row.get("revenue")
    city      = row.get("city", "") or ""
    state     = row.get("state", "") or ""
    contact_n = row.get("contact_name", "") or ""
    contact_p = row.get("contact_phone", "") or ""
    broker_ac = row.get("broker_account", "") or ""
    url       = row.get("url", "") or ""
    norm_t    = normalize_title(title)

    # ── POSITIVE ──────────────────────────────────────────────────────────────

    # Direct broker URL (not a marketplace domain)
    if url and not is_marketplace_url(url):
        points += 25; fired.append("direct_broker_url"); breakdown["direct_broker_url"] = 25
    elif url:
        points += 5; fired.append("marketplace_url"); breakdown["marketplace_url"] = 5

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

    # Unique description
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
    elif state:
        points += 2; fired.append("has_state"); breakdown["has_state"] = 2

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

    score = max(0, min(100, points))

    if score >= 80:   tier = "Verified"
    elif score >= 60: tier = "Likely Real"
    elif score >= 40: tier = "Unverified"
    else:             tier = "Likely Junk"

    return {"score": score, "tier": tier, "fired_rules": fired, "breakdown": breakdown}


def get_all_listings(sb, sample=None):
    """Fetch all active listings from DealLedger."""
    rows = []
    offset = 0
    limit = 1000
    while True:
        q = sb.table("listings").select(
            "listing_number,header,price,cash_flow,revenue,city,state,"
            "contact_name,contact_phone,broker_account,url,notes"
        ).eq("is_active", True).range(offset, offset + limit - 1).execute()
        batch = q.data or []
        rows.extend(batch)
        log.info(f"  Fetched {len(rows)} listings so far...")
        if len(batch) < limit:
            break
        offset += limit
        if sample and len(rows) >= sample:
            rows = rows[:sample]
            break
    return rows


def upsert_scores(sb, scored_rows, dry_run=False):
    """Write quality scores back to DealLedger listings table."""
    updates = [
        {
            "listing_number": r["listing_number"],
            "quality_score":       r["quality_score"],
            "quality_tier":        r["quality_tier"],
            "quality_rules_fired": json.dumps(r["quality_rules_fired"]),
            "quality_breakdown":   json.dumps(r["quality_breakdown"]),
        }
        for r in scored_rows if r.get("listing_number")
    ]
    if dry_run:
        log.info(f"[DRY RUN] Would upsert {len(updates)} scores")
        return
    ok = err = 0
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]
        try:
            sb.table("listings").upsert(batch, on_conflict="listing_number").execute()
            ok += len(batch)
        except Exception as e:
            log.error(f"Upsert error: {e}")
            err += len(batch)
    log.info(f"Scores written: {ok} ok, {err} errors")


def run(dry_run=False, sample=None):
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("Set SUPABASE_URL and SUPABASE_SERVICE_KEY env vars")
        sys.exit(1)

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    log.info("Fetching DealLedger listings...")
    rows = get_all_listings(sb, sample=sample)
    log.info(f"Scoring {len(rows)} listings...")

    # Build duplicate title index
    title_counts = Counter(normalize_title(r.get("header", "")) for r in rows)

    results = []
    for r in rows:
        scored = score_listing(r, title_counts)
        results.append({
            **r,
            "quality_score":       scored["score"],
            "quality_tier":        scored["tier"],
            "quality_rules_fired": scored["fired_rules"],
            "quality_breakdown":   scored["breakdown"],
        })

    # Summary
    tiers = Counter(r["quality_tier"] for r in results)
    total = len(results)
    log.info("── Quality Score Summary ──────────────────")
    for tier in ["Verified", "Likely Real", "Unverified", "Likely Junk"]:
        n = tiers.get(tier, 0)
        pct = n / total * 100 if total else 0
        log.info(f"  {tier:<15} {n:>6,}  ({pct:.1f}%)")
    log.info(f"  {'TOTAL':<15} {total:>6,}")

    upsert_scores(sb, results, dry_run=dry_run)
    log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sample", type=int, default=None)
    args = parser.parse_args()
    run(dry_run=args.dry_run, sample=args.sample)
