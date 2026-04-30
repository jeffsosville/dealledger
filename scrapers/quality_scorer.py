"""
quality_scorer_v2.py  —  DealLedger listing quality scoring
PATCHED: field names now match actual listings schema.

Key changes from v1:
  - `notes` → check if exists, fall back to header for template detection
  - `revenue` → removed (column doesn't exist)
  - `broker_account` → `bbs_account_id`
  - `url` → `direct_broker_url`
  - Now scores ALL listings (active + inactive) on a flag, default active-only
  - Adjusted weights since we lost some signals
"""

import os, sys, re, json, logging, argparse
from collections import Counter

from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

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
    fired = []
    breakdown = {}

    title     = row.get("header", "") or ""
    price     = row.get("price")
    cf        = row.get("cash_flow")
    state     = row.get("state", "") or ""
    contact_n = row.get("contact_name", "") or ""
    contact_p = row.get("contact_phone", "") or ""
    bbs_acct  = row.get("bbs_account_id")  # FIXED: was broker_account
    url       = row.get("direct_broker_url", "") or ""  # FIXED: was url
    norm_t    = normalize_title(title)

    # ── POSITIVE ──────────────────────────────────────────────────────────────

    # Direct broker URL is the strongest "this is real" signal
    if url and not is_marketplace_url(url):
        points += 30; fired.append("direct_broker_url"); breakdown["direct_broker_url"] = 30
    elif url:
        points += 5; fired.append("marketplace_url"); breakdown["marketplace_url"] = 5

    # Has price
    try:
        p = float(price) if price is not None else 0
        if p > 0:
            points += 15; fired.append("has_price"); breakdown["has_price"] = 15
    except (TypeError, ValueError):
        pass

    # Has cash flow (a key real-broker signal — junk listings rarely have it)
    try:
        c = float(cf) if cf is not None else 0
        if c > 0:
            points += 15; fired.append("has_cash_flow"); breakdown["has_cash_flow"] = 15
    except (TypeError, ValueError):
        pass

    # Has contact info
    if contact_n or contact_p:
        points += 10; fired.append("has_contact"); breakdown["has_contact"] = 10

    # Reasonable price/cash-flow multiple (1x to 5x cash flow is normal SMB territory)
    try:
        if price and cf:
            mult = float(price) / float(cf)
            if 0.5 <= mult <= 5.0:
                points += 10; fired.append("reasonable_multiple"); breakdown["reasonable_multiple"] = 10
    except (TypeError, ValueError, ZeroDivisionError):
        pass

    # Has state
    if state:
        points += 5; fired.append("has_state"); breakdown["has_state"] = 5

    # Has bbs_account_id (means we know which broker posted it)
    if bbs_acct:
        points += 5; fired.append("has_broker_account"); breakdown["has_broker_account"] = 5

    # Title length suggests effort (not a 5-word junk title)
    if len(title) > 30:
        points += 5; fired.append("descriptive_title"); breakdown["descriptive_title"] = 5

    # ── NEGATIVE ──────────────────────────────────────────────────────────────

    # Duplicate title across many listings = templated junk
    dup_count = duplicate_titles.get(norm_t, 0)
    if dup_count >= 5:
        points -= 30; fired.append("duplicate_title_5plus"); breakdown["duplicate_title_5plus"] = -30
    elif dup_count >= 3:
        points -= 15; fired.append("duplicate_title_3to4"); breakdown["duplicate_title_3to4"] = -15

    # Franchise/territory keywords in title
    franchise_kw = ["territory", "franchise opportunity", "area developer",
                    "master franchise", "protected territory", "franchise resale"]
    if any(k in title.lower() for k in franchise_kw):
        points -= 20; fired.append("franchise_territory"); breakdown["franchise_territory"] = -20

    # Template fragments in title
    title_lower = title.lower()
    template_hits = sum(1 for f in TEMPLATE_FRAGMENTS if f in title_lower)
    if template_hits >= 2:
        points -= 10; fired.append("template_title"); breakdown["template_title"] = -10

    # Impossible multiple
    try:
        if price and cf:
            p, c = float(price), float(cf)
            if c > 0 and p > 0:
                mult = p / c
                if mult < 0.1 or mult > 50:
                    points -= 20; fired.append("impossible_multiple"); breakdown["impossible_multiple"] = -20
    except (TypeError, ValueError):
        pass

    # No financials at all (no price AND no cash flow)
    has_price_val = False
    has_cf_val = False
    try:
        has_price_val = bool(price) and float(price) > 0
    except (TypeError, ValueError):
        pass
    try:
        has_cf_val = bool(cf) and float(cf) > 0
    except (TypeError, ValueError):
        pass
    if not has_price_val and not has_cf_val:
        points -= 15; fired.append("no_financials"); breakdown["no_financials"] = -15

    score = max(0, min(100, points))

    if score >= 80:   tier = "Verified"
    elif score >= 60: tier = "Likely Real"
    elif score >= 40: tier = "Unverified"
    else:             tier = "Likely Junk"

    return {"score": score, "tier": tier, "fired_rules": fired, "breakdown": breakdown}


def get_listings(sb, active_only=True, sample=None):
    """Fetch listings from DealLedger."""
    rows = []
    offset = 0
    limit = 1000
    while True:
        # FIXED: using actual column names
        q = sb.table("listings").select(
            "listing_number,header,price,cash_flow,state,"
            "contact_name,contact_phone,bbs_account_id,direct_broker_url"
        )
        if active_only:
            q = q.eq("is_active", True)
        q = q.range(offset, offset + limit - 1)
        result = q.execute()
        batch = result.data or []
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
    """Write quality scores back to listings table."""
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
        # Show some sample scores
        for r in scored_rows[:10]:
            log.info(f"  Sample: {r.get('header','')[:60]:<60} | {r['quality_tier']} ({r['quality_score']}) | rules: {r['quality_rules_fired']}")
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


def run(dry_run=False, sample=None, all_listings=False):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        log.error("Set SUPABASE_URL and SUPABASE_SERVICE_KEY env vars")
        sys.exit(1)

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    active_only = not all_listings
    log.info(f"Fetching listings (active_only={active_only}, sample={sample})...")
    rows = get_listings(sb, active_only=active_only, sample=sample)
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
    parser.add_argument("--dry-run", action="store_true", help="Compute scores but don't write")
    parser.add_argument("--sample", type=int, default=None, help="Score only N listings (testing)")
    parser.add_argument("--all", action="store_true", help="Score all listings (incl inactive)")
    args = parser.parse_args()
    run(dry_run=args.dry_run, sample=args.sample, all_listings=args.all)