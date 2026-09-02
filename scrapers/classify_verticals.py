#!/usr/bin/env python3
"""
classify_verticals.py — assign `vertical` to rows in listings_direct.

WHY THIS EXISTS
Whatever used to set `vertical` stopped writing on 15 July 2026. Nothing
noticed for six weeks. By late August, 21,871 rows were untagged and roughly
70% of each day's new inventory arrived invisible to every vertical
marketplace — VendingExits showed 25 listings while the warehouse held far
more, because the vending ones were sitting untagged.

This replaces that. It runs nightly, is idempotent, and only ever fills in
NULLs — it never overwrites a vertical that already exists.

TWO STAGES, and the order matters for cost and for accuracy.

  1. RULES. Unambiguous phrases only. "laundromat" is always a laundromat.
     Roughly two thirds of rows resolve here for free.

  2. CLAUDE. Everything the rules won't touch, in batches. Costs a few cents
     a night at current volume.

WHAT THE RULES DELIBERATELY DO NOT DO

  - They never look at `description`. A backfill that matched on description
    tagged an amusement park as vending because its listing mentioned vending
    machines, and that one listing then rendered 42 times on VendingExits.
    Title and category are signal. Description is noise.

  - They never match the bare word "route". Bread routes, FedEx routes, and
    vending routes are three different businesses. "Snack Route in Tolland
    County" is DSD delivery, not vending, and no keyword list separates those
    reliably. That is exactly what the model is for.

Env:
  SUPABASE_URL, SUPABASE_SERVICE_KEY   required
  ANTHROPIC_API_KEY                    required for stage 2
  BATCH_LIMIT                          rows per run, default 2000
  DRY_RUN=1                            classify and report, write nothing
"""

import json
import os
import re
import sys
import time

import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
BATCH_LIMIT = int(os.environ.get("BATCH_LIMIT", "2000"))
DRY_RUN = os.environ.get("DRY_RUN", "") == "1"

MODEL = "claude-haiku-4-5-20251001"
LLM_BATCH = 40          # titles per model call
MAX_LLM_CALLS = 60      # ceiling per run, so a bad night can't run up a bill

# The taxonomy already in use. Anything outside this list is 'other'.
VERTICALS = [
    "vending", "atm", "cleaning", "landscaping", "laundromat", "dry_cleaner",
    "pool_service", "pressure_washing", "junk_removal", "pest_control",
    "hvac", "plumbing", "electrical", "roofing", "construction",
    "automotive", "restaurant", "retail", "healthcare", "manufacturing",
    "technology", "amusement", "delivery_route", "other",
]

# ── Stage 1: rules ─────────────────────────────────────────────────────────────
# Ordered. First match wins, so the specific ones come before the general.
# Every pattern here must be unambiguous on a title alone.
RULES = [
    ("laundromat",       r"\blaundromat\b|\bcoin laundry\b|\bcoin-op laundry\b"),
    ("dry_cleaner",      r"\bdry clean|\bdry-clean|\bgarment care\b"),
    ("atm",              r"\batm route\b|\batm portfolio\b|\batm business\b|\batm machines?\b"),
    ("vending",          r"\bvending\b|\bmicro ?market\b|\bunattended retail\b"),
    ("pest_control",     r"\bpest control\b|\bexterminat|\btermite\b"),
    ("junk_removal",     r"\bjunk removal\b|\bjunk haul"),
    ("pressure_washing", r"\bpressure wash|\bpower wash|\bsoft wash"),
    ("pool_service",     r"\bpool service\b|\bpool route\b|\bpool cleaning\b"),
    ("roofing",          r"\broofing\b|\broof replacement\b"),
    ("hvac",             r"\bhvac\b|\bheating and (air|cooling)\b|\bair conditioning\b"),
    ("plumbing",         r"\bplumbing\b|\bplumber\b"),
    ("electrical",       r"\belectrical contract|\belectrician\b"),
    ("landscaping",      r"\blandscap|\blawn care\b|\blawn maintenance\b|\btree service\b"),
    ("cleaning",         r"\bjanitorial\b|\bcommercial cleaning\b|\bresidential cleaning\b|"
                         r"\bmaid service\b|\bhouse cleaning\b|\bcarpet cleaning\b|"
                         r"\bwindow cleaning\b|\bduct cleaning\b|\bcleaning (company|business|service)\b"),
    ("amusement",        r"\barcade\b|\bclaw machine\b|\bgame room\b|\bamusement\b"),
    ("delivery_route",   r"\bfedex\b|\bbread route\b|\bp&d route\b|\bdistribution route\b|"
                         r"\bsnack route\b|\bchip route\b|\bdairy route\b"),
]
COMPILED = [(v, re.compile(p, re.I)) for v, p in RULES]

# Titles that are junk, not businesses. Tag 'other' without spending a call.
JUNK = re.compile(
    r"^\s*$|^[\d\W]+$|^(view|see|click|register|sign|log)\b|"
    r"^\s*(listing|business|company)\s*$",
    re.I,
)


def rule_match(title: str, category: str | None):
    hay = f"{title or ''} {category or ''}"
    if JUNK.match(title or ""):
        return "other"
    for vertical, pat in COMPILED:
        if pat.search(hay):
            return vertical
    return None


# ── Supabase ───────────────────────────────────────────────────────────────────
def headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }


def fetch_untagged(limit: int):
    """Newest and still-active first — those are the ones a site would show."""
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/listings_direct",
        headers=headers(),
        params={
            "select": "id,title,category",
            "vertical": "is.null",
            "status": "eq.active",
            "order": "last_seen.desc",
            "limit": str(limit),
        },
        timeout=120,
    )
    r.raise_for_status()
    return r.json()


def write_vertical(row_id, vertical: str) -> bool:
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/listings_direct",
        headers={**headers(), "Prefer": "return=minimal"},
        params={"id": f"eq.{row_id}", "vertical": "is.null"},  # never overwrite
        json={"vertical": vertical, "updated_at": "now()"},
        timeout=60,
    )
    if not r.ok:
        print(f"  write failed for {row_id}: {r.status_code} {r.text[:160]}")
    return r.ok


# ── Stage 2: Claude ────────────────────────────────────────────────────────────
PROMPT = """You are categorizing small businesses that are listed for sale.

Assign each listing to exactly one category from this list:
{verticals}

Rules:
- Judge only from the title and category given. Do not infer from what a
  business might incidentally contain. A restaurant that mentions vending
  machines is a restaurant.
- "delivery_route" means DSD distribution — bread, chips, FedEx, dairy. A
  driver restocking shelves from a truck.
- "vending" means machine-based unattended retail. Someone servicing machines.
  These two are commonly confused. Read carefully.
- "cleaning" is janitorial and cleaning services. A laundromat is
  "laundromat", a dry cleaner is "dry_cleaner". Keep them separate.
- If it does not clearly fit, answer "other". A wrong specific answer is worse
  than "other" — these feed public marketplaces where a miscategorized listing
  is visible to buyers.

Return ONLY a JSON array of objects, no prose, no code fences:
[{{"i": 0, "v": "vending"}}, {{"i": 1, "v": "other"}}]

Listings:
{listings}"""


def classify_with_claude(rows):
    """rows: list of (index, title, category). Returns {index: vertical}."""
    listing_lines = "\n".join(
        f'{i}. {t or "(no title)"}' + (f" [{c}]" if c else "")
        for i, t, c in rows
    )
    prompt = PROMPT.format(
        verticals=", ".join(VERTICALS),
        listings=listing_lines,
    )

    for attempt in range(3):
        try:
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": MODEL,
                    "max_tokens": 2000,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=120,
            )
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            r.raise_for_status()

            text = r.json()["content"][0]["text"].strip()
            text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.M).strip()
            parsed = json.loads(text)

            out = {}
            for item in parsed:
                v = str(item.get("v", "other")).strip().lower()
                if v not in VERTICALS:
                    v = "other"
                out[int(item["i"])] = v
            return out

        except Exception as exc:
            print(f"  claude attempt {attempt + 1} failed: {exc}")
            time.sleep(3 * (attempt + 1))

    return {}


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY")
        sys.exit(2)

    rows = fetch_untagged(BATCH_LIMIT)
    print(f"Untagged active rows pulled: {len(rows)}")
    if not rows:
        print("Nothing to classify.")
        return

    counts, written, needs_llm = {}, 0, []

    # Stage 1
    for row in rows:
        v = rule_match(row.get("title"), row.get("category"))
        if v:
            counts[v] = counts.get(v, 0) + 1
            if not DRY_RUN and write_vertical(row["id"], v):
                written += 1
        else:
            needs_llm.append(row)

    print(f"Rules matched: {len(rows) - len(needs_llm)}  |  left for Claude: {len(needs_llm)}")

    # Stage 2
    if needs_llm and not ANTHROPIC_KEY:
        print("ANTHROPIC_API_KEY not set — skipping stage 2. Rules-only run.")
    elif needs_llm:
        calls = 0
        for start in range(0, len(needs_llm), LLM_BATCH):
            if calls >= MAX_LLM_CALLS:
                print(f"Hit MAX_LLM_CALLS ({MAX_LLM_CALLS}); remainder waits for the next run.")
                break
            chunk = needs_llm[start:start + LLM_BATCH]
            payload = [(i, r.get("title"), r.get("category")) for i, r in enumerate(chunk)]
            result = classify_with_claude(payload)
            calls += 1

            for i, row in enumerate(chunk):
                v = result.get(i, "other")
                counts[v] = counts.get(v, 0) + 1
                if not DRY_RUN and write_vertical(row["id"], v):
                    written += 1
            time.sleep(0.4)
        print(f"Claude calls made: {calls}")

    print("\n--- classified ---")
    for v, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {v:18} {n}")
    print(f"\nRows written: {written}{'  (DRY RUN - nothing saved)' if DRY_RUN else ''}")


if __name__ == "__main__":
    main()
