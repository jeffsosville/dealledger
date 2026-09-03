#!/usr/bin/env python3
"""
match_explore.py — read-only. Three questions at once:

  1. LOOSENED CEILING: how many live listings match a viewed BBS listing on
     state+price ALONE (no cashflow) — the basis the old matcher used.
  2. URL SIGNAL (the potential game-changer): does listings.direct_broker_url /
     website exact-match your broker-direct url? If so that's a precise link that
     carries BOTH views and true DOM — no attribute guessing.
  3. DOM REACH: of listings that carry a real BBS listing_number, how many could
     hand a matched broker-direct listing an accurate days-on-market.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY.
"""
import os, re, sys
import requests as http

URL = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
H = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}


def page(table, params):
    out, off, size = [], 0, 1000
    while True:
        r = http.get(f"{URL}/rest/v1/{table}",
                     headers={**H, "Range-Unit": "items", "Range": f"{off}-{off+size-1}"},
                     params=params, timeout=120)
        r.raise_for_status()
        b = r.json()
        if not b:
            break
        out.extend(b)
        if len(b) < size:
            break
        off += size
    return out


def norm_url(u):
    u = str(u or "").strip().lower()
    if not u or not u.startswith("http"):
        return None
    u = re.sub(r"^https?://(www\.)?", "", u)
    u = u.split("?")[0].split("#")[0].rstrip("/")
    return u or None


def to_int(v):
    try:
        n = int(float(str(v).replace(",", "").replace("$", "")))
        return n if n > 0 else None
    except Exception:
        return None


def st2(v):
    s = str(v or "").strip().upper()
    return s if len(s) == 2 and s.isalpha() else None


def main():
    if not KEY:
        sys.exit("Set SUPABASE_SERVICE_KEY")

    print("Loading live listings (id, url, state, asking_price)...")
    live = page("listings_direct_live", {"select": "id,url,state,asking_price,days_on_market"})
    live_urls = {}
    for r in live:
        nu = norm_url(r.get("url"))
        if nu:
            live_urls.setdefault(nu, r["id"])
    print(f"  {len(live)} live, {len(live_urls)} distinct normalized urls")

    # ---- 1. loosened ceiling: state+price ----
    print("\nLoading viewed pool (state, price)...")
    pool = page("listings", {"select": "state,price,listing_views", "listing_views": "gt.0"})
    sp = set()
    for r in pool:
        s, p = st2(r.get("state")), to_int(r.get("price"))
        if s and p:
            sp.add((s, p))
    sp_hits = sum(1 for r in live if st2(r.get("state")) and to_int(r.get("asking_price"))
                  and (st2(r.get("state")), to_int(r.get("asking_price"))) in sp)
    print(f"  viewed pool: {len(sp)} distinct state+price keys")
    print(f"  LOOSENED ceiling (state+price, no cashflow): {sp_hits}  ({100*sp_hits/len(live):.1f}%)")

    # ---- 2. URL signal ----
    print("\nLoading listings with a direct_broker_url / website...")
    bbs = page("listings",
               {"select": "listing_number,direct_broker_url,website,listing_views,days_on_market",
                "direct_broker_url": "not.is.null"})
    print(f"  {len(bbs)} listings have direct_broker_url")
    if bbs:
        for s in bbs[:5]:
            print(f"    e.g. {str(s.get('direct_broker_url'))[:80]}")
    url_match = url_match_views = url_match_dom = 0
    for r in bbs:
        for field in ("direct_broker_url", "website"):
            nu = norm_url(r.get(field))
            if nu and nu in live_urls:
                url_match += 1
                if to_int(r.get("listing_views")):
                    url_match_views += 1
                if to_int(r.get("days_on_market")):
                    url_match_dom += 1
                break
    print(f"  EXACT url matches to a live listing: {url_match}")
    print(f"     ...that carry views: {url_match_views}   ...that carry DOM: {url_match_dom}")

    # ---- 3. DOM reach via existing matches ----
    print("\nDOM via existing listing_matches...")
    matches = page("listing_matches", {"select": "direct_id,bbs_listing_number"})
    nums = sorted({str(m["bbs_listing_number"]) for m in matches if m.get("bbs_listing_number")})
    dom_map = {}
    for i in range(0, len(nums), 200):
        chunk = ",".join(nums[i:i+200])
        for r in page("listings", {"select": "listing_number,days_on_market,estimated_listed_date",
                                   "listing_number": f"in.({chunk})"}):
            if to_int(r.get("days_on_market")):
                dom_map[str(r["listing_number"])] = r["days_on_market"]
    live_ids = {r["id"] for r in live}
    dom_reach = 0
    for m in matches:
        did = str(m["direct_id"])
        cand = did if did in live_ids else (f"spec:{did}" if f"spec:{did}" in live_ids else None)
        if cand and dom_map.get(str(m.get("bbs_listing_number"))):
            dom_reach += 1
    print(f"  matched live listings that could get REAL DOM from BBS listing_number: {dom_reach}")


if __name__ == "__main__":
    main()
