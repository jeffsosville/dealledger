#!/usr/bin/env python3
"""
apply_dom_final.py — accurate DOM for BBS-number matches + safe monotonic backfill.

For each live listing, take the EARLIEST provable "listed / first-seen" date from:
  A. real BizBuySell listing date  (via listing_matches -> listings.estimated_listed_date)  [TRUE age]
  B. earlier tracking date         (via exact direct_broker_url match -> listings first_seen/est date)
  C. its own listings_direct_live.first_seen
DOM = today - earliest. We only WRITE when the new DOM is >= current (monotonic:
never understate a listing's age). dom_source records which tier won.

Dry run (default) shows the source breakdown + DOM medians so you can confirm the
BBS tier looks like real market age (~200d) before writing. --apply bulk-updates.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY.
"""
import os, re, sys, argparse
from datetime import date, datetime
import requests as http

URL = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
H = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}
TODAY = date.today()


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
    u = re.sub(r"^https?://(www\.)?", "", u).split("?")[0].split("#")[0].rstrip("/")
    return u or None


def as_date(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v)[:19].replace(" ", "T")).date() if len(str(v)) > 10 \
               else datetime.fromisoformat(str(v)[:10]).date()
    except Exception:
        return None


def dom(d):
    return max(0, (TODAY - d).days) if d else None


def med(xs):
    xs = sorted(xs)
    return xs[len(xs)//2] if xs else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if not KEY:
        sys.exit("Set SUPABASE_SERVICE_KEY")

    print("Loading live listings...")
    live = page("listings_direct_live", {"select": "id,url,first_seen,days_on_market"})
    live_ids = {r["id"] for r in live}
    live_by_url = {}
    for r in live:
        nu = norm_url(r.get("url"))
        if nu:
            live_by_url.setdefault(nu, r)
    print(f"  {len(live)} live")

    # A. real BBS dates via listing_matches
    print("Loading listing_matches + BBS listed dates...")
    matches = page("listing_matches", {"select": "direct_id,bbs_listing_number"})
    nums = sorted({str(m["bbs_listing_number"]) for m in matches if m.get("bbs_listing_number")})
    bbs_date = {}
    for i in range(0, len(nums), 200):
        chunk = ",".join(nums[i:i+200])
        for r in page("listings", {"select": "listing_number,estimated_listed_date",
                                   "listing_number": f"in.({chunk})"}):
            d = as_date(r.get("estimated_listed_date"))
            if d:
                bbs_date[str(r["listing_number"])] = d
    a_date = {}  # live id -> real BBS date
    for m in matches:
        did = str(m["direct_id"])
        cand = did if did in live_ids else (f"spec:{did}" if f"spec:{did}" in live_ids else None)
        d = bbs_date.get(str(m.get("bbs_listing_number")))
        if cand and d:
            if cand not in a_date or d < a_date[cand]:
                a_date[cand] = d
    print(f"  {len(a_date)} live listings with a real BBS listed date")

    # B. earlier tracking date via exact direct_broker_url match
    print("Loading direct_broker_url tracking dates...")
    b_date = {}  # live id -> earliest tracking date
    for r in page("listings", {"select": "url,direct_broker_url,first_seen,estimated_listed_date",
                               "direct_broker_url": "not.is.null"}):
        for f in ("direct_broker_url", "url"):
            nu = norm_url(r.get(f))
            if nu and nu in live_by_url:
                cands = [as_date(r.get("first_seen")), as_date(r.get("estimated_listed_date"))]
                cands = [c for c in cands if c]
                if cands:
                    lid = live_by_url[nu]["id"]
                    e = min(cands)
                    if lid not in b_date or e < b_date[lid]:
                        b_date[lid] = e
                break
    print(f"  {len(b_date)} live listings with an earlier tracking date")

    # Combine — earliest wins, monotonic guard.
    updates, src_count, src_doms = [], {"bbs": 0, "url": 0, "self": 0}, {"bbs": [], "url": []}
    for r in live:
        lid = r["id"]
        own = as_date(r.get("first_seen"))
        cur_dom = r.get("days_on_market")
        cur_dom = int(cur_dom) if str(cur_dom).lstrip("-").isdigit() else None

        cands = []
        if lid in a_date: cands.append((a_date[lid], "bbs"))
        if lid in b_date: cands.append((b_date[lid], "url"))
        if own:           cands.append((own, "self"))
        if not cands:
            continue
        best_date, src = min(cands, key=lambda t: t[0])
        nd = dom(best_date)
        if nd is None:
            continue
        # monotonic: never write a smaller DOM than what's already there
        if cur_dom is not None and nd <= cur_dom:
            continue
        updates.append({"id": lid, "days_on_market": nd, "dom": nd,
                        "dom_source": {"bbs": "bbs_listing_number",
                                       "url": "url_match_backfill",
                                       "self": "first_seen"}[src]})
        src_count[src] += 1
        if src in src_doms:
            src_doms[src].append(nd)

    print("\n──────── DOM plan ────────")
    print(f"listings to update (DOM improves) ... {len(updates)}")
    print(f"  from real BBS listed date ......... {src_count['bbs']:>6}   median {med(src_doms['bbs'])}d")
    print(f"  from url-match earlier date ....... {src_count['url']:>6}   median {med(src_doms['url'])}d")
    print(f"  from own first_seen ............... {src_count['self']:>6}")

    if not args.apply:
        print("\nDRY RUN — re-run with --apply. (BBS median should look like real age, ~150-250d.)")
        return

    print(f"\nApplying {len(updates)} DOM updates (bulk upsert)...")
    hdr = {**H, "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"}
    ok = err = 0
    for i in range(0, len(updates), 500):
        batch = updates[i:i+500]
        r = http.post(f"{URL}/rest/v1/listings_direct_live", headers=hdr, json=batch, timeout=90)
        if r.status_code in (200, 201, 204):
            ok += len(batch)
        else:
            err += len(batch)
            if err <= 500:
                print(f"  err {r.status_code}: {r.text[:200]}")
    print(f"\nDONE — updated {ok}, errors {err}.")


if __name__ == "__main__":
    main()
