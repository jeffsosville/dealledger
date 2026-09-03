#!/usr/bin/env python3
"""
views_bridge.py — connect the already-built pieces.

Chain:  listings_direct_live.id  ==  'spec:' + listing_matches.direct_id
        listing_matches.bbs_listing_number  ->  listings.listing_views

Dry run (default): reports how many LIVE listings would get a view count right
now from the existing 6,498 matches — no writes.

--apply: writes profile_views, bbs_listing_id, match_confidence onto
         listings_direct_live for those rows. If listings_direct_live turns out
         to be a VIEW (not a table), the PATCH will error clearly and we'll know
         the fix belongs in the view's SQL instead.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY.
"""
import os, sys, argparse
import requests as http

URL = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
H = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}


def page(table, params):
    """Paginate a select, return all rows."""
    out, off, size = [], 0, 1000
    while True:
        r = http.get(f"{URL}/rest/v1/{table}",
                     headers={**H, "Range-Unit": "items", "Range": f"{off}-{off+size-1}"},
                     params=params, timeout=90)
        r.raise_for_status()
        b = r.json()
        if not b:
            break
        out.extend(b)
        if len(b) < size:
            break
        off += size
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--min-confidence", type=int, default=0,
                    help="only use matches with confidence >= this")
    args = ap.parse_args()
    if not KEY:
        sys.exit("Set SUPABASE_SERVICE_KEY")

    print("Loading listing_matches...")
    matches = page("listing_matches", {"select": "direct_id,bbs_listing_number,confidence"})
    matches = [m for m in matches if int(m.get("confidence") or 0) >= args.min_confidence]
    print(f"  {len(matches)} matches (confidence >= {args.min_confidence})")

    # Views for the matched BBS numbers.
    want = sorted({str(m["bbs_listing_number"]) for m in matches if m.get("bbs_listing_number")})
    print(f"Loading listing_views for {len(want)} matched BBS numbers...")
    views = {}
    for i in range(0, len(want), 200):
        chunk = ",".join(want[i:i+200])
        rows = page("listings", {"select": "listing_number,listing_views",
                                 "listing_number": f"in.({chunk})",
                                 "listing_views": "gt.0"})
        for r in rows:
            views[str(r["listing_number"])] = r["listing_views"]
    print(f"  {len(views)} of those have listing_views > 0")

    # Which direct_ids are actually live?
    print("Loading listings_direct_live ids...")
    live = {r["id"] for r in page("listings_direct_live", {"select": "id"})}
    print(f"  {len(live)} live listings")

    # Build the update set.
    updates, live_matches, has_views = [], 0, 0
    for m in matches:
        did = str(m["direct_id"])
        cand = did if did in live else (f"spec:{did}" if f"spec:{did}" in live else None)
        if cand:
            live_matches += 1
        v = views.get(str(m.get("bbs_listing_number")))
        if v is not None:
            has_views += 1
        if cand and v is not None:
            updates.append({"id": cand,
                            "bbs_listing_id": str(m["bbs_listing_number"]),
                            "profile_views": v,
                            "match_confidence": int(m.get("confidence") or 0)})

    print("\n──────── coverage ────────")
    print(f"matches total ............ {len(matches)}")
    print(f"  ...that are live ....... {live_matches}")
    print(f"  ...that have views ..... {has_views}")
    print(f"LIVE listings that would light up with views: {len(updates)}")
    if updates:
        top = sorted(updates, key=lambda u: u["profile_views"], reverse=True)[:10]
        print("\n  top by views:")
        for u in top:
            print(f"    {u['profile_views']:>6} views  conf={u['match_confidence']:<3} {u['id']}")

    if not args.apply:
        print("\nDRY RUN — re-run with --apply to write these onto listings_direct_live.")
        return

    print(f"\nApplying {len(updates)} updates to listings_direct_live...")
    ok = err = 0
    for u in updates:
        pr = http.patch(f"{URL}/rest/v1/listings_direct_live",
                        headers={**H, "Content-Type": "application/json"},
                        params={"id": f"eq.{u['id']}"},
                        json={"bbs_listing_id": u["bbs_listing_id"],
                              "profile_views": u["profile_views"],
                              "match_confidence": u["match_confidence"]},
                        timeout=60)
        if pr.status_code in (200, 204):
            ok += 1
        else:
            err += 1
            if err <= 3:
                print(f"  err {pr.status_code}: {pr.text[:200]}")
    print(f"\nDONE — wrote {ok}, errors {err}.")
    if err and ok == 0:
        print("All writes failed — listings_direct_live is likely a VIEW; "
              "the fix belongs in its SQL definition, not a row update.")


if __name__ == "__main__":
    main()
