#!/usr/bin/env python3
"""
apply_url_dom.py — wire real DOM (and any views) onto listings_direct_live via
EXACT url match: listings_direct_live.url  ==  listings.direct_broker_url (or listings.url).

This is high precision (same URL string), and pulls whatever DOM the listings
table already computed (prefer estimated_listed_date so DOM is current), plus
views + bbs listing_number when present.

Dry run (default): reports coverage + a DOM sanity sample (are the numbers real,
i.e. hundreds of days, not ~0). --apply writes days_on_market, dom,
dom_source='bbs_url_match', and profile_views/bbs_listing_id where available.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY.
"""
import os, re, sys, argparse
from datetime import date, datetime
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
        return n
    except Exception:
        return None


def dom_from(row):
    """Prefer a current DOM from estimated_listed_date; else stored days_on_market."""
    eld = row.get("estimated_listed_date")
    if eld:
        try:
            d = datetime.fromisoformat(str(eld)[:10]).date()
            return max(0, (date.today() - d).days), "estimated_listed_date"
        except Exception:
            pass
    dm = to_int(row.get("days_on_market"))
    if dm is not None and dm >= 0:
        return dm, "days_on_market"
    return None, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if not KEY:
        sys.exit("Set SUPABASE_SERVICE_KEY")

    print("Loading live listings...")
    live = page("listings_direct_live", {"select": "id,url,days_on_market"})
    live_by_url = {}
    for r in live:
        nu = norm_url(r.get("url"))
        if nu:
            live_by_url.setdefault(nu, r)
    print(f"  {len(live)} live; {len(live_by_url)} distinct urls")

    print("Loading listings with direct_broker_url or matching url...")
    src = page("listings", {"select": "listing_number,url,direct_broker_url,days_on_market,"
                                       "estimated_listed_date,listing_views",
                            "direct_broker_url": "not.is.null"})
    # index best row per normalized url (prefer rows with a real listed date, then views)
    best = {}
    for r in src:
        for f in ("direct_broker_url", "url"):
            nu = norm_url(r.get(f))
            if nu and nu in live_by_url:
                cur = best.get(nu)
                score = (1 if r.get("estimated_listed_date") else 0, to_int(r.get("listing_views")) or 0)
                if cur is None or score > cur[0]:
                    best[nu] = (score, r)
                break

    updates, dom_real, dom_samples, with_views = [], 0, [], 0
    for nu, (_, r) in best.items():
        live_row = live_by_url[nu]
        dom, dsrc = dom_from(r)
        if dom is None:
            continue
        upd = {"id": live_row["id"], "days_on_market": dom, "dom": dom,
               "dom_source": "bbs_url_match"}
        if to_int(r.get("listing_views")):
            upd["profile_views"] = to_int(r["listing_views"])
            with_views += 1
        if r.get("listing_number"):
            upd["bbs_listing_id"] = str(r["listing_number"])
        updates.append(upd)
        if dsrc == "estimated_listed_date":
            dom_real += 1
        if len(dom_samples) < 12:
            dom_samples.append((dom, dsrc, live_row["id"]))

    n = len(live)
    print(f"\n──────── url-match DOM ────────")
    print(f"distinct live listings matched by url ... {len(best)}")
    print(f"  ...that yield a DOM value ............. {len(updates)}")
    print(f"     of which DOM from a real listed date {dom_real}")
    print(f"  ...that also carry views ............. {with_views}")
    if updates:
        doms = sorted(u["days_on_market"] for u in updates)
        mid = doms[len(doms)//2]
        print(f"  DOM spread: min {doms[0]}, median {mid}, max {doms[-1]}")
        print("  samples:")
        for dom, dsrc, lid in dom_samples:
            print(f"    {dom:>5} days  ({dsrc})  {lid}")

    if not args.apply:
        print("\nDRY RUN — re-run with --apply to write DOM onto listings_direct_live.")
        return

    print(f"\nApplying {len(updates)} DOM updates...")
    ok = err = 0
    for u in updates:
        body = {k: v for k, v in u.items() if k != "id"}
        pr = http.patch(f"{URL}/rest/v1/listings_direct_live",
                        headers={**H, "Content-Type": "application/json"},
                        params={"id": f"eq.{u['id']}"}, json=body, timeout=60)
        ok += 1 if pr.status_code in (200, 204) else 0
        if pr.status_code not in (200, 204):
            err += 1
            if err <= 3:
                print(f"  err {pr.status_code}: {pr.text[:160]}")
    print(f"\nDONE — wrote DOM on {ok}, errors {err}.")


if __name__ == "__main__":
    main()
