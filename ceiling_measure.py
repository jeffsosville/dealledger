#!/usr/bin/env python3
"""
ceiling_measure.py — how many live listings COULD ever get a BizQuest view count?

Read-only. Compares listings_direct_live against the pool of BizBuySell/BizQuest
listings that actually have views (listings.listing_views > 0), using the two
broker-name-independent signals:

  * phone            — contact_phone (last 10 digits), near-unique
  * state+price+CF   — same state, asking price, and cash flow (a "same business" proxy)

Also reports field availability on the live side, because you can't match on a
field that's null. This is the realistic ceiling; the actual matcher will land
somewhat below it (precision guards) — but it tells us if the target is ~8k or ~20k.

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


def phone10(v):
    d = re.sub(r"\D", "", str(v or ""))
    return d[-10:] if len(d) >= 10 else None


def to_int(v):
    try:
        n = int(float(str(v).replace(",", "").replace("$", "")))
        return n if n > 0 else None
    except Exception:
        return None


def norm_state(v):
    s = str(v or "").strip().upper()
    return s if len(s) == 2 and s.isalpha() else None


def main():
    if not KEY:
        sys.exit("Set SUPABASE_SERVICE_KEY")

    print("Loading viewed BBS/BizQuest pool (listings.listing_views > 0)...")
    pool = page("listings", {"select": "listing_number,state,price,cash_flow,contact_phone,listing_views",
                             "listing_views": "gt.0"})
    print(f"  {len(pool)} viewed listings")

    phone_idx, spc_idx = set(), set()
    for r in pool:
        ph = phone10(r.get("contact_phone"))
        if ph:
            phone_idx.add(ph)
        st, p, cf = norm_state(r.get("state")), to_int(r.get("price")), to_int(r.get("cash_flow"))
        if st and p and cf:
            spc_idx.add((st, p, cf))
    print(f"  index: {len(phone_idx)} distinct phones, {len(spc_idx)} distinct state+price+CF keys")

    print("Loading live listings...")
    live = page("listings_direct_live",
                {"select": "id,state,asking_price,cash_flow,contact_phone,profile_views"})
    print(f"  {len(live)} live listings")

    # field availability
    has_phone = sum(1 for r in live if phone10(r.get("contact_phone")))
    has_price = sum(1 for r in live if to_int(r.get("asking_price")))
    has_spc = sum(1 for r in live if norm_state(r.get("state")) and to_int(r.get("asking_price")) and to_int(r.get("cash_flow")))
    already = sum(1 for r in live if r.get("profile_views") not in (None, 0, "0"))

    m_phone = m_spc = m_any = m_phone_only = 0
    for r in live:
        ph = phone10(r.get("contact_phone"))
        st, p, cf = norm_state(r.get("state")), to_int(r.get("asking_price")), to_int(r.get("cash_flow"))
        hit_phone = bool(ph and ph in phone_idx)
        hit_spc = bool(st and p and cf and (st, p, cf) in spc_idx)
        if hit_phone:
            m_phone += 1
        if hit_spc:
            m_spc += 1
        if hit_phone or hit_spc:
            m_any += 1
        if hit_phone and not hit_spc:
            m_phone_only += 1

    n = len(live)
    def pct(x): return f"{x:>6}  ({100*x/n:4.1f}%)"

    print("\n──────── field availability (live side) ────────")
    print(f"live listings ................ {n}")
    print(f"  have a phone ............... {pct(has_phone)}")
    print(f"  have an asking price ....... {pct(has_price)}")
    print(f"  have state+price+cashflow .. {pct(has_spc)}")
    print(f"  already have profile_views . {pct(already)}")

    print("\n──────── matchable ceiling (vs viewed pool) ────────")
    print(f"  via phone .................. {pct(m_phone)}")
    print(f"  via state+price+cashflow ... {pct(m_spc)}")
    print(f"  via EITHER (the ceiling) ... {pct(m_any)}")
    print(f"  phone catches that S+P+CF misses: {m_phone_only}")


if __name__ == "__main__":
    main()
