#!/usr/bin/env python3
"""
marketplace_scan.py - find brokers whose own website sends buyers to a
marketplace (BizBuySell, BizQuest, BusinessesForSale, DealStream) instead of
showing listings on their own site.

Prospects for the free broker listings page (Oct 2026 pilot). For each broker
it reads ONLY the broker's own pages: the homepage and up to two likely
listings pages from its menu. It records links that point to a marketplace;
it never fetches the marketplace itself.

Writes one row per domain to broker_marketplace_links (upsert).

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY, SHARD, SHARDS, LIMIT (per shard),
     WORKERS (threads, default 8)
"""
import os, re, sys, time, json
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import discovery_v2 as dv  # get_page (curl_cffi fallback), score_link

SB = os.environ.get("SUPABASE_URL", "").rstrip("/")
KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
H = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
SHARDS = max(1, int(os.environ.get("SHARDS", "1")))
SHARD = int(os.environ.get("SHARD", "0")) % SHARDS
LIMIT = int(os.environ.get("LIMIT", "100000"))
WORKERS = int(os.environ.get("WORKERS", "8"))

MARKETS = {
    "bizbuysell": re.compile(r"bizbuysell\.com", re.I),
    "bizquest": re.compile(r"bizquest\.com", re.I),
    "businessesforsale": re.compile(r"businessesforsale\.com", re.I),
    "dealstream": re.compile(r"dealstream\.com", re.I),
    "loopnet": re.compile(r"loopnet\.com", re.I),
}
PHONE = re.compile(r"\(?\b\d{3}\)?[ .-]\d{3}[ .-]\d{4}\b")
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[a-z]{2,}", re.I)
PRICE = re.compile(r"\$\s?\d{1,3}(?:,\d{3})+")


def candidates():
    rows, off = [], 0
    while True:
        r = requests.get(f"{SB}/rest/v1/broker_sources", headers=H, timeout=90, params={
            "select": "domain,company_name,discovery_stage,last_error_type",
            "discovery_stage": "in.(0_unusable,0_blocked,1_needs_discovery,2_listings_url_known,3_crawlable)",
            "order": "domain.asc", "limit": "1000", "offset": str(off)})
        r.raise_for_status()
        b = r.json(); rows += b
        if len(b) < 1000:
            break
        off += 1000
    rows = [x for x in rows if (x.get("last_error_type") or "") not in
            ("FRANCHISE_COVERED", "DUPLICATE_LISTINGS_URL")
            and not re.search(r"bizbuysell|bizquest|businessesforsale|dealstream|loopnet", x["domain"])]
    return rows[SHARD::SHARDS][:LIMIT]


def links_in(html, base):
    out = []
    for m in re.finditer(r'<a\b[^>]*href=["\']([^"\'#]+)["\'][^>]*>(.*?)</a>', html or "", re.I | re.S):
        href = urljoin(base, m.group(1).strip())
        label = re.sub(r"<[^>]+>|\s+", " ", m.group(2)).strip()[:80]
        out.append((href, label))
    for m in re.finditer(r'<iframe\b[^>]*src=["\']([^"\']+)', html or "", re.I):
        out.append((urljoin(base, m.group(1)), "[iframe]"))
    return out


def scan(row):
    domain = row["domain"]
    res = {"domain": domain, "company_name": row.get("company_name"), "marketplaces": [],
           "marketplace_links": {}, "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    pages = []
    try:
        r = None
        for base in (f"https://{domain}", f"https://www.{domain}"):
            try:
                r = dv.get_page(base, timeout=15)
                if r.status_code == 200 and len(r.text or "") > 800:
                    break
            except Exception:
                r = None
        if r is None:
            res["note"] = "unreachable"; return res
        res["homepage_status"] = r.status_code
        res["final_url"] = getattr(r, "url", None)
        if r.status_code != 200:
            res["note"] = f"homepage HTTP {r.status_code}"; return res
        home = r.text or ""
        landed = res["final_url"] or f"https://{domain}"
        pages.append((landed, home))
        host = urlparse(landed).netloc.lower().removeprefix("www.")
        ranked = []
        for href, label in links_in(home, landed):
            if urlparse(href).netloc.lower().removeprefix("www.") != host:
                continue
            s = dv.score_link(label, urlparse(href).path)
            if s >= 3:
                ranked.append((s, href))
        for _, href in sorted(set(ranked), reverse=True)[:2]:
            try:
                p = dv.get_page(href, timeout=12)
                if p.status_code == 200:
                    pages.append((getattr(p, "url", None) or href, p.text or ""))
            except Exception:
                pass
        found = {}
        best_count, best_page = 0, None
        for url, html in pages:
            for href, label in links_in(html, url):
                for name, rx in MARKETS.items():
                    if rx.search(href):
                        found.setdefault(name, []).append({"href": href[:300], "label": label, "on": url[:200]})
            n = len(set(PRICE.findall(html)))
            if url != landed and n > best_count:
                best_count, best_page = n, url
        res["marketplaces"] = sorted(found)
        res["marketplace_links"] = {k: v[:3] for k, v in found.items()}
        res["listings_page"] = best_page
        res["page_listing_count"] = best_count
        text = re.sub(r"<[^>]+>", " ", home)
        res["phones"] = list(dict.fromkeys(PHONE.findall(text)))[:3]
        res["emails"] = [e for e in dict.fromkeys(EMAIL.findall(text))
                         if not re.search(r"\.(png|jpg|gif|webp)$|example|sentry|wixpress", e, re.I)][:3]
    except Exception as exc:
        res["note"] = f"error: {type(exc).__name__}"
    return res


COLS = ("domain", "checked_at", "homepage_status", "final_url", "marketplaces",
        "marketplace_links", "listings_page", "page_listing_count", "company_name",
        "phones", "emails", "note")


def save(rows):
    if not rows:
        return
    # PostgREST bulk upserts need the same keys on every row.
    rows = [{c: r.get(c) for c in COLS} for r in rows]
    r = requests.post(f"{SB}/rest/v1/broker_marketplace_links",
                      headers={**H, "Prefer": "resolution=merge-duplicates,return=minimal"},
                      data=json.dumps(rows, default=str), timeout=90)
    if not r.ok:
        print("save failed", r.status_code, r.text[:300])


def main():
    if not SB or not KEY:
        sys.exit("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
    rows = candidates()
    print(f"Shard {SHARD + 1}/{SHARDS}: {len(rows)} brokers")
    batch, hits = [], 0
    with ThreadPoolExecutor(WORKERS) as ex:
        for i, res in enumerate(ex.map(scan, rows), 1):
            if res["marketplaces"]:
                hits += 1
                print(f"[{i}] {res['domain']}: {', '.join(res['marketplaces'])}")
            batch.append(res)
            if len(batch) >= 50:
                save(batch); batch = []
    save(batch)
    print(f"done: {len(rows)} scanned, {hits} link to a marketplace")


if __name__ == "__main__":
    main()
