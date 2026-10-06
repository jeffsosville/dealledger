#!/usr/bin/env python3
"""
stale_check.py — visit every listing in v_stale_check_queue and record what the URL says now.

    python3 agents/stale_check.py                 # dry run, prints verdicts + per-domain summary
    python3 agents/stale_check.py --write         # updates listings_direct
    python3 agents/stale_check.py --domain companysellers.com --limit 20

Verdicts (same conventions as the Oct 4 checker):
    gone_404       404/410                                -> status inactive, delisted_at
    gone_redirect  redirected away from the listing page  -> status inactive, delisted_at
    sold_text      page says sold / under contract        -> status sold, delisted_at
    live           page still up                          -> stays active (stale_result only)
    error          403/429/5xx/timeout                    -> nothing written; retried next run

A domain that comes back mostly `live` is not dead — its crawl is truncated
(pagination or page cap). Those are listed at the end as crawl-fix candidates.
"""
import argparse, os, re, sys, time, threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from curl_cffi import requests as creq
from supabase import create_client

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scrapers"))
from dealledger_scraper_v6 import _needs_proxy, build_proxy_url  # noqa: E402

SOLD_HEAD = re.compile(r"\b(sold|under contract|sale pending|pending sale|off the market)\b", re.I)
SOLD_BODY = re.compile(r"(this (business|listing) (has been|is) (sold|no longer available|removed|under contract)"
                       r"|listing (is )?no longer available|no longer (for sale|on the market))", re.I)
PER_DOMAIN_DELAY = 1.5
_locks = defaultdict(threading.Lock)


def db():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])


def load_queue(client, domain, limit, all_active=False):
    rows, start = [], 0
    while True:
        if all_active:
            # one-off sweep of a whole domain (e.g. eatz: its WP-REST feed held
            # every sold listing as active)
            q = (client.table("listings_direct").select("id,url,broker_domain")
                 .eq("status", "active").eq("broker_domain", domain))
        else:
            q = client.table("v_stale_check_queue").select("id,url,broker_domain,reason")
        if domain and not all_active:
            q = q.eq("broker_domain", domain)
        page = q.range(start, start + 999).execute().data
        rows += page
        if len(page) < 1000 or (limit and len(rows) >= limit):
            break
        start += 1000
    return rows[:limit] if limit else rows


def _redirected_away(orig, final):
    o, f = urlparse(orig), urlparse(final)
    if o.netloc.replace("www.", "") != f.netloc.replace("www.", ""):
        return True
    op, fp = o.path.rstrip("/"), f.path.rstrip("/")
    if op == fp:
        return False
    slug = op.split("/")[-1]
    return fp == "" or fp.count("/") < op.count("/") or (slug and slug not in final)


def check(row):
    url, domain = row["url"], row["broker_domain"]
    proxy = build_proxy_url(f"stale{abs(hash(domain)) % 10_000}") if _needs_proxy(domain) else None
    with _locks[domain]:
        time.sleep(PER_DOMAIN_DELAY)
        try:
            r = creq.get(url, impersonate="chrome", timeout=25, allow_redirects=True,
                         proxies={"http": proxy, "https": proxy} if proxy else None)
        except Exception as e:
            return row, "error", type(e).__name__
    if r.status_code in (404, 410):
        return row, "gone_404", r.status_code
    if r.status_code >= 400:
        return row, "error", r.status_code
    if _redirected_away(url, str(r.url)):
        return row, "gone_redirect", str(r.url)[:80]
    soup = BeautifulSoup(r.text, "html.parser")
    head = " ".join(filter(None, [
        soup.title.get_text(" ", strip=True) if soup.title else "",
        *(h.get_text(" ", strip=True) for h in soup.find_all("h1")[:2]),
        (soup.find("meta", property="og:title") or {}).get("content", ""),
    ]))
    body = soup.get_text(" ", strip=True)[:4000]
    badge = soup.find(class_=re.compile(r"^(listing-)?status-(sold|under-contract|pending)$", re.I))
    if badge is not None:
        return row, "sold_text", f"badge:{badge.get('class')} {head[:60]}"
    if SOLD_HEAD.search(head) or SOLD_BODY.search(body):
        return row, "sold_text", head[:80]
    return row, "live", ""


def write(client, row, verdict):
    now = datetime.now(timezone.utc).isoformat()
    patch = {"stale_checked_at": now, "stale_result": verdict}
    if verdict in ("gone_404", "gone_redirect"):
        patch.update(status="inactive", delisted_at=now)
    elif verdict == "sold_text":
        patch.update(status="sold", delisted_at=now)
    client.table("listings_direct").update(patch).eq("id", row["id"]).eq("status", "active").execute()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--domain")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--all-active", action="store_true", help="check every active row on --domain, not just the unseen queue")
    ap.add_argument("--workers", type=int, default=12)
    a = ap.parse_args()

    client = db()
    if a.all_active and not a.domain:
        sys.exit("--all-active needs --domain")
    queue = load_queue(client, a.domain, a.limit, a.all_active)
    print(f"{len(queue)} URLs across {len({r['broker_domain'] for r in queue})} domains"
          + ("" if a.write else "  (dry run — add --write)"), flush=True)

    totals, by_domain = Counter(), defaultdict(Counter)
    reason = {r["broker_domain"]: r.get("reason", "all_active") for r in queue}
    with ThreadPoolExecutor(a.workers) as pool:
        for i, (row, verdict, detail) in enumerate(pool.map(check, queue), 1):
            totals[verdict] += 1
            by_domain[row["broker_domain"]][verdict] += 1
            if a.write and verdict != "error":
                try:
                    write(client, row, verdict)
                except Exception as e:
                    print(f"  write failed {row['id']}: {e}", flush=True)
            if verdict != "live":
                print(f"  {verdict:13} {row['broker_domain']:32} {detail}", flush=True)
            if i % 250 == 0:
                print(f"--- {i}/{len(queue)} {dict(totals)}", flush=True)

    print(f"\nTOTAL {dict(totals)}")
    print("\nPer domain (gone = 404+redirect+sold):")
    for d, c in sorted(by_domain.items(), key=lambda x: -sum(x[1].values())):
        gone = c["gone_404"] + c["gone_redirect"] + c["sold_text"]
        print(f"  {d:34} {reason[d]:19} n={sum(c.values()):4} gone={gone:4} live={c['live']:4} err={c['error']:4}")
    trunc = [d for d, c in by_domain.items()
             if reason[d] == "unseen_on_ok_crawl" and c["live"] >= 10 and c["live"] > sum(c.values()) / 2]
    if trunc:
        print("\nCrawl-fix candidates (crawl succeeds, but most unseen listings are still live):")
        for d in sorted(trunc, key=lambda d: -by_domain[d]["live"]):
            print(f"  {d}  live={by_domain[d]['live']}")


if __name__ == "__main__":
    main()
