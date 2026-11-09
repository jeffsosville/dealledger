#!/usr/bin/env python3
"""
held_audit.py — settle the removal rule's held listings without hand counts.

The removal rule holds a broker when more than 30% of its active listings went
missing from three complete crawls: usually the crawl is short, not the broker.
For every held listing this agent does what a person checking the site does:

  1. Opens the listing's own page (agents/stale_check.py rules).
       404 / redirect away        -> gone_404 / gone_redirect   (inactive)
       broker's own sold label    -> sold_text                  (sold)
  2. Opens the broker's listings page in a real browser, scrolls, presses
     "load more", and follows links up to two clicks deep. Every link found is
     the site's inventory.
       page live and linked       -> observed  (dated observation, source page_visit)
       page live, not linked, and the inventory covers >=80% of what the
       crawl currently sees       -> unlisted  (left observation; restored if seen again)
       anything less certain      -> unresolved (nothing written, listed as an exception)
  3. Reads the sitemap only to find listing-shaped URLs we don't have yet.
     Sitemaps keep sold pages, so a sitemap entry never counts as "still listed".

    python3 agents/held_audit.py --domain dbi-businessadvisors.com        # dry run, one broker
    python3 agents/held_audit.py --domain dbi-businessadvisors.com --write
    python3 agents/held_audit.py --write                                  # every held broker

A captcha or challenge page stops that broker and is reported, never worked around.
"""
import argparse, json, os, re, sys, time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse, urljoin, unquote, parse_qsl, urlencode
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parent))
from stale_check import check as page_check, db  # noqa: E402
from dealledger_scraper_v6 import _needs_proxy, build_proxy_url  # noqa: E402  (path set by stale_check)
from curl_cffi import requests as creq  # noqa: E402

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
COVERAGE_MIN = 0.80
MAX_PAGES = 60          # pages opened per broker while collecting links
MAX_DEPTH = 2           # listings page -> category / page 2 -> listing
FOLLOW_HINT = re.compile(r"(listing|business|for-?sale|opportunit|available|inventory|categor|industr|"
                         r"search|portfolio|deal|propert|page|offer|buy)", re.I)
SKIP_EXT = re.compile(r"\.(jpg|jpeg|png|gif|webp|svg|pdf|docx?|xlsx?|zip|mp4|mp3|css|js|ico)(\?|$)", re.I)
SKIP_PATH = re.compile(r"/(wp-admin|wp-login|cart|checkout|account|login|signin|register|privacy|terms|"
                       r"cookie|feed|tag|author|contact|about|team|blog|news|careers|sell|valuation)(/|$)", re.I)
CHALLENGE = re.compile(r"(captcha|cf-challenge|just a moment\.\.\.|verify you are human|attention required)", re.I)
MORE_BUTTON = re.compile(r"^\s*(load|show|view|see)\s+more", re.I)


def norm(u):
    """Comparable form of a URL: host without www, decoded lower-case path, no trailing slash."""
    p = urlparse((u or "").strip())
    host = re.sub(r"^www\.", "", (p.hostname or "").lower())
    path = re.sub(r"/+$", "", unquote(p.path)).lower() or "/"
    q = [(k, v) for k, v in parse_qsl(p.query) if not re.match(r"(utm_|fbclid|gclid|_ga)", k, re.I)]
    return host + path + ("?" + urlencode(sorted(q)) if q else "")


def bare(d):
    return re.sub(r"^www\.", "", (d or "").lower())


def template(u):
    """Path shape of a listing URL: parent path + depth (e.g. ('/businesses', 2))."""
    segs = [s for s in unquote(urlparse(u).path).lower().split("/") if s]
    return ("/" + "/".join(segs[:-1]), len(segs)) if segs else None


# ---------------------------------------------------------------- inventory

def pw_proxy(domain):
    if not _needs_proxy(domain):
        return None
    p = urlparse(build_proxy_url(f"held{abs(hash(domain)) % 10_000}") or "")
    return {"server": f"http://{p.hostname}:{p.port}", "username": p.username, "password": p.password} if p.hostname else None


def site_links(start_url, host, known_listing_norms, log):
    """Every same-site link reachable from the listings page within MAX_DEPTH clicks."""
    from playwright.sync_api import sync_playwright
    links, seen, queue, pages = set(), set(), [(start_url, 0)], 0
    with sync_playwright() as pw:
        browser = pw.chromium.launch(proxy=pw_proxy(host))
        ctx = browser.new_context(user_agent=UA, ignore_https_errors=True)
        page = ctx.new_page()
        try:
            while queue and pages < MAX_PAGES:
                url, depth = queue.pop(0)
                n = norm(url)
                if n in seen:
                    continue
                seen.add(n)
                try:
                    resp = page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(1500)
                except Exception as e:
                    log(f"    open failed {url[:80]}: {type(e).__name__}")
                    continue
                pages += 1
                html = page.content()
                if CHALLENGE.search(html[:20000]) and (resp is None or resp.status in (403, 429, 503) or len(html) < 30000):
                    if depth == 0:
                        raise RuntimeError("challenge/captcha on listings page — manual check")
                    continue
                for _ in range(8):                        # lazy load + "load more"
                    page.mouse.wheel(0, 20000)
                    page.wait_for_timeout(600)
                    btn = page.get_by_role("button", name=MORE_BUTTON).first
                    try:
                        if btn.count() and btn.is_visible():
                            btn.click(timeout=3000)
                            page.wait_for_timeout(1500)
                            continue
                    except Exception:
                        pass
                    break
                hrefs = page.eval_on_selector_all(
                    "a[href], [data-href], [data-url]",
                    "els => els.map(e => e.href || e.getAttribute('data-href') || e.getAttribute('data-url'))")
                for h in hrefs:
                    if not h or h.startswith(("mailto:", "tel:", "javascript:")):
                        continue
                    absu = urljoin(url, h).split("#")[0]
                    if bare(urlparse(absu).hostname) != host or SKIP_EXT.search(absu):
                        continue
                    na = norm(absu)
                    links.add(na)
                    if (depth < MAX_DEPTH and na not in seen and na not in known_listing_norms
                            and not SKIP_PATH.search(urlparse(absu).path)
                            and (FOLLOW_HINT.search(absu) or urlparse(absu).path.startswith(urlparse(start_url).path.rstrip("/") + "/")
                                 or "page" in urlparse(absu).query.lower())):
                        queue.append((absu, depth + 1))
        finally:
            browser.close()
    return links, pages, not any(norm(u) not in seen for u, _ in queue)


def sitemap_urls(host, log, cap_docs=40):
    """URLs listed in the site's sitemaps (used only to find listings we don't have)."""
    proxy = build_proxy_url(f"held{abs(hash(host)) % 10_000}") if _needs_proxy(host) else None
    kw = {"impersonate": "chrome", "timeout": 20, "proxies": {"http": proxy, "https": proxy} if proxy else None}
    docs, out, done = [], set(), set()
    try:
        r = creq.get(f"https://{host}/robots.txt", **kw)
        docs += re.findall(r"(?im)^\s*sitemap:\s*(\S+)", r.text if r.status_code == 200 else "")
    except Exception:
        pass
    docs += [f"https://{host}/sitemap.xml", f"https://{host}/sitemap_index.xml", f"https://{host}/wp-sitemap.xml"]
    while docs and len(done) < cap_docs:
        d = docs.pop(0)
        if d in done:
            continue
        done.add(d)
        try:
            r = creq.get(d, **kw)
            if r.status_code != 200 or "<" not in r.text[:200]:
                continue
            root = ET.fromstring(r.content)
        except Exception:
            continue
        for loc in root.iter():
            if not loc.tag.endswith("loc") or not loc.text:
                continue
            u = loc.text.strip()
            if re.search(r"sitemap[^/]*\.xml", u, re.I):
                docs.append(u)
            elif bare(urlparse(u).hostname) == host:
                out.add(u)
    return out


# ---------------------------------------------------------------- per broker

def known_rows(client, host):
    rows, start = [], 0
    while True:
        page = (client.table("listings_direct").select("url,status,last_seen")
                .in_("broker_domain", [host, "www." + host]).range(start, start + 999).execute().data)
        rows += page
        if len(page) < 1000:
            return rows
        start += 1000


def audit_broker(client, host, held, log):
    listing_url = next((r["listing_url"] for r in held if r.get("listing_url")), None) or f"https://{host}/"
    rows = known_rows(client, host)
    recent_cut = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 7 * 86400))
    recent = {norm(r["url"]) for r in rows if r["status"] == "active" and (r["last_seen"] or "") >= recent_cut}
    all_known = {norm(r["url"]) for r in rows}
    tpl = Counter(template(r["url"]) for r in rows if r["url"])
    listing_tpls = {t for t, n in tpl.items() if t and n >= 3}
    s = {"broker_domain": host, "listing_url": listing_url, "held": len(held), "crawl_recent": len(recent),
         "site_links": 0, "coverage": None, "observed": 0, "gone": 0, "unlisted": 0, "unresolved": 0,
         "new_urls": 0, "new_url_sample": [], "exception": ""}
    items = []

    # 1. each held listing's own page
    with ThreadPoolExecutor(4) as pool:
        checked = list(pool.map(page_check, [{"url": r["url"], "broker_domain": r["broker_domain"], "id": r["listing_id"]}
                                             for r in held]))

    # 2. the site's own links
    try:
        links, npages, exhausted = site_links(listing_url, host, all_known, log)
    except Exception as e:
        links, npages, exhausted, s["exception"] = set(), 0, False, str(e)[:200]
    s["site_links"] = len(links)
    cov = (len(recent & links) / len(recent)) if recent else None
    s["coverage"] = round(cov, 3) if cov is not None else None
    shaped = {u for u in links if template("https://" + u) in listing_tpls}
    # "Not linked" only counts when the link walk is demonstrably complete: it reached
    # >=80% of what the crawl sees, found at least as many listing-shaped links as the
    # crawl does, and ran out of pages to open (didn't stop at the page cap).
    trusted = cov is not None and cov >= COVERAGE_MIN and len(shaped) >= len(recent) and exhausted
    if not s["exception"] and not trusted:
        why = []
        if cov is None:
            why.append("no recent crawl to compare against")
        elif cov < COVERAGE_MIN:
            why.append(f"site links cover {cov:.0%} of the current crawl ({len(recent & links)}/{len(recent)})")
        if cov is not None and len(shaped) < len(recent):
            why.append(f"{len(shaped)} listing-shaped links vs {len(recent)} crawled")
        if not exhausted:
            why.append(f"stopped at the {MAX_PAGES}-page cap")
        s["exception"] = "; ".join(why) + " — live, unlinked pages left for review"

    for row, verdict, detail in checked:
        if verdict in ("gone_404", "gone_redirect", "sold_text"):
            items.append({"id": row["id"], "broker_domain": row["broker_domain"], "verdict": verdict, "detail": str(detail)})
            s["gone"] += 1
        elif verdict == "live" and norm(row["url"]) in links:
            items.append({"id": row["id"], "broker_domain": row["broker_domain"], "verdict": "observed", "detail": "linked from site"})
            s["observed"] += 1
        elif verdict == "live" and trusted:
            items.append({"id": row["id"], "broker_domain": row["broker_domain"], "verdict": "unlisted",
                          "detail": f"page live, not linked; site links cover {cov:.0%} of crawl"})
            s["unlisted"] += 1
        else:
            s["unresolved"] += 1

    # 3. listing-shaped URLs we've never stored (site links + sitemap)
    if listing_tpls:
        cand = {u for u in links} | {norm(u) for u in sitemap_urls(host, log)}
        new = sorted(u for u in cand if u not in all_known and template("https://" + u) in listing_tpls)
        s["new_urls"], s["new_url_sample"] = len(new), ["https://" + u for u in new[:25]]
    return s, items


# ---------------------------------------------------------------- main

def load_queue(client, domain):
    rows, off = [], 0
    while True:
        page = client.rpc("held_audit_queue", {"p_domain": domain, "p_offset": off, "p_limit": 1000}).execute().data
        rows += page
        if len(page) < 1000:
            return rows
        off += 1000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--domain")
    ap.add_argument("--json", help="also write the per-broker summary here")
    a = ap.parse_args()

    client = db()
    queue = load_queue(client, a.domain)
    by_host = defaultdict(list)
    for r in queue:
        by_host[bare(r["broker_domain"])].append(r)
    print(f"{len(queue)} held listings across {len(by_host)} brokers" + ("" if a.write else "  (dry run — add --write)"), flush=True)

    summaries, all_items = [], []
    for host, held in sorted(by_host.items(), key=lambda x: -len(x[1])):
        t0 = time.time()
        s, items = audit_broker(client, host, held, lambda m: print(m, flush=True))
        summaries.append(s)
        all_items += items
        cov = f"{s['coverage']:.0%}" if s["coverage"] is not None else "—"
        print(f"  {host:34} held={s['held']:4} observed={s['observed']:4} gone={s['gone']:3} "
              f"unlisted={s['unlisted']:3} unresolved={s['unresolved']:3} links={s['site_links']:5} "
              f"coverage={cov:>4} new={s['new_urls']:3}  {time.time() - t0:4.0f}s"
              + (f"\n      ! {s['exception']}" if s["exception"] else ""), flush=True)
        if s["observed"]:
            print(f"      crawl misses {s['observed']} listings the site links to — crawler fix", flush=True)
        for u in s["new_url_sample"][:5]:
            print(f"      + new listing? {u}", flush=True)

    tot = Counter()
    for s in summaries:
        tot.update({k: s[k] for k in ("held", "observed", "gone", "unlisted", "unresolved", "new_urls")})
    print(f"\nTOTAL {dict(tot)}")
    exc = [s for s in summaries if s["exception"]]
    if exc:
        print("\nExceptions (nothing written for their unresolved listings):")
        for s in exc:
            print(f"  {s['broker_domain']:34} {s['exception']}")
    if a.json:
        Path(a.json).write_text(json.dumps(summaries, indent=1))
    if a.write and (all_items or summaries):
        res = client.rpc("apply_held_audit", {"p_items": all_items, "p_brokers": summaries}).execute().data
        print(f"\nWritten: {res}")


if __name__ == "__main__":
    main()
