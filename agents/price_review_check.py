#!/usr/bin/env python3
"""
price_review_check.py — evidence for the price detector v7 validation sample.

Read-only. For every pending crawl price change (public.price_review_queue(),
sql/2026-11-09_price_review_queue.sql), fetch the listing's page on the
broker's own site and record what it shows now, so each change can be judged
correct or wrong against the broker's page. Writes one CSV; changes nothing in
the database. Decisions are applied separately, with Jeff's approval.

    python3 agents/price_review_check.py                     # every pending crawl change
    python3 agents/price_review_check.py --auto-eligible     # only the validation sample
    python3 agents/price_review_check.py --out ~/Desktop/price_review.csv

Per row it reports:
    http, final_url        what the page returned
    page_title             <title>/<h1>, to confirm it is the same listing
    next_price_on_page     the new price appears on the page (formatted any common way)
    prev_price_on_page     the old price still appears (a sign of a misread field)
    other_amounts          the other dollar amounts on the page, largest first
    sold_label             the page carries a sold / under-contract label
    check                  live_matches | live_other_price | gone | sold_label | error
                           (a first reading; the reviewer decides)

Same fetch rules as agents/stale_check.py (curl_cffi, proxy only for domains
that need it, one request per domain every 1.5 s). CAPTCHA pages are reported
as error, never worked around.
"""
import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup
from curl_cffi import requests as creq
from supabase import create_client

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scrapers"))
sys.path.insert(0, str(ROOT / "agents"))
from dealledger_scraper_v6 import _needs_proxy, build_proxy_url  # noqa: E402
from stale_check import DOMAIN_SOLD, SOLD_BODY, SOLD_HEAD, _redirected_away  # noqa: E402

DELAY = 1.5
MONEY = re.compile(r"\$\s?((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)\s*([kKmM](?:il(?:lion)?)?\b)?")
CAPTCHA = re.compile(r"(captcha|cf-challenge|are you a human|verify you are human|sgcaptcha)", re.I)

FIELDS = [
    "listing_id", "broker_domain", "url", "auto_eligible", "screen_result", "screen_reasons",
    "prev_price", "next_price", "pct_change", "prev_last_seen", "next_first_seen",
    "prev_source", "next_source", "db_title", "db_cash_flow", "db_revenue",
    "http", "final_url", "page_title", "next_price_on_page", "prev_price_on_page",
    "other_amounts", "sold_label", "check", "checked_at",
]


def amounts(text):
    out = set()
    for num, unit in MONEY.findall(text):
        v = float(num.replace(",", ""))
        u = (unit or "").lower()
        if u.startswith("m"):
            v *= 1_000_000
        elif u.startswith("k"):
            v *= 1_000
        out.add(int(round(v)))
    return out


def fetch(url, domain):
    proxy = build_proxy_url(f"price{abs(hash(domain)) % 10_000}") if _needs_proxy(domain) else None
    return creq.get(url, impersonate="chrome", timeout=25, allow_redirects=True,
                    proxies={"http": proxy, "https": proxy} if proxy else None)


def check_row(c):
    out = {
        "listing_id": c["listing_id"], "broker_domain": c["broker_domain"], "url": c["url"],
        "auto_eligible": c["auto_eligible"], "screen_result": c["screen_result"],
        "screen_reasons": c["screen_reasons"], "prev_price": c["prev_price"], "next_price": c["next_price"],
        "pct_change": c["pct_change"], "prev_last_seen": c["prev_last_seen"],
        "next_first_seen": c["next_first_seen"], "prev_source": c["prev_source"],
        "next_source": c["next_source"], "db_title": (c.get("title") or "")[:160],
        "db_cash_flow": c.get("cash_flow"), "db_revenue": c.get("revenue"),
        "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    try:
        r = fetch(c["url"], c["broker_domain"])
    except Exception as e:  # noqa: BLE001
        return {**out, "http": "", "check": f"error:{type(e).__name__}"}
    out["http"], out["final_url"] = r.status_code, str(r.url)
    if r.status_code in (404, 410) or _redirected_away(c["url"], str(r.url)):
        return {**out, "check": "gone"}
    if r.status_code >= 400 or CAPTCHA.search(r.text[:20000]):
        return {**out, "check": "error:blocked_or_captcha" if r.status_code < 500 else f"error:{r.status_code}"}

    soup = BeautifulSoup(r.text, "html.parser")
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    head = " ".join(filter(None, [
        soup.title.get_text(" ", strip=True) if soup.title else "",
        *(h.get_text(" ", strip=True) for h in soup.find_all("h1")[:2]),
    ]))
    main = soup.find("main") or soup.find(attrs={"role": "main"}) or soup.body or soup
    text = main.get_text(" ", strip=True)
    seen = amounts(text)
    nxt, prv = int(float(c["next_price"])), int(float(c["prev_price"]))
    out["page_title"] = head[:160]
    out["next_price_on_page"] = nxt in seen
    out["prev_price_on_page"] = prv in seen
    out["other_amounts"] = " ".join(f"{v:,}" for v in sorted(seen - {nxt, prv}, reverse=True)[:8])
    dom_rule = DOMAIN_SOLD.get(re.sub(r"^www\.", "", (c["broker_domain"] or "").lower()))
    sold = bool((dom_rule and dom_rule.search(text)) or SOLD_HEAD.search(head) or SOLD_BODY.search(text[:4000]))
    out["sold_label"] = sold
    if sold:
        out["check"] = "sold_label"
    elif out["next_price_on_page"]:
        out["check"] = "live_matches"
    else:
        out["check"] = "live_other_price"
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--auto-eligible", action="store_true", help="only changes tagged auto-eligible:v7")
    ap.add_argument("--out", default=f"price_review_{datetime.now().strftime('%Y-%m-%d')}.csv")
    args = ap.parse_args()

    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    queue = sb.rpc("price_review_queue", {"p_only_auto_eligible": args.auto_eligible}).execute().data or []
    print(f"{len(queue)} pending crawl changes{' (auto-eligible only)' if args.auto_eligible else ''}")

    last_hit = {}
    rows = []
    for c in queue:
        dom = c["broker_domain"]
        wait = DELAY - (time.time() - last_hit.get(dom, 0))
        if wait > 0:
            time.sleep(wait)
        row = check_row(c)
        last_hit[dom] = time.time()
        rows.append(row)
        print(f"  {row['check']:18} {dom:40} {c['prev_price']} -> {c['next_price']}")

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
