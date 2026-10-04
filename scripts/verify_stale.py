#!/usr/bin/env python3
"""
verify_stale.py - confirm whether listings a broker's feed stopped returning are gone.

WHY
  Nothing ever expired a listing. When a business sells or is withdrawn, its
  row stays status='active' and public forever, so sold businesses show as
  for sale and days-on-market keeps counting. On 2026-10-04 there were 7,468
  such rows in feeds that otherwise scrape fine (vestedbb 2,102, tworld 1,426).

  But "missing from the feed" is not proof: a page-capped or flaky scraper also
  drops live listings (vestedbb at 53% missing looks like that). So this opens
  each listing and decides from the page itself.

VERDICTS (stale_result)
  gone_404       HTTP 404/410                         -> status inactive
  gone_redirect  redirected to a page that isn't it   -> status inactive
  sold_text      page says sold / no longer available -> status sold
  live           the listing page is still up         -> stays active (scraper gap)
  error          403/429/5xx/timeout                  -> unchanged, retried next run

  Gone rows get delisted_at = last_seen (when it was last really listed), which
  is the end date DOM needs. Every checked row gets stale_checked_at; 'live'
  rows come back for a recheck after 7 days via v_stale_candidates.
  If a scraper sees a gone listing again, its upsert sets it active again.

Env:
  SUPABASE_URL, SUPABASE_SERVICE_KEY   required
  LIMIT     candidates this run (default 50)
  DRY_RUN   1 = report only (default 1)
  DELAY     seconds between requests (default 1.0); domains are interleaved,
            so any one site sees far less than that
  DOMAIN    optional: only this broker_domain

  DRY_RUN=1 LIMIT=60 python3 scripts/verify_stale.py
  DRY_RUN=0 LIMIT=8000 nohup python3 scripts/verify_stale.py > verify_stale.log 2>&1 &
"""
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote

import requests

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
LIMIT = int(os.environ.get("LIMIT", "50"))
DRY_RUN = os.environ.get("DRY_RUN", "1") == "1"
DELAY = float(os.environ.get("DELAY", "1.0"))
DOMAIN = os.environ.get("DOMAIN", "").strip()

# Phrases that mean THIS listing is gone. Deliberately narrow: nav links like
# "Recently Sold" or "Sold Listings" appear on every page and must not count.
_SOLD = re.compile(
    r"this (?:business|listing|opportunity) (?:has been|is|was) (?:sold|removed|withdrawn)"
    r"|(?:listing|business|opportunity) (?:is )?no longer (?:available|active|listed|for sale)"
    r"|no longer (?:available|on the market)"
    r"|(?:listing|page) (?:has been|was) (?:removed|deleted|expired)"
    r"|listing (?:not found|has expired|is inactive)"
    r"|\bstatus:?\s*(?:sold|closed|under contract|pending)\b"
    r"|\b(?:sold|under contract|sale pending)\s*[!.]?\s*</h[1-3]",
    re.I)
_WORD = re.compile(r"[a-z0-9]+")


def sb(method, path, **kw):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
         "Content-Type": "application/json", "Prefer": "return=minimal"}
    r = requests.request(method, f"{SUPABASE_URL}/rest/v1/{path}", headers=h, timeout=90, **kw)
    r.raise_for_status()
    return r.json() if r.text else None


def candidates():
    params = {"select": "id,url,title,broker_domain,last_seen,days_missing",
              "order": "days_missing.desc", "limit": str(LIMIT)}
    if DOMAIN:
        params["broker_domain"] = f"eq.{DOMAIN}"
    rows = sb("GET", "v_stale_candidates", params=params)
    # Interleave domains so no single site gets consecutive requests.
    by = defaultdict(list)
    for r in rows:
        by[r["broker_domain"]].append(r)
    out = []
    while any(by.values()):
        for d in list(by):
            if by[d]:
                out.append(by[d].pop(0))
    return out


def _ident(url):
    """The part of a listing URL that identifies it: last path segment / id."""
    path = unquote(urlparse(url).path.rstrip("/"))
    seg = path.rsplit("/", 1)[-1].lower()
    ids = re.findall(r"\d{4,}", url)
    return seg, ids


def _same_listing(orig, final):
    if not final or orig.rstrip("/") == final.rstrip("/"):
        return True
    seg, ids = _ident(orig)
    f = unquote(final).lower()
    if ids and any(i in f for i in ids):
        return True
    return bool(seg) and len(seg) > 6 and seg[:40] in f


def _title_on_page(title, text_low):
    words = [w for w in _WORD.findall((title or "").lower()) if len(w) > 3][:6]
    if len(words) < 2:
        return True          # too short to judge; don't call it missing
    hits = sum(1 for w in words if w in text_low)
    return hits >= max(2, len(words) // 2)


def verdict(row, resp):
    if resp.status_code in (404, 410):
        return "gone_404"
    if resp.status_code != 200:
        return "error"
    final = str(getattr(resp, "url", "") or "")
    if not _same_listing(row["url"], final):
        return "gone_redirect"
    html = resp.text or ""
    if _SOLD.search(html[:400_000]):
        return "sold_text"
    text_low = re.sub(r"<[^>]+>", " ", html[:400_000]).lower()
    if not _title_on_page(row.get("title"), text_low):
        # 200 but the listing isn't on it: a soft 404 / generic search page.
        return "gone_redirect"
    return "live"


def session():
    if curl_requests:
        return curl_requests.Session(impersonate="chrome124")
    s = requests.Session()
    s.headers["User-Agent"] = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                               "AppleWebKit/537.36 Chrome/124.0 Safari/537.36")
    return s


def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        sys.exit("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
    rows = candidates()
    print(f"verify_stale  dry_run={DRY_RUN}  candidates={len(rows)}  "
          f"domains={len({r['broker_domain'] for r in rows})}", flush=True)
    s = session()
    tally = defaultdict(lambda: defaultdict(int))
    samples = defaultdict(list)
    for i, row in enumerate(rows, 1):
        try:
            resp = s.get(row["url"], timeout=30, allow_redirects=True)
            v = verdict(row, resp)
        except Exception as e:
            v = "error"
            resp = None
        dom = row["broker_domain"]
        tally[dom][v] += 1
        if len(samples[v]) < 4:
            samples[v].append(f"{dom}  {row['days_missing']}d  {row['url'][:90]}")

        if not DRY_RUN and v != "error":
            now = datetime.now(timezone.utc).isoformat()
            patch = {"stale_checked_at": now, "stale_result": v}
            if v in ("gone_404", "gone_redirect", "sold_text"):
                patch["status"] = "sold" if v == "sold_text" else "inactive"
                patch["delisted_at"] = row["last_seen"]
            try:
                sb("PATCH", "listings_direct",
                   params={"id": f"eq.{row['id']}", "status": "eq.active"}, json=patch)
            except Exception as e:
                print(f"  ! write failed {row['id']}: {e}", flush=True)

        if i % 100 == 0:
            done = defaultdict(int)
            for t in tally.values():
                for k, n in t.items():
                    done[k] += n
            print(f"[{i}/{len(rows)}] " + "  ".join(f"{k}={n}" for k, n in sorted(done.items())),
                  flush=True)
        time.sleep(DELAY)

    print("\n--- by domain (gone = 404 + redirect + sold) ---")
    for dom, t in sorted(tally.items(), key=lambda kv: -sum(kv[1].values())):
        n = sum(t.values())
        gone = t["gone_404"] + t["gone_redirect"] + t["sold_text"]
        print(f"  {dom:38} {n:5}  gone {gone:5} ({100 * gone // n:3}%)  live {t['live']:5}  "
              f"error {t['error']:4}")
    for v, lst in samples.items():
        print(f"\n{v}:")
        for line in lst:
            print("   ", line)
    print("\nDRY RUN - nothing written" if DRY_RUN else "\nWritten.")


if __name__ == "__main__":
    main()
