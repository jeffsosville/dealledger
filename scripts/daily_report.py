#!/usr/bin/env python3
"""daily_report.py — the daily scoreboard + health flags as one GitHub issue.

Runs after the bridge and homepage refresh (daily_report.yml). Calls the
Supabase function daily_health() and posts "Today — YYYY-MM-DD" with label
`today`, closing yesterday's. Flags only REPORT — nothing here changes data.

    python3 scripts/daily_report.py            # print markdown
    python3 scripts/daily_report.py --post     # create the GitHub issue (needs gh + GH_TOKEN)
"""
import argparse, json, os, subprocess, sys, tempfile
from datetime import datetime, timezone
import requests

def health():
    url, key = os.environ["SUPABASE_URL"].rstrip("/"), os.environ["SUPABASE_SERVICE_KEY"]
    r = requests.post(f"{url}/rest/v1/rpc/daily_health",
                      headers={"apikey": key, "Authorization": f"Bearer {key}",
                               "Content-Type": "application/json"}, json={}, timeout=120)
    r.raise_for_status()
    return r.json()

def count_mismatch():
    """Brokers whose active URLs differ from their own displayed count by more
    than 10%, biggest gap first (public.count_mismatch_report, 2026-11-11).
    Returns (rows, None) or (None, reason): a missing or failing RPC shows up
    in the issue as a line, not as a silently empty section (principle 17)."""
    try:
        url, key = os.environ["SUPABASE_URL"].rstrip("/"), os.environ["SUPABASE_SERVICE_KEY"]
        r = requests.post(f"{url}/rest/v1/rpc/count_mismatch_report",
                          headers={"apikey": key, "Authorization": f"Bearer {key}",
                                   "Content-Type": "application/json"},
                          json={"p_days": 2, "p_tol": 0.10}, timeout=120)
        if not r.ok:
            return None, f"HTTP {r.status_code}: {r.text[:150]}"
        return r.json(), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"

def fmt(h, mismatch=None):
    b, f = h["scoreboard"], h["flags"]
    out = [f"_Generated {h['generated_at'][:16]} UTC by daily_report.yml. Flags report only — nothing was changed._", "",
           "## Scoreboard", "",
           "| Goal | Metric | Value |", "|---|---|---|",
           f"| 1 Discovery | Producing brokers (7d) / target 1,700 | **{b['producing_brokers_7d']:,}** |",
           f"| 1 Discovery | Discovery decisions (24h) | {b['discovery_decisions_24h']} |",
           f"| 2 Fields | Price / revenue / cash flow / state (published) | {b['pct_price']}% / {b['pct_revenue']}% / {b['pct_cash_flow']}% / {b['pct_state']}% |",
           f"| 2 Listings | Active / published / live on site | {b['active']:,} / {b['published']:,} / {b['live_on_site']:,} |",
           f"| 2 Listings | New listings (24h) | {b['new_listings_24h']:,} |",
           f"| 3 Clean | Retired by stale check (24h) | {b['retired_24h']:,} |",
           f"| 3 Clean | Quarantined (24h) | {b['quarantined_24h']:,} |",
           f"| 3 Clean | Waiting in stale-check queue | {b['stale_queue']:,} |", ""]
    def section(title, why, rows, cols):
        out.append(f"## {title} ({len(rows)})")
        out.append(f"_{why}_")
        if not rows:
            out.append("None. ✅"); out.append(""); return
        out.append("| " + " | ".join(c for c, _ in cols) + " |")
        out.append("|" + "---|" * len(cols))
        for r in rows:
            out.append("| " + " | ".join(str(r.get(k, "")) for _, k in cols) + " |")
        out.append("")
    section("Truncated crawl or dead rows piling up",
            "Crawled in the last 2 days but saw under half its active listings. Either the crawler stops early (fix the scraper) or sold listings aren't being retired (stale check).",
            f["truncated_or_stale"], [("Domain", "domain"), ("Active", "active"), ("Seen 2d", "seen_2d")])
    section("Frozen brokers", "Not reached in 7+ days.",
            f["frozen"], [("Domain", "domain"), ("Active", "active"), ("Days", "days")])
    section("No financials on a whole domain",
            "90%+ of active listings have no price and no cash flow: the broker doesn't publish, or the scraper misses it.",
            f["no_financials"], [("Domain", "domain"), ("Active", "active"), ("No price/CF", "priceless")])
    section("Database job failures (24h)", "pg_cron jobs that failed (bridge, homepage refresh…).",
            f["cron_failures_24h"], [("Job", "job"), ("At", "at"), ("Message", "msg")])
    if mismatch is not None:
        rows, err = mismatch
        why = ("Active listing URLs vs the count the broker's own page shows, latest crawl in 2 days, "
               "off by more than 10%, biggest gap first. Under = we miss listings (or the crawl stops early); "
               "over = dead or duplicate rows still active. 'stated' = the page says it; 'counted' = our card count.")
        if err:
            out.append("## Our count vs the broker's count (unavailable)")
            out.append(f"_{why}_")
            out.append(f"⚠️ count_mismatch_report() failed: {err}"); out.append("")
        else:
            section("Our count vs the broker's count", why, rows,
                    [("Domain", "domain"), ("Broker shows", "displayed"), ("How", "source"),
                     ("Active URLs", "active_urls"), ("Seen last crawl", "seen_last_crawl"),
                     ("Gap", "gap"), ("%", "pct"), ("Run", "status"), ("Note", "note")])
    return "\n".join(out)

def post(body):
    repo = os.environ.get("GITHUB_REPOSITORY", "jeffsosville/dealledger")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run = lambda *a: subprocess.run(["gh", *a, "-R", repo], check=False, capture_output=True, text=True)
    run("label", "create", "today", "--color", "5319e7", "--description", "Daily scoreboard", "--force")
    old = run("issue", "list", "--label", "today", "--state", "open", "--json", "number,title")
    with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False) as fh:
        fh.write(body); path = fh.name
    r = run("issue", "create", "--title", f"Today — {today}", "--label", "today", "--body-file", path)
    print(r.stdout or r.stderr)
    if r.returncode != 0:
        sys.exit(1)
    for i in json.loads(old.stdout or "[]"):
        if i["title"] != f"Today — {today}":
            run("issue", "close", str(i["number"]), "--comment", f"Superseded by Today — {today}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--post", action="store_true")
    body = fmt(health(), count_mismatch())
    post(body) if ap.parse_args().post else print(body)
