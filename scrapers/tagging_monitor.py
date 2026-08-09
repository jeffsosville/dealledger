#!/usr/bin/env python3
"""
tagging_monitor.py — asserts the two invariants that broke silently in July.

`freshness_monitor.py` already watches broker staleness and coverage age. It
does not watch these, and neither did anything else:

  1. TAGGING  — the vertical classifier stopped writing between Jul 15-17 2026.
                Every row scraped afterwards landed with vertical = NULL, so
                every vertical marketplace read from a bucket that had stopped
                growing. Scrapers were healthy the whole time; jobs exited 0.

  2. SCHEMA   — the marketplace apps query specific columns. When the source
                shape changed, `location` and `last_verified_at` disappeared,
                search started returning 500s and a freshness stat silently
                rendered 0. Builds stayed green.

Both are "two numbers that should match" checks. Same shape as the diagnostics
that have caught every other silent freeze.

Env (matches the rest of this repo):
  SUPABASE_URL          required
  SUPABASE_SERVICE_KEY  required
  RESEND_API_KEY        optional — emails on failure if set
  ALERT_EMAIL_TO        optional — defaults to jeff@sosville.co
  ALERT_EMAIL_FROM      optional — defaults to alerts@dealledger.org
  DRY_RUN               optional — "1" to skip sending mail

Exits 1 if any check fails so the Actions run goes red.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import requests

SUPABASE_URL         = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
RESEND_API_KEY       = os.environ.get("RESEND_API_KEY", "")
ALERT_EMAIL_TO       = os.environ.get("ALERT_EMAIL_TO", "jeff@sosville.co")
ALERT_EMAIL_FROM     = os.environ.get("ALERT_EMAIL_FROM", "alerts@dealledger.org")
DRY_RUN              = os.environ.get("DRY_RUN", "") == "1"

# Columns the vertical marketplaces (VendingExits, CleaningExits, and the views
# that feed them) depend on. If one goes missing, something breaks silently.
REQUIRED_COLUMNS = [
    "id", "title", "url", "broker_name", "broker_domain", "city", "state",
    "asking_price", "cash_flow", "category", "vertical", "status",
    "quality_score", "quality_tier", "days_on_market", "profile_views",
    "first_seen", "last_seen", "true_first_seen", "url_is_listing_specific",
    "relist_count", "contact_name", "contact_phone",
]

LOOKBACK_DAYS       = 7
MIN_ROWS_TO_JUDGE   = 20     # ignore days too small to draw a conclusion from
UNTAGGED_SHARE_MAX  = 0.20   # >20% of a day's inserts untagged = failure
VERTICAL_FREEZE_DAYS = 7     # a tagged vertical going quiet this long = failure


def _headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _count(params: dict) -> int:
    """Row count via HEAD-style request reading Content-Range."""
    headers = {**_headers(), "Prefer": "count=exact",
               "Range-Unit": "items", "Range": "0-0"}
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/listings_direct",
        headers=headers, params={**params, "select": "id"}, timeout=60,
    )
    r.raise_for_status()
    content_range = r.headers.get("Content-Range", "")
    total = content_range.split("/")[-1] if "/" in content_range else "0"
    try:
        return int(total)
    except ValueError:
        return 0


# ── Check 1: tagging ────────────────────────────────────────────────────────────
def check_tagging(now: datetime) -> dict:
    """
    For each of the last N days, compare rows created against rows created with
    vertical = NULL. On Jul 18 2026 that ratio went to 1.0 and stayed there for
    three weeks with nothing noticing.
    """
    bad, detail = [], []
    for offset in range(LOOKBACK_DAYS):
        day_start = (now - timedelta(days=offset)).replace(
            hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        window = {
            "created_at": f"gte.{_iso(day_start)}",
            "and": f"(created_at.lt.{_iso(day_end)})",
        }
        inserted = _count(window)
        if inserted < MIN_ROWS_TO_JUDGE:
            continue
        untagged = _count({**window, "vertical": "is.null"})
        share = untagged / inserted
        line = f"{day_start.date()}: {untagged}/{inserted} untagged ({share:.0%})"
        detail.append(line)
        if share > UNTAGGED_SHARE_MAX:
            bad.append(line)

    return {
        "name": "Vertical tagging",
        "failed": bool(bad),
        "summary": ("classifier is writing"
                    if not bad
                    else f"{len(bad)} of the last {LOOKBACK_DAYS} days "
                         f"exceeded {UNTAGGED_SHARE_MAX:.0%} untagged"),
        "detail": detail,
    }


# ── Check 2: vertical freshness ────────────────────────────────────────────────
def check_vertical_freshness(now: datetime) -> dict:
    """
    A tagged vertical whose newest row is stale means that marketplace is
    serving a frozen snapshot — even while the platform overall looks healthy.
    """
    cutoff = _iso(now - timedelta(days=VERTICAL_FREEZE_DAYS))
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/listings_direct",
        headers=_headers(),
        params={"select": "vertical,last_seen", "vertical": "not.is.null",
                "order": "last_seen.desc", "limit": "20000"},
        timeout=120,
    )
    r.raise_for_status()
    rows = r.json()

    newest: dict[str, str] = {}
    counts: dict[str, int] = {}
    for row in rows:
        v = row.get("vertical")
        ls = row.get("last_seen") or ""
        counts[v] = counts.get(v, 0) + 1
        if v not in newest or ls > newest[v]:
            newest[v] = ls

    frozen = [f"{v} — {counts[v]} rows, newest {newest[v][:10]}"
              for v in sorted(newest) if newest[v] < cutoff]

    return {
        "name": "Vertical freshness",
        "failed": bool(frozen),
        "summary": ("all verticals fresh" if not frozen
                    else f"{len(frozen)} verticals frozen "
                         f"{VERTICAL_FREEZE_DAYS}+ days"),
        "detail": frozen,
    }


# ── Check 3: schema ────────────────────────────────────────────────────────────
def check_schema() -> dict:
    """
    Ask PostgREST for one row and read the keys. Cheaper than information_schema
    and it tests exactly what the apps see through the API.
    """
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/listings_direct",
        headers=_headers(), params={"select": "*", "limit": "1"}, timeout=60,
    )
    r.raise_for_status()
    rows = r.json()
    if not rows:
        return {"name": "Schema", "failed": True,
                "summary": "listings_direct returned no rows", "detail": []}

    present = set(rows[0].keys())
    missing = [c for c in REQUIRED_COLUMNS if c not in present]
    return {
        "name": "Schema",
        "failed": bool(missing),
        "summary": ("all required columns present" if not missing
                    else f"{len(missing)} required columns missing"),
        "detail": missing,
    }


# ── Reporting ──────────────────────────────────────────────────────────────────
def render(results, now: datetime) -> str:
    out = [f"DealLedger tagging + schema monitor — "
           f"{now.strftime('%Y-%m-%d %H:%M UTC')}", ""]
    for res in results:
        mark = "FAIL" if res["failed"] else "ok"
        out.append(f"[{mark}] {res['name']}: {res['summary']}")
        for line in res["detail"][:25]:
            out.append(f"      {line}")
        out.append("")
    return "\n".join(out)


def send_alert(subject: str, text: str) -> bool:
    if DRY_RUN or not RESEND_API_KEY:
        print(f"(not sending: dry_run={DRY_RUN}, key_set={bool(RESEND_API_KEY)})")
        return False
    r = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {RESEND_API_KEY}",
                 "Content-Type": "application/json"},
        json={"from": ALERT_EMAIL_FROM, "to": [ALERT_EMAIL_TO],
              "subject": subject, "text": text},
        timeout=30,
    )
    if r.status_code >= 300:
        print(f"Resend error {r.status_code}: {r.text[:300]}")
        return False
    return True


def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY")
        sys.exit(2)

    now = datetime.now(timezone.utc)
    results = []
    for fn, args in ((check_schema, ()),
                     (check_tagging, (now,)),
                     (check_vertical_freshness, (now,))):
        try:
            results.append(fn(*args))
        except Exception as exc:
            results.append({"name": fn.__name__, "failed": True,
                            "summary": f"check errored: {exc}", "detail": []})

    body = render(results, now)
    print(body)

    failed = any(r["failed"] for r in results)
    if failed:
        send_alert("DealLedger monitor: tagging or schema needs attention", body)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
