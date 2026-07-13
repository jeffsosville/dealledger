#!/usr/bin/env python3
"""
freshness_monitor.py

Freshness monitor for DealLedger's broker-direct listing coverage.

Queries the `listings_direct` table in DealLedger Supabase and reports on
data staleness. Sends an email alert (via Resend) when coverage looks stale.

WHAT IT COMPUTES (all from listings_direct):
  (a) stale_over_week   — count of active listings with last_seen older than 7 days
  (b) stale_hv_brokers  — high-value brokers (>=20 listings) whose most recent
                          listing (max last_seen) is older than 7 days
  (c) stale_franchise   — national-brand franchise brokers whose most recent
                          listing is older than 2 days

ALERT LOGIC:
  Trip if stale_over_week > 20000  OR  any franchise broker is stale > 2 days.
  When tripped, send an email via Resend.

RESEND / DRY-RUN:
  Reads RESEND_API_KEY from env. If unset (or --dry-run), the email body is
  printed to stdout and no request is made. from/to overridable via
  DL_ALERT_FROM / DL_ALERT_TO env vars.

EXIT CODE:
  0 normally. With --fail-on-alert, exits 1 when the alert trips (for CI).

Usage:
    python3 scrapers/freshness_monitor.py
    python3 scrapers/freshness_monitor.py --dry-run
    python3 scrapers/freshness_monitor.py --fail-on-alert
"""

import os
import sys
import argparse
from datetime import datetime, timedelta, timezone

import requests

# ── Config ─────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
ALERT_FROM = os.environ.get("DL_ALERT_FROM", "alerts@dealledger.org")
ALERT_TO = os.environ.get("DL_ALERT_TO", "jeff@dealledger.org")

# Alert thresholds
STALE_WEEK_THRESHOLD = 20000        # trip if stale_over_week exceeds this
WEEK_DAYS = 7                       # "stale over a week" window
FRANCHISE_STALE_DAYS = 2            # franchise brokers must refresh every 2 days
HIGH_VALUE_MIN_LISTINGS = 20        # broker is "high value" at >= this many listings

# "Active-ish" statuses. listings_direct uses status='active' on upsert; we also
# tolerate other common active markers just in case.
ACTIVE_STATUSES = ["active", "Active", "live", "for_sale"]

# National-brand franchise broker patterns (matched case-insensitively against
# broker_name OR broker_domain).
FRANCHISE_PATTERNS = {
    "Sunbelt": ["sunbelt"],
    "Transworld": ["transworld"],
    "FCBB / First Choice Business Brokers": ["fcbb", "first choice", "firstchoice"],
    "Murphy": ["murphy"],
    "VR / VR Business Brokers": ["vr business", "vrbb", "vrbusiness"],
    "We Sell Restaurants": ["we sell restaurants", "wesellrestaurants"],
}


def _headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _status_filter() -> str:
    """PostgREST `in.(...)` filter for active-ish statuses."""
    quoted = ",".join(f'"{s}"' for s in ACTIVE_STATUSES)
    return f"in.({quoted})"


# ── Supabase queries ────────────────────────────────────────────────────────────
def count_stale_over_week(now: datetime) -> int:
    """
    Count active listings whose last_seen is older than 7 days.
    Uses a HEAD request with Prefer: count=exact and reads Content-Range.
    """
    cutoff = _iso(now - timedelta(days=WEEK_DAYS))
    params = {
        "select": "id",
        "status": _status_filter(),
        "last_seen": f"lt.{cutoff}",
    }
    headers = {**_headers(), "Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0"}
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/listings_direct",
        headers=headers,
        params=params,
        timeout=60,
    )
    r.raise_for_status()
    # Content-Range looks like "0-0/12345" (or "*/0" when empty)
    content_range = r.headers.get("Content-Range", "")
    total = content_range.split("/")[-1] if "/" in content_range else "0"
    try:
        return int(total)
    except ValueError:
        return 0


def fetch_all_direct_rows() -> list[dict]:
    """
    Page through listings_direct pulling only the fields we need to compute
    per-broker staleness. Keeps payloads small (id/name/domain/status/last_seen).
    """
    rows: list[dict] = []
    page_size = 1000
    offset = 0
    base_params = {
        "select": "broker_name,broker_domain,status,last_seen",
        "order": "id.asc",
    }
    while True:
        headers = {**_headers(), "Range-Unit": "items", "Range": f"{offset}-{offset + page_size - 1}"}
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/listings_direct",
            headers=headers,
            params=base_params,
            timeout=120,
        )
        r.raise_for_status()
        batch = r.json()
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


# ── Analysis ────────────────────────────────────────────────────────────────────
def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    s = str(value)
    # Normalize trailing Z to +00:00 for fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # Fall back to date-only or space-separated timestamps
        try:
            dt = datetime.fromisoformat(s.replace(" ", "T"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _is_active(status) -> bool:
    return str(status or "").strip().lower() in {s.lower() for s in ACTIVE_STATUSES}


def _match_franchise(broker_name: str, broker_domain: str) -> str | None:
    """Return the franchise label if broker_name/domain matches a known brand."""
    hay = f"{broker_name or ''} {broker_domain or ''}".lower()
    for label, needles in FRANCHISE_PATTERNS.items():
        if any(n in hay for n in needles):
            return label
    return None


def analyze(rows: list[dict], now: datetime) -> dict:
    """
    Roll up per-broker stats:
      - listing count (active only)
      - max(last_seen) across that broker's active listings
    Then derive high-value-stale and franchise-stale sets.
    """
    week_cutoff = now - timedelta(days=WEEK_DAYS)
    franchise_cutoff = now - timedelta(days=FRANCHISE_STALE_DAYS)

    brokers: dict[str, dict] = {}
    for row in rows:
        if not _is_active(row.get("status")):
            continue
        name = (row.get("broker_name") or "Unknown").strip() or "Unknown"
        domain = (row.get("broker_domain") or "").strip()
        key = name.lower() + "|" + domain.lower()
        last_seen = _parse_dt(row.get("last_seen"))

        b = brokers.get(key)
        if b is None:
            b = {
                "broker_name": name,
                "broker_domain": domain,
                "count": 0,
                "max_last_seen": None,
                "franchise": _match_franchise(name, domain),
            }
            brokers[key] = b
        b["count"] += 1
        if last_seen is not None:
            if b["max_last_seen"] is None or last_seen > b["max_last_seen"]:
                b["max_last_seen"] = last_seen

    # (b) High-value brokers (>=20 listings) that are stale over a week.
    stale_hv = []
    for b in brokers.values():
        if b["count"] >= HIGH_VALUE_MIN_LISTINGS:
            mls = b["max_last_seen"]
            if mls is None or mls < week_cutoff:
                stale_hv.append(b)
    stale_hv.sort(key=lambda x: x["count"], reverse=True)

    # (c) Franchise brokers stale over 2 days.
    stale_franchise = []
    for b in brokers.values():
        if not b["franchise"]:
            continue
        mls = b["max_last_seen"]
        if mls is None or mls < franchise_cutoff:
            stale_franchise.append(b)
    stale_franchise.sort(key=lambda x: (x["franchise"], -x["count"]))

    return {
        "brokers": brokers,
        "stale_hv": stale_hv,
        "stale_franchise": stale_franchise,
    }


def _fmt_age(max_last_seen: datetime | None, now: datetime) -> str:
    if max_last_seen is None:
        return "never seen"
    days = (now - max_last_seen).total_seconds() / 86400.0
    return f"{days:.1f}d ago"


# ── Reporting ───────────────────────────────────────────────────────────────────
def build_summary(stale_over_week: int, analysis: dict, now: datetime, tripped: bool) -> str:
    lines = []
    lines.append("=" * 66)
    lines.append("DealLedger Freshness Monitor")
    lines.append(f"Run: {now.isoformat()}")
    lines.append("=" * 66)
    lines.append("")
    lines.append(f"(a) Active listings stale > {WEEK_DAYS}d : {stale_over_week:,}"
                 f"   (alert threshold {STALE_WEEK_THRESHOLD:,})")
    lines.append("")

    stale_hv = analysis["stale_hv"]
    lines.append(f"(b) High-value brokers (>={HIGH_VALUE_MIN_LISTINGS} listings) stale > {WEEK_DAYS}d: {len(stale_hv)}")
    if stale_hv:
        lines.append(f"    {'BROKER':<42}{'#':>6}  LAST SEEN")
        lines.append(f"    {'-'*42}{'-'*6}  {'-'*12}")
        for b in stale_hv[:50]:
            lines.append(f"    {b['broker_name'][:40]:<42}{b['count']:>6}  {_fmt_age(b['max_last_seen'], now)}")
        if len(stale_hv) > 50:
            lines.append(f"    ... and {len(stale_hv) - 50} more")
    else:
        lines.append("    (none)")
    lines.append("")

    stale_fr = analysis["stale_franchise"]
    lines.append(f"(c) Franchise brokers stale > {FRANCHISE_STALE_DAYS}d: {len(stale_fr)}")
    lines.append(f"    {'FRANCHISE':<38}{'BROKER':<28}{'#':>5}  LAST SEEN")
    lines.append(f"    {'-'*38}{'-'*28}{'-'*5}  {'-'*12}")
    if stale_fr:
        for b in stale_fr:
            lines.append(
                f"    {b['franchise'][:36]:<38}{b['broker_name'][:26]:<28}"
                f"{b['count']:>5}  {_fmt_age(b['max_last_seen'], now)}"
            )
    else:
        lines.append("    (none — all franchise brokers fresh)")
    lines.append("")
    lines.append("=" * 66)
    lines.append(f"ALERT: {'TRIPPED' if tripped else 'ok — not tripped'}")
    lines.append("=" * 66)
    return "\n".join(lines)


def build_email_html(stale_over_week: int, analysis: dict, now: datetime) -> str:
    stale_hv = analysis["stale_hv"]
    stale_fr = analysis["stale_franchise"]

    def rows_hv():
        if not stale_hv:
            return "<tr><td colspan='3'>(none)</td></tr>"
        out = []
        for b in stale_hv[:50]:
            out.append(
                f"<tr><td>{_esc(b['broker_name'])}</td>"
                f"<td style='text-align:right'>{b['count']}</td>"
                f"<td>{_fmt_age(b['max_last_seen'], now)}</td></tr>"
            )
        return "".join(out)

    def rows_fr():
        if not stale_fr:
            return "<tr><td colspan='4'>(none — all fresh)</td></tr>"
        out = []
        for b in stale_fr:
            out.append(
                f"<tr><td>{_esc(b['franchise'])}</td>"
                f"<td>{_esc(b['broker_name'])}</td>"
                f"<td style='text-align:right'>{b['count']}</td>"
                f"<td>{_fmt_age(b['max_last_seen'], now)}</td></tr>"
            )
        return "".join(out)

    return f"""\
<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;color:#111">
  <h2>DealLedger Freshness Alert</h2>
  <p>Run: {now.isoformat()}</p>
  <p><strong>Active listings stale &gt; {WEEK_DAYS} days:</strong>
     {stale_over_week:,} (threshold {STALE_WEEK_THRESHOLD:,})</p>

  <h3>High-value brokers (&ge;{HIGH_VALUE_MIN_LISTINGS} listings) stale &gt; {WEEK_DAYS}d — {len(stale_hv)}</h3>
  <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse">
    <tr><th>Broker</th><th>Listings</th><th>Last seen</th></tr>
    {rows_hv()}
  </table>

  <h3>Franchise brokers stale &gt; {FRANCHISE_STALE_DAYS}d — {len(stale_fr)}</h3>
  <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse">
    <tr><th>Franchise</th><th>Broker</th><th>Listings</th><th>Last seen</th></tr>
    {rows_fr()}
  </table>
</div>"""


def _esc(s: str) -> str:
    return (str(s or "")
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


# ── Email ───────────────────────────────────────────────────────────────────────
def send_alert(subject: str, html: str, text: str, dry_run: bool) -> bool:
    """
    Send the alert via Resend. Returns True if actually sent, False for dry-run.
    """
    if dry_run or not RESEND_API_KEY:
        reason = "--dry-run" if dry_run else "RESEND_API_KEY unset"
        print("\n" + "#" * 66)
        print(f"# DRY-RUN ({reason}) — email NOT sent. Body below:")
        print("#" * 66)
        print(f"From:    {ALERT_FROM}")
        print(f"To:      {ALERT_TO}")
        print(f"Subject: {subject}")
        print("-" * 66)
        print(text)
        print("#" * 66)
        return False

    payload = {
        "from": ALERT_FROM,
        "to": [ALERT_TO],
        "subject": subject,
        "html": html,
        "text": text,
    }
    r = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    if r.status_code in (200, 201):
        print(f"[resend] Alert email sent to {ALERT_TO} (id={r.json().get('id','?')})")
        return True
    print(f"[resend] ERROR {r.status_code}: {r.text[:300]}", file=sys.stderr)
    return False


# ── Main ────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="DealLedger freshness monitor")
    parser.add_argument("--dry-run", action="store_true",
                        help="Never send email; print the body instead.")
    parser.add_argument("--fail-on-alert", action="store_true",
                        help="Exit 1 when the alert trips (for CI).")
    args = parser.parse_args()

    if not SUPABASE_SERVICE_KEY:
        print("ERROR: SUPABASE_SERVICE_KEY is not set.", file=sys.stderr)
        sys.exit(2)

    now = datetime.now(timezone.utc)

    # (a) fast count via HEAD/count=exact
    stale_over_week = count_stale_over_week(now)

    # (b) + (c) need per-broker rollups
    rows = fetch_all_direct_rows()
    analysis = analyze(rows, now)

    # Alert logic
    tripped = (stale_over_week > STALE_WEEK_THRESHOLD) or bool(analysis["stale_franchise"])

    summary = build_summary(stale_over_week, analysis, now, tripped)
    print(summary)

    if tripped:
        reasons = []
        if stale_over_week > STALE_WEEK_THRESHOLD:
            reasons.append(f"{stale_over_week:,} listings stale >{WEEK_DAYS}d")
        if analysis["stale_franchise"]:
            reasons.append(f"{len(analysis['stale_franchise'])} franchise broker(s) stale >{FRANCHISE_STALE_DAYS}d")
        subject = "DealLedger freshness ALERT: " + "; ".join(reasons)
        html = build_email_html(stale_over_week, analysis, now)
        send_alert(subject, html, summary, args.dry_run)

    if tripped and args.fail_on_alert:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
